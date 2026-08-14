// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Alarm wake for inactive cells (on by default).
//!
//! Committed alarm state is mirrored into the bucket as
//! `wake/<YYYY-MM-DDTHH:MM>/<cell>` so a wake hint survives fence, crash,
//! and deploy; the sweep evicts alarm-bearing cells behind a durable
//! entry, a per-node heap plus boot scan re-activates them, and a per-fleet
//! advisory waker revives orphans whose owner died.
//!
//! Invariants, verified by deterministic simulation:
//! - arm durable ⟹ entry exists, within one sweep tick of the commit;
//! - only completed activation or a durable consume deletes an entry — a
//!   stale entry costs one spurious wake, a missing entry costs a lost wake;
//! - the flusher never touches the request path: it reads the lock-free
//!   `next_alarm_ms` mirror on the existing 5 s sweep tick.
use crate::bucket::Bucket;
use celld_logic::wake::parse_entry_key;
use celld_logic::wake::Op;
use celld_logic::wake::Step;
use celld_logic::wake::WakeCore;
use std::sync::Mutex;
use std::time::Duration;
use tracing::warn;

/// Stay-resident threshold: alarms due sooner than this keep their cell
/// resident when residency is cheaper than a wake cycle. Alarms further out
/// are evicted behind an entry.
pub fn resident_ms() -> i64 {
    static MS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *MS.get_or_init(|| {
        crate::env_vars::with_default("CELLD_ALARM_RESIDENT_MS", 3_600_000)
            .expect("validated CELLD_ALARM_RESIDENT_MS")
    })
}

/// Scan the due wake buckets: used by the boot-time orphan scan and the
/// Phase 3 waker tick. The whole `wake/` prefix is listed and filtered
/// locally; entries are deleted as their alarms are consumed, so the listing
/// stays O(armed entries).
/// Due entries as (cell, minute_ms). The minute is carried out so a reviving
/// node can adopt the entry it acted on: without it, a cell whose restored
/// truth has no alarm leaves the entry that woke it in the bucket forever.
pub async fn due_scan(bucket: &Bucket, now_ms: i64) -> Vec<(String, i64)> {
    let mut due = Vec::new();
    let objects = match bucket.list("wake/").await {
        Ok(objects) => objects,
        Err(e) => {
            warn!(error = %e, "wake due scan list failed");
            return due;
        }
    };
    for object in objects {
        if let Some((minute_ms, cell)) = parse_entry_key(object.location.as_ref()) {
            if minute_ms <= now_ms {
                due.push((cell, minute_ms));
            }
        }
    }
    due.sort();
    due.dedup_by(|a, b| a.0 == b.0);
    due
}

/// Advisory waker-role lease: one holder per fleet to avoid N nodes polling.
/// Correctness never depends on it — concurrent wakers race activation CAS
/// harmlessly — so every failure path just returns false and skips a tick.
pub async fn try_hold_waker(bucket: &Bucket, node: &str, now_ms: i64, ttl_ms: i64) -> bool {
    const KEY: &str = "wake/waker.json";
    let body =
        |expires: i64| format!("{{\"node\":{node:?},\"expires_ms\":{expires}}}").into_bytes();
    match bucket.get(KEY).await {
        // absent (or unreadable): claim if absent
        Ok(None) | Err(_) => matches!(
            bucket.put_cas(KEY, body(now_ms + ttl_ms), None).await,
            Ok(Some(_))
        ),
        Ok(Some((bytes, etag))) => {
            let text = String::from_utf8_lossy(&bytes);
            let held_by_us = text.contains(&format!("\"node\":{node:?}"));
            let expires = text
                .rsplit("\"expires_ms\":")
                .next()
                .and_then(|t| t.trim_end_matches('}').trim().parse::<i64>().ok())
                .unwrap_or(0);
            if celld_logic::wake::waker_may_claim(held_by_us, expires, now_ms) {
                matches!(
                    bucket
                        .put_cas(KEY, body(now_ms + ttl_ms), Some(&etag))
                        .await,
                    Ok(Some(_))
                )
            } else {
                false
            }
        }
    }
}

/// Mirrors each resident cell's committed next-alarm into the bucket. One
/// instance per node, driven from the eviction sweep tick. The pure transition
/// (`decide` / `covered` / `due_cells` / `adopt`) is the sans-IO
/// `celld_logic::wake::WakeCore`; this facade adds the lock, the async bucket
/// executor, and the shadow-pin log.
pub struct WakeFlusher {
    core: Mutex<WakeCore>,
    /// Signals each settled delete, so `await_no_inflight_delete` waiters
    /// can re-check.
    quiesce: tokio::sync::Notify,
}

impl Default for WakeFlusher {
    fn default() -> Self {
        Self::new()
    }
}

impl WakeFlusher {
    pub fn new() -> Self {
        WakeFlusher {
            core: Mutex::new(WakeCore::new()),
            quiesce: tokio::sync::Notify::new(),
        }
    }

    /// Take responsibility for the entry a restored alarm implies.
    ///
    /// Hibernation forgets the cell, so whichever process revives it holds no
    /// record of the entry its alarm still has in the bucket -- consuming the
    /// alarm would delete nothing, and every later due scan would find the
    /// entry and wake a cell with nothing to do. Only when nothing is tracked,
    /// per `should_adopt_hint`: re-adopting what this process already knows
    /// about resurrects an entry it is in the middle of deleting.
    pub fn adopt(&self, cell: &str, due_ms: i64) {
        let mut core = self.core.lock().unwrap();
        if celld_logic::wake::should_adopt_hint(core.tracks(cell), due_ms) {
            core.adopt(cell, due_ms);
        }
    }

    /// Is this cell's entry state already known to this process?
    pub fn tracks(&self, cell: &str) -> bool {
        self.core.lock().unwrap().tracks(cell)
    }

    /// Reconcile one cell against the bucket — the async executor for `WakeCore::decide`.
    /// Failed PUTs keep local state unchanged so the next tick retries (an entry
    /// may be late, never silently absent); failed deletes drop matching state
    /// anyway — a stale entry is one spurious wake. `consume_durable` gates the
    /// final delete of a consumed alarm on the consuming commit's replication.
    pub async fn reconcile(
        &self,
        bucket: &Bucket,
        cell: &str,
        next_alarm_ms: i64,
        consume_durable: bool,
    ) {
        // A PUT must never race an in-flight delete for the same cell: the
        // bucket gives concurrent same-key writes no order, so the PUT can lose and
        // leave a confirmed belief with no entry. Sequence behind any delete
        // a previous reconcile still has on the wire.
        self.await_no_inflight_delete(cell).await;
        // The ordering rules -- abort the batch on a failed PUT, re-check a
        // delete against the core immediately before issuing it -- live in
        // `Reconcile`, so this executor performs steps and reports outcomes
        // and cannot quietly diverge from the one the simulation drives.
        let mut plan =
            self.core
                .lock()
                .unwrap()
                .reconcile_plan(cell, next_alarm_ms, consume_durable);
        // Take each step with the lock held only long enough to choose it:
        // a guard living in a `while let` scrutinee is not released until the
        // end of the body, and the body locks again.
        loop {
            let step = {
                let mut core = self.core.lock().unwrap();
                plan.next(&mut core)
            };
            let Some(step) = step else { break };
            match step {
                Step::Put { key, due_ms, body } => {
                    match bucket.put(&key, body.into_bytes()).await {
                        Ok(()) => plan.put_done(&mut self.core.lock().unwrap(), key, due_ms),
                        Err(e) => {
                            warn!(%cell, %key, error = %e, "wake entry put failed");
                            plan.put_failed();
                        }
                    }
                }
                Step::Delete { key } => {
                    // A transient store error must not orphan the entry — a
                    // dropped delete was one of the immortal-orphan producers
                    // (the item-6 audit) — so retry briefly. A delete that
                    // still fails drops the state anyway: a stale entry is
                    // one spurious wake, and keeping it would block the arm
                    // that replaces it.
                    for attempt in 0..3u32 {
                        match bucket.delete(&key).await {
                            Ok(()) => break,
                            Err(e) if attempt == 2 => {
                                warn!(%cell, %key, error = %e, "wake entry delete failed");
                            }
                            Err(_) => {
                                tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
                            }
                        }
                    }
                    plan.delete_done(&mut self.core.lock().unwrap(), &key);
                    self.quiesce.notify_waiters();
                }
            }
        }
    }

    /// Wait until no consume-delete for `cell` is on the wire. An arm-time
    /// PUT — and any new reconcile — sequences behind it; see `reconcile`.
    pub async fn await_no_inflight_delete(&self, cell: &str) {
        loop {
            let notified = self.quiesce.notified();
            tokio::pin!(notified);
            // Register the waiter BEFORE the check. `notified()` only enrolls
            // when the future is first polled, so a delete settling between
            // the check and the first poll would notify no one and this wait
            // would hang until some unrelated delete settled — with the arm
            // gate holding the response that waits on it.
            notified.as_mut().enable();
            if !self.core.lock().unwrap().delete_in_flight(cell) {
                return;
            }
            notified.await;
        }
    }

    /// Arm-time decision — the PUT that must land before this arm is acked,
    /// or `None` when the durable bound already covers it. Pure passthrough to
    /// `WakeCore::arm`; the caller performs the PUT and then `confirm_arm`.
    pub fn arm_op(&self, cell: &str, next_alarm_ms: i64) -> Option<Op> {
        self.core.lock().unwrap().arm(cell, next_alarm_ms)
    }

    /// An arm-time PUT landed: record the proven entry.
    pub fn confirm_arm(&self, cell: &str, due_ms: i64, key: String) {
        self.core.lock().unwrap().confirm_put(cell, due_ms, key);
    }

    /// Is this exact committed alarm durably covered by a proven entry? The
    /// fail-closed gate: eviction of an alarm-bearing cell requires it.
    pub fn covered(&self, cell: &str, next_alarm_ms: i64) -> bool {
        self.core.lock().unwrap().covered(cell, next_alarm_ms)
    }

    /// Entries whose cells this node evicted and whose due time has
    /// arrived — the tier-2 wake heap, derived from flusher state.
    pub fn due_cells(&self, now_ms: i64) -> Vec<String> {
        self.core.lock().unwrap().due_cells(now_ms)
    }

    /// A wake for `cell` resolved to a remote owner: its alarm is no longer
    /// this node's to track.
    pub fn forget(&self, cell: &str) {
        self.core.lock().unwrap().forget(cell);
    }
}

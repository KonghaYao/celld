// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_macros)]

//! v0 of the in-fleet replicated log tier (`CELLD_DURABILITY=fleet`).
//!
//! Each node streams the per-cell L0 LTX segments it has captured but not
//! yet uploaded to a small follower ensemble over the signed peer
//! transport. A write acknowledges when every member holds its segment on
//! disk — write-all, ack-all — or when the ordinary bucket upload proves
//! it first, whichever wins. The bucket upload path is unchanged and
//! remains the tiering mechanism, so node-log recovery re-creates exactly
//! the objects the dead leader would have uploaded and every per-cell
//! restore and compaction mechanism stays byte-for-byte as it is.
//!
//! `log/<node>.json` is the CAS-guarded root of truth for the ensemble and
//! the log epoch. It is created before the node's first fleet-durable ack
//! and never deleted, so a takeover that finds no record may treat the
//! bucket as complete. The decisions are `celld_logic::log_tier`; this
//! module is their executor.
//!
//! v0 limits, deliberate: entries travel as base64 JSON; a follower
//! failure degrades the node to bucket-proof acks until a periodic
//! re-recruit CASes a fresh ensemble; recovery gathers from every
//! reachable sealed member and requires at least one.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use anyhow::anyhow;
use anyhow::Context;
use celld_logic::log_tier;
use celld_logic::log_tier::LogState;
use tracing::info;
use tracing::warn;

use futures_util::StreamExt;

use crate::bucket::Bucket;
use crate::ltx_repl::ShipEntry;
use crate::peer_auth::PeerAuth;

/// The one peer-POST boundary used by the node log.
///
/// Production installs the signed `reqwest` implementation below. A
/// scheduler-controlled implementation can replace the transport while all
/// codecs and follower handlers remain the shipping ones.
pub(crate) trait LogTransport: Send + Sync + 'static {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    >;
}

/// Concurrent per-cell upload lanes during node-log recovery. The
/// sequential version cost ~47 s for 180 entries on the lab fleet; more
/// than a few lanes multiplied across RACING recoverers (self-recovery
/// plus every survivor's eager sweep) tripped R2's same-object 429 rate
/// limit into a livelock, so the lanes are modest and each upload retries
/// 429-class refusals with backoff.
const RECOVERY_UPLOAD_CONCURRENCY: usize = 8;

/// Process-monotonic milliseconds for follower-health bookkeeping: latency
/// windows and quarantine arithmetic must not jump with the wall clock.
fn mono_ms() -> u64 {
    crate::asyncrt::mono_ms()
}

/// The eviction policy, defaults per the design doc, every constant an E8
/// target the lab can override.
pub fn eviction_policy_from_env() -> anyhow::Result<celld_logic::log_evict::EvictionPolicy> {
    let default = celld_logic::log_evict::EvictionPolicy::default();
    Ok(celld_logic::log_evict::EvictionPolicy {
        budget_ms: crate::env_vars::with_default("CELLD_LOG_EVICT_BUDGET_MS", default.budget_ms)?,
        sibling_factor: default.sibling_factor,
        sustain_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_SUSTAIN_MS",
            default.sustain_ms,
        )?,
        backstop_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_BACKSTOP_MS",
            default.backstop_ms,
        )?,
        quarantine_ms: crate::env_vars::with_default(
            "CELLD_LOG_EVICT_QUARANTINE_MS",
            default.quarantine_ms,
        )?,
        min_swap_interval_ms: default.min_swap_interval_ms,
        window_ms: default.window_ms,
    })
}

// ── The record ──────────────────────────────────────────────────────────────
//
// Since the lease-fold, the log record LIVES in the node lease record:
// nodes/<node>.json carries a folded `log` object
// beside its authority fields, and a session's identity is the record's
// generation. Reading "session X/G" means reading X's lease and answering
// None unless it still carries generation G — a replaced record is a
// recovered-then-superseded session, and absence keeps meaning complete.
// Writes here are for DEAD sessions only (recovery's fence and seal): they
// CAS the full wire record, carrying every authority field through
// unchanged and never touching expiry. A LIVE session's own writes go
// through the core's lease chain instead (write_own_log below), because
// the lease has exactly one writer per process and a second one would
// race the renewal guard.

fn lease_key(node: &str) -> String {
    format!("nodes/{node}.json")
}

struct FoldedRead {
    record: log_tier::LogRecord,
    active: bool,
    token: String,
    wire: crate::ownership_store::NodeLeaseWire,
}

fn log_from_wire(log: &crate::ownership_store::NodeLogWire) -> anyhow::Result<log_tier::LogRecord> {
    Ok(log_tier::LogRecord {
        epoch: log.epoch,
        ensemble: log.ensemble.iter().cloned().collect(),
        tiered: log.tiered,
        state: match log.state.as_str() {
            "open" => LogState::Open,
            "recovering" => LogState::Recovering,
            "sealed" => LogState::Sealed,
            other => return Err(anyhow!("unknown log record state {other:?}")),
        },
    })
}

pub(crate) fn log_to_wire(
    record: &log_tier::LogRecord,
    active: bool,
) -> crate::ownership_store::NodeLogWire {
    crate::ownership_store::NodeLogWire {
        state: match record.state {
            LogState::Open => "open",
            LogState::Recovering => "recovering",
            LogState::Sealed => "sealed",
        }
        .to_string(),
        epoch: record.epoch,
        ensemble: record.ensemble.iter().cloned().collect(),
        tiered: record.tiered,
        active,
    }
}

async fn read_record(bucket: &Bucket, session: &str) -> anyhow::Result<Option<FoldedRead>> {
    let (node, generation) = session.split_once('/').unwrap_or((session, ""));
    let Some((bytes, token)) = bucket.get(&lease_key(node)).await? else {
        return Ok(None);
    };
    let wire: crate::ownership_store::NodeLeaseWire = serde_json::from_slice(&bytes)?;
    // A bare node name reads whatever session the record carries; a full
    // <node>/<generation> pins it, and a superseded generation is a
    // recovered-then-replaced session whose absence means complete.
    if !generation.is_empty() && wire.generation != generation {
        return Ok(None);
    }
    let Some(log) = wire.log.as_ref() else {
        return Ok(None);
    };
    Ok(Some(FoldedRead {
        record: log_from_wire(log)?,
        active: log.active,
        token,
        wire,
    }))
}

/// CAS a DEAD session's folded log fields. Authority fields ride through
/// from the wire the caller read; expiry is never extended, so this write
/// can only fence, never revive.
async fn write_dead_record(
    bucket: &Bucket,
    session: &str,
    prior: &crate::ownership_store::NodeLeaseWire,
    record: &log_tier::LogRecord,
    active: bool,
    token: &str,
) -> anyhow::Result<Option<String>> {
    let (node, _) = session.split_once('/').unwrap_or((session, ""));
    let mut wire = prior.clone();
    wire.log = Some(log_to_wire(record, active));
    let body = serde_json::to_vec(&wire)?;
    bucket.put_cas(&lease_key(node), body, Some(token)).await
}

/// The LIVE session's writer for its own folded log: publish the desired
/// object to the ownership store, nudge the core into an immediate
/// renewal, and wait until an APPLIED lease write carries it. The lease
/// chain stays single-writer — this is how open, activation, and the
/// graceful seal become durable without racing the renewal guard. A
/// fenced node's renewals stop applying, so the wait times out and the
/// caller refuses, which is the fence doing its job.
pub struct OwnLog {
    pub ownership: Arc<crate::ownership_store::BucketOwnership>,
    pub nudge: Box<dyn Fn() + Send + Sync>,
    /// One publish outstanding at a time: with the seq-tagged applied
    /// notification, "applied seq >= mine" then implies the applied body
    /// IS my object. Also serializes maintain, activation, and the
    /// graceful seal against each other (cold review, S3).
    pub write_lock: tokio::sync::Mutex<()>,
}

impl OwnLog {
    pub(crate) async fn write(
        &self,
        log: Option<crate::ownership_store::NodeLogWire>,
    ) -> anyhow::Result<()> {
        let _serialized = self.write_lock.lock().await;
        let mut rx = self.ownership.applied_log();
        let seq = self.ownership.set_own_log(log);
        (self.nudge)();
        let deadline = crate::asyncrt::mono_ms().saturating_add(10_000);
        loop {
            if rx.borrow_and_update().1 >= seq {
                return Ok(());
            }
            crate::asyncrt::select! {
                changed = rx.changed() => {
                    anyhow::ensure!(changed.is_ok(), "the lease writer is gone");
                },
                _ = crate::asyncrt::sleep_until(deadline) => {
                    anyhow::bail!(
                        "no lease write carried the folded log within 10s;                          treating this session as fenced"
                    );
                }
            }
        }
    }

    fn current(&self) -> Option<crate::ownership_store::NodeLogWire> {
        self.ownership.own_log()
    }
}

// ── Wire types for the peer endpoints ───────────────────────────────────────
//
// The two byte-dominated messages — the append request and the tail
// response — travel as a small binary framing; every control message
// (append response, seal, tail request) stays JSON. The same entry
// encoding is the follower's on-disk `<seq>.entry` format, so one decoder
// serves the wire and the disk. All integers little-endian:
//
//   append body:   "CLA1" u16 leader_len leader u64 epoch u64 truncate_to
//                  u32 count entry*
//   tail response: "CLT1" u32 count entry*
//   entry:         "CLE1" u64 seq u16 cell_len cell u64 cell_epoch
//                  u64 txid u32 len bytes

const APPEND_MAGIC: &[u8; 4] = b"CLA1";
const TAIL_MAGIC: &[u8; 4] = b"CLT1";
const ENTRY_MAGIC: &[u8; 4] = b"CLE1";

pub struct Entry {
    pub seq: u64,
    pub cell: String,
    pub cell_epoch: u64,
    pub txid: u64,
    pub bytes: Vec<u8>,
}

pub struct AppendReq {
    pub leader: String,
    pub epoch: u64,
    /// Follower may drop entries at or below this sequence; they are in the
    /// bucket.
    pub truncate_to: u64,
    pub entries: Vec<Entry>,
}

fn take<'a>(buf: &mut &'a [u8], n: usize, what: &str) -> anyhow::Result<&'a [u8]> {
    if buf.len() < n {
        return Err(anyhow!("log wire: truncated {what}"));
    }
    let (head, rest) = buf.split_at(n);
    *buf = rest;
    Ok(head)
}

fn take_u16(buf: &mut &[u8], what: &str) -> anyhow::Result<u16> {
    Ok(u16::from_le_bytes(take(buf, 2, what)?.try_into().unwrap()))
}

fn take_u32(buf: &mut &[u8], what: &str) -> anyhow::Result<u32> {
    Ok(u32::from_le_bytes(take(buf, 4, what)?.try_into().unwrap()))
}

fn take_u64(buf: &mut &[u8], what: &str) -> anyhow::Result<u64> {
    Ok(u64::from_le_bytes(take(buf, 8, what)?.try_into().unwrap()))
}

fn take_string(buf: &mut &[u8], what: &str) -> anyhow::Result<String> {
    let len = take_u16(buf, what)? as usize;
    Ok(std::str::from_utf8(take(buf, len, what)?)
        .map_err(|_| anyhow!("log wire: {what} not utf-8"))?
        .to_string())
}

fn encode_entry(entry: &Entry, out: &mut Vec<u8>) {
    out.extend_from_slice(ENTRY_MAGIC);
    out.extend_from_slice(&entry.seq.to_le_bytes());
    out.extend_from_slice(&(entry.cell.len() as u16).to_le_bytes());
    out.extend_from_slice(entry.cell.as_bytes());
    out.extend_from_slice(&entry.cell_epoch.to_le_bytes());
    out.extend_from_slice(&entry.txid.to_le_bytes());
    out.extend_from_slice(&(entry.bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&entry.bytes);
}

fn decode_entry(buf: &mut &[u8]) -> anyhow::Result<Entry> {
    if take(buf, 4, "entry magic")? != ENTRY_MAGIC {
        return Err(anyhow!("log wire: bad entry magic"));
    }
    let seq = take_u64(buf, "entry seq")?;
    let cell = take_string(buf, "entry cell")?;
    let cell_epoch = take_u64(buf, "entry cell_epoch")?;
    let txid = take_u64(buf, "entry txid")?;
    let len = take_u32(buf, "entry len")? as usize;
    let bytes = take(buf, len, "entry bytes")?.to_vec();
    Ok(Entry {
        seq,
        cell,
        cell_epoch,
        txid,
        bytes,
    })
}

pub fn encode_append(req: &AppendReq) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(APPEND_MAGIC);
    out.extend_from_slice(&(req.leader.len() as u16).to_le_bytes());
    out.extend_from_slice(req.leader.as_bytes());
    out.extend_from_slice(&req.epoch.to_le_bytes());
    out.extend_from_slice(&req.truncate_to.to_le_bytes());
    out.extend_from_slice(&(req.entries.len() as u32).to_le_bytes());
    for entry in &req.entries {
        encode_entry(entry, &mut out);
    }
    out
}

pub fn decode_append(mut body: &[u8]) -> anyhow::Result<AppendReq> {
    let buf = &mut body;
    if take(buf, 4, "append magic")? != APPEND_MAGIC {
        return Err(anyhow!("log wire: bad append magic"));
    }
    let leader = take_string(buf, "append leader")?;
    let epoch = take_u64(buf, "append epoch")?;
    let truncate_to = take_u64(buf, "append truncate_to")?;
    let entries = decode_entries(buf, "append")?;
    Ok(AppendReq {
        leader,
        epoch,
        truncate_to,
        entries,
    })
}

/// The shared tail of both framed bodies: a count, that many entries,
/// and nothing after them.
fn decode_entries(buf: &mut &[u8], what: &str) -> anyhow::Result<Vec<Entry>> {
    let count = take_u32(buf, what)? as usize;
    let mut entries = Vec::with_capacity(count.min(4096));
    for _ in 0..count {
        entries.push(decode_entry(buf)?);
    }
    if !buf.is_empty() {
        return Err(anyhow!("log wire: trailing bytes after {what}"));
    }
    Ok(entries)
}

pub fn encode_tail_resp(resp: &TailResp) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(TAIL_MAGIC);
    out.extend_from_slice(&(resp.entries.len() as u32).to_le_bytes());
    for entry in &resp.entries {
        encode_entry(entry, &mut out);
    }
    out
}

pub fn decode_tail_resp(mut body: &[u8]) -> anyhow::Result<TailResp> {
    let buf = &mut body;
    if take(buf, 4, "tail magic")? != TAIL_MAGIC {
        return Err(anyhow!("log wire: bad tail magic"));
    }
    Ok(TailResp {
        entries: decode_entries(buf, "tail")?,
    })
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct AppendResp {
    pub ok: bool,
    pub end: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SealReq {
    pub leader: String,
    pub epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct SealResp {
    pub end: u64,
    /// The fragment epoch this follower actually holds. A name-reused
    /// machine with a fresh disk answers 0 — reachable, sealable, and a
    /// witness to nothing. Recovery requires at least one member whose
    /// fragment epoch matches the record before it may declare the log
    /// gathered (the spec's certify guard, `CelldLogTier.tla`).
    #[serde(default)]
    pub fragment_epoch: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TailReq {
    pub leader: String,
}

pub struct TailResp {
    pub entries: Vec<Entry>,
}

// ── The follower side ───────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Default)]
struct FollowerState {
    fragment_epoch: u64,
    base: u64,
    end: u64,
    sealed_to: u64,
}

#[cfg(all(test, celld_internal_tests))]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    let filesystem = celld_ltx::DirectFileSystem;
    celld_ltx::FileSystem::sync_all(&filesystem, path)
}

#[cfg(all(test, celld_internal_tests))]
type DirectorySyncForTest = dyn Fn(&Path) -> std::io::Result<()> + Send + Sync;

/// One node's store of the log fragments that it follows.
///
/// Each follower session uses `<root>/peerlog/<node>/<generation>/`. The store
/// persists the seal mark before it sends the response, so the mark survives a
/// follower restart. The store also reads the former flat layout during an
/// upgrade.
pub struct FollowerStore {
    root: PathBuf,
    filesystem: Arc<dyn celld_ltx::FileSystem>,
    bucket: Option<Arc<Bucket>>,
    node: String,
    logs: Mutex<HashMap<String, FollowerState>>,
    /// Per-leader mutual exclusion over the whole read-modify-write of a
    /// fragment. Without it, an append that loaded state before a seal
    /// persists writes the stale `sealed_to` back afterwards — the seal
    /// mark is atomic in the model and must be atomic here.
    guards: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    #[cfg(all(test, celld_internal_tests))]
    directory_sync_for_test: Arc<DirectorySyncForTest>,
}

impl FollowerStore {
    pub fn new(root: &Path, bucket: Option<Arc<Bucket>>, node: &str) -> Self {
        let filesystem = crate::asyncrt::fs();
        #[cfg(all(test, celld_internal_tests))]
        let directory_filesystem = filesystem.clone();
        Self {
            root: root.join("peerlog"),
            filesystem,
            bucket,
            node: node.to_string(),
            logs: Mutex::new(HashMap::new()),
            guards: Mutex::new(HashMap::new()),
            #[cfg(all(test, celld_internal_tests))]
            directory_sync_for_test: Arc::new(move |path| directory_filesystem.sync_all(path)),
        }
    }

    #[cfg(all(test, celld_internal_tests))]
    fn new_with_directory_sync_for_test(
        root: &Path,
        bucket: Option<Arc<Bucket>>,
        node: &str,
        directory_sync: impl Fn(&Path) -> std::io::Result<()> + Send + Sync + 'static,
    ) -> Self {
        let mut store = Self::new(root, bucket, node);
        store.directory_sync_for_test = Arc::new(directory_sync);
        store
    }

    #[cfg(all(test, celld_internal_tests))]
    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        (self.directory_sync_for_test)(path)
    }

    #[cfg(not(all(test, celld_internal_tests)))]
    fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
        self.filesystem.sync_all(path)
    }

    /// Barrier every directory entry from the session leaf through the data
    /// root. A follower session is `<node>/<generation>`, so the chain is
    /// four directories deep, and a leaf-only fsync leaves the two
    /// intermediate entries outside the barrier the acknowledgment claims.
    ///
    /// The chain is walked on every persist, not only when `create_dir_all`
    /// reports a fresh directory. A predecessor process can create the
    /// chain and die before its own barrier completes; this process would
    /// then find the directories present, skip the fsync, and acknowledge
    /// over a chain that was never durable. The price is four directory
    /// fsyncs for each persist — nine for a steady-state append, which
    /// persists twice for the truncate and syncs the leaf once more for the
    /// entry files.
    fn sync_namespace_to_data_root(&self, leaf: &Path) -> anyhow::Result<()> {
        let data_root = self
            .root
            .parent()
            .ok_or_else(|| anyhow!("peerlog root has no data-root parent"))?;
        for directory in leaf.ancestors() {
            self.sync_directory(directory)?;
            if directory == data_root {
                return Ok(());
            }
        }
        Err(anyhow!(
            "follower directory {} is outside data root {}",
            leaf.display(),
            data_root.display()
        ))
    }

    fn guard(&self, leader: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.guards
            .lock()
            .unwrap()
            .entry(leader.to_string())
            .or_default()
            .clone()
    }

    fn dir(&self, leader: &str) -> PathBuf {
        self.root.join(leader)
    }

    fn followed_sessions(&self) -> Vec<String> {
        let mut sessions = Vec::new();
        let Ok(nodes) = self.filesystem.read_dir(&self.root) else {
            return sessions;
        };
        for node in nodes.into_iter().filter(|item| item.is_dir) {
            let Some(node_name) = node.file_name.to_str().map(str::to_string) else {
                continue;
            };
            if self
                .filesystem
                .metadata(&node.path.join("state.json"))
                .is_ok_and(|metadata| metadata.is_file)
            {
                // Keep fragments written by the former flat leader identity
                // reachable during an upgrade.
                sessions.push(node_name.clone());
            }
            let Ok(generations) = self.filesystem.read_dir(&node.path) else {
                continue;
            };
            for generation in generations.into_iter().filter(|item| item.is_dir) {
                let Some(generation_name) = generation.file_name.to_str().map(str::to_string)
                else {
                    continue;
                };
                if self
                    .filesystem
                    .metadata(&generation.path.join("state.json"))
                    .is_ok_and(|metadata| metadata.is_file)
                {
                    sessions.push(format!("{node_name}/{generation_name}"));
                }
            }
        }
        sessions.sort();
        sessions.dedup();
        sessions
    }

    fn load(&self, leader: &str) -> FollowerState {
        if let Some(state) = self.logs.lock().unwrap().get(leader) {
            return *state;
        }
        let state = self
            .filesystem
            .read(&self.dir(leader).join("state.json"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        self.logs.lock().unwrap().insert(leader.to_string(), state);
        state
    }

    fn persist(&self, leader: &str, state: FollowerState) -> anyhow::Result<()> {
        let dir = self.dir(leader);
        self.filesystem.create_dir_all(&dir)?;
        let path = dir.join("state.json");
        let tmp = dir.join("state.json.tmp");
        self.filesystem.write(&tmp, &serde_json::to_vec(&state)?)?;
        self.filesystem.sync_all(&tmp)?;
        self.filesystem.rename(&tmp, &path)?;
        self.sync_namespace_to_data_root(&dir)?;
        self.logs.lock().unwrap().insert(leader.to_string(), state);
        Ok(())
    }

    /// Adopt a new fragment epoch — only against the record. A leader's
    /// stream announcing an epoch is not authority: a fenced leader could
    /// invent one above our seal mark and talk past the fence, so the view
    /// change is verified against `log/<leader>.json` before any entry at
    /// the new epoch is accepted.
    async fn adopt(&self, leader: &str, epoch: u64, first_seq: u64) -> bool {
        let Some(bucket) = &self.bucket else {
            return false;
        };
        let record = match read_record(bucket, leader).await {
            Ok(Some(folded)) => folded.record,
            _ => return false,
        };
        let member = record.ensemble.contains(&self.node);
        let state = self.load(leader);
        if record.epoch != epoch
            || record.state != LogState::Open
            || !member
            || state.sealed_to >= epoch
        {
            return false;
        }
        // Old-fragment entries are garbage the record no longer references.
        let _ = self.remove_entries_below(leader, u64::MAX);
        self.persist(
            leader,
            FollowerState {
                fragment_epoch: epoch,
                base: first_seq.saturating_sub(1),
                end: first_seq.saturating_sub(1),
                sealed_to: state.sealed_to,
            },
        )
        .is_ok()
    }

    fn remove_entries_below(&self, leader: &str, seq: u64) -> anyhow::Result<()> {
        let dir = self.dir(leader);
        let Ok(read) = self.filesystem.read_dir(&dir) else {
            return Ok(());
        };
        for item in read {
            let name = item.file_name;
            let Some(stem) = name.to_str().and_then(|n| n.strip_suffix(".entry")) else {
                continue;
            };
            if stem.parse::<u64>().is_ok_and(|s| s <= seq) {
                let _ = self.filesystem.remove_file(&item.path);
            }
        }
        Ok(())
    }

    pub async fn append(&self, req: AppendReq) -> AppendResp {
        let guard = self.guard(&req.leader);
        let _held = guard.lock().await;
        let mut state = self.load(&req.leader);
        if state.fragment_epoch != req.epoch {
            if req.entries.is_empty() {
                // An idle probe at an epoch we do not hold: adopting here
                // would fix the fragment base at zero and poison the next
                // real append, so refuse — the probe measured the guard
                // lock and nothing else.
                return AppendResp {
                    ok: false,
                    end: state.end,
                };
            }
            let first = req.entries.first().map_or(0, |entry| entry.seq);
            if !self.adopt(&req.leader, req.epoch, first).await {
                // A refusal here degrades the leader to bucket acks; the
                // silent version cost the lab an unattributed 85 s window.
                warn!(
                    leader = req.leader,
                    epoch = req.epoch,
                    held_epoch = state.fragment_epoch,
                    sealed_to = state.sealed_to,
                    "append refused: fragment adoption failed against the record"
                );
                return AppendResp {
                    ok: false,
                    end: state.end,
                };
            }
            state = self.load(&req.leader);
        }
        // The shipping decision is celld_logic::log_tier::FollowerLog; drive
        // it entry by entry so the seal and contiguity refusals are exactly
        // the modeled ones.
        let mut log = log_tier::FollowerLog {
            fragment_epoch: state.fragment_epoch,
            base: state.base,
            end: state.end,
            sealed_to: state.sealed_to,
        };
        let dir = self.dir(&req.leader);
        if self.filesystem.create_dir_all(&dir).is_err() {
            return AppendResp {
                ok: false,
                end: state.end,
            };
        }
        let mut synced = false;
        for entry in &req.entries {
            #[cfg(all(test, celld_internal_tests))]
            let accept = if crate::asyncrt::sabotage_active(
                crate::host_services::EngineSabotage::AcceptAppendPastSeal,
            ) && req.epoch == log.fragment_epoch
                && entry.seq == log.end + 1
            {
                log.end = entry.seq;
                true
            } else {
                log.accept_append(req.epoch, entry.seq)
            };
            #[cfg(not(all(test, celld_internal_tests)))]
            let accept = log.accept_append(req.epoch, entry.seq);
            if !accept {
                warn!(
                    leader = req.leader,
                    epoch = req.epoch,
                    seq = entry.seq,
                    end = log.end,
                    sealed_to = log.sealed_to,
                    "append refused: seal or contiguity"
                );
                break;
            }
            let path = dir.join(format!("{}.entry", entry.seq));
            let mut encoded = Vec::new();
            encode_entry(entry, &mut encoded);
            let write = self
                .filesystem
                .write(&path, &encoded)
                .and_then(|()| self.filesystem.sync_all(&path));
            if write.is_err() {
                log.end = entry.seq - 1;
                break;
            }
            synced = true;
        }
        let ok = log.end >= req.entries.last().map_or(log.end, |entry| entry.seq);
        if synced {
            if let Err(error) = self.sync_directory(&dir) {
                // The entry files written above stay on disk. They are
                // debris above the persisted `end` — the same shape a crash
                // between the entry writes and `persist` leaves — so `tail`
                // reads them as unacked debris, a retransmission overwrites
                // them, and `adopt` or the fragment GC removes them.
                warn!(
                    leader = req.leader,
                    epoch = req.epoch,
                    end = state.end,
                    %error,
                    "append refused: entry directory sync failed"
                );
                return AppendResp {
                    ok: false,
                    end: state.end,
                };
            }
        }
        let new_state = FollowerState {
            fragment_epoch: log.fragment_epoch,
            base: log.base,
            end: log.end,
            sealed_to: log.sealed_to,
        };
        if self.persist(&req.leader, new_state).is_err() {
            return AppendResp {
                ok: false,
                end: state.end,
            };
        }
        if req.truncate_to > 0 {
            let mut truncated = log_tier::FollowerLog {
                fragment_epoch: new_state.fragment_epoch,
                base: new_state.base,
                end: new_state.end,
                sealed_to: new_state.sealed_to,
            };
            truncated.truncate(req.truncate_to.min(new_state.end));
            let _ = self.remove_entries_below(&req.leader, truncated.base);
            let _ = self.persist(
                &req.leader,
                FollowerState {
                    base: truncated.base,
                    ..new_state
                },
            );
        }
        AppendResp { ok, end: log.end }
    }

    /// Persist the seal mark BEFORE answering: once the response leaves,
    /// this follower must refuse the sealed epoch forever, including across
    /// a restart.
    pub async fn seal(&self, req: &SealReq) -> anyhow::Result<SealResp> {
        let guard = self.guard(&req.leader);
        let _held = guard.lock().await;
        let state = self.load(&req.leader);
        let mut log = log_tier::FollowerLog {
            fragment_epoch: state.fragment_epoch,
            base: state.base,
            end: state.end,
            sealed_to: state.sealed_to,
        };
        let end = log.seal(req.epoch);
        self.persist(
            &req.leader,
            FollowerState {
                sealed_to: log.sealed_to,
                ..state
            },
        )?;
        Ok(SealResp {
            end,
            fragment_epoch: state.fragment_epoch,
        })
    }

    /// One fragment-GC pass over every leader this node follows. A
    /// fragment is garbage when its epoch is closed: the record moved past
    /// it (reconfiguration or a reopened incarnation force-tiered or
    /// recovered it away), or the record at its epoch is Sealed (recovery
    /// certified and uploaded the tail, so this copy is redundant by
    /// write-all). The deletion runs under the per-leader guard, keeps the
    /// state file, and extends the seal mark over the closed epoch, so a
    /// straggling append at it is refused rather than resurrected.
    pub async fn gc_fragments(&self) {
        let Some(bucket) = &self.bucket else { return };
        let leaders = self.followed_sessions();
        for leader in leaders {
            let Ok(Some(folded)) = read_record(bucket, &leader).await else {
                continue;
            };
            let record = folded.record;
            let guard = self.guard(&leader);
            let _held = guard.lock().await;
            let state = self.load(&leader);
            if state.fragment_epoch == 0 {
                continue;
            }
            #[cfg(all(test, celld_internal_tests))]
            let closed = crate::asyncrt::sabotage_active(
                crate::host_services::EngineSabotage::CollectOpenFragment,
            ) || log_tier::fragment_closed(&record, state.fragment_epoch);
            #[cfg(not(all(test, celld_internal_tests)))]
            let closed = log_tier::fragment_closed(&record, state.fragment_epoch);
            if !closed {
                continue;
            }
            if state.base == state.end
                && self
                    .filesystem
                    .metadata(&self.dir(&leader).join("state.json"))
                    .is_ok_and(|metadata| metadata.is_file)
            {
                // Already empty; nothing to remove, and the state file
                // stays as the seal-mark carrier.
                let _ = self.persist(
                    &leader,
                    FollowerState {
                        sealed_to: state.sealed_to.max(state.fragment_epoch),
                        ..state
                    },
                );
                continue;
            }
            let _ = self.remove_entries_below(&leader, u64::MAX);
            if self
                .persist(
                    &leader,
                    FollowerState {
                        fragment_epoch: state.fragment_epoch,
                        base: state.end,
                        end: state.end,
                        sealed_to: state.sealed_to.max(state.fragment_epoch),
                    },
                )
                .is_ok()
            {
                info!(
                    leader,
                    epoch = state.fragment_epoch,
                    "fragment GC: closed epoch's fragments removed"
                );
            }
        }
    }

    pub fn tail(&self, req: &TailReq) -> TailResp {
        // Entries above the persisted end are unacked debris a crash may
        // legitimately tear (the entry syncs before the state does), and
        // including or losing an unacked frame is free. A torn entry AT OR
        // BELOW the end would be an acked frame's only local copy, so it
        // is skipped LOUDLY — write-all means another member still has it,
        // and the line is what attributes the anomaly.
        let end = self.load(&req.leader).end;
        let mut entries = Vec::new();
        let dir = self.dir(&req.leader);
        if let Ok(read) = self.filesystem.read_dir(&dir) {
            for item in read {
                if item.path.extension().is_none_or(|ext| ext != "entry") {
                    continue;
                }
                if let Ok(bytes) = self.filesystem.read(&item.path) {
                    match decode_entry(&mut bytes.as_slice()) {
                        Ok(entry) => entries.push(entry),
                        Err(error) => {
                            let torn_acked = item
                                .file_name
                                .to_str()
                                .and_then(|name| name.strip_suffix(".entry"))
                                .and_then(|stem| stem.parse::<u64>().ok())
                                .is_some_and(|seq| seq <= end);
                            if torn_acked {
                                warn!(
                                    leader = req.leader,
                                    path = %item.path.display(),
                                    %error,
                                    "torn entry at or below the fragment end skipped in tail"
                                );
                            }
                        }
                    }
                }
            }
        }
        entries.sort_by_key(|entry| entry.seq);
        TailResp { entries }
    }
}

/// The signed production implementation. It preserves the previous request
/// construction, authentication, response validation, and timeout behavior.
struct SignedPeerTransport {
    http: reqwest::Client,
    auth: Arc<PeerAuth>,
}

impl SignedPeerTransport {
    fn new(auth: Arc<PeerAuth>) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .tcp_nodelay(true)
                .build()
                .expect("build log peer client"),
            auth,
        }
    }
}

impl LogTransport for SignedPeerTransport {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            let mut builder = self.http.post(format!("http://{addr}{path}"));
            if let Some(deadline) = deadline {
                builder = builder.timeout(deadline);
            }
            let request = self.auth.sign(builder, "POST", path, &body, node)?;
            let response = request.body(body).send().await?;
            // Status before response-auth: an older peer can answer an
            // unsigned route error, and callers need that status to classify
            // a protocol-incapable member separately from transport failure.
            if !response.status().is_success() {
                let status = response.status();
                return Err(anyhow::Error::new(PeerHttpError { status })
                    .context(format!("peer {node} answered {status}")));
            }
            crate::peer_auth::validate_response(response.headers())?;
            Ok(response.bytes().await?)
        })
    }
}

/// A peer answered with an HTTP error status: typed so callers can tell
/// "the route is not there" from transport silence.
#[derive(Debug)]
struct PeerHttpError {
    status: reqwest::StatusCode,
}

impl std::fmt::Display for PeerHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "peer answered {}", self.status)
    }
}

impl std::error::Error for PeerHttpError {}

// ── The leader side ─────────────────────────────────────────────────────────

struct Member {
    node: String,
    addr: String,
}

/// The fleet shipper for ONE ensemble at one epoch: assigns sequence
/// numbers, POSTs one in-flight batch to every member, and reports
/// all-acked. One batch at a time is what keeps each follower's fragment
/// contiguous. Membership and epoch are immutable — an ensemble change
/// builds a new shipper and the manager swaps it.
pub struct FleetShipper {
    node: String,
    transport: Arc<dyn LogTransport>,
    own_log: Arc<OwnLog>,
    record: log_tier::LogRecord,
    /// The record's CAS token from the open; consumed by the activation
    /// CAS below.
    /// The first successful batch CASes `active` onto the record BEFORE its
    /// acks are credited. Recovery uses `active` to tell a never-adopted
    /// fragment (safe to seal empty) from an all-amnesiac ensemble (refuse
    /// the silent seal). One CAS per ensemble epoch.
    activated: std::sync::atomic::AtomicBool,
    epoch: u64,
    members: Vec<Member>,
    seq: std::sync::atomic::AtomicU64,
    /// A failed member degrades the shipper permanently: fleet proofs stop
    /// and every ack rides the bucket, which is always safe, until the
    /// maintenance loop CASes a fresh ensemble.
    degraded: std::sync::atomic::AtomicBool,
    /// A batch between capture and credit. The reconfigure barrier cannot
    /// see such a batch in the shipped/tiered counters — its frames credit
    /// only after `ship` returns — so `maintain` must refuse to step epochs
    /// while one is out, or a frame covered only by the old ensemble could
    /// ack under a record that no longer names its holders.
    in_flight: std::sync::atomic::AtomicBool,
    /// The gray-follower ledger, shared with the manager: append timings
    /// feed it, the eviction watch reads it, and it outlives this shipper
    /// so quarantine survives the swap.
    health: Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    policy: Arc<celld_logic::log_evict::EvictionPolicy>,
}

impl crate::ltx_repl::Shipper for FleetShipper {
    fn ship<'a>(
        &'a self,
        batch: &'a [ShipEntry],
        covered_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<u64>> + Send + 'a>> {
        Box::pin(self.ship_batch(batch, covered_seq))
    }

    fn active(&self) -> bool {
        self.is_active()
    }

    fn epoch(&self) -> u64 {
        self.epoch
    }

    fn batch_credited(&self) {
        self.in_flight.store(false, Ordering::SeqCst);
    }
}

impl FleetShipper {
    fn is_active(&self) -> bool {
        !self.degraded.load(Ordering::SeqCst)
    }
}

/// One member's answer to one append POST, classified for the health
/// ledger: a parsed response, a protocol-level incapability (route
/// missing or body unparseable — quarantine), or a transient transport
/// failure (retry).
enum AppendSend {
    Answered(AppendResp),
    Incapable(anyhow::Error),
    Failed(#[allow(dead_code)] anyhow::Error),
}

impl FleetShipper {
    /// The append POST: a binary body (the entries dominate it), a JSON
    /// response. Bounded by the eviction backstop, not the generic client
    /// timeout: an append slower than the backstop triggers the evict
    /// regardless, and a gray follower must not pin the in-flight batch
    /// (and with it the reconfigure barrier) for ten seconds.
    async fn post_append(&self, member: &Member, req: &AppendReq) -> AppendSend {
        let deadline = std::time::Duration::from_millis(self.policy.backstop_ms + 100);
        let bytes = match self
            .transport
            .post(
                &member.node,
                &member.addr,
                "/__log/append",
                encode_append(req),
                Some(deadline),
            )
            .await
        {
            Ok(bytes) => bytes,
            // A missing or unimplemented route is a binary that does not
            // speak the log tier — the mixed-version seam. Everything
            // else (timeouts, resets, 5xx) stays a transient failure.
            Err(error) => {
                let incapable = error
                    .downcast_ref::<PeerHttpError>()
                    .is_some_and(|http| matches!(http.status.as_u16(), 404 | 405 | 501));
                return if incapable {
                    AppendSend::Incapable(error)
                } else {
                    AppendSend::Failed(error)
                };
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(resp) => AppendSend::Answered(resp),
            // A 200 whose body is not an AppendResp is no celld follower.
            Err(error) => AppendSend::Incapable(error.into()),
        }
    }

    /// Ship one batch to every member. `Some(last_seq)` only when every
    /// member confirmed every entry fsync'd — the ack-all rule. Any failure
    /// marks the shipper degraded: fleet proofs stop and the gate rides the
    /// bucket upload, which is always safe. `covered_seq` rides along as
    /// the followers' truncate_to: those entries are bucket-covered and the
    /// fragments behind them can be dropped.
    async fn ship_batch(&self, batch: &[ShipEntry], covered_seq: u64) -> Option<u64> {
        if self.members.is_empty() {
            return None;
        }
        // The flag must outlive this call on SUCCESS: ship_loop applies
        // the shipped credits after ship() returns, and clearing here
        // left a window where frames were invisible to both in_flight and
        // all_shipped_tiered — the reconfigure/seal barriers could pass
        // over acked frames (fidelity audit, DRIFTED #1). The guard now
        // clears only on the failure paths; batch_credited() clears it
        // after the credits land.
        struct InFlight<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for InFlight<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        // in_flight rises BEFORE the degraded check (cold review, S3):
        // maintain sets degraded and then reads in_flight as its drain
        // barrier, so the old order let a batch slip past a
        // reconfiguration's decision and fleet-ack frames on the retired
        // ensemble that the barrier never counted.
        self.in_flight.store(true, Ordering::SeqCst);
        let in_flight = InFlight(&self.in_flight);
        if self.degraded.load(Ordering::SeqCst) {
            return None;
        }
        let first = self.seq.fetch_add(batch.len() as u64, Ordering::SeqCst) + 1;
        let entries: Vec<Entry> = batch
            .iter()
            .enumerate()
            .map(|(index, entry)| Entry {
                seq: first + index as u64,
                cell: entry.cell.clone(),
                cell_epoch: entry.epoch,
                txid: entry.txid,
                bytes: entry.bytes.clone(),
            })
            .collect();
        let last = first + batch.len() as u64 - 1;
        let req = AppendReq {
            leader: self.node.clone(),
            epoch: self.epoch,
            truncate_to: covered_seq,
            entries,
        };
        let req = &req;
        let sends = self.members.iter().map(|member| async move {
            let started = mono_ms();
            self.health
                .lock()
                .unwrap()
                .append_started(&member.node, started);
            let resp = self.post_append(member, req).await;
            let done = mono_ms();
            self.health.lock().unwrap().append_completed(
                &member.node,
                done,
                done.saturating_sub(started),
            );
            match resp {
                AppendSend::Answered(AppendResp { ok: true, end }) => {
                    Some((member.node.clone(), end))
                }
                AppendSend::Incapable(error) => {
                    // Fast rejections read as healthy latency samples, so
                    // the gray verdicts never fire on this member and the
                    // rebuild re-picks it forever (#95): quarantine it
                    // here and the next rebuild recruits around it.
                    warn!(
                        member = member.node,
                        %error,
                        "follower cannot serve log appends; quarantined from recruitment"
                    );
                    self.health
                        .lock()
                        .unwrap()
                        .append_incapable(&self.policy, &member.node, done);
                    None
                }
                AppendSend::Answered(AppendResp { ok: false, .. }) | AppendSend::Failed(_) => None,
            }
        });
        // Write-all, ack-all is the core's decision: every ensemble member
        // must confirm a contiguous end at or past the batch — a member
        // that refused, errored, or answered short is a failed batch.
        let ends: BTreeMap<String, u64> = futures_util::future::join_all(sends)
            .await
            .into_iter()
            .flatten()
            .collect();
        let view = log_tier::LeaderView {
            epoch: self.epoch,
            ensemble: self.record.ensemble.clone(),
        };
        if log_tier::ack_fleet_allowed(&view, &ends, last) {
            // The activation fence: before the epoch's first fleet ack is
            // credited, the record must say `active`, or a later recovery
            // meeting only amnesiac members would seal an empty gather as
            // if nothing had ever been acked.
            if !self.activated.load(Ordering::SeqCst) {
                #[cfg(all(test, celld_internal_tests))]
                let active = if crate::asyncrt::sabotage_active(
                    crate::host_services::EngineSabotage::SkipActivationFenceCas,
                ) {
                    self.activated.store(true, Ordering::SeqCst);
                    true
                } else {
                    self.mark_active().await
                };
                #[cfg(not(all(test, celld_internal_tests)))]
                let active = self.mark_active().await;
                if !active {
                    self.degrade("activation CAS lost");
                    return None;
                }
            }
            #[cfg(all(test, celld_internal_tests))]
            let clear_early = crate::asyncrt::sabotage_active(
                crate::host_services::EngineSabotage::ClearShipInFlightEarly,
            );
            #[cfg(not(all(test, celld_internal_tests)))]
            let clear_early = false;
            if !clear_early {
                std::mem::forget(in_flight);
            }
            return Some(last);
        }
        self.degrade("member append failed");
        None
    }

    fn degrade(&self, why: &str) {
        if !self.degraded.swap(true, Ordering::SeqCst) {
            warn!(
                epoch = self.epoch,
                why, "log ensemble degraded; acks ride the bucket"
            );
        }
    }

    async fn mark_active(&self) -> bool {
        // Through the core's lease chain (lease-fold): a fenced session's
        // renewals stop applying, so the wait fails and the ack must not
        // credit — the same refusal the old CAS token gave, proved by the
        // one writer the record has.
        match self
            .own_log
            .write(Some(log_to_wire(&self.record, true)))
            .await
        {
            Ok(()) => {
                self.activated.store(true, Ordering::SeqCst);
                true
            }
            Err(_) => false,
        }
    }
}

// ── Recovery and the takeover interlock ─────────────────────────────────────

/// Everything node-log recovery needs from the node: the bucket, the signed
/// peer client, address resolution, and the raw per-cell upload.
pub struct NodeLogManager {
    node: String,
    /// The single-writer path for our own folded log state: publishes to
    /// the ownership store and rides the core's immediate renewal.
    own_log: Arc<OwnLog>,
    /// This process session's log identity: `<node>/<generation>`, the
    /// generation from the node lease record. Every self record, bundle,
    /// fragment, and loss key hangs off it.
    session: String,
    bucket: Arc<Bucket>,
    ownership: Arc<crate::ownership_store::BucketOwnership>,
    ltx: Arc<crate::ltx_repl::LtxRepl>,
    transport: Arc<dyn LogTransport>,
    /// The current ensemble's shipper, swapped whole by the maintenance
    /// loop. The manager itself is the installed `Shipper`, delegating here.
    inner: Mutex<Option<Arc<FleetShipper>>>,
    /// The record epoch THIS incarnation CASed open (0 = none). An open
    /// record at any other epoch belongs to a previous incarnation and must
    /// be recovered — its acked tail may exist only on the old followers —
    /// before the maintenance loop may step past it.
    /// Bundle the paced tiering (`CELLD_LOG_BUNDLE`): one PUT per node per
    /// flush interval instead of one per cell-transaction.
    bundle_mode: bool,
    bundle_seq: std::sync::atomic::AtomicU64,
    /// The leader's own index of the bundles it wrote this run:
    /// (object key, rows). Bounded; a restart loses it safely, because
    /// self-recovery folds the previous incarnation's bundles anyway.
    bundle_index: Mutex<std::collections::VecDeque<(String, Vec<celld_ltx::bundle::BundleRow>)>>,
    /// Sessions whose bundle subtree one sweep pass confirmed empty:
    /// a permanent tombstone (dead-lease GC never deletes a folded
    /// record) must not cost a bundle LIST on every sweep tick forever
    /// (third cold review). Process-local; a restart re-confirms once.
    gc_confirmed_empty: Mutex<std::collections::HashSet<String>>,
    /// One-slot cache for the compactor's fetches: bundles are read many
    /// rows at a time, and re-GETting per row would refund the savings.
    bundle_cache: tokio::sync::Mutex<Option<(String, Arc<Vec<u8>>)>>,
    /// The gray-follower ledger: append timings in, eviction verdicts and
    /// the quarantine out. Outlives every shipper swap.
    health: Arc<Mutex<celld_logic::log_evict::FollowerHealth>>,
    policy: Arc<celld_logic::log_evict::EvictionPolicy>,
    /// A correlated member stall latched self-suspicion: every ensemble
    /// member stuck at once means our own connectivity is the suspect,
    /// so recruitment parks — no reopen CAS churn into a partition —
    /// until any peer answers anything successfully. Cleared by a probe
    /// or append Ok; 37 doomed epochs in one 45 s partition taught the
    /// alternative.
    suspect_self: std::sync::atomic::AtomicBool,
    /// The shutdown latch: once set, the bundle sink refuses new flushes
    /// so the graceful seal's uncovered-scan cannot go stale between the
    /// LIST and the seal CAS (fidelity audit, DRIFTED #2).
    closing: std::sync::atomic::AtomicBool,
    /// A bundle flush between its PUT and its credit; the graceful seal
    /// waits this out after latching `closing`.
    flush_in_flight: std::sync::atomic::AtomicBool,
    /// Every predecessor session's log is proven recovered; see
    /// `ensure_predecessors_recovered` for why this can latch.
    predecessors_clean: std::sync::atomic::AtomicBool,
}

impl crate::ltx_repl::Shipper for NodeLogManager {
    fn ship<'a>(
        &'a self,
        batch: &'a [ShipEntry],
        covered_seq: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<u64>> + Send + 'a>> {
        Box::pin(async move {
            let inner = self.inner.lock().unwrap().clone();
            let shipped = match inner {
                Some(shipper) => shipper.ship_batch(batch, covered_seq).await,
                None => None,
            };
            if shipped.is_some() {
                self.suspect_self.store(false, Ordering::SeqCst);
            }
            shipped
        })
    }

    fn active(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|shipper| shipper.is_active())
    }

    fn epoch(&self) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .map_or(0, |shipper| shipper.epoch)
    }

    fn batch_credited(&self) {
        // The barrier (may_reconfigure) forbids a swap while the flag is
        // up, so the inner shipper here is the one that shipped.
        let inner = self.inner.lock().unwrap().clone();
        if let Some(shipper) = inner {
            crate::ltx_repl::Shipper::batch_credited(shipper.as_ref());
        }
    }
}

impl NodeLogManager {
    /// `session` is the process's full log identity, `<node>/<generation>`,
    /// with the generation taken from the node lease record.
    pub fn new(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        auth: Arc<PeerAuth>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        Self::new_with_log_transport(
            session,
            bucket,
            own_log,
            ltx,
            Arc::new(SignedPeerTransport::new(auth)),
            bundle_mode,
            policy,
        )
    }

    #[cfg(all(test, celld_internal_tests))]
    pub(crate) fn new_with_transport(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        transport: Arc<dyn LogTransport>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        Self::new_with_log_transport(
            session,
            bucket,
            own_log,
            ltx,
            transport,
            bundle_mode,
            policy,
        )
    }

    fn new_with_log_transport(
        session: &str,
        bucket: Arc<Bucket>,
        own_log: Arc<OwnLog>,
        ltx: Arc<crate::ltx_repl::LtxRepl>,
        transport: Arc<dyn LogTransport>,
        bundle_mode: bool,
        policy: celld_logic::log_evict::EvictionPolicy,
    ) -> Self {
        let ownership = own_log.ownership.clone();
        Self {
            node: session.split('/').next().unwrap_or(session).to_string(),
            session: session.to_string(),
            own_log,
            bucket,
            ownership,
            ltx,
            transport,
            inner: Mutex::new(None),
            bundle_mode,
            bundle_seq: std::sync::atomic::AtomicU64::new(0),
            bundle_index: Mutex::new(std::collections::VecDeque::new()),
            bundle_cache: tokio::sync::Mutex::new(None),
            health: Arc::new(Mutex::new(celld_logic::log_evict::FollowerHealth::default())),
            policy: Arc::new(policy),
            suspect_self: std::sync::atomic::AtomicBool::new(false),
            closing: std::sync::atomic::AtomicBool::new(false),
            flush_in_flight: std::sync::atomic::AtomicBool::new(false),
            predecessors_clean: std::sync::atomic::AtomicBool::new(false),
            gc_confirmed_empty: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Recover the predecessor session's log, read from this NODE's own
    /// lease record: until our install replaces it, the record carries the
    /// predecessor's generation and folded state, and recovery-before-
    /// install is the invariant that lets a successor write log: None.
    /// DONE-ONCE per process: after one clean pass the latch holds — a
    /// predecessor state can only reappear if another process supersedes
    /// our lease, and then we are fenced regardless. The latch sets only
    /// on success; a failed pass retries on the next cold path.
    async fn ensure_predecessors_recovered(&self) -> anyhow::Result<()> {
        if self.predecessors_clean.load(Ordering::SeqCst) {
            return Ok(());
        }
        if let Some(folded) = read_record(&self.bucket, &self.node).await? {
            let session = format!("{}/{}", self.node, folded.wire.generation);
            if session != self.session
                && log_tier::takeover_gate(Some(&folded.record))
                    == log_tier::TakeoverGate::RecoverFirst
            {
                info!(session, "recovering a predecessor session's node log");
                self.recover(&session).await?;
            }
        }
        // The install about to follow erases the record's only pointer to
        // the predecessor generation, and no GC path can rediscover a
        // sealed subtree with no pointer (third cold review): the boot is
        // the one moment that knows every stale generation, so it sweeps
        // them here. Non-fatal — the leak is storage cost, not safety,
        // and the next restart retries.
        if let Err(error) = self.gc_stale_generation_bundles().await {
            warn!(%error, "stale-generation bundle GC failed; retried at the next restart");
        }
        self.predecessors_clean.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Delete every bundle object under this node that belongs to a
    /// generation other than the running session's. Recovery-before-
    /// install has already sealed the predecessor by the time this runs,
    /// and a sealed session's bundles are garbage (recovery folded every
    /// acked row per-cell first). Loss declarations and every non-bundle
    /// key stay untouched.
    async fn gc_stale_generation_bundles(&self) -> anyhow::Result<()> {
        let prefix = format!("log/{}/", self.node);
        let mut stale: Vec<String> = Vec::new();
        for meta in self.bucket.list(&prefix).await? {
            let key = meta.location.as_ref().to_string();
            let Some(rest) = key.strip_prefix(&prefix) else {
                continue;
            };
            let Some((generation, tail)) = rest.split_once('/') else {
                continue;
            };
            if !tail.starts_with("bundle/") {
                continue;
            }
            if format!("{}/{generation}", self.node) == self.session {
                continue;
            }
            stale.push(key);
        }
        if stale.is_empty() {
            return Ok(());
        }
        let count = stale.len();
        let gone = self.bucket.delete_many(&stale).await;
        if gone.len() != count {
            anyhow::bail!(
                "{} of {count} stale-generation bundle objects survived the delete",
                count - gone.len()
            );
        }
        info!(
            bundles = count,
            "stale predecessor generations' bundles retired"
        );
        Ok(())
    }

    /// The fast eviction watch, one poll: any member the policy says to
    /// evict degrades the shipper now (acks fall to the bucket, which is
    /// always safe) and joins the quarantine; the caller runs `maintain`
    /// immediately so the swap costs detection plus flush plus CAS, not a
    /// maintenance tick. Returns whether anything was evicted.
    pub fn evict_gray_followers(&self) -> bool {
        let inner = self.inner.lock().unwrap().clone();
        let Some(shipper) = inner else { return false };
        if !shipper.is_active() {
            return false;
        }
        let now = mono_ms();
        let members: Vec<String> = shipper
            .members
            .iter()
            .map(|member| member.node.clone())
            .collect();
        let mut health = self.health.lock().unwrap();
        // Every member stuck at once is OUR fault, not theirs: degrade
        // without quarantining anyone, so the pool is intact the moment
        // connectivity returns.
        if health.correlated_stall(&self.policy, &members, now) {
            drop(health);
            self.suspect_self.store(true, Ordering::SeqCst);
            shipper.degrade("correlated member stall; suspecting ourselves");
            return true;
        }
        if !health.swap_allowed(&self.policy, now) {
            return false;
        }
        for member in &members {
            if health.verdict(&self.policy, member, &members, now)
                == celld_logic::log_evict::Verdict::Evict
            {
                health.evicted(&self.policy, member, now);
                drop(health);
                shipper.degrade(&format!("gray follower {member} evicted"));
                return true;
            }
        }
        false
    }

    /// Idle disk probes: an empty append still persists (and fsyncs) the
    /// follower's state file, so a quiet fleet finds a dying follower disk
    /// before load does. One probe per quiet member per interval, spawned
    /// detached — a hanging probe marks the member outstanding and the
    /// backstop does the rest.
    pub fn probe_followers(self: &Arc<Self>) {
        const PROBE_QUIET_MS: u64 = 2_000;
        let inner = self.inner.lock().unwrap().clone();
        let Some(shipper) = inner else { return };
        // Probes run DEGRADED too — a degraded shipper is exactly when
        // connectivity evidence matters: the probe's signed 200 is what
        // lifts self-suspicion after a partition heals, and gating probes
        // on health once parked recruitment forever.
        let now = mono_ms();
        for member in &shipper.members {
            if !self
                .health
                .lock()
                .unwrap()
                .probe_due(&member.node, now, PROBE_QUIET_MS)
            {
                continue;
            }
            let shipper = shipper.clone();
            let node = member.node.clone();
            let health = self.health.clone();
            let manager = self.clone();
            crate::asyncrt::spawn(async move {
                let member = shipper
                    .members
                    .iter()
                    .find(|member| member.node == node)
                    .expect("probed member is in the ensemble");
                let req = AppendReq {
                    leader: shipper.node.clone(),
                    epoch: shipper.epoch,
                    truncate_to: 0,
                    entries: Vec::new(),
                };
                let started = mono_ms();
                health.lock().unwrap().append_started(&node, started);
                let outcome = shipper.post_append(member, &req).await;
                let done = mono_ms();
                health
                    .lock()
                    .unwrap()
                    .append_completed(&node, done, done.saturating_sub(started));
                // ANY well-formed peer response — even an append refusal,
                // which is still a signed HTTP 200 — proves connectivity
                // and lifts self-suspicion. An incapable answer proves
                // the peer is the wrong binary, not that we are cut off,
                // and it quarantines here exactly as a shipped batch
                // would — an idle ensemble must not keep a 0.2.x member
                // recruit-eligible just because no writes arrive.
                match outcome {
                    AppendSend::Answered(_) => {
                        manager.suspect_self.store(false, Ordering::SeqCst);
                    }
                    AppendSend::Incapable(error) => {
                        warn!(
                            member = node,
                            %error,
                            "follower cannot serve log appends; quarantined from recruitment"
                        );
                        health
                            .lock()
                            .unwrap()
                            .append_incapable(&shipper.policy, &node, done);
                    }
                    AppendSend::Failed(_) => {}
                }
            })
            .detach();
        }
    }

    async fn post<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
        &self,
        node: &str,
        addr: &str,
        path: &str,
        req: &Req,
    ) -> anyhow::Result<Resp> {
        let bytes = self
            .transport
            .post(node, addr, path, serde_json::to_vec(req)?, None)
            .await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    /// The tail POST: a JSON request, a binary response (the entries
    /// dominate it).
    async fn post_tail(&self, node: &str, addr: &str, req: &TailReq) -> anyhow::Result<TailResp> {
        let bytes = self
            .transport
            .post(node, addr, "/__log/tail", serde_json::to_vec(req)?, None)
            .await?;
        decode_tail_resp(&bytes)
    }

    /// Node-log recovery, the model's StartRecovery/SealFollower/
    /// FinishRecovery in one pass: fence via CAS, seal every reachable
    /// member (at least one must succeed), gather their tails, upload each
    /// entry to the exact per-cell key the dead leader would have used, and
    /// CAS the record sealed. Every step is a CAS or an idempotent PUT, so
    /// racing recoverers re-run harmlessly.
    /// Upload gathered rows into the per-cell layout: grouped per
    /// (cell, epoch), each group's contiguous tail merged into ONE L0
    /// segment (per-row fallback for non-contiguous chains), skipping
    /// rows the per-cell watermark already covers. Shared by recovery's
    /// gather and the reopen healing pass. Any failure propagates —
    /// callers must not seal past an incomplete fold.
    async fn upload_gathered(
        &self,
        gathered: BTreeMap<(String, u64, u64), Vec<u8>>,
    ) -> anyhow::Result<usize> {
        type CellRows = Vec<(u64, Vec<u8>)>;
        let mut groups: BTreeMap<(String, u64), CellRows> = BTreeMap::new();
        for ((cell, cell_epoch, txid), bytes) in gathered {
            groups
                .entry((cell, cell_epoch))
                .or_default()
                .push((txid, bytes));
        }
        let uploads = groups.into_iter().map(|((cell, cell_epoch), rows)| {
            let ltx = self.ltx.clone();
            async move {
                let watermark = ltx.covered_txid(&cell, cell_epoch).await;
                let rows: Vec<(u64, Vec<u8>)> = rows
                    .into_iter()
                    .filter(|(txid, _)| *txid > watermark)
                    .collect();
                if rows.is_empty() {
                    return anyhow::Ok(0_usize);
                }
                let uploaded = rows.len();
                let puts: Vec<(u64, u64, Vec<u8>)> =
                    match crate::ltx_repl::LtxRepl::merge_l0_rows(&rows) {
                        Some(merged) => {
                            vec![(rows[0].0, rows[uploaded - 1].0, merged)]
                        }
                        None => rows
                            .into_iter()
                            .map(|(txid, bytes)| (txid, txid, bytes))
                            .collect(),
                    };
                for (min_txid, max_txid, bytes) in puts {
                    // Racing recoverers PUT the same idempotent keys, and
                    // the object store answers the collision with a
                    // retryable refusal, not corruption — one recoverer
                    // backing off is all it takes to converge.
                    let mut attempt = 0_u32;
                    loop {
                        match ltx
                            .upload_raw_l0(&cell, cell_epoch, min_txid, max_txid, &bytes)
                            .await
                        {
                            Ok(()) => break,
                            Err(error) if attempt < 4 => {
                                attempt += 1;
                                let jitter = (min_txid.wrapping_mul(2654435761) >> 27) % 97;
                                let wait = 150_u64 * (1 << attempt) + jitter;
                                warn!(%error, cell, min_txid, attempt, "recovery upload refused; backing off");
                                crate::asyncrt::sleep(std::time::Duration::from_millis(wait)).await;
                            }
                            Err(error) => {
                                return Err(error).with_context(|| {
                                    format!(
                                        "recovery upload {cell} e{cell_epoch} \
                                         t{min_txid}-{max_txid}"
                                    )
                                });
                            }
                        }
                    }
                }
                anyhow::Ok(uploaded)
            }
        });
        let mut count = 0_usize;
        let mut uploads =
            futures_util::stream::iter(uploads).buffer_unordered(RECOVERY_UPLOAD_CONCURRENCY);
        while let Some(uploaded) = futures_util::StreamExt::next(&mut uploads).await {
            count += uploaded?;
        }
        Ok(count)
    }

    /// Recover one dead SESSION's log: `dead` is `<node>/<generation>`.
    pub async fn recover(&self, dead: &str) -> anyhow::Result<()> {
        for _attempt in 0..5 {
            let Some(folded) = read_record(&self.bucket, dead).await? else {
                return Ok(());
            };
            let FoldedRead {
                record,
                active,
                token,
                wire,
            } = folded;
            // Re-judge deadness on the record actually being fenced (cold
            // review, S1): the caller's verdict may be stale — an owner
            // can restart between the read that justified this call and
            // now, and the spec's RecoverLog is enabled only past expiry
            // AT the step. A live lease is an error, not a skip: the
            // caller's claim must refuse and re-resolve to routing.
            let now = crate::ownership_store::now_ms();
            anyhow::ensure!(
                wire.expires_ms <= now,
                "refusing to fence {dead}: its lease is live again (expires in {}ms)",
                wire.expires_ms.saturating_sub(now)
            );
            // No justification pin: the record is keyed by session, so a
            // revived process writes a NEW key and can never step this one.
            // The only concurrent writer is a rival recoverer, and every
            // recovery step is a CAS or an idempotent upload — a lost CAS
            // re-reads and converges on the rival's outcome.
            match record.state {
                LogState::Sealed => return Ok(()),
                LogState::Open => {
                    let Some(recovering) = log_tier::start_recovery(&record) else {
                        continue;
                    };
                    if write_dead_record(&self.bucket, dead, &wire, &recovering, active, &token)
                        .await?
                        .is_none()
                    {
                        continue; // lost the CAS; re-read
                    }
                }
                LogState::Recovering => {}
            }
            let Some(folded) = read_record(&self.bucket, dead).await? else {
                return Ok(());
            };
            let FoldedRead {
                record,
                active,
                token,
                wire,
            } = folded;
            if record.state == LogState::Sealed {
                return Ok(());
            }

            let now = crate::ownership_store::now_ms();
            let pass_started = mono_ms();
            let mut witnesses = 0_usize;
            // A member is CONCLUSIVE when its fate is known: lease provably
            // expired and unreachable (its disk is not coming back on its
            // own), or reachable but holding a different fragment epoch (a
            // fresh disk under a reused name — the data is already gone).
            // Only a fully conclusive, witness-free, active log may declare
            // bounded loss; an unreachable member with a live lease is a
            // transient and keeps the loud retry.
            let mut inconclusive = 0_usize;
            let mut gathered: BTreeMap<(String, u64, u64), Vec<u8>> = BTreeMap::new();
            // "A blink is not death" applies to loss declaration too: a
            // member whose lease merely lapsed (a restart, a fleet-wide
            // power cycle) is NOT conclusively gone — its fsync'd fragments
            // boot back in seconds, and declaring loss against boot order
            // would discard acked writes sitting intact on disk — so a
            // 3x-TTL grace applies to MEMBER fate here, unrelated to the
            // sweep (which, under the fold, judges the record's own
            // published expiry with no grace).
            let grace_ms = (self.ownership.lease_ttl_ms() * 3).max(20_000);
            for member in &record.ensemble {
                let lease = self.ownership.read_node_lease(member).await;
                let lease_live = matches!(&lease, Ok(Some(lease)) if lease.expires_ms > now);
                let lease_long_dead = matches!(
                    &lease,
                    Ok(Some(lease)) if lease.expires_ms.saturating_add(grace_ms) < now
                );
                let addr = match lease {
                    Ok(Some(lease)) => Some(lease.addr),
                    _ => None,
                };
                let Some(addr) = addr else {
                    if lease_live || !lease_long_dead {
                        inconclusive += 1;
                    }
                    continue;
                };
                let seal = SealReq {
                    leader: dead.to_string(),
                    epoch: record.epoch,
                };
                let Ok::<SealResp, _>(sealed) =
                    self.post(member, &addr, "/__log/seal", &seal).await
                else {
                    if lease_live || !lease_long_dead {
                        inconclusive += 1;
                    }
                    continue;
                };
                // A member holding a different fragment epoch is reachable
                // but witnesses nothing — a name-reused fresh disk answers
                // 0. Only a member at the record's epoch proves the gather
                // is the fragment, not an amnesiac's silence.
                if sealed.fragment_epoch == record.epoch {
                    witnesses += 1;
                }
                let tail = TailReq {
                    leader: dead.to_string(),
                };
                let Ok(tail) = self.post_tail(member, &addr, &tail).await else {
                    continue;
                };
                for entry in tail.entries {
                    gathered.insert((entry.cell, entry.cell_epoch, entry.txid), entry.bytes);
                }
            }
            let members_ms = mono_ms().saturating_sub(pass_started);
            // The dead leader's un-drained bundles are bucket-durable
            // coverage that recovery folds into the per-cell prefixes —
            // one GET per bundle, sliced locally, the same idempotent
            // per-cell PUTs as the follower gather. This runs regardless
            // of the witness outcome: even a declared loss drains what
            // the bucket already holds. EVERY retained bundle, not only
            // the record epoch's: a live reconfiguration steps the epoch
            // behind a barrier that counts bundle coverage as tiered, so
            // rows can be durable only in a prior epoch's bundle — an
            // epoch filter here sealed them out of the per-cell layout
            // forever (the RecoveryEpochFilter tooth). What bounds this
            // gather to the true un-drained window is bundle GC deleting
            // covered bundles, not a filter that can orphan acked rows;
            // the covered-txid check below still bounds the uploads.
            // Concurrent GETs: the profiling round measured the serial
            // gather at 89-112 s over ~1,300 bundles — the whole outage.
            // Order does not matter: a row duplicated across bundles
            // carries identical bytes (same cell, epoch, txid), and
            // or_insert keeps follower-gathered bytes authoritative.
            let bundle_metas = self
                .bucket
                .list(&format!("log/{dead}/bundle/"))
                .await
                .unwrap_or_default();
            #[cfg(all(test, celld_internal_tests))]
            let filter_epoch = crate::asyncrt::sabotage_active(
                crate::host_services::EngineSabotage::FilterRecoveryBundlesByRecordEpoch,
            );
            #[cfg(not(all(test, celld_internal_tests)))]
            let filter_epoch = false;
            let bundle_metas: Vec<_> = bundle_metas
                .into_iter()
                .filter(|meta| {
                    !filter_epoch
                        || meta
                            .location
                            .as_ref()
                            .rsplit('/')
                            .next()
                            .is_some_and(|name| name.starts_with(&format!("e{}-", record.epoch)))
                })
                .collect();
            let bundles_read = bundle_metas.len();
            let fetches = bundle_metas.into_iter().map(|meta| {
                let bucket = self.bucket.clone();
                async move {
                    let key = meta.location.as_ref().to_string();
                    match bucket.get(&key).await {
                        Ok(Some((bytes, _))) => Some((key, bytes)),
                        _ => None,
                    }
                }
            });
            let mut fetches = futures_util::stream::iter(fetches)
                .buffer_unordered(RECOVERY_UPLOAD_CONCURRENCY * 2);
            while let Some(fetched) = futures_util::StreamExt::next(&mut fetches).await {
                let Some((key, bytes)) = fetched else {
                    continue;
                };
                let Ok(rows) = celld_ltx::bundle::decode_rows(&bytes) else {
                    warn!(key, "unreadable bundle skipped during recovery");
                    continue;
                };
                for row in rows {
                    let Ok(payload) = celld_ltx::bundle::slice(&bytes, &row) else {
                        continue;
                    };
                    gathered
                        .entry((row.cell.clone(), row.cell_epoch, row.txid))
                        .or_insert_with(|| payload.to_vec());
                }
            }
            drop(fetches);

            let bundles_ms = mono_ms()
                .saturating_sub(pass_started)
                .saturating_sub(members_ms);
            // `active` was CASed before the first fleet ack of this epoch
            // was credited, so: no witness + active means acked frames
            // existed and every holder is gone or wiped. If any member's
            // fate is still open (live lease, unreachable), keep failing
            // loudly — the data may come back. If every member is
            // conclusively gone, declare the bounded loss AS A RECORD: a
            // permanent object beside the log record stating what was
            // unrecoverable, then seal and let the fleet proceed.
            // "Loss is a record, never a prompt."
            if witnesses == 0 && active && !record.ensemble.is_empty() {
                anyhow::ensure!(
                    inconclusive == 0,
                    "node-log recovery for {dead}: no true witness among {:?} and \
                     {inconclusive} member(s) undecided; refusing to seal what may \
                     be an amnesiac's silence",
                    record.ensemble
                );
                let loss = serde_json::json!({
                    "leader": dead,
                    "epoch": record.epoch,
                    "declared_at_ms": now,
                    "declared_by": self.node,
                    "ensemble": record.ensemble.iter().collect::<Vec<_>>(),
                    "note": "no true witness survived; acked writes within the \
                             final flush window may be unrecovered",
                });
                #[cfg(all(test, celld_internal_tests))]
                let skip_loss_record = crate::asyncrt::sabotage_active(
                    crate::host_services::EngineSabotage::SealAmnesiacWithoutLossRecord,
                );
                #[cfg(not(all(test, celld_internal_tests)))]
                let skip_loss_record = false;
                if !skip_loss_record {
                    self.bucket
                        .put(
                            &format!("log/{dead}.e{}.loss.json", record.epoch),
                            serde_json::to_vec(&loss)?,
                        )
                        .await?;
                }
                warn!(
                    dead,
                    epoch = record.epoch,
                    "declared bounded loss: no true witness for an active log; \
                     recovery record written"
                );
            }
            // Skip rows the drain points already folded into the per-cell
            // prefix: one LIST per cell bounds the uploads to the true
            // un-drained tail, so recovery cost tracks the flush window,
            // not the epoch's age. LTX TXIDs are contiguous per epoch, so
            // coverage up to the listed maximum is coverage of everything
            // at or below it. Cells drive concurrently — the sequential
            // version cost the lab ~47 s for 180 entries — while rows
            // within a cell stay ordered; any failure aborts the pass
            // before the record can seal.
            let upload_started = mono_ms();
            let count = self.upload_gathered(gathered).await?;
            let upload_ms = mono_ms().saturating_sub(upload_started);
            let Some(done) = log_tier::finish_recovery(&record, record.tiered) else {
                continue;
            };
            if write_dead_record(&self.bucket, dead, &wire, &done, active, &token)
                .await?
                .is_some()
            {
                info!(
                    dead,
                    entries = count,
                    pass = _attempt,
                    members_ms,
                    bundles_read,
                    bundles_ms,
                    upload_ms,
                    total_ms = mono_ms().saturating_sub(pass_started),
                    "node log recovered and sealed"
                );
                return Ok(());
            }
        }
        Err(anyhow!("node-log recovery for {dead} lost every CAS race"))
    }

    /// The takeover interlock: may this cell's takeover treat the bucket as
    /// complete? Runs before the restore reads or seals anything. `prior`
    /// arrives through the decision core's Claim, so an acquire confirmed by
    /// reconciliation names the displaced owner exactly like one confirmed
    /// by the CAS response — the v0 ambiguous-CAS window is closed. `None`
    /// means the consumed record was released or absent, which the release
    /// path already proved durable. Absence of a log record is a proof (the
    /// node never acked past the bucket); a sealed record means recovery
    /// already ran; anything else runs it now.
    pub async fn ensure_recovered(&self, prior: Option<&str>) -> anyhow::Result<()> {
        self.ensure_predecessors_recovered().await?;
        let Some(prior) = prior else {
            return Ok(());
        };
        if prior == self.node {
            return Ok(());
        }
        // ONE rule for every cold path, at the cost of ONE read the core
        // usually already performed: before restoring a cell last owned
        // by node X, X's folded log state must be sealed or absent. The
        // lease record holds at most one session's state — recovery-
        // before-install is what keeps a predecessor's Open state from
        // being replaced unrecovered — so a single GET decides.
        let folded = read_record(&self.bucket, prior).await?;
        let session = folded
            .as_ref()
            .map(|folded| format!("{prior}/{}", folded.wire.generation));
        match log_tier::takeover_gate(folded.as_ref().map(|folded| &folded.record)) {
            log_tier::TakeoverGate::BucketComplete => {}
            log_tier::TakeoverGate::RecoverFirst => {
                self.recover(&session.expect("a record implies a session"))
                    .await?
            }
        }
        Ok(())
    }

    fn shipper_batch_in_flight(shipper: &FleetShipper) -> bool {
        shipper.in_flight.load(Ordering::SeqCst)
    }

    /// The graceful-shutdown drain point: stop fleet acks, wait for the
    /// ticking bundle loop to tier what was shipped, then seal our own
    /// record. The next incarnation finds Sealed and opens a fresh epoch
    /// with no gather at all — without this, a routine restart hands
    /// recovery a whole epoch of already-drained bundles to re-fold.
    /// Best-effort: any failure leaves the record Open, and recovery
    /// does what it always does.
    pub async fn close_gracefully(&self) {
        let inner = self.inner.lock().unwrap().clone();
        let Some(shipper) = inner else { return };
        shipper.degrade("graceful shutdown");
        for _ in 0..50 {
            if self.ltx.all_shipped_tiered() {
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(200)).await;
        }
        // eprintln, not tracing: this runs on the way out of the process,
        // and buffered stdout may never flush before exit.
        let Some(current) = self.own_log.current() else {
            eprintln!("node-log close: no folded log; nothing to seal");
            return;
        };
        let Ok(record) = log_from_wire(&current) else {
            eprintln!("node-log close: folded log unreadable; left as is");
            return;
        };
        let active = current.active;
        // Quiesce the sink before the scan: without the latch, a flush
        // crediting between the scan's LIST and the seal CAS re-creates
        // the orphaned-seal class one layer down — the record CAS fences
        // record writers, and bundle credits never write the record.
        self.closing.store(true, Ordering::SeqCst);
        for _ in 0..30 {
            if !self.flush_in_flight.load(Ordering::SeqCst) {
                break;
            }
            crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await;
        }
        // In bundle mode "tiered" includes bundle coverage, but a sealed
        // record tells every future recovery there is nothing to gather —
        // so the seal requires every acked row as a per-cell object. The
        // barrier is the RETAINED BUNDLE SCAN, not a cell counter: the
        // gate's synced_seq is advanced by bundle credits (it must be —
        // it is the ack counter), so the old all_synced_per_cell check
        // never actually demanded the per-cell layout, and the class-A
        // hour sealed 1,300 acked rows into orphanhood behind epoch 156.
        // An Open record is always safe: the next incarnation's recovery
        // drains the bundles.
        #[cfg(all(test, celld_internal_tests))]
        let credited_only = crate::asyncrt::sabotage_active(
            crate::host_services::EngineSabotage::GracefulSealUsesCreditedCoverage,
        );
        #[cfg(not(all(test, celld_internal_tests)))]
        let credited_only = false;
        let per_cell_complete = self.ltx.all_shipped_tiered()
            && (credited_only
                || !self.bundle_mode
                || match self.uncovered_bundle_rows().await {
                    Ok(uncovered) => uncovered.is_empty(),
                    Err(_) => false,
                });
        if !log_tier::graceful_seal_allowed(
            &record,
            shipper.epoch,
            Self::shipper_batch_in_flight(&shipper),
            per_cell_complete,
        ) {
            eprintln!(
                "node-log close: not sealable (record epoch {} state {:?}, shipper epoch {}); record left open",
                record.epoch, record.state, shipper.epoch
            );
            return;
        }
        let sealed = log_tier::LogRecord {
            state: LogState::Sealed,
            ..record
        };
        match self.own_log.write(Some(log_to_wire(&sealed, active))).await {
            Ok(()) => eprintln!("node-log close: sealed epoch {}", sealed.epoch),
            Err(error) => eprintln!("node-log close: seal not durable: {error:#}"),
        }
    }

    /// Startup: recover every predecessor session's open log — their
    /// acked tails may sit on our old followers or our own staged files,
    /// and nothing may ack against a fresh ensemble until those tails are
    /// in the bucket. Our own session's record cannot exist yet, so this
    /// is ordinary dead-session recovery under other keys.
    pub async fn recover_self(&self) -> anyhow::Result<()> {
        self.ensure_predecessors_recovered().await
    }

    pub fn healthy(&self) -> bool {
        self.inner
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|shipper| shipper.is_active() && shipper.members.len() >= 2)
    }

    /// One ensemble-maintenance pass: if the shipper is absent, degraded,
    /// or under-strength while better peers exist, rebuild it. The order is
    /// the model's reconfiguration discipline, enforced against live
    /// uploads: deactivate (acks fall to the bucket), drain until every
    /// shipped frame is tiered (the force-tier barrier — old fragments
    /// become garbage), CAS the record at the next epoch, then install the
    /// new shipper. A lost CAS leaves us at bucket posture; the next pass
    /// re-reads and retries.
    pub async fn maintain(&self) -> anyhow::Result<()> {
        if self.healthy() {
            return Ok(());
        }
        // Self-suspicion parks recruitment: opening epochs into our own
        // partition churns records and proves nothing. Any successful
        // peer response lifts it.
        if self.suspect_self.load(Ordering::SeqCst) {
            return Ok(());
        }
        let peers = self.ownership.read_capacity_peers().await?;
        let now = crate::ownership_store::now_ms();
        let mono = mono_ms();
        let members: Vec<Member> = peers
            .into_iter()
            .filter(|peer| peer.node != self.node && peer.expires_ms > now)
            // An evicted follower sits out the quarantine: re-recruiting
            // the disk that just cost an eviction is how flapping starts.
            .filter(|peer| !self.health.lock().unwrap().quarantined(&peer.node, mono))
            .take(2)
            .map(|peer| Member {
                node: peer.node,
                addr: peer.addr,
            })
            .collect();
        {
            let inner = self.inner.lock().unwrap().clone();
            if let Some(current) = inner {
                // Same strength available: nothing to improve.
                if current.is_active() && members.len() <= current.members.len() {
                    return Ok(());
                }
                // Deactivate first: acks fall to the bucket and pacing
                // stops, so the drain below converges.
                current.degraded.store(true, Ordering::SeqCst);
                // The reconfiguration barrier is a core decision: a batch
                // between capture and credit is invisible to the coverage
                // counters, and every fleet-shipped frame must be
                // bucket-covered before the old fragments become
                // abandonable. Wait it out; the next tick retries.
                if !log_tier::may_reconfigure(
                    current.in_flight.load(Ordering::SeqCst),
                    self.ltx.all_shipped_tiered(),
                ) {
                    return Ok(());
                }
            }
        }
        if members.is_empty() {
            return Ok(());
        }
        if !self.ltx.all_shipped_tiered() {
            return Ok(()); // re-checked: the drain may regress between locks
        }
        let ensemble: BTreeSet<String> = members.iter().map(|member| member.node.clone()).collect();
        // The in-process folded state IS the record (single lease writer):
        // no bucket read, and no CAS-lost re-read loop — the only
        // concurrent mutation is a peer's recovery of our EXPIRED lease,
        // and then our renewals stop applying and write_own_log fails.
        let prior = self
            .own_log
            .current()
            .as_ref()
            .map(log_from_wire)
            .transpose()?;
        let step = log_tier::maintain_step(prior.as_ref());
        let record = match step {
            log_tier::MaintainStep::CreateFresh => log_tier::create_record(ensemble.clone(), 0)
                .ok_or_else(|| anyhow!("empty ensemble"))?,
            log_tier::MaintainStep::Wait => return Ok(()),
            // v0 tiers per cell, so the record's tiered offset stays 0
            // and the drain barrier above is what makes the old
            // fragments abandonable — the same precondition
            // plan_reconfigure encodes for the bundle tier. The reopen
            // healing pass is deleted with the per-session key: a
            // within-session reopen sits behind the drain barrier (every
            // shipped row tiered), and a new session recovers its
            // predecessors before its first open, so no reopen can strand
            // bundle rows behind a record it never owned.
            log_tier::MaintainStep::Reopen(epoch) => log_tier::LogRecord {
                epoch,
                ensemble: ensemble.clone(),
                tiered: 0,
                state: LogState::Open,
            },
        };
        // A fresh epoch always opens inactive: `active` flips — through
        // the lease chain — before the first fleet ack of the epoch is
        // credited. The open itself rides an immediate renewal; a failure
        // means our lease is not applying and the posture stays bucket.
        self.own_log
            .write(Some(log_to_wire(&record, false)))
            .await?;
        info!(
            epoch = record.epoch,
            members = ?record.ensemble,
            "log ensemble open; fleet acks enabled"
        );
        self.health.lock().unwrap().reset();
        *self.inner.lock().unwrap() = Some(Arc::new(FleetShipper {
            // The wire leader identity IS the session string: followers key
            // fragments and seal marks by it, so a restarted process's
            // appends can never collide with its predecessor's fragments.
            node: self.session.clone(),
            transport: self.transport.clone(),
            record: record.clone(),
            own_log: self.own_log.clone(),
            activated: std::sync::atomic::AtomicBool::new(false),
            epoch: record.epoch,
            members,
            seq: std::sync::atomic::AtomicU64::new(0),
            degraded: std::sync::atomic::AtomicBool::new(false),
            in_flight: std::sync::atomic::AtomicBool::new(false),
            health: self.health.clone(),
            policy: self.policy.clone(),
        }));
        Ok(())
    }
}

impl NodeLogManager {
    /// Bundle GC: delete a bundle object once every row it carries is
    /// covered by the per-cell layout. The watermark is what keeps
    /// recovery's whole-prefix gather bounded to the true un-drained
    /// window without an epoch filter that could orphan acked rows — a
    /// bundle is deletable exactly when nothing could ever need to gather
    /// it. Bounded per tick; rows come from the in-memory index when this
    /// incarnation wrote the bundle, one GET otherwise. An unreadable
    /// bundle is never deleted on a guess: it stays, and stays cheap.
    /// Every retained bundle row above its (cell, epoch) per-cell
    /// covered watermark — the rows a sealed record would ORPHAN. The
    /// graceful seal refuses while any exist, and the reopen path folds
    /// them per-cell first (the healing pass). The class-A-hour P0 was
    /// exactly this set sealed over: `all_synced_per_cell` counted
    /// bundle credits (the gate's counter must), so the old barrier
    /// never actually demanded the per-cell layout.
    pub async fn uncovered_bundle_rows(
        &self,
    ) -> anyhow::Result<BTreeMap<(String, u64, u64), Vec<u8>>> {
        let prefix = format!("log/{}/bundle/", self.session);
        let mut covered: HashMap<(String, u64), u64> = HashMap::new();
        let mut uncovered: BTreeMap<(String, u64, u64), Vec<u8>> = BTreeMap::new();
        // The index first, GETs only for the misses, and those
        // concurrently: the first cut GET every retained bundle serially
        // and blew systemd's stop budget — the graceful close timed out
        // into SIGKILL and the seal never happened at all.
        let listed = self.bucket.list(&prefix).await?;
        let fetches = listed.into_iter().map(|meta| {
            let key = meta.location.as_ref().to_string();
            let indexed = {
                let index = self.bundle_index.lock().unwrap();
                index.iter().any(|(indexed, _)| *indexed == key)
            };
            let bucket = self.bucket.clone();
            async move {
                if indexed {
                    return Some((key, None));
                }
                match bucket.get(&key).await {
                    Ok(Some((bytes, _))) => Some((key, Some(bytes))),
                    _ => None,
                }
            }
        });
        let mut fetched = Vec::new();
        let mut fetches =
            futures_util::stream::iter(fetches).buffer_unordered(RECOVERY_UPLOAD_CONCURRENCY * 2);
        while let Some(item) = futures_util::StreamExt::next(&mut fetches).await {
            if let Some(item) = item {
                fetched.push(item);
            }
        }
        drop(fetches);
        for (key, bytes) in fetched {
            let (rows, bytes) = match bytes {
                Some(bytes) => match celld_ltx::bundle::decode_rows(&bytes) {
                    Ok(rows) => (rows, Some(bytes)),
                    Err(_) => continue,
                },
                None => {
                    let index = self.bundle_index.lock().unwrap();
                    let Some((_, rows)) = index.iter().find(|(indexed, _)| *indexed == key) else {
                        continue;
                    };
                    (rows.clone(), None)
                }
            };
            for row in rows {
                let cache_key = (row.cell.clone(), row.cell_epoch);
                let watermark = match covered.get(&cache_key) {
                    Some(watermark) => *watermark,
                    None => {
                        let watermark = self.ltx.covered_txid(&row.cell, row.cell_epoch).await;
                        covered.insert(cache_key, watermark);
                        watermark
                    }
                };
                // The twin-gated decision IS the predicate: a row the
                // per-cell layout covers is deletable; anything else is
                // exactly what a seal would orphan. Routing through
                // bundle_deletable keeps this barrier inside the ratchet
                // instead of an inline comparison free to drift again.
                if !log_tier::bundle_deletable([(row.txid, watermark)]) {
                    // Index-hit bundles were not fetched; an uncovered row
                    // forces the one lazy GET its payload needs.
                    let bytes = match &bytes {
                        Some(bytes) => bytes.clone(),
                        None => match self.bucket.get(&key).await {
                            Ok(Some((bytes, _))) => bytes,
                            _ => continue,
                        },
                    };
                    if let Ok(payload) = celld_ltx::bundle::slice(&bytes, &row) {
                        uncovered
                            .entry((row.cell.clone(), row.cell_epoch, row.txid))
                            .or_insert_with(|| payload.to_vec());
                    }
                }
            }
        }
        Ok(uncovered)
    }

    pub async fn gc_bundles(&self) -> anyhow::Result<()> {
        // 512, not 32: the lab's profiling round found 1,300+ retained
        // bundles — at ~1 bundle/s produced and 32 examined per 30 s tick
        // the backlog only ever grew, and recovery's whole-prefix gather
        // paid for it (89-112 s of a 97-116 s outage). The examined set
        // costs one LIST plus mostly index-hits; the covered_txid cache
        // bounds the per-tick LIST fan-out to the cell count. The TIME
        // budget is the other half: un-indexed bundles cost a GET each,
        // and an unbounded drain pass competed with serving hard enough
        // to gray followers and trigger eviction churn (the ~300 ms
        // bucket-riding window the faceted latency lanes exposed). The
        // pass stops at the budget; the next tick continues where the
        // listing puts it.
        const EXAMINED_PER_TICK: usize = 512;
        const TICK_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);
        let started = mono_ms();
        let prefix = format!("log/{}/bundle/", self.session);
        let mut covered: HashMap<(String, u64), u64> = HashMap::new();
        let mut deletable: Vec<(String, usize)> = Vec::new();
        for meta in self
            .bucket
            .list(&prefix)
            .await?
            .into_iter()
            .take(EXAMINED_PER_TICK)
        {
            if mono_ms().saturating_sub(started) > TICK_BUDGET.as_millis() as u64 {
                break;
            }
            let key = meta.location.as_ref().to_string();
            let indexed = {
                let index = self.bundle_index.lock().unwrap();
                index
                    .iter()
                    .find(|(indexed, _)| *indexed == key)
                    .map(|(_, rows)| rows.clone())
            };
            let rows = match indexed {
                Some(rows) => rows,
                None => {
                    let Ok(Some((bytes, _))) = self.bucket.get(&key).await else {
                        continue;
                    };
                    match celld_ltx::bundle::decode_rows(&bytes) {
                        Ok(rows) => rows,
                        Err(_) => continue,
                    }
                }
            };
            let mut paired = Vec::with_capacity(rows.len());
            for row in &rows {
                let cache_key = (row.cell.clone(), row.cell_epoch);
                let watermark = match covered.get(&cache_key) {
                    Some(watermark) => *watermark,
                    None => {
                        let watermark = self.ltx.covered_txid(&row.cell, row.cell_epoch).await;
                        covered.insert(cache_key, watermark);
                        watermark
                    }
                };
                paired.push((row.txid, watermark));
            }
            #[cfg(all(test, celld_internal_tests))]
            let delete_uncovered = crate::asyncrt::sabotage_active(
                crate::host_services::EngineSabotage::DeleteUncoveredBundle,
            );
            #[cfg(not(all(test, celld_internal_tests)))]
            let delete_uncovered = false;
            if !delete_uncovered && !log_tier::bundle_deletable(paired) {
                continue;
            }
            deletable.push((key, rows.len()));
        }
        if deletable.is_empty() {
            return Ok(());
        }
        // One DeleteObjects request instead of one DELETE per bundle: the
        // lab priced the per-key path at 9k class A operations an hour.
        let keys: Vec<String> = deletable.iter().map(|(key, _)| key.clone()).collect();
        let gone = self.bucket.delete_many(&keys).await;
        if !gone.is_empty() {
            let rows: usize = deletable
                .iter()
                .filter(|(key, _)| gone.contains(key))
                .map(|(_, rows)| rows)
                .sum();
            self.bundle_index
                .lock()
                .unwrap()
                .retain(|(indexed, _)| !gone.contains(indexed));
            info!(
                bundles = gone.len(),
                rows, "bundle GC: drained bundles deleted in one batch"
            );
        }
        Ok(())
    }

    /// Eager recovery ("Recovery is one verb"): every maintenance tick,
    /// sweep `log/` for a foreign, unsealed record whose owner's lease has
    /// expired, and recover it — traffic or none. Lazy-only recovery left
    /// an idle dead owner's un-tiered tail on two follower disks for an
    /// unbounded time; the sweep bounds that exposure at roughly one tick
    /// past lease expiry. Every survivor sweeps; racing recoverers collapse
    /// onto the CAS like every other recovery race.
    pub async fn sweep_dead_leaders(&self) -> anyhow::Result<()> {
        let now = crate::ownership_store::now_ms();
        for meta in self.bucket.list("nodes/").await? {
            let Some(node) = meta
                .location
                .as_ref()
                .strip_prefix("nodes/")
                .and_then(|name| name.strip_suffix(".json"))
                .filter(|name| !name.contains('/'))
                .map(str::to_string)
            else {
                continue;
            };
            if node == self.node {
                continue;
            }
            // One unreadable record must not end the sweep for every node
            // sorted after it.
            let Ok(Some(folded)) = read_record(&self.bucket, &node).await else {
                continue;
            };
            let session = format!("{node}/{}", folded.wire.generation);
            let record = folded.record;
            // Under the fold, the lease we just read IS the record: a
            // session is dead the moment its published expiry passed. A
            // restarted node replaces the record (generation and all)
            // through recovery-before-install, so the sweep never
            // contends with a returning leader; an expired same-
            // generation lease is a process that self-fenced or is about
            // to, and recovery's CAS fences its remaining renewals.
            let dead = folded.wire.expires_ms <= now;
            if record.state == LogState::Sealed {
                // A dead session's sealed subtree is one GC unit: recovery
                // folded every acked row per-cell before the seal, so the
                // retained bundles are garbage, and deleting the record
                // afterwards keeps the invariant — absence means complete,
                // exactly as sealed did. Bundles go FIRST so a session
                // record never vanishes while its subtree still holds
                // objects; loss records are declarations and stay forever.
                // Racing sweepers double-delete idempotently. Without this,
                // log/ grows one record per restart for the fleet's
                // lifetime, and every sweep and takeover LIST scales with
                // restart history.
                if dead && !self.gc_confirmed_empty.lock().unwrap().contains(&session) {
                    match self.gc_sealed_session(&session).await {
                        Ok(empty) => {
                            if empty {
                                self.gc_confirmed_empty.lock().unwrap().insert(session);
                            }
                        }
                        Err(error) => warn!(session, %error, "sealed-session GC failed"),
                    }
                }
                continue;
            }
            if !dead {
                continue;
            }
            info!(session, "eager recovery: dead session with an open log");
            if let Err(error) = self.recover(&session).await {
                warn!(session, %error, "eager node-log recovery failed");
            }
        }
        Ok(())
    }

    /// Delete a dead, sealed session's retained bundles. Under the fold
    /// the record itself lives in the lease and is retired by dead-lease
    /// GC; what a sealed session leaves behind is its bundle subtree, and
    /// recovery folded every acked row per-cell before the seal, so the
    /// bundles are garbage. Batched: a session can retain hundreds, and
    /// the lab priced one-key-at-a-time GC at 9k class A operations an
    /// hour. A key that fails stays for the next sweep tick.
    async fn gc_sealed_session(&self, session: &str) -> anyhow::Result<bool> {
        let keys: Vec<String> = self
            .bucket
            .list(&format!("log/{session}/bundle/"))
            .await?
            .into_iter()
            .map(|meta| meta.location.as_ref().to_string())
            .collect();
        if keys.is_empty() {
            return Ok(true);
        }
        let count = keys.len();
        let gone = self.bucket.delete_many(&keys).await;
        if gone.len() != count {
            anyhow::bail!(
                "{} of {count} bundle objects survived the delete; retrying next tick",
                count - gone.len()
            );
        }
        info!(session, bundles = count, "sealed session's bundles retired");
        Ok(true)
    }
}

/// The maintenance cadence: recruit at startup, repair forever after, and
/// sweep for dead leaders' open logs. Beside it, the fast eviction watch:
/// gray-follower detection cannot wait thirty seconds when one slow fsync
/// tail is every ack's tail, so verdicts poll at a sub-second cadence and
/// an eviction repairs the ensemble immediately.
pub fn spawn_maintenance(manager: Arc<NodeLogManager>) {
    let watcher = manager.clone();
    crate::asyncrt::spawn(async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_secs(30));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            if let Err(error) = manager.maintain().await {
                warn!(%error, "log ensemble maintenance failed");
            }
            if let Err(error) = manager.sweep_dead_leaders().await {
                warn!(%error, "dead-leader sweep failed");
            }
            if let Err(error) = manager.gc_bundles().await {
                warn!(%error, "bundle GC failed");
            }
        }
    })
    .detach();
    crate::asyncrt::spawn(async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_millis(250));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        let mut repair_since: Option<u64> = None;
        let mut last_repair_try = mono_ms().saturating_sub(2_000);
        let mut repair_interval = std::time::Duration::from_secs(1);
        loop {
            tick.tick().await;
            watcher.probe_followers();
            watcher.evict_gray_followers();
            // Posture repair does not wait for the 30 s maintenance tick:
            // while the shipper is absent or degraded, retry about once a
            // second — the first attempt after an eviction usually loses
            // to the drain barrier, and E8's first light measured a 22 s
            // hole where nothing retried until the tick. The repair
            // latency line is the E8 recruit-repair metric.
            if watcher.healthy() {
                if let Some(since) = repair_since.take() {
                    info!(
                        event = "posture_repair",
                        degraded_ms = mono_ms().saturating_sub(since),
                        "fleet posture repaired"
                    );
                }
                repair_interval = std::time::Duration::from_secs(1);
                continue;
            }
            repair_since.get_or_insert_with(mono_ms);
            // The first repair after a degrade is immediate — that is the
            // 0.7s swap — but successive failed repairs back off to the
            // swap rate cap: a full peer partition once opened seventeen
            // doomed epochs in twenty seconds at the flat retry rate.
            if mono_ms().saturating_sub(last_repair_try) >= repair_interval.as_millis() as u64 {
                last_repair_try = mono_ms();
                let epoch_before = crate::ltx_repl::Shipper::epoch(&*watcher);
                if let Err(error) = watcher.maintain().await {
                    warn!(%error, "posture repair failed");
                }
                let stepped = crate::ltx_repl::Shipper::epoch(&*watcher) != epoch_before;
                repair_interval = if watcher.healthy() {
                    std::time::Duration::from_secs(1)
                } else if stepped {
                    (repair_interval * 2).min(std::time::Duration::from_secs(10))
                } else {
                    std::time::Duration::from_secs(1)
                };
            }
        }
    })
    .detach();
}

/// The follower-side fragment GC: a fragment whose epoch the record has
/// moved past (a reconfiguration or a reopened incarnation), or whose
/// epoch's record is Sealed (recovery certified and uploaded the tail), is
/// garbage no gather will ever consult — without this sweep a follower
/// that is never re-recruited keeps one closed epoch's fragments per
/// leader forever. The seal mark is preserved and extended: a closed
/// epoch is refused from here on, which is also what makes the deletion
/// safe against any straggling append.
pub fn spawn_fragment_gc(store: Arc<FollowerStore>) {
    crate::asyncrt::spawn(async move {
        let mut tick = crate::asyncrt::interval(std::time::Duration::from_secs(600));
        tick.set_missed_tick_behavior(crate::asyncrt::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            store.gc_fragments().await;
        }
    })
    .detach();
}

impl crate::ltx_repl::BundleSink for NodeLogManager {
    /// One object per node-flush: `log/<node>/bundle/e<epoch>-<seq>.ltxb`,
    /// verbatim L0 segments plus the footer (`crate::bundle`). Keys are
    /// unique per (epoch, seq), so the PUT needs no condition; the epoch in
    /// the key scopes recovery's gather and the eventual GC sweep.
    fn put_bundle<'a>(
        &'a self,
        entries: Vec<celld_ltx::bundle::BundleEntry>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + Send + 'a>> {
        Box::pin(async move {
            struct FlushGuard<'a>(&'a std::sync::atomic::AtomicBool);
            impl Drop for FlushGuard<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::SeqCst);
                }
            }
            self.flush_in_flight.store(true, Ordering::SeqCst);
            let _flush = FlushGuard(&self.flush_in_flight);
            let (epoch, flusher) = {
                let inner = self.inner.lock().unwrap();
                match inner.as_ref() {
                    Some(shipper) => (shipper.epoch, shipper.clone()),
                    None => return false,
                }
            };
            let seq = self.bundle_seq.fetch_add(1, Ordering::SeqCst);
            let body = match celld_ltx::bundle::encode(&entries) {
                Ok(body) => body,
                Err(error) => {
                    warn!(%error, "bundle encode failed");
                    return false;
                }
            };
            let rows = match celld_ltx::bundle::decode_rows(&body) {
                Ok(rows) => rows,
                Err(error) => {
                    warn!(%error, "bundle self-decode failed");
                    return false;
                }
            };
            let key = format!("log/{}/bundle/e{epoch}-{seq:08}.ltxb", self.session);
            match self.bucket.put(&key, body).await {
                Ok(()) => {
                    // The credit check: the PUT is unconditional, so by
                    // itself it proves nothing to the ack path — a healed
                    // zombie whose record a recoverer already sealed can
                    // still land bundle objects, and crediting them would
                    // ack rows no future takeover reads (the sealed record
                    // says the bucket is complete, and takeovers read
                    // per-cell prefixes). Durability credits only if the
                    // record is still Open at this shipper's epoch AFTER
                    // the PUT: any recovery that fences later must list
                    // after this PUT completed and therefore gathers it.
                    #[cfg(all(test, celld_internal_tests))]
                    let skip_record_read = crate::asyncrt::sabotage_active(
                        crate::host_services::EngineSabotage::SkipBundleCreditRecordRead,
                    );
                    #[cfg(not(all(test, celld_internal_tests)))]
                    let skip_record_read = false;
                    // A BUCKET read on purpose: the hazard is a peer's
                    // recovery CAS fencing this record, which the
                    // in-process copy cannot see.
                    let credit = skip_record_read
                        || log_tier::bundle_credit_allowed(
                            read_record(&self.bucket, &self.session)
                                .await
                                .ok()
                                .flatten()
                                .as_ref()
                                .map(|folded| &folded.record),
                            epoch,
                        );
                    if !credit {
                        // Degrade the shipper that OWNED this flush's
                        // epoch, not whichever is installed now: a flush
                        // racing a legitimate reconfiguration must not
                        // poison the successor ensemble it knows nothing
                        // about. For a true zombie the flusher IS the
                        // installed shipper and stops exactly as before;
                        // for a swap race this degrades a retired object,
                        // which is the correct no-op — the lab measured
                        // the alternative as an epoch-churn loop.
                        flusher.degrade("record moved under a bundle flush");
                        warn!(key, "bundle flush not credited: record moved");
                        return false;
                    }
                    let mut index = self.bundle_index.lock().unwrap();
                    index.push_back((key, rows));
                    // Bounded: older bundles are compacted past or folded
                    // by drains; the cap only limits the overlay's view.
                    while index.len() > 512 {
                        index.pop_front();
                    }
                    true
                }
                Err(error) => {
                    warn!(%error, key, "bundle put failed");
                    false
                }
            }
        })
    }

    fn active(&self) -> bool {
        // Draining rides the bundle path even while the shipper is
        // DEGRADED: degrade stops fleet proofs, not tiering. The credit
        // check against the record is the safety gate (a sealed or
        // stepped record refuses the credit), and demoting the
        // post-eviction drain to sequential per-cell PUTs was measured at
        // 57 s of bucket-posture acks under load — where the design
        // promised one flush. The one exception is the shutdown latch:
        // once the graceful close begins its seal scan, a new flush could
        // credit rows the scan never saw, so `closing` quiesces the sink
        // and late writes ride per-cell acks instead.
        self.bundle_mode
            && !self.closing.load(Ordering::SeqCst)
            && self.inner.lock().unwrap().is_some()
    }

    fn rows_for(&self, cell: &str, epoch: u64) -> Vec<celld_ltx::LocatedRow> {
        let index = self.bundle_index.lock().unwrap();
        index
            .iter()
            .flat_map(|(key, rows)| {
                rows.iter()
                    .filter(|row| row.cell == cell && row.cell_epoch == epoch)
                    .map(|row| celld_ltx::LocatedRow {
                        source: key.clone(),
                        row: row.clone(),
                    })
            })
            .collect()
    }

    fn fetch_bundle<'a>(
        &'a self,
        source: &'a str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Vec<u8>>> + Send + 'a>>
    {
        Box::pin(async move {
            {
                let cache = self.bundle_cache.lock().await;
                if let Some((key, bytes)) = cache.as_ref() {
                    if key == source {
                        return Ok(bytes.as_ref().clone());
                    }
                }
            }
            let Some((bytes, _)) = self.bucket.get(source).await? else {
                anyhow::bail!("bundle {source} vanished");
            };
            let bytes: Vec<u8> = bytes.to_vec();
            *self.bundle_cache.lock().await = Some((source.to_string(), Arc::new(bytes.clone())));
            Ok(bytes)
        })
    }
}

#[cfg(all(test, celld_internal_tests))]
include!(env!("CELLD_INTERNAL_NODE_LOG_OBSERVERS"));

#[cfg(all(test, celld_internal_tests))]
// The private durability test creates and inspects real directory fixtures.
#[allow(clippy::disallowed_methods)]
mod conformance_node_log_tests {
    include!(env!("CELLD_CONFORMANCE_NODE_LOG_TESTS"));
}

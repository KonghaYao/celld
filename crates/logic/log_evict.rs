// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The gray-follower eviction policy (sans-IO), the log tier's answer to
//! write-all's classic price: one slow follower stalls every ack, so the
//! leader evicts on suspicion and the empty-join reconfiguration makes a
//! false positive cost one flush plus one CAS.
//!
//! The decision: evict a follower
//! whose windowed append-latency tail exceeds
//! `max(absolute budget, k x sibling median)` sustained for a short
//! window, or whose single append has been outstanding past a hard
//! backstop. Flapping is bounded by hysteresis: an evicted follower is
//! quarantined from re-recruitment, and reconfigurations are rate-capped.
//! The executor feeds observations and performs the swap; nothing here
//! performs I/O or reads a clock.

use std::collections::BTreeMap;
use std::collections::VecDeque;

use crate::NodeId;

/// The constants are E8 targets, not commitments: the lab sweep measures
/// them, and the env overrides exist so it can.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvictionPolicy {
    /// The absolute tail budget: a follower slower than this is suspect
    /// regardless of its sibling.
    pub budget_ms: u64,
    /// The relative signal: suspect when the windowed median exceeds this
    /// multiple of the sibling's windowed median.
    pub sibling_factor: u64,
    /// How long the windowed median must stay over budget before the
    /// evict fires — one slow fsync is not a dying disk, and a median
    /// cannot be captured by a single outlier the way a tail quantile is.
    pub sustain_ms: u64,
    /// The hard backstop: any single append outstanding this long evicts
    /// immediately, sustain or none.
    pub backstop_ms: u64,
    /// An evicted follower is not re-recruited for this long.
    pub quarantine_ms: u64,
    /// At most one eviction-driven reconfiguration per interval.
    pub min_swap_interval_ms: u64,
    /// The sliding window the medians are computed over.
    pub window_ms: u64,
}

impl Default for EvictionPolicy {
    fn default() -> Self {
        EvictionPolicy {
            budget_ms: 25,
            sibling_factor: 4,
            sustain_ms: 2_000,
            backstop_ms: 400,
            quarantine_ms: 300_000,
            min_swap_interval_ms: 10_000,
            window_ms: 3_000,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Healthy,
    /// Over budget, sustain running; the executor need not act yet.
    Suspect,
    /// Swap this member out now.
    Evict,
}

#[derive(Clone, Debug, Default)]
struct MemberHealth {
    /// (completed_at_ms, latency_ms), oldest first, pruned to the window.
    samples: VecDeque<(u64, u64)>,
    /// When the current in-flight append started, if one is out.
    outstanding_since: Option<u64>,
    /// When the windowed median first exceeded the threshold, while it
    /// has stayed exceeded.
    suspect_since: Option<u64>,
}

impl MemberHealth {
    fn prune(&mut self, now_ms: u64, window_ms: u64) {
        while let Some((at, _)) = self.samples.front() {
            if at.saturating_add(window_ms) < now_ms {
                self.samples.pop_front();
            } else {
                break;
            }
        }
    }

    fn median_ms(&self) -> Option<u64> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = self.samples.iter().map(|(_, ms)| *ms).collect();
        sorted.sort_unstable();
        Some(sorted[sorted.len() / 2])
    }
}

/// The tracker for one ensemble's followers. Survives shipper swaps on the
/// executor side; `reset` starts a fresh ensemble's bookkeeping while the
/// quarantine ledger persists across it.
#[derive(Clone, Debug, Default)]
pub struct FollowerHealth {
    members: BTreeMap<NodeId, MemberHealth>,
    quarantined_until: BTreeMap<NodeId, u64>,
    last_swap_ms: Option<u64>,
}

impl FollowerHealth {
    /// A new ensemble: drop the members' windows, keep the quarantine
    /// ledger — it is exactly the memory that stops flapping.
    pub fn reset(&mut self) {
        self.members.clear();
    }

    /// An append POST went out to `member`.
    pub fn append_started(&mut self, member: &str, now_ms: u64) {
        let health = self.members.entry(member.to_string()).or_default();
        health.outstanding_since.get_or_insert(now_ms);
    }

    /// The append completed (ok or not) after `latency_ms`.
    pub fn append_completed(&mut self, member: &str, now_ms: u64, latency_ms: u64) {
        let health = self.members.entry(member.to_string()).or_default();
        health.outstanding_since = None;
        health.samples.push_back((now_ms, latency_ms));
    }

    /// The eviction decision for `member`, judged against its sibling's
    /// windowed median. The executor calls this per member per tick (and
    /// on every completion) and swaps on the first `Evict`.
    pub fn verdict(
        &mut self,
        policy: &EvictionPolicy,
        member: &str,
        siblings: &[NodeId],
        now_ms: u64,
    ) -> Verdict {
        let sibling_median = siblings
            .iter()
            .filter(|sibling| sibling.as_str() != member)
            .filter_map(|sibling| {
                let health = self.members.get_mut(sibling)?;
                health.prune(now_ms, policy.window_ms);
                health.median_ms()
            })
            .min();
        let Some(health) = self.members.get_mut(member) else {
            return Verdict::Healthy;
        };
        if let Some(since) = health.outstanding_since {
            if now_ms.saturating_sub(since) >= policy.backstop_ms {
                return Verdict::Evict;
            }
        }
        health.prune(now_ms, policy.window_ms);
        let Some(own_median) = health.median_ms() else {
            return Verdict::Healthy;
        };
        let threshold = sibling_median
            .map(|median| median.saturating_mul(policy.sibling_factor))
            .unwrap_or(0)
            .max(policy.budget_ms);
        if own_median <= threshold {
            health.suspect_since = None;
            return Verdict::Healthy;
        }
        let since = *health.suspect_since.get_or_insert(now_ms);
        if now_ms.saturating_sub(since) >= policy.sustain_ms {
            Verdict::Evict
        } else {
            Verdict::Suspect
        }
    }

    /// Record the eviction: quarantine the member and stamp the swap for
    /// the rate cap.
    pub fn evicted(&mut self, policy: &EvictionPolicy, member: &str, now_ms: u64) {
        self.quarantined_until.insert(
            member.to_string(),
            now_ms.saturating_add(policy.quarantine_ms),
        );
        self.members.remove(member);
        self.last_swap_ms = Some(now_ms);
    }

    /// The member answered an append with a protocol-level rejection a
    /// healthy follower cannot produce: the route is missing, or the
    /// response does not parse — a binary that does not speak the log
    /// tier (the 0.2.x rolling-upgrade seam). The latency verdicts are
    /// blind to this member, because its rejection is FAST and reads as
    /// a healthy sample, so recruiting re-picks it forever and the
    /// posture flaps once per repair tick. Incapability therefore feeds
    /// the quarantine directly: the member sits out `quarantine_ms` and
    /// the next rebuild recruits around it, or parks when no capable
    /// peer remains. An epoch refusal (`ok: false` in a well-formed
    /// response) is NOT incapability — a sealed or reconfigured
    /// follower answers that way and recruits fine at the next epoch.
    /// No `last_swap_ms` here: this is not an eviction-driven swap, and
    /// the rebuild that follows must not be rate-limited away from the
    /// capable peers that remain.
    pub fn append_incapable(&mut self, policy: &EvictionPolicy, member: &str, now_ms: u64) {
        self.quarantined_until.insert(
            member.to_string(),
            now_ms.saturating_add(policy.quarantine_ms),
        );
        self.members.remove(member);
    }

    /// May an eviction-driven swap happen now? A failed follower (a hard
    /// error, not a gray tail) bypasses this — that swap is v0's existing
    /// degrade path and correctness needs it regardless of cadence.
    pub fn swap_allowed(&self, policy: &EvictionPolicy, now_ms: u64) -> bool {
        self.last_swap_ms
            .is_none_or(|last| now_ms.saturating_sub(last) >= policy.min_swap_interval_ms)
    }

    /// Is this candidate quarantined out of re-recruitment?
    pub fn quarantined(&self, candidate: &str, now_ms: u64) -> bool {
        self.quarantined_until
            .get(candidate)
            .is_some_and(|until| *until > now_ms)
    }

    /// Every member's append is simultaneously outstanding past the
    /// backstop: the common cause is us — our NIC, our partition — not
    /// two disks dying in the same instant. The executor degrades to
    /// bucket posture WITHOUT quarantining anyone, so recruitment
    /// recovers the moment connectivity does; quarantining every peer
    /// for our own partition held a healed fleet on the bucket floor for
    /// the full quarantine term. One member is not evidence of
    /// correlation — a single-member stall keeps the ordinary eviction.
    pub fn correlated_stall(
        &self,
        policy: &EvictionPolicy,
        members: &[NodeId],
        now_ms: u64,
    ) -> bool {
        members.len() >= 2
            && members.iter().all(|member| {
                self.members
                    .get(member)
                    .and_then(|health| health.outstanding_since)
                    .is_some_and(|since| now_ms.saturating_sub(since) >= policy.backstop_ms)
            })
    }

    /// Does this member owe an idle probe? Quiet means no append in flight
    /// and no completion within the interval — a member under real load
    /// needs no synthetic samples.
    pub fn probe_due(&self, member: &str, now_ms: u64, quiet_ms: u64) -> bool {
        let Some(health) = self.members.get(member) else {
            return true;
        };
        if health.outstanding_since.is_some() {
            return false;
        }
        health
            .samples
            .back()
            .is_none_or(|(at, _)| at.saturating_add(quiet_ms) < now_ms)
    }
}

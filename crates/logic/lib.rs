// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Clean-sheet celld decision core.
//!
//! [`on_event`] is the only way behavioral state advances. The production
//! executor and deterministic simulator both feed it events and perform the
//! returned effects. No adapter may mutate [`State`] directly.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub mod alarm;
pub mod cache;
pub mod cell;
pub mod dead_node_reconciliation;
pub mod gate;
pub mod isolate;
pub mod peer;
pub mod pressure;
pub mod restore;
pub mod routing;
pub mod schedule;
pub mod sqlite;
pub mod wake;

mod types;
pub use types::*;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Activation {
    Claim(Claim),
    Restore(RestoreSpec),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ColdStart {
    ReadOwner,
    Restore(RestoreSpec),
}

/// Facts already decided by ownership resolution that select a safe restore
/// source. The effect adapter must not rediscover or guess these.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RestoreSpec {
    pub epoch: Epoch,
    /// This activation conditionally created epoch one, so no replica can
    /// precede it.
    pub fresh: bool,
    /// Ownership was seized from a different node (or a released record), so
    /// a previous local eviction cache is not authoritative.
    pub took_over: bool,
    /// The node-level lease handoff proved that this exact local epoch remains
    /// authoritative. The adapter must not consult the remote replica.
    pub resume_local: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claim {
    pub guard: CasGuard,
    pub epoch: Epoch,
    pub takeover: bool,
    /// Ambiguous acquires already reconciled for this claim.
    ///
    /// An ambiguous compare-and-swap is re-read rather than retried blindly,
    /// which is correct — it may have applied. But the re-read leads to
    /// another acquire, and if that is ambiguous too the cycle repeats. With
    /// no bound a persistently unanswered store turns a request that used to
    /// hang into one that spins, which is worse: it burns a slot and an
    /// object-store budget forever instead of merely waiting.
    pub reconciles: u32,
}

/// How many times one claim may reconcile an ambiguous acquire before the
/// request is failed and the caller left to decide. Small: each pass is a full
/// read plus a write, and a store answering ambiguously three times running is
/// not about to start answering.
pub const MAX_ACQUIRE_RECONCILES: u32 = 3;

/// Phases map onto the Durable Objects lifecycle states where Cloudflare has
/// a name for them:
///
/// - `Resident` is active or idle in memory (states 1-3).
/// - `Dormant` is out of memory but still owned by this node, so the cell has
///   not been moved off its host. A dormant cell with surviving hibernatable
///   sockets is *hibernated* (state 4); see [`Core::is_hibernated`].
/// - `Inactive` is out of memory and owned by nobody: removed from the host,
///   needing a cold start (state 5, and the initial state of every cell).
///
/// Note that celld and Cloudflare use "evict" differently. Cloudflare evicts a
/// cell *off its host*, which produces `Inactive`. celld evicts a cell *out of
/// memory*, which produces `Dormant`, and shedding is what then publishes it
/// unowned.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Inactive,
    /// Cold demand queued behind `max_activations`, before any I/O begins.
    WaitingActivation,
    ReadingOwner {
        op: OpId,
    },
    ReadingNodeLease {
        op: OpId,
        owner: OwnerRecord,
    },
    ReadingCapacity {
        op: OpId,
        claim: Claim,
    },
    WaitingCapacity,
    Acquiring {
        op: OpId,
        claim: Claim,
    },
    ReconcilingAcquire {
        op: OpId,
        claim: Claim,
    },
    Restoring {
        op: OpId,
        spec: RestoreSpec,
    },
    Starting {
        op: OpId,
        epoch: Epoch,
    },
    Publishing {
        op: OpId,
        epoch: Epoch,
    },
    /// The runtime remains published and routable while replica durability is
    /// being proved. A failed or ambiguous proof returns to `Resident`.
    EnsuringDurability {
        op: OpId,
        epoch: Epoch,
    },
    Cleaning {
        op: OpId,
        epoch: Epoch,
        cause: StopCause,
    },
    /// Out of memory, still owned here. Hibernated when sockets survived.
    Dormant {
        epoch: Epoch,
    },
    Resident {
        epoch: Epoch,
    },
    Remote {
        node: NodeId,
        addr: String,
        epoch: Epoch,
        peer_protocol: u16,
        /// Present only for epoch-zero capacity candidates. It identifies the
        /// exact advisory sample a refusal disproved.
        capacity_sampled_ms: Option<u64>,
    },
    Fenced,
}

/// The reported name of a phase. Stable across internal renames, because
/// `/state` and `celld diagnose` publish it.
fn phase_name(phase: &Phase) -> &'static str {
    match phase {
        Phase::Inactive => "inactive",
        Phase::WaitingActivation => "waiting_activation",
        Phase::ReadingOwner { .. } => "reading_owner",
        Phase::ReadingNodeLease { .. } => "reading_node_lease",
        Phase::ReadingCapacity { .. } => "reading_capacity",
        Phase::WaitingCapacity => "waiting_capacity",
        Phase::Acquiring { .. } => "acquiring",
        Phase::ReconcilingAcquire { .. } => "reconciling_acquire",
        Phase::Restoring { .. } => "restoring",
        Phase::Starting { .. } => "starting",
        Phase::Publishing { .. } => "publishing",
        Phase::EnsuringDurability { .. } => "ensuring_durability",
        Phase::Cleaning { .. } => "cleaning",
        Phase::Dormant { .. } => "dormant",
        Phase::Resident { .. } => "resident",
        Phase::Remote { .. } => "remote",
        Phase::Fenced => "fenced",
    }
}

/// One local write response held open by the output gate.
#[derive(Clone, Debug, PartialEq, Eq)]
struct GatedWrite {
    request: RequestId,
    cell: CellId,
    epoch: Epoch,
    position: u64,
}

#[derive(Clone, Debug)]
struct Cell {
    phase: Phase,
    requests: BTreeSet<RequestId>,
    websockets: BTreeMap<WebSocketId, WebSocketKind>,
    waiting_for: Option<Activation>,
    waiting_activation: Option<ColdStart>,
    /// The in-flight release of this cell's ownership record, if any.
    releasing: Option<OpId>,
    /// Whether the eviction now under way hands this cell to the fleet.
    evict_rebalance: bool,
    alarm: Option<AlarmState>,
    alarm_wake: bool,
    /// Startup, rather than a request or alarm, demands this activation.
    resume_demand: bool,
    /// When this cell's eviction was last refused, if it has been.
    ///
    /// A cell that cannot prove its replica durable goes back to residency,
    /// and being cold is exactly why nobody registered it -- so it settles at
    /// the head of the eviction order and is chosen again on the next pass,
    /// and the one after. Enough of those and the node has no shed candidate
    /// it will ever succeed with. Recording the refusal lets the order prefer
    /// cells that have not just failed, while still coming back to them when
    /// they are all that is left.
    eviction_refused_mono_ms: Option<u64>,
    /// When this cell last did something, on the remembered monotonic clock.
    ///
    /// Eviction order is the whole reason this exists. Without it the shed
    /// candidate is whichever cell sorts first by id, so a node under
    /// sustained pressure evicts its alphabetically-first cell over and over
    /// -- restoring and shedding the busiest cell on the node while an idle
    /// one further down the alphabet is never touched.
    last_used_mono_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum AlarmState {
    Armed {
        at_ms: i64,
        generation: u64,
        covered: bool,
    },
    Firing {
        op: OpId,
        at_ms: i64,
        generation: u64,
        covered: bool,
    },
}

#[derive(Clone, Debug)]
struct HeldNodeLease {
    spec: NodeLeaseSpec,
    record: NodeLeaseRecord,
    last_ok_mono_ms: u64,
    last_attempt_mono_ms: u64,
    timer_generation: u64,
    idle_since_mono_ms: Option<u64>,
    shadow_release_reported: bool,
}

#[derive(Clone, Debug)]
struct PendingNodeLease {
    spec: NodeLeaseSpec,
    desired: NodeLeaseRecord,
    prior: Option<HeldNodeLease>,
    /// This read follows a write whose result was ambiguous. It may prove the
    /// desired record landed, but must not turn a failed create into an
    /// unbounded read/write loop.
    readback_only: bool,
    /// The monotonic instant `desired.expires_ms` was computed from.
    ///
    /// Authority is bounded by what the *bucket* says, and the bucket says
    /// `expires_ms`, which is sampled here rather than when the write lands.
    /// Anchoring the local fence deadline to the write's completion instead
    /// would let this node serve for the whole duration of the round trip
    /// after every peer is entitled to declare it dead -- the store is under
    /// no obligation to answer quickly, so that window is unbounded.
    anchor_mono_ms: u64,
    /// This initial CAS replaces the exact live generation certified by the
    /// clean local shutdown. Renewals never set this bit.
    resume_local: bool,
}

#[derive(Clone, Debug)]
enum NodeAuthority {
    Unstarted,
    /// Lazy lifecycle with no live lease. The spec is retained so the next
    /// local dependency can acquire before touching cell ownership. Named
    /// `Lapsed`, not `Inactive`: `inactive` is a cell state (state 5, gone
    /// from this host) and must mean only that.
    Lapsed(NodeLeaseSpec),
    Reading {
        op: OpId,
        pending: PendingNodeLease,
    },
    Writing {
        op: OpId,
        pending: PendingNodeLease,
    },
    Held(HeldNodeLease),
    /// A continuous node whose initial acquisition did not succeed, waiting to
    /// try again.
    ///
    /// `StartNodeLease` is emitted exactly once, at process start, so without
    /// this the first failed acquisition is permanent: the process stays up,
    /// holds no authority, and answers every request `NodeUnavailable` for as
    /// long as it runs. One slow or rate-limited bucket response at startup
    /// should not cost a node its whole lifetime. A lazy node already
    /// recovers, because its next request re-acquires from `Lapsed`.
    Retrying {
        spec: NodeLeaseSpec,
        generation: u64,
    },
    Failed,
    Fenced,
}

impl Default for Cell {
    fn default() -> Self {
        Self {
            phase: Phase::Inactive,
            requests: BTreeSet::new(),
            websockets: BTreeMap::new(),
            waiting_for: None,
            waiting_activation: None,
            releasing: None,
            evict_rebalance: false,
            alarm: None,
            alarm_wake: false,
            resume_demand: false,
            eviction_refused_mono_ms: None,
            last_used_mono_ms: 0,
        }
    }
}

/// All authoritative coordination state for the first vertical slice.
pub struct State {
    node: NodeId,
    config: Config,
    fenced: bool,
    /// A shutdown handoff is under way: every resident cell is to be
    /// released, pumped at most `max_releases` at a time. Sticky -- a
    /// draining node never goes back to serving.
    draining: bool,
    /// An exact clean predecessor was replaced and its local inventory is
    /// being scanned or materialized. Readiness remains false until every
    /// discovered cell either publishes or fails closed.
    resuming: bool,
    resuming_cells: BTreeSet<CellId>,
    next_op: OpId,
    next_timer_generation: u64,
    cells: BTreeMap<CellId, Cell>,
    request_cells: BTreeMap<RequestId, CellId>,
    /// Local routes handed to the executor but not yet reported complete.
    /// These are eviction pins: the adapter may still be running user code.
    active_requests: BTreeMap<RequestId, CellId>,
    /// Reverse index of `active_requests`: how many active requests target
    /// each cell. `is_active` reads it in O(1) where scanning the values was
    /// O(active_requests), and `worker_request` calls `is_active` once per
    /// resident cell — the product wedged cold-cell activation, the rate
    /// falling with the cell count (engine/pathological-load.md).
    active_cells: BTreeMap<CellId, usize>,
    /// Local write responses withheld by the output gate until their cell is
    /// proven durable to the written position, keyed by the durability op. An
    /// open gate makes its cell active, so the cell cannot be evicted
    /// underneath it; a fence drains these to a failed response so a write is
    /// never acknowledged after the node loses authority.
    gated_writes: BTreeMap<OpId, GatedWrite>,
    /// Requests whose activity ended while a write of theirs was still on the
    /// output gate. The pin outlives the activity: it keeps the cell from being
    /// evicted under the gate, and keeps a later write of the same request able
    /// to open one. See `activity_finished`.
    gate_pinned: BTreeSet<RequestId>,
    /// Last resident selected for top-level Worker execution. Cell IDs are
    /// ordered, so this is enough to replay a fair round-robin without shell
    /// atomics or registry iteration order leaking into behavior.
    worker_cursor: Option<CellId>,
    activity: ActivitySnapshot,
    activation_waiters: VecDeque<CellId>,
    /// Cells holding a complete cold-route admission. Keeping this explicit,
    /// instead of inferring it from executor tasks, makes the concurrency bound
    /// part of the replayable state machine.
    activation_permits: BTreeSet<CellId>,
    /// Cells with an eviction in flight -- from the durability proof through
    /// the runtime stop. Explicit for the same reason as the activation
    /// permits: the bound belongs in the replayable state machine rather than
    /// being implied by how many executor tasks happen to exist.
    eviction_permits: BTreeSet<CellId>,
    /// Cells waiting for a residency slot, in arrival order. FIFO is the
    /// whole admission policy: waking every waiter on a release and letting
    /// them race is unfair by construction — under sustained eviction a
    /// waiter can time out while thousands of slots are freed around it.
    /// The queue converts "eventually, probably" into a bound: a waiter with
    /// `k` waiters ahead of it is admitted within `k` releases, so no
    /// arrival pattern can starve it.
    capacity_waiters: VecDeque<CellId>,
    /// Requests received as epoch-zero capacity handoffs. Keeping the mode on
    /// the request rather than in the HTTP adapter makes the admit/refuse race
    /// atomic with every other lifecycle transition.
    capacity_requests: BTreeSet<RequestId>,
    /// Reservations made against each unchanged advisory sample. Concurrent
    /// lookups are applied one event at a time, so projected load cannot pick
    /// the same advertised final slot twice by accident.
    capacity_reservations: BTreeMap<NodeId, usize>,
    capacity_samples: BTreeMap<NodeId, u64>,
    /// A refusal disproves exactly one load sample. The node becomes eligible
    /// again only after its lease advertises a newer sample.
    capacity_rejections: BTreeMap<NodeId, u64>,
    /// Live routing authority is shared by every cell owned by the same node.
    /// Keeping that cache here makes expiry and invalidation deterministic,
    /// rather than an invisible executor optimization.
    node_lease_cache: BTreeMap<NodeId, NodeLeaseRecord>,
    /// Start/publish effects invalidated by fencing may still commit. Their
    /// late completion must trigger compensating cleanup, not be ignored.
    retired_runtime_ops: BTreeMap<OpId, (CellId, Epoch)>,
    /// The cell each in-flight cell-scoped operation belongs to. Completion
    /// events name only the op; this index is what makes resolving one a
    /// lookup instead of a walk of every cell.
    cell_ops: BTreeMap<OpId, CellId>,
    /// How many cells currently occupy capacity, maintained at every phase
    /// transition. `has_capacity` asks on every admission and `validate` on
    /// every debug event; counting by walking the map priced both by the
    /// total cell count rather than the answer.
    occupied: usize,
    node_authority: NodeAuthority,
    /// Requests admitted while a lazy node is inactive. They are not entered
    /// into per-cell routing state until the node lease is authoritative, so
    /// a failed lease acquisition cannot create cell ownership.
    node_lease_waiters: BTreeMap<RequestId, CellId>,
    /// Alarm/wake demand has no request to complete, but it obeys the same
    /// acquire-before-ownership rule.
    node_wake_waiters: BTreeSet<CellId>,
    lazy_lease_shadow: LeaseLifecycleShadowBatch,
    next_shadow_sequence: u64,
    /// The most recent wall-clock instant any event carried. Alarms are wall
    /// clock, so judging whether one is imminent needs this rather than the
    /// monotonic reading next to it.
    now_ms: u64,
    /// The most recent monotonic instant any event carried. Not a clock read:
    /// the core never asks what time it is, it remembers what it was told, so
    /// a handler that was not handed a timestamp can still arm a deadline
    /// relative to the event being processed.
    now_mono_ms: u64,
    /// How far down the current sample asked the node to shed.
    ///
    /// The walk down runs at eviction speed rather than sample speed: each
    /// completed eviction starts the next while residency is still above this
    /// floor. Without one it is a cell per sample, so a node fifteen over its
    /// watermark refuses work for fifteen sampling periods. Recomputed every
    /// sample, so a resource trigger comes down by a proportion of what was
    /// last measured rather than aiming at a cell count that means nothing to
    /// it.
    shed_floor: usize,
    /// The measurement the current shed floor was set from, and its value. A
    /// later latched sample compares against this: a completed cut that left
    /// the number flat makes another cut futile.
    ///
    /// The measurement travels with the value because the two are not
    /// comparable. When the latches change, so does the number the walk down
    /// reads, and comparing a fresh resident set size against a stale in-use
    /// figure finds them flat by coincidence.
    shed_cut: Option<(pressure::Metric, u64, usize)>,
    /// Which ceilings are latched. Held apart from `shed_reason`, which names
    /// only the more serious crossing.
    latches: pressure::Latches,
    /// The shedding latch. celld kept this in the executor, which meant the
    /// hysteresis -- the part with actual behaviour -- was the one piece the
    /// simulation could not reach. It is carried here so a sample
    /// sequence is replayable.
    shedding: bool,
    /// Which resource is holding the latch, for the effect the shell logs.
    shed_reason: Option<&'static str>,
}

impl State {
    pub fn new(node: impl Into<NodeId>, config: Config) -> Self {
        assert!(config.max_evictions > 0, "max_evictions must be positive");
        assert!(config.max_releases > 0, "max_releases must be positive");
        assert!(
            config.max_activations > 0,
            "max_activations must be positive"
        );
        Self {
            node: node.into(),
            config,
            fenced: false,
            draining: false,
            resuming: false,
            resuming_cells: BTreeSet::new(),
            next_op: 1,
            next_timer_generation: 1,
            cells: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            active_requests: BTreeMap::new(),
            active_cells: BTreeMap::new(),
            gated_writes: BTreeMap::new(),
            gate_pinned: BTreeSet::new(),
            worker_cursor: None,
            activity: ActivitySnapshot::default(),
            activation_waiters: VecDeque::new(),
            activation_permits: BTreeSet::new(),
            eviction_permits: BTreeSet::new(),
            capacity_waiters: VecDeque::new(),
            capacity_requests: BTreeSet::new(),
            capacity_reservations: BTreeMap::new(),
            capacity_samples: BTreeMap::new(),
            capacity_rejections: BTreeMap::new(),
            node_lease_cache: BTreeMap::new(),
            retired_runtime_ops: BTreeMap::new(),
            cell_ops: BTreeMap::new(),
            occupied: 0,
            node_authority: NodeAuthority::Unstarted,
            node_lease_waiters: BTreeMap::new(),
            node_wake_waiters: BTreeSet::new(),
            lazy_lease_shadow: LeaseLifecycleShadowBatch::default(),
            next_shadow_sequence: 1,
            now_ms: 0,
            now_mono_ms: 0,
            shed_floor: 0,
            shed_cut: None,
            latches: pressure::Latches::default(),
            shedding: false,
            shed_reason: None,
        }
    }

    pub fn node(&self) -> &str {
        &self.node
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// Authority is the lease's published validity evaluated at ask time on
    /// the monotonic clock — not the `Held` variant. A peer treats the record
    /// as dead the instant `now_ms` reaches `expires_ms`, so a holder whose
    /// lease lapsed must answer `false` here even if `Timer::NodeLeaseFence`
    /// has not fired yet: the timer is the liveness backstop that halts the
    /// process, never the safety mechanism. A suspended VM's timer fires
    /// late, but a request evaluated after resume still refuses. The `<` /
    /// `>=` polarity matches the fence timer at `handle_timer`, so the
    /// predicate and the timer cannot disagree.
    pub fn node_authoritative(&self) -> bool {
        if !self.config.require_node_lease {
            return !self.fenced;
        }
        let fresh = |held: &HeldNodeLease| {
            self.now_mono_ms.saturating_sub(held.last_ok_mono_ms) < held.spec.ttl_ms
        };
        match &self.node_authority {
            NodeAuthority::Held(held) => fresh(held),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.as_ref().is_some_and(fresh)
            }
            _ => false,
        }
    }

    /// Whether the process can accept new traffic. An idle lazy node is ready
    /// even though it intentionally holds no authority: a DO request first
    /// acquires the lease, while stateless Worker traffic needs none.
    pub fn ready_to_serve(&self) -> bool {
        if self.fenced || self.resuming {
            return false;
        }
        self.node_authoritative()
            || matches!(
                &self.node_authority,
                NodeAuthority::Lapsed(NodeLeaseSpec {
                    mode: NodeLeaseMode::Lazy,
                    ..
                }) | NodeAuthority::Reading {
                    pending: PendingNodeLease {
                        spec: NodeLeaseSpec {
                            mode: NodeLeaseMode::Lazy,
                            ..
                        },
                        prior: None,
                        ..
                    },
                    ..
                } | NodeAuthority::Writing {
                    pending: PendingNodeLease {
                        spec: NodeLeaseSpec {
                            mode: NodeLeaseMode::Lazy,
                            ..
                        },
                        prior: None,
                        ..
                    },
                    ..
                }
            )
    }

    pub fn phase(&self, cell: &str) -> Option<&Phase> {
        self.cells.get(cell).map(|cell| &cell.phase)
    }

    /// Which resource is currently holding the shedding latch, if any.
    ///
    /// A node that is refusing work should be able to say why; without this
    /// the operator sees only that admissions stopped.
    pub fn shed_reason(&self) -> Option<&'static str> {
        self.shed_reason
    }

    /// Whether this node is currently shedding, for peers ranking it as a
    /// placement target. A node that says no while walking down invites the
    /// work it is trying to get rid of.
    pub fn shedding(&self) -> bool {
        self.shedding
    }

    /// Host-side sockets open across every cell. Each one pins its cell
    /// against eviction, so a node holding many is a poor landing place
    /// even when its residency looks unremarkable.
    pub fn host_websockets(&self) -> usize {
        self.cells.values().map(|cell| cell.websockets.len()).sum()
    }

    /// Does this cell still hold that socket? False when the core declined
    /// it, so the shell can answer the opener instead of closing underneath.
    pub fn holds_websocket(&self, id: &str, websocket: WebSocketId) -> bool {
        self.cells
            .get(id)
            .is_some_and(|cell| cell.websockets.contains_key(&websocket))
    }

    pub fn occupied(&self) -> usize {
        self.occupied
    }

    /// Are fewer than `ceiling` cells occupying capacity?
    fn occupied_below(&self, ceiling: usize) -> bool {
        self.occupied < ceiling
    }

    pub fn residents(&self) -> Vec<CellId> {
        self.cells
            .iter()
            .filter(|(_, cell)| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            })
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return the exact management projection for the current event boundary.
    /// Activations are not visible until publication; a runtime being held for
    /// durability remains visible because it is still published and routable.
    pub fn presence_snapshot(&self) -> PresenceSnapshot {
        let cells = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Resident { epoch } | Phase::EnsuringDurability { epoch, .. } => {
                    Some(PresenceCell {
                        id: id.clone(),
                        epoch,
                    })
                }
                _ => None,
            })
            .collect();
        PresenceSnapshot {
            serving: self.ready_to_serve(),
            cells,
            activity: self.activity,
            lazy_lease_shadow: self.lazy_lease_shadow.clone(),
        }
    }

    pub fn waiting(&self) -> Vec<CellId> {
        self.capacity_waiters.iter().cloned().collect()
    }

    pub fn activation_waiting(&self) -> Vec<CellId> {
        self.activation_waiters.iter().cloned().collect()
    }

    pub fn activating(&self) -> usize {
        self.activation_permits.len()
    }

    /// Evictions in flight, from the durability proof through the runtime
    /// stop. During a drain this saturates at `max_releases`.
    pub fn evicting(&self) -> usize {
        self.eviction_permits.len()
    }

    /// How many cells sit in each phase, by a stable short name, omitting the
    /// phases with no cells.
    ///
    /// A node that refuses cells is diagnosed by where its cells are, and
    /// `occupied` cannot say: it counts residency, so a node holding thousands
    /// of cells part-way through a cold start reports almost none. A fleet held
    /// that state for fifteen minutes and the record could not say what the
    /// cells were doing (issue #50).
    ///
    /// The names are part of the operator interface. A chart and a human read
    /// them, so they do not change with an internal rename.
    pub fn phase_census(&self) -> Vec<(&'static str, usize)> {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for cell in self.cells.values() {
            *counts.entry(phase_name(&cell.phase)).or_default() += 1;
        }
        counts.into_iter().collect()
    }

    /// Cold routes that have not finished: cells that hold an activation
    /// permit plus cells queued behind the activation ceiling. A capacity
    /// waiter already holds a permit, so this counts every cell once. A
    /// rollout waits for zero before it removes more warm capacity.
    pub fn activation_backlog(&self) -> usize {
        self.activation_permits.len() + self.activation_waiters.len()
    }

    /// Ownership releases still in flight. The shutdown drain waits for
    /// zero: a released cell stops occupying capacity before its record
    /// write commits, so an exit gated on occupancy alone can outrun its
    /// own releases and leave records a successor must wait out the node
    /// lease for -- the takeover-at-once promise broken on every real
    /// store, where the write loses a race against a 50ms drain poll that
    /// a local fixture always wins.
    pub fn releasing(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| cell.releasing.is_some())
            .count()
    }

    /// Cheap internal consistency gate run by both executors after every event.
    pub fn validate(&self) -> Result<(), String> {
        if !self.occupied_below(self.config.max_resident.saturating_add(1)) {
            return Err(format!(
                "occupied {} exceeds ceiling {}",
                self.occupied(),
                self.config.max_resident
            ));
        }
        let eviction_ceiling = if self.draining {
            self.config.max_releases.max(self.config.max_evictions)
        } else {
            self.config.max_evictions
        };
        if self.eviction_permits.len() > eviction_ceiling {
            return Err(format!(
                "evicting {} exceeds ceiling {}",
                self.eviction_permits.len(),
                eviction_ceiling
            ));
        }
        if self.activation_permits.len() > self.config.max_activations {
            return Err(format!(
                "activating {} exceeds ceiling {}",
                self.activation_permits.len(),
                self.config.max_activations
            ));
        }

        let mut activation_queued = BTreeSet::new();
        for id in &self.activation_waiters {
            if !activation_queued.insert(id) {
                return Err(format!("activation waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingActivation || cell.waiting_activation.is_none() {
                return Err(format!("activation waiter {id:?} is not waiting"));
            }
            if self.activation_permits.contains(id) {
                return Err(format!("activation waiter {id:?} also holds a permit"));
            }
        }
        for id in &self.activation_permits {
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("activation permit {id:?} has no cell state"));
            };
            if !phase_holds_activation(&cell.phase) {
                return Err(format!(
                    "activation permit {id:?} is held in terminal phase {:?}",
                    cell.phase
                ));
            }
        }

        let mut queued = BTreeSet::new();
        for id in &self.capacity_waiters {
            if !queued.insert(id) {
                return Err(format!("capacity waiter {id:?} is queued twice"));
            }
            let Some(cell) = self.cells.get(id) else {
                return Err(format!("capacity waiter {id:?} has no cell state"));
            };
            if cell.phase != Phase::WaitingCapacity || cell.waiting_for.is_none() {
                return Err(format!("capacity waiter {id:?} is not waiting"));
            }
        }
        for (id, cell) in &self.cells {
            let is_activation_queued = activation_queued.contains(id);
            if (cell.phase == Phase::WaitingActivation) != is_activation_queued {
                return Err(format!("cell {id:?} activation queue and phase disagree"));
            }
            let is_queued = queued.contains(id);
            if (cell.phase == Phase::WaitingCapacity) != is_queued {
                return Err(format!("cell {id:?} queue and phase disagree"));
            }
            for request in &cell.requests {
                if self.request_cells.get(request) != Some(id) {
                    return Err(format!(
                        "request {request} index disagrees with cell {id:?}"
                    ));
                }
            }
            // Every in-flight cell op must be resolvable through the index,
            // or its completion event will never find this cell.
            let indexed = |op: OpId| self.cell_ops.get(&op) == Some(id);
            if let Some(op) = phase_op(&cell.phase) {
                if !indexed(op) {
                    return Err(format!("cell {id:?} phase op {op} is not indexed"));
                }
            }
            if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                if !indexed(op) {
                    return Err(format!("cell {id:?} firing alarm op {op} is not indexed"));
                }
            }
            if let Some(op) = cell.releasing {
                if !indexed(op) {
                    return Err(format!("cell {id:?} release op {op} is not indexed"));
                }
            }
        }
        let walked = self
            .cells
            .values()
            .filter(|cell| phase_occupies_capacity(&cell.phase))
            .count();
        if walked != self.occupied {
            return Err(format!(
                "occupied counter {} disagrees with the walk {walked}",
                self.occupied
            ));
        }
        for (request, id) in &self.request_cells {
            if !self
                .cells
                .get(id)
                .is_some_and(|cell| cell.requests.contains(request))
            {
                return Err(format!("request index {request} has no matching waiter"));
            }
        }
        for (request, id) in &self.node_lease_waiters {
            if self.request_cells.contains_key(request)
                || self.active_requests.contains_key(request)
            {
                return Err(format!(
                    "request {request} waits for node authority and is already routed"
                ));
            }
            if id.is_empty() {
                return Err(format!("request {request} has an empty node-lease target"));
            }
        }
        // The reverse index must agree with the map it summarizes, exactly.
        let mut recount: BTreeMap<CellId, usize> = BTreeMap::new();
        for id in self.active_requests.values() {
            *recount.entry(id.clone()).or_insert(0) += 1;
        }
        if recount != self.active_cells {
            return Err(format!(
                "active_cells index {:?} disagrees with active_requests {:?}",
                self.active_cells, recount
            ));
        }
        for (request, id) in &self.active_requests {
            if self.request_cells.contains_key(request) {
                return Err(format!("request {request} is both pending and active"));
            }
            if !self.cells.get(id).is_some_and(|cell| {
                matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            }) {
                return Err(format!(
                    "active request {request} has no resident cell {id:?}"
                ));
            }
        }
        for (op, gate) in &self.gated_writes {
            // A held write keeps its request pinned, so the cell cannot be
            // evicted underneath the gate and a later write of the same
            // request can still find it.
            if self.active_requests.get(&gate.request) != Some(&gate.cell) {
                return Err(format!(
                    "gated write op {op} for request {} is not pinned on {:?}",
                    gate.request, gate.cell
                ));
            }
        }
        for (id, cell) in &self.cells {
            if matches!(cell.alarm, Some(AlarmState::Firing { .. }))
                && !matches!(cell.phase, Phase::Resident { .. })
            {
                return Err(format!("cell {id:?} is firing an alarm while not resident"));
            }
            if cell
                .websockets
                .values()
                .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
                && !matches!(
                    cell.phase,
                    Phase::Resident { .. } | Phase::EnsuringDurability { .. }
                )
            {
                return Err(format!(
                    "cell {id:?} has a live transport while not resident"
                ));
            }
        }
        Ok(())
    }

    fn op(&mut self) -> OpId {
        let op = self.next_op;
        self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
        op
    }

    /// Allocate an op that will live in `cell`'s phase, alarm, or release
    /// slot, and index it.
    fn cell_op(&mut self, cell: &str) -> OpId {
        let op = self.op();
        self.cell_ops.insert(op, cell.to_string());
        op
    }

    /// Resolve a completion's op to its cell, consuming the index entry —
    /// but only when `predicate` confirms the cell still holds the op. On a
    /// mismatch the entry stays: the op's real completion has not arrived
    /// yet (an expiry can probe the wrong handler), and it must still find
    /// its way here later.
    fn take_cell_op(&mut self, op: OpId, predicate: impl Fn(&Cell) -> bool) -> Option<CellId> {
        let id = self.cell_ops.get(&op)?.clone();
        if !self.cells.get(&id).is_some_and(predicate) {
            return None;
        }
        self.cell_ops.remove(&op);
        Some(id)
    }

    fn has_capacity(&self) -> bool {
        // Residency is a hard cap, known exactly and counted -- never sampled.
        // A node at its cell cap is at capacity, not overloaded: it refuses
        // more and holds what it has, rather than shedding a live cell it must
        // then place again elsewhere. The only sampled fact that refuses
        // admission is genuine memory pressure, which a cell count
        // cannot see.
        self.occupied_below(self.config.max_resident) && !self.shedding
    }

    pub fn is_active(&self, id: &str) -> bool {
        self.active_cells.contains_key(id)
            || self.cells.get(id).is_some_and(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| matches!(kind, WebSocketKind::Regular | WebSocketKind::Outbound))
            })
    }

    /// Record `request` as active on `cell`, keeping the `active_cells`
    /// reverse index in step with `active_requests`. Every insert into
    /// `active_requests` goes through here so the two never disagree.
    fn activate_request(&mut self, request: RequestId, cell: CellId) {
        *self.active_cells.entry(cell.clone()).or_insert(0) += 1;
        self.active_requests.insert(request, cell);
    }

    /// Drop `request` from the active set, decrementing its cell's index and
    /// forgetting the cell once no request targets it. Returns the cell the
    /// request held, as `active_requests.remove` did.
    fn deactivate_request(&mut self, request: RequestId) -> Option<CellId> {
        let cell = self.active_requests.remove(&request)?;
        if let Some(count) = self.active_cells.get_mut(&cell) {
            *count -= 1;
            if *count == 0 {
                self.active_cells.remove(&cell);
            }
        }
        Some(cell)
    }

    /// Hibernated (Durable Objects state 4): out of memory, still on this
    /// host, with WebSocket clients parked at the network layer. Nothing in
    /// memory survives -- the next event runs the constructor again, exactly
    /// as a cold start does -- so the surviving sockets are the whole of the
    /// difference from `Inactive`.
    pub fn is_hibernated(&self, id: &str) -> bool {
        self.cells.get(id).is_some_and(|cell| {
            matches!(cell.phase, Phase::Dormant { .. })
                && cell
                    .websockets
                    .values()
                    .any(|kind| matches!(kind, WebSocketKind::Hibernatable))
        })
    }

    /// Hibernatable (Durable Objects state 2): resident, doing no work, and
    /// holding nothing that must survive in memory. State 3 is the negation --
    /// resident and idle, but pinned by a `Regular` or `Outbound` socket that
    /// cannot outlive the isolate.
    ///
    /// This is the cell's own criteria, not a decision to evict. celld also
    /// refuses eviction for an imminent or uncovered alarm and for state the
    /// bucket cannot restore; those are policy and are checked separately.
    pub fn is_hibernatable(&self, id: &str) -> bool {
        self.cells
            .get(id)
            .is_some_and(|cell| matches!(cell.phase, Phase::Resident { .. }))
            && !self.is_active(id)
    }

    pub fn websocket_count(&self, id: &str) -> usize {
        self.cells.get(id).map_or(0, |cell| cell.websockets.len())
    }

    /// Does anything local still need this process's node authority?
    ///
    /// The question every caller but the shadow snapshot actually asks, and
    /// asking it directly is what makes it cheap: no set, no cloned ids, and
    /// a stop at the first dependent cell.
    ///
    /// Counting instead put an `O(n log n)` walk of every resident cell on
    /// the path taken after *every* state transition, which is to say on
    /// every request. A profile of `/c/hello` with 800 resident cells spent
    /// more time in `BTreeSet<String>` than in the request: throughput fell
    /// from 57k to 3.5k rps, linearly in the number of resident cells.
    fn node_lease_is_depended_on(&self) -> bool {
        !self.node_lease_waiters.is_empty()
            || !self.node_wake_waiters.is_empty()
            || self
                .cells
                .values()
                .any(|cell| phase_depends_on_node_lease(&cell.phase) || !cell.websockets.is_empty())
    }

    /// How many distinct local cell lifecycles require this process's node
    /// authority. Hibernated cells do not pin a lazy lease by themselves, but
    /// a hibernated cell does, because its host-held WebSocket outlives the
    /// isolate.
    ///
    /// The exact figure, for the shadow snapshot that reports it. Everything
    /// else wants [`node_lease_is_depended_on`](Self::node_lease_is_depended_on),
    /// which answers the same question without building the set.
    fn node_lease_dependency_count(&self) -> usize {
        let mut cells = BTreeSet::new();
        cells.extend(self.node_lease_waiters.values().cloned());
        cells.extend(self.node_wake_waiters.iter().cloned());
        for (id, cell) in &self.cells {
            if phase_depends_on_node_lease(&cell.phase) || !cell.websockets.is_empty() {
                cells.insert(id.clone());
            }
        }
        cells.len()
    }

    /// Update idle/liveness bookkeeping after every state transition. This is
    /// deliberately derived from core state rather than an executor-owned
    /// active-cell counter, which used to let the model and production drift.
    fn update_node_lease_dependencies(&mut self, now_mono_ms: u64) {
        let depended_on = self.node_lease_is_depended_on();
        let held = match &mut self.node_authority {
            NodeAuthority::Held(held) => Some(held),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.as_mut()
            }
            _ => None,
        };
        let Some(held) = held else {
            return;
        };
        if !depended_on {
            held.idle_since_mono_ms.get_or_insert(now_mono_ms);
        } else {
            held.idle_since_mono_ms = None;
            held.shadow_release_reported = false;
        }
    }

    fn record_shadow_release(
        &mut self,
        held: &HeldNodeLease,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        const MAX_SHADOW_DECISIONS: usize = 8;
        let sequence = self.next_shadow_sequence;
        self.next_shadow_sequence = self.next_shadow_sequence.saturating_add(1);
        if self.lazy_lease_shadow.decisions.len() == MAX_SHADOW_DECISIONS {
            self.lazy_lease_shadow.decisions.remove(0);
            self.lazy_lease_shadow.dropped = self.lazy_lease_shadow.dropped.saturating_add(1);
        }
        let idle_ms = held
            .idle_since_mono_ms
            .map_or(0, |inactive| now_mono_ms.saturating_sub(inactive));
        self.lazy_lease_shadow
            .decisions
            .push(LeaseLifecycleShadowDecision {
                sequence,
                observed_at_ms: now_ms,
                snapshot: LeaseLifecycleShadowSnapshot {
                    mode: NodeLeaseMode::Shadow,
                    active_cells: self.node_lease_dependency_count(),
                    serving_cells: self.residents().len(),
                    idle_ms,
                    linger_ms: held.spec.linger_ms,
                    lease_active: true,
                    elapsed_since_ok_ms: now_mono_ms.saturating_sub(held.last_ok_mono_ms),
                    elapsed_since_renew_ms: now_mono_ms.saturating_sub(held.last_attempt_mono_ms),
                    ttl_ms: held.spec.ttl_ms,
                    shadow_release_reported: false,
                },
                expected: LeaseLifecycleShadowExpected {
                    shadow_release: true,
                    authority_action: NodeLeaseAuthorityAction::Renew,
                },
            });
        effects.push(Effect::ObserveNodeLeaseShadowRelease { sequence });
    }

    fn complete_request(
        &mut self,
        id: &str,
        request: RequestId,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        self.capacity_requests.remove(&request);
        match &result {
            Ok(Route::Local) => {
                self.activate_request(request, id.to_string());
                let now = self.now_mono_ms;
                if let Some(cell) = self.cells.get_mut(id) {
                    cell.last_used_mono_ms = now;
                }
            }
            Ok(Route::Remote { .. }) => {
                self.activity.proxied = self.activity.proxied.saturating_add(1);
            }
            Err(_) => {}
        }
        effects.push(Effect::Complete { request, result });
    }

    fn record_acquisition(&mut self, spec: &RestoreSpec) {
        self.activity.acquired = self.activity.acquired.saturating_add(1);
        if spec.took_over {
            self.activity.expired_owner_leases =
                self.activity.expired_owner_leases.saturating_add(1);
        }
        if spec.epoch > 1 {
            self.activity.advanced_epochs = self.activity.advanced_epochs.saturating_add(1);
        }
    }

    fn worker_request(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        let route = self
            .cells
            .iter()
            .filter_map(|(id, cell)| {
                let (epoch, retired_durability) = match cell.phase {
                    Phase::Resident { epoch } => (epoch, None),
                    Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                    _ => return None,
                };
                if self.is_active(id) || matches!(cell.alarm, Some(AlarmState::Firing { .. })) {
                    return None;
                }
                Some((id.clone(), epoch, retired_durability))
            })
            .find(|(id, _, _)| self.worker_cursor.as_ref().is_none_or(|cursor| id > cursor))
            .or_else(|| {
                self.cells.iter().find_map(|(id, cell)| {
                    let (epoch, retired_durability) = match cell.phase {
                        Phase::Resident { epoch } => (epoch, None),
                        Phase::EnsuringDurability { op, epoch } => (epoch, Some(op)),
                        _ => return None,
                    };
                    (!self.is_active(id) && !matches!(cell.alarm, Some(AlarmState::Firing { .. })))
                        .then(|| (id.clone(), epoch, retired_durability))
                })
            })
            .map(|(cell, epoch, retired_durability)| WorkerRoute {
                cell,
                epoch,
                retired_durability,
            });

        if let Some(route) = &route {
            self.worker_cursor = Some(route.cell.clone());
            self.activate_request(request, route.cell.clone());
            if let Some(cell) = self.cells.get_mut(&route.cell) {
                if matches!(cell.phase, Phase::EnsuringDurability { .. }) {
                    // Same rescue as `request_authorized`: the permit taken
                    // at nomination comes back with the cell.
                    if let Some(stale) = phase_op(&cell.phase) {
                        self.cell_ops.remove(&stale);
                    }
                    set_phase(
                        &mut self.occupied,
                        cell,
                        Phase::Resident { epoch: route.epoch },
                    );
                    self.eviction_permits.remove(&route.cell);
                }
                cell.last_used_mono_ms = self.now_mono_ms;
            }
        }
        effects.push(Effect::CompleteWorker { request, route });
    }

    fn finish_requests(
        &mut self,
        id: &str,
        cell: &mut Cell,
        result: Result<Route, RequestError>,
        effects: &mut Vec<Effect>,
    ) {
        for request in std::mem::take(&mut cell.requests) {
            self.request_cells.remove(&request);
            self.complete_request(id, request, result.clone(), effects);
        }
    }

    fn begin_cold_route(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        debug_assert!(self.activation_permits.contains(id));
        cell.waiting_activation = None;
        match start {
            ColdStart::ReadOwner => {
                let op = self.cell_op(id);
                set_phase(&mut self.occupied, cell, Phase::ReadingOwner { op });
                effects.push(Effect::ReadOwner {
                    op,
                    cell: id.to_string(),
                });
            }
            ColdStart::Restore(spec) => {
                self.activate_or_wait(id, cell, Activation::Restore(spec), effects)
            }
        }
    }

    fn admit_or_queue_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        start: ColdStart,
        effects: &mut Vec<Effect>,
    ) {
        if self.activation_permits.contains(id) {
            self.begin_cold_route(id, cell, start, effects);
        } else if self.activation_permits.len() < self.config.max_activations {
            self.activation_permits.insert(id.to_string());
            self.begin_cold_route(id, cell, start, effects);
        } else {
            set_phase(&mut self.occupied, cell, Phase::WaitingActivation);
            cell.waiting_activation = Some(start);
            self.activation_waiters.push_back(id.to_string());
        }
    }

    fn pump_activations(&mut self, effects: &mut Vec<Effect>) {
        self.activation_permits.retain(|id| {
            self.cells
                .get(id)
                .is_some_and(|cell| phase_holds_activation(&cell.phase))
        });

        while self.activation_permits.len() < self.config.max_activations {
            let Some(id) = self.activation_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingActivation {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(start) = cell.waiting_activation.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand {
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    match start {
                        ColdStart::ReadOwner => Phase::Inactive,
                        ColdStart::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                    },
                );
            } else {
                self.activation_permits.insert(id.clone());
                self.begin_cold_route(&id, &mut cell, start, effects);
            }
            self.cells.insert(id, cell);
        }
    }

    /// Adopt the exact epochs encoded by a clean predecessor's local paths.
    /// The node lease CAS that caused this event is the ownership proof, so
    /// this path deliberately emits no per-cell ownership effects.
    fn local_cells_read(
        &mut self,
        result: Result<Vec<LocalCell>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        if !self.resuming {
            return;
        }
        let Ok(mut cells) = result else {
            self.resuming = false;
            return;
        };
        cells.sort();
        cells.dedup();
        if cells.len() > self.config.max_resident || !self.cells.is_empty() {
            self.resuming = false;
            return;
        }
        for local in cells {
            let mut cell = Cell {
                resume_demand: true,
                ..Cell::default()
            };
            self.resuming_cells.insert(local.id.clone());
            self.admit_or_queue_activation(
                &local.id,
                &mut cell,
                ColdStart::Restore(RestoreSpec {
                    epoch: local.epoch,
                    fresh: false,
                    took_over: false,
                    resume_local: true,
                }),
                effects,
            );
            self.cells.insert(local.id, cell);
        }
        if self.resuming_cells.is_empty() {
            self.resuming = false;
        }
    }

    fn settle_local_resume(&mut self, id: &str) {
        if self.resuming_cells.remove(id) && self.resuming_cells.is_empty() {
            self.resuming = false;
        }
        if let Some(cell) = self.cells.get_mut(id) {
            cell.resume_demand = false;
        }
    }

    fn begin_activation(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        debug_assert!(self.has_capacity());
        cell.waiting_for = None;
        let op = self.cell_op(id);
        match activation {
            Activation::Claim(claim) => {
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Acquiring {
                        op,
                        claim: claim.clone(),
                    },
                );
                effects.push(Effect::CasOwner {
                    op,
                    cell: id.to_string(),
                    guard: claim.guard,
                    epoch: claim.epoch,
                    takeover: claim.takeover,
                });
            }
            Activation::Restore(spec) => {
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Restoring {
                        op,
                        spec: spec.clone(),
                    },
                );
                effects.push(Effect::Restore {
                    op,
                    cell: id.to_string(),
                    spec,
                });
            }
        }
    }

    fn activate_or_wait(
        &mut self,
        id: &str,
        cell: &mut Cell,
        activation: Activation,
        effects: &mut Vec<Effect>,
    ) {
        if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand {
            set_phase(
                &mut self.occupied,
                cell,
                match activation {
                    Activation::Claim(_) => Phase::Inactive,
                    Activation::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                },
            );
        } else if self.has_capacity() {
            self.begin_activation(id, cell, activation, effects);
        } else {
            set_phase(&mut self.occupied, cell, Phase::WaitingCapacity);
            cell.waiting_for = Some(activation);
            self.capacity_waiters.push_back(id.to_string());
            self.shed_one(effects);
        }
    }

    fn pump_capacity(&mut self, effects: &mut Vec<Effect>) {
        while self.has_capacity() {
            let Some(id) = self.capacity_waiters.pop_front() else {
                break;
            };
            let Some(mut cell) = self.cells.remove(&id) else {
                continue;
            };
            if cell.phase != Phase::WaitingCapacity {
                self.cells.insert(id, cell);
                continue;
            }
            let Some(activation) = cell.waiting_for.take() else {
                self.cells.insert(id, cell);
                continue;
            };
            if cell.requests.is_empty() && !cell.alarm_wake && !cell.resume_demand {
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    match activation {
                        Activation::Claim(_) => Phase::Inactive,
                        Activation::Restore(spec) => Phase::Dormant { epoch: spec.epoch },
                    },
                );
            } else {
                self.begin_activation(&id, &mut cell, activation, effects);
            }
            self.cells.insert(id, cell);
        }
        self.shed_one(effects);
    }

    fn start_node_lease(&mut self, now_ms: u64, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        if !matches!(
            self.node_authority,
            NodeAuthority::Unstarted | NodeAuthority::Failed | NodeAuthority::Lapsed(_)
        ) {
            return;
        }
        if spec.mode == NodeLeaseMode::Lazy {
            self.node_authority = NodeAuthority::Lapsed(spec);
            return;
        }
        self.begin_node_lease_acquisition(now_ms, spec, effects);
    }

    fn begin_node_lease_acquisition(
        &mut self,
        now_ms: u64,
        spec: NodeLeaseSpec,
        effects: &mut Vec<Effect>,
    ) {
        let desired = NodeLeaseRecord {
            node: self.node.clone(),
            addr: spec.addr.clone(),
            expires_ms: now_ms.saturating_add(spec.ttl_ms),
            peer_protocol: spec.peer_protocol,
            generation: spec.generation.clone(),
            etag: String::new(),
        };
        let pending = PendingNodeLease {
            spec,
            desired,
            prior: None,
            readback_only: false,
            anchor_mono_ms: self.now_mono_ms,
            resume_local: false,
        };
        let op = self.op();
        self.node_authority = NodeAuthority::Reading { op, pending };
        effects.push(Effect::ReadSelfNodeLease { op });
    }

    fn fail_initial_node_lease(&mut self, spec: NodeLeaseSpec, effects: &mut Vec<Effect>) {
        self.node_authority = if spec.mode == NodeLeaseMode::Lazy {
            // The next request re-acquires from here.
            NodeAuthority::Lapsed(spec)
        } else {
            // Nothing else will ever ask: `StartNodeLease` is a one-shot at
            // process start. Arm the retry here or this node is done serving.
            // The renewal cadence is the right interval -- it is already the
            // rate this fleet has decided its bucket can carry.
            let generation = self.next_timer_generation;
            self.next_timer_generation = self
                .next_timer_generation
                .checked_add(1)
                .expect("timer generation exhausted");
            let retry_after = (spec.ttl_ms / 3).max(1);
            effects.push(Effect::ScheduleTimer {
                timer: Timer::NodeLeaseRenew { generation },
                at_mono_ms: self.now_mono_ms.saturating_add(retry_after),
            });
            NodeAuthority::Retrying { spec, generation }
        };
        for (request, _) in std::mem::take(&mut self.node_lease_waiters) {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeUnavailable),
            });
        }
        self.node_wake_waiters.clear();
    }

    /// A lease write landed. Decide whether it actually preserved authority,
    /// and if so install it anchored to the instant its `expires_ms` was
    /// sampled.
    ///
    /// A renewal that lands at or after the PRIOR lease's expiry has not
    /// extended anything: every peer was already entitled to read that record,
    /// find it dead, and seize this node's cells, and one of them may have
    /// begun doing exactly that. Rewriting `nodes/<node>.json` does not
    /// retract a takeover already in flight -- the ownership compare-and-swap
    /// a peer is about to issue is guarded on the ownership record's ETag,
    /// which a resident owner never touches -- so resurrecting the lease here
    /// is precisely how two nodes end up serving one cell. The gap is a fence,
    /// not a hiccup.
    fn complete_node_lease_write(
        &mut self,
        pending: PendingNodeLease,
        record: NodeLeaseRecord,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        // The record just published expires at `anchor + ttl`. A round trip
        // that outlived that window landed a lease which is already dead to
        // every peer reading the bucket, so this process must not act on it --
        // and acting on it is exactly what installing it does, because
        // `hold_node_lease` drains the waiting requests inline, before the
        // past-due fence timer it arms can possibly be delivered.
        let expired = now_mono_ms.saturating_sub(pending.anchor_mono_ms) >= pending.spec.ttl_ms;
        match &pending.prior {
            // A renewal only preserves authority if it lands while the PRIOR
            // lease is still valid. Losing that continuity means peers were
            // already entitled to seize this node's cells.
            Some(prior) => {
                if expired || now_mono_ms.saturating_sub(prior.last_ok_mono_ms) >= prior.spec.ttl_ms
                {
                    self.fence_node(effects);
                    return;
                }
            }
            // An initial acquisition that lands expired never became
            // authoritative: this node owns nothing and is serving nothing, so
            // there is no runtime to fence and no reason to halt. Fail the
            // waiters and let a later request retry from a fresh sample.
            None => {
                if expired {
                    self.fail_initial_node_lease(pending.spec, effects);
                    return;
                }
            }
        }
        let resume_local = pending.resume_local;
        self.hold_node_lease(
            pending.spec,
            record,
            pending.prior,
            pending.anchor_mono_ms,
            resume_local,
            effects,
        );
    }

    /// Install a held lease. `anchor_mono_ms` is the monotonic instant the
    /// record's `expires_ms` was computed from -- NOT the instant the write
    /// completed. See [`PendingNodeLease::anchor_mono_ms`].
    fn hold_node_lease(
        &mut self,
        spec: NodeLeaseSpec,
        record: NodeLeaseRecord,
        prior: Option<HeldNodeLease>,
        anchor_mono_ms: u64,
        resume_local: bool,
        effects: &mut Vec<Effect>,
    ) {
        let generation = self.next_timer_generation;
        self.next_timer_generation = self
            .next_timer_generation
            .checked_add(1)
            .expect("timer generation exhausted");
        let renew_after = (spec.ttl_ms / 3).max(1);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew { generation },
            at_mono_ms: anchor_mono_ms.saturating_add(renew_after),
        });
        // Exactly the published deadline. A peer treats the record as live
        // while `expires_ms > now_ms`, so this node must have stopped serving
        // by the time `now_ms` reaches `expires_ms` -- not one millisecond
        // after it.
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseFence { generation },
            at_mono_ms: anchor_mono_ms.saturating_add(spec.ttl_ms),
        });
        let idle_since_mono_ms = prior
            .as_ref()
            .and_then(|held| held.idle_since_mono_ms)
            .or_else(|| (!self.node_lease_is_depended_on()).then_some(anchor_mono_ms));
        let shadow_release_reported = prior
            .as_ref()
            .is_some_and(|held| held.shadow_release_reported);
        self.node_authority = NodeAuthority::Held(HeldNodeLease {
            spec,
            record,
            last_ok_mono_ms: anchor_mono_ms,
            last_attempt_mono_ms: anchor_mono_ms,
            timer_generation: generation,
            idle_since_mono_ms,
            shadow_release_reported,
        });
        if resume_local {
            self.resuming = true;
            effects.push(Effect::ReadLocalCells);
        }
        self.drain_node_lease_waiters(effects);
    }

    fn resume_node_lease_after_failure(
        &mut self,
        prior: HeldNodeLease,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let retry_after = (prior.spec.ttl_ms / 3).max(1);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::NodeLeaseRenew {
                generation: prior.timer_generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(retry_after),
        });
        self.node_authority = NodeAuthority::Held(prior);
    }

    fn release_or_fence_node_lease(&mut self, held: HeldNodeLease, effects: &mut Vec<Effect>) {
        if held.spec.mode == NodeLeaseMode::Lazy && !self.node_lease_is_depended_on() {
            self.node_authority = NodeAuthority::Lapsed(held.spec);
            effects.push(Effect::ObserveNodeLeaseReleased);
        } else {
            self.fence_node(effects);
        }
    }

    fn begin_node_lease_write(
        &mut self,
        pending: PendingNodeLease,
        guard: CasGuard,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.op();
        let record = pending.desired.clone();
        let authority_expires_ms = pending.prior.as_ref().map(|prior| prior.record.expires_ms);
        self.node_authority = NodeAuthority::Writing { op, pending };
        effects.push(Effect::CasNodeLease {
            op,
            guard,
            record,
            authority_expires_ms,
        });
    }

    fn read_self_node_lease(
        &mut self,
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Reading {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Reading {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Err(_) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
            Ok(Some(record)) if same_node_lease(&record, &pending.desired) => {
                self.complete_node_lease_write(pending, record, now_mono_ms, effects);
            }
            Ok(Some(record))
                if pending
                    .prior
                    .as_ref()
                    .is_some_and(|prior| same_node_lease(&record, &prior.record)) =>
            {
                let mut prior = pending.prior.expect("checked above");
                prior.record.etag = record.etag;
                self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
            }
            Ok(record) if pending.prior.is_some() => {
                // A renewal was ambiguous and read-back no longer names our
                // exact generation, or the record vanished. Authority is lost.
                let _ = record;
                self.release_or_fence_node_lease(pending.prior.expect("checked above"), effects);
            }
            Ok(_) if pending.readback_only => {
                // The ambiguous initial write did not publish the exact
                // desired generation. Fail this activation; a later request
                // may retry from a fresh read, but this one cannot spin.
                self.fail_initial_node_lease(pending.spec, effects);
            }
            Ok(Some(record)) => {
                // A configured node id is a singleton key: restarting that
                // node replaces its prior process generation immediately. The
                // ETag still serializes competing replacements, and a process
                // which loses that CAS never becomes authoritative.
                let mut pending = pending;
                pending.resume_local = pending.prior.is_none()
                    && !pending.readback_only
                    && record.expires_ms > now_ms
                    && pending.spec.resume_generation.as_deref()
                        == Some(record.generation.as_str());
                self.begin_node_lease_write(pending, CasGuard::Match(record.etag), effects);
            }
            Ok(None) => self.begin_node_lease_write(pending, CasGuard::Absent, effects),
        }
    }

    fn node_lease_cas_completed(
        &mut self,
        op: OpId,
        now_mono_ms: u64,
        result: Result<LeaseCasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let authority = std::mem::replace(&mut self.node_authority, NodeAuthority::Failed);
        let NodeAuthority::Writing {
            op: current,
            pending,
        } = authority
        else {
            self.node_authority = authority;
            return;
        };
        if current != op {
            self.node_authority = NodeAuthority::Writing {
                op: current,
                pending,
            };
            return;
        }
        match result {
            Ok(LeaseCasOutcome::Applied { etag }) => {
                let mut record = pending.desired.clone();
                record.etag = etag;
                self.complete_node_lease_write(pending, record, now_mono_ms, effects);
            }
            Ok(LeaseCasOutcome::Rejected) if pending.prior.is_some() => {
                self.release_or_fence_node_lease(pending.prior.expect("checked above"), effects);
            }
            Ok(LeaseCasOutcome::Rejected) => {
                let read = self.op();
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Ambiguous) => {
                let read = self.op();
                let mut pending = pending;
                pending.readback_only = true;
                self.node_authority = NodeAuthority::Reading { op: read, pending };
                effects.push(Effect::ReadSelfNodeLease { op: read });
            }
            Err(Failure::Definite) => {
                if let Some(prior) = pending.prior {
                    self.resume_node_lease_after_failure(prior, now_mono_ms, effects);
                } else {
                    self.fail_initial_node_lease(pending.spec, effects);
                }
            }
        }
    }

    /// Arm a deadline for every activation effect this event produced.
    ///
    /// Done once, centrally, rather than at each of the eleven sites that emit
    /// one: those are eleven chances to forget, and a forgotten deadline is
    /// invisible until something hangs. The effect list already names every
    /// operation the executor is about to start, so it is the natural place to
    /// decide which of them are worth watching.
    fn arm_operation_deadlines(&mut self, effects: &mut Vec<Effect>) {
        let Some(deadline_ms) = self.config.operation_deadline_ms else {
            return;
        };
        let watched: Vec<OpId> = effects
            .iter()
            .filter_map(|effect| match effect {
                // Every operation that holds something back. The activation
                // stages hold a caller; a durability proof holds an eviction;
                // a firing alarm holds the cell out of dormancy. All three
                // keep the ownership record claimed while they wait.
                //
                // `StopRuntime` is the deliberate exception. It has no failure
                // handling to reuse -- a stop cannot fail, it can only not
                // finish -- so abandoning one would mean declaring a runtime
                // gone while it may still be running. That needs a decision,
                // not a timer.
                // A restore is a sequence of individually bounded object-store
                // requests. Its total duration scales with the replica. The
                // shell cannot cancel the task, so retiring the core op would
                // discard late success and let another request duplicate it.
                Effect::Restore { .. } => None,
                Effect::ReadOwner { op, .. }
                | Effect::ReadNodeLease { op, .. }
                | Effect::ReadCapacityPeers { op, .. }
                | Effect::CasOwner { op, .. }
                | Effect::StartRuntime { op, .. }
                | Effect::Publish { op, .. }
                | Effect::EnsureDurable { op, .. }
                // A held write response: a swallowed durability proof must not
                // hang a client forever.
                | Effect::AwaitDurable { op, .. }
                | Effect::FireAlarm { op, .. } => Some(*op),
                _ => None,
            })
            .collect();
        let at_mono_ms = self.now_mono_ms.saturating_add(deadline_ms);
        for op in watched {
            effects.push(Effect::ScheduleTimer {
                timer: Timer::OperationDeadline { op },
                at_mono_ms,
            });
        }
    }

    /// An activation effect outlived its deadline.
    ///
    /// Expiry deliberately reuses each stage's own failure handling rather
    /// than introducing a second way to abandon work: the core already knows
    /// how to reconcile an ambiguous acquire and how to fail a read, and a
    /// deadline is only a different reason for reaching those paths.
    ///
    /// The classification is the whole substance. A read cannot have committed
    /// anything, so it is definite. Everything past it may have taken effect
    /// on the far side while the answer was lost, so it is ambiguous — the
    /// same distinction that decides whether a retry is safe. Calling a
    /// timed-out compare-and-swap definite would let a second attempt
    /// overwrite an epoch that had in fact been applied.
    fn expire_operation(&mut self, op: OpId, now_ms: u64, effects: &mut Vec<Effect>) {
        // A gated write is tracked in `gated_writes`, not the op index.
        // Ambiguous is the only safe class: the write may or may not be
        // durable, so the client must not be told it succeeded.
        if self.gated_writes.contains_key(&op) {
            self.durable_reached(op, Err(Failure::Ambiguous), effects);
            return;
        }
        // A peek, not a take: each arm below re-enters the op's own
        // completion handler, and that handler consumes the index entry.
        let Some(id) = self.cell_ops.get(&op).cloned() else {
            // Already answered, superseded, or fenced. A stale deadline has
            // nothing to expire, which is the ordinary case.
            return;
        };
        let Some(cell) = self.cells.get(&id) else {
            return;
        };
        // A firing alarm is tracked on the cell rather than in its phase, so
        // it is looked for first. Expiry re-arms it exactly as a failed
        // handler would, which keeps alarms at-least-once instead of turning
        // a stuck handler into a lost one.
        if matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op) {
            let now_mono_ms = self.now_mono_ms;
            self.alarm_finished(op, now_ms, now_mono_ms, Err(Failure::Ambiguous), effects);
            return;
        }
        if phase_op(&cell.phase) != Some(op) {
            return;
        }
        let phase = Some(cell.phase.clone());
        match phase {
            Some(Phase::ReadingOwner { .. }) => {
                self.owner_read(op, 0, Err(Failure::Definite), effects)
            }
            Some(Phase::ReadingNodeLease { .. }) => {
                self.node_lease_read(op, now_ms, Err(Failure::Definite), effects)
            }
            Some(Phase::ReadingCapacity { .. }) => {
                self.capacity_peers_read(op, now_ms, Err(Failure::Definite), effects)
            }
            Some(Phase::Acquiring { .. }) | Some(Phase::ReconcilingAcquire { .. }) => {
                self.owner_cas_completed(op, Err(Failure::Ambiguous), effects)
            }
            // A restore has no aggregate deadline. Each object-store request
            // is bounded in the shell, and the complete task must retain its
            // op so a late success cannot be discarded and duplicated.
            Some(Phase::Restoring { .. }) => {}
            Some(Phase::Starting { .. }) => {
                self.runtime_started(op, Err(Failure::Ambiguous), effects)
            }
            Some(Phase::Publishing { .. }) => self.published(op, Err(Failure::Ambiguous), effects),
            // An unprovable snapshot leaves the cell resident. Evicting on a
            // proof that never arrived is the one outcome that loses data.
            Some(Phase::EnsuringDurability { .. }) => {
                self.durability_checked(op, Err(Failure::Ambiguous), effects)
            }
            _ => {}
        }
    }

    fn timer_fired(
        &mut self,
        timer: Timer,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        self.update_node_lease_dependencies(now_mono_ms);
        let timer = match timer {
            Timer::CellAlarm { cell, generation } => {
                self.cell_alarm_timer(&cell, generation, now_ms, now_mono_ms, effects);
                return;
            }
            // An activation deadline is not conditional on node authority: a
            // request stalled behind a swallowed effect must be released
            // whatever the lease is doing.
            Timer::OperationDeadline { op } => {
                self.expire_operation(op, now_ms, effects);
                return;
            }
            timer => timer,
        };
        // A retry has no held lease to validate against, so it is resolved
        // before the authority lookup below discards it.
        if let NodeAuthority::Retrying { spec, generation } = &self.node_authority {
            if matches!(timer, Timer::NodeLeaseRenew { generation: fired } if fired == *generation)
            {
                let spec = spec.clone();
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
            return;
        }
        let active = match &self.node_authority {
            NodeAuthority::Held(held) => Some(held.clone()),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. } => {
                pending.prior.clone()
            }
            _ => None,
        };
        let Some(held) = active else {
            return;
        };
        match timer {
            // `>=`, not `>`: a peer stops treating the record as live the
            // instant `now_ms` reaches `expires_ms`, so authority must end
            // there too. Serving through that one millisecond overlaps a
            // takeover that is already entitled to proceed.
            Timer::NodeLeaseFence { generation }
                if generation == held.timer_generation
                    && now_mono_ms.saturating_sub(held.last_ok_mono_ms) >= held.spec.ttl_ms =>
            {
                self.release_or_fence_node_lease(held, effects);
            }
            // A renew timer that arrives after the lease it belongs to has
            // already expired is not a renewal opportunity; authority is
            // already gone. Fence rather than issue a write that could
            // resurrect it behind a peer's in-flight takeover.
            Timer::NodeLeaseRenew { generation }
                if generation == held.timer_generation
                    && now_mono_ms.saturating_sub(held.last_ok_mono_ms) >= held.spec.ttl_ms =>
            {
                self.release_or_fence_node_lease(held, effects);
            }
            Timer::NodeLeaseRenew { generation }
                if generation == held.timer_generation
                    && matches!(self.node_authority, NodeAuthority::Held(_)) =>
            {
                let NodeAuthority::Held(mut prior) =
                    std::mem::replace(&mut self.node_authority, NodeAuthority::Failed)
                else {
                    unreachable!("held checked above")
                };
                let idle_ms = prior
                    .idle_since_mono_ms
                    .map_or(0, |inactive| now_mono_ms.saturating_sub(inactive));
                let idle_long_enough =
                    prior.idle_since_mono_ms.is_some() && idle_ms >= prior.spec.linger_ms;
                if prior.spec.mode == NodeLeaseMode::Shadow
                    && idle_long_enough
                    && !prior.shadow_release_reported
                {
                    self.record_shadow_release(&prior, now_ms, now_mono_ms, effects);
                    prior.shadow_release_reported = true;
                }
                if prior.spec.mode == NodeLeaseMode::Lazy && idle_long_enough {
                    self.node_authority = NodeAuthority::Lapsed(prior.spec);
                    effects.push(Effect::ObserveNodeLeaseReleased);
                    return;
                }
                prior.last_attempt_mono_ms = now_mono_ms;
                let spec = prior.spec.clone();
                let desired = NodeLeaseRecord {
                    node: self.node.clone(),
                    addr: spec.addr.clone(),
                    expires_ms: now_ms.saturating_add(spec.ttl_ms),
                    peer_protocol: spec.peer_protocol,
                    generation: spec.generation.clone(),
                    etag: String::new(),
                };
                let guard = CasGuard::Match(prior.record.etag.clone());
                self.begin_node_lease_write(
                    PendingNodeLease {
                        spec,
                        desired,
                        prior: Some(prior),
                        readback_only: false,
                        anchor_mono_ms: now_mono_ms,
                        resume_local: false,
                    },
                    guard,
                    effects,
                );
            }
            Timer::CellAlarm { .. } => unreachable!("cell timers returned above"),
            _ => {}
        }
    }

    fn schedule_alarm_timer(
        &self,
        cell: &str,
        generation: u64,
        at_ms: i64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let at_ms = u64::try_from(at_ms).unwrap_or(0);
        effects.push(Effect::ScheduleTimer {
            timer: Timer::CellAlarm {
                cell: cell.to_string(),
                generation,
            },
            at_mono_ms: now_mono_ms.saturating_add(at_ms.saturating_sub(now_ms)),
        });
    }

    fn alarm_observed(
        &mut self,
        id: &str,
        at_ms: Option<i64>,
        covered: bool,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        if matches!(cell.phase, Phase::Fenced) {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if let Phase::EnsuringDurability { op, epoch } = cell.phase {
            self.cell_ops.remove(&op);
            set_phase(&mut self.occupied, &mut cell, Phase::Resident { epoch });
        }
        // A request that armed this alarm can finish its output gate after
        // the alarm has started. Its activity report still carries the same
        // cached deadline, but that is not a new arm: replacing `Firing`
        // would retire the live op and schedule the due deadline again while
        // its handler is still running. Keep the claim and only refresh its
        // wake coverage. A real move to another deadline (or to no alarm)
        // still replaces the firing state below, and the handler's own final
        // observation settles an explicit re-arm to this same deadline.
        if let Some(AlarmState::Firing {
            at_ms: firing_at_ms,
            covered: firing_covered,
            ..
        }) = &mut cell.alarm
        {
            if at_ms == Some(*firing_at_ms) {
                *firing_covered = covered;
                self.cells.insert(id.to_string(), cell);
                return;
            }
        }
        let observed_before = match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => at_ms,
            None => -1,
        };
        // The assignment below replaces a firing alarm unconditionally. Its
        // op entry is usually consumed already (`alarm_finished` routes
        // through here), but a direct observation can land mid-fire, and the
        // replaced op will then never match a lookup again.
        if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
            self.cell_ops.remove(&op);
        }
        cell.alarm = at_ms.filter(|at_ms| *at_ms >= 0).map(|at_ms| {
            let generation = self.next_timer_generation;
            self.next_timer_generation = self
                .next_timer_generation
                .checked_add(1)
                .expect("timer generation exhausted");
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            AlarmState::Armed {
                at_ms,
                generation,
                covered,
            }
        });
        if cell.alarm.is_none() {
            cell.alarm_wake = false;
        }
        // Both ways an alarm settles arrive here -- a request that changed it
        // and a firing that consumed it (`alarm_finished` routes through this
        // function). Saying so once, here, is what keeps the bucket entry
        // from depending on the shell noticing each path separately. Only on
        // a change: an activity that left the alarm alone has nothing to
        // mirror, and re-stating it would put a bucket round trip on the end
        // of every request.
        let settled = at_ms.filter(|at_ms| *at_ms >= 0).unwrap_or(-1);
        if settled != observed_before {
            effects.push(Effect::ReconcileWakeEntry {
                cell: id.to_string(),
                next_alarm_ms: settled,
            });
        }
        self.cells.insert(id.to_string(), cell);
    }

    fn cell_alarm_timer(
        &mut self,
        id: &str,
        generation: u64,
        now_ms: u64,
        now_mono_ms: u64,
        effects: &mut Vec<Effect>,
    ) {
        if !self.node_authoritative() {
            if let Some(spec) = self.lazy_node_lease_spec() {
                self.node_wake_waiters.insert(id.to_string());
                if matches!(self.node_authority, NodeAuthority::Lapsed(_)) {
                    self.begin_node_lease_acquisition(now_ms, spec, effects);
                }
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
            }
            return;
        }
        let Some(mut cell) = self.cells.remove(id) else {
            return;
        };
        let Some(AlarmState::Armed {
            at_ms,
            generation: current,
            covered,
        }) = cell.alarm
        else {
            self.cells.insert(id.to_string(), cell);
            return;
        };
        if current != generation {
            self.cells.insert(id.to_string(), cell);
            return;
        }
        if i64::try_from(now_ms).unwrap_or(i64::MAX) < at_ms {
            self.schedule_alarm_timer(id, generation, at_ms, now_ms, now_mono_ms, effects);
            self.cells.insert(id.to_string(), cell);
            return;
        }
        let epoch = match cell.phase {
            Phase::Resident { epoch } => epoch,
            Phase::EnsuringDurability { op, epoch } => {
                self.cell_ops.remove(&op);
                set_phase(&mut self.occupied, &mut cell, Phase::Resident { epoch });
                epoch
            }
            Phase::Fenced => {
                cell.alarm = None;
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::Dormant { epoch } => {
                cell.alarm_wake = true;
                self.admit_or_queue_activation(
                    id,
                    &mut cell,
                    ColdStart::Restore(RestoreSpec {
                        epoch,
                        fresh: false,
                        took_over: false,
                        resume_local: false,
                    }),
                    effects,
                );
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            Phase::Inactive => {
                cell.alarm_wake = true;
                self.admit_or_queue_activation(id, &mut cell, ColdStart::ReadOwner, effects);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
            _ => {
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id.to_string(),
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(100),
                });
                self.cells.insert(id.to_string(), cell);
                return;
            }
        };
        let op = self.cell_op(id);
        cell.alarm = Some(AlarmState::Firing {
            op,
            at_ms,
            generation,
            covered,
        });
        cell.alarm_wake = false;
        self.cells.insert(id.to_string(), cell);
        effects.push(Effect::FireAlarm {
            op,
            cell: id.to_string(),
            epoch,
            scheduled_ms: at_ms,
        });
    }

    fn alarm_finished(
        &mut self,
        op: OpId,
        now_ms: u64,
        now_mono_ms: u64,
        result: Result<(Option<i64>, bool), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.alarm, Some(AlarmState::Firing { op: current, .. }) if current == op)
        }) else {
            return;
        };
        let Some(mut cell) = self.cells.remove(&id) else {
            return;
        };
        let Some(AlarmState::Firing {
            at_ms,
            generation,
            covered,
            ..
        }) = cell.alarm
        else {
            self.cells.insert(id, cell);
            return;
        };
        match result {
            Ok((at_ms, covered)) => {
                // Leave the fired alarm in place for `alarm_observed` to
                // replace: it assigns unconditionally, and it compares against
                // what was there to decide whether the bucket entry has to
                // follow. Clearing it here first makes a consumed alarm look
                // like it was never armed, and the entry is never deleted.
                self.cells.insert(id.clone(), cell);
                self.alarm_observed(&id, at_ms, covered, now_ms, now_mono_ms, effects);
            }
            Err(_) => {
                cell.alarm = Some(AlarmState::Armed {
                    at_ms,
                    generation,
                    covered,
                });
                self.cells.insert(id.clone(), cell);
                effects.push(Effect::ScheduleTimer {
                    timer: Timer::CellAlarm {
                        cell: id,
                        generation,
                    },
                    at_mono_ms: now_mono_ms.saturating_add(500),
                });
            }
        }
    }

    fn fence_node(&mut self, effects: &mut Vec<Effect>) {
        self.node_authority = NodeAuthority::Fenced;
        self.fence(effects);
        effects.push(Effect::Halt {
            code: 3,
            reason: HaltReason::NodeLeaseExpired,
        });
    }

    fn lazy_node_lease_spec(&self) -> Option<NodeLeaseSpec> {
        match &self.node_authority {
            NodeAuthority::Lapsed(spec) if spec.mode == NodeLeaseMode::Lazy => Some(spec.clone()),
            NodeAuthority::Reading { pending, .. } | NodeAuthority::Writing { pending, .. }
                if pending.prior.is_none() && pending.spec.mode == NodeLeaseMode::Lazy =>
            {
                Some(pending.spec.clone())
            }
            _ => None,
        }
    }

    fn request(
        &mut self,
        request: RequestId,
        id: CellId,
        now_ms: u64,
        capacity_handoff: bool,
        effects: &mut Vec<Effect>,
    ) {
        if self.request_cells.contains_key(&request)
            || self.node_lease_waiters.contains_key(&request)
        {
            return;
        }
        if self.fenced {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
            return;
        }
        if capacity_handoff {
            self.capacity_requests.insert(request);
        }
        if self.node_authoritative() {
            self.request_authorized(request, id, effects);
            return;
        }
        if let Some(spec) = self.lazy_node_lease_spec() {
            self.node_lease_waiters.insert(request, id);
            if matches!(self.node_authority, NodeAuthority::Lapsed(_)) {
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
        } else {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeUnavailable),
            });
        }
    }

    fn drain_node_lease_waiters(&mut self, effects: &mut Vec<Effect>) {
        let requests = std::mem::take(&mut self.node_lease_waiters);
        for (request, cell) in requests {
            self.request_authorized(request, cell, effects);
        }
        let wakes = std::mem::take(&mut self.node_wake_waiters);
        for cell in wakes {
            self.wake_hint_authorized(cell, effects);
        }
    }

    fn request_authorized(&mut self, request: RequestId, id: CellId, effects: &mut Vec<Effect>) {
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        match &cell.phase {
            Phase::Resident { .. } => {
                self.complete_request(&id, request, Ok(Route::Local), effects)
            }
            Phase::EnsuringDurability { op, epoch } => {
                // The runtime is still published, so a new request wins the
                // race with voluntary eviction. Retiring the operation makes
                // its eventual durability completion harmless. The permit
                // taken at nomination comes back with the rescue: leaked, it
                // counts against `max_evictions` forever and eventually
                // stands every future eviction down.
                let epoch = *epoch;
                self.cell_ops.remove(op);
                set_phase(&mut self.occupied, &mut cell, Phase::Resident { epoch });
                self.eviction_permits.remove(&id);
                self.complete_request(&id, request, Ok(Route::Local), effects);
            }
            Phase::Remote {
                node,
                addr,
                epoch,
                peer_protocol,
                ..
            } => effects.push(Effect::Complete {
                request,
                result: Ok(Route::Remote {
                    node: node.clone(),
                    addr: addr.clone(),
                    epoch: *epoch,
                    peer_protocol: *peer_protocol,
                }),
            }),
            Phase::Fenced => effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            }),
            phase => {
                cell.requests.insert(request);
                self.request_cells.insert(request, id.clone());
                match phase {
                    Phase::Inactive => {
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::ReadOwner,
                            effects,
                        );
                    }
                    Phase::Dormant { epoch } => {
                        let epoch = *epoch;
                        self.admit_or_queue_activation(
                            &id,
                            &mut cell,
                            ColdStart::Restore(RestoreSpec {
                                epoch,
                                fresh: false,
                                took_over: false,
                                resume_local: false,
                            }),
                            effects,
                        );
                    }
                    _ => {}
                }
            }
        }
        self.cells.insert(id, cell);
    }

    fn wake_hint(&mut self, id: CellId, now_ms: u64, effects: &mut Vec<Effect>) {
        if self.fenced {
            return;
        }
        if self.node_authoritative() {
            self.wake_hint_authorized(id, effects);
            return;
        }
        if let Some(spec) = self.lazy_node_lease_spec() {
            self.node_wake_waiters.insert(id);
            if matches!(self.node_authority, NodeAuthority::Lapsed(_)) {
                self.begin_node_lease_acquisition(now_ms, spec, effects);
            }
        }
    }

    fn wake_hint_authorized(&mut self, id: CellId, effects: &mut Vec<Effect>) {
        let mut cell = self.cells.remove(&id).unwrap_or_default();
        cell.alarm_wake = true;
        match cell.phase {
            Phase::Inactive | Phase::Remote { .. } => {
                self.admit_or_queue_activation(&id, &mut cell, ColdStart::ReadOwner, effects);
            }
            Phase::Dormant { epoch } => self.admit_or_queue_activation(
                &id,
                &mut cell,
                ColdStart::Restore(RestoreSpec {
                    epoch,
                    fresh: false,
                    took_over: false,
                    resume_local: false,
                }),
                effects,
            ),
            _ => {}
        }
        self.cells.insert(id, cell);
    }

    fn cancel(&mut self, request: RequestId) {
        self.capacity_requests.remove(&request);
        if self.node_lease_waiters.remove(&request).is_some() {
            return;
        }
        let Some(id) = self.request_cells.remove(&request) else {
            return;
        };
        let Some(cell) = self.cells.get_mut(&id) else {
            return;
        };
        cell.requests.remove(&request);
        if cell.requests.is_empty() && !cell.alarm_wake && cell.phase == Phase::WaitingActivation {
            let start = cell.waiting_activation.take();
            set_phase(
                &mut self.occupied,
                cell,
                match start {
                    Some(ColdStart::Restore(spec)) => Phase::Dormant { epoch: spec.epoch },
                    _ => Phase::Inactive,
                },
            );
            self.activation_waiters.retain(|queued| queued != &id);
        } else if cell.requests.is_empty()
            && !cell.alarm_wake
            && cell.phase == Phase::WaitingCapacity
        {
            let activation = cell.waiting_for.take();
            set_phase(
                &mut self.occupied,
                cell,
                match activation {
                    Some(Activation::Restore(spec)) => Phase::Dormant { epoch: spec.epoch },
                    _ => Phase::Inactive,
                },
            );
            self.capacity_waiters.retain(|queued| queued != &id);
        }
    }

    /// Retire cold work that cannot contribute to a clean local reload.
    ///
    /// Published runtimes remain untouched. The shell drains their active
    /// turns before it snapshots the resident inventory. A restore has no
    /// runtime to preserve, and it can legitimately outlive every caller, so
    /// waiting for it would make one cold cell disable local reuse for the
    /// complete node. Retire those phases and fail their callers. A later
    /// completion is ignored because its operation is no longer current.
    fn begin_preserve(&mut self, effects: &mut Vec<Effect>) {
        self.activation_waiters.clear();
        self.capacity_waiters.clear();
        for (request, _) in std::mem::take(&mut self.node_lease_waiters) {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
        }
        self.node_wake_waiters.clear();

        let ids: Vec<CellId> = self.cells.keys().cloned().collect();
        for id in ids {
            let mut cell = self.cells.remove(&id).expect("id came from map");
            let fallback = match &cell.phase {
                Phase::WaitingActivation => match cell.waiting_activation.as_ref() {
                    Some(ColdStart::Restore(spec)) => Some(Phase::Dormant { epoch: spec.epoch }),
                    _ => Some(Phase::Inactive),
                },
                Phase::ReadingOwner { .. }
                | Phase::ReadingNodeLease { .. }
                | Phase::ReadingCapacity { .. }
                | Phase::Acquiring { .. }
                | Phase::ReconcilingAcquire { .. } => Some(Phase::Inactive),
                Phase::WaitingCapacity => match cell.waiting_for.as_ref() {
                    Some(Activation::Restore(spec)) => Some(Phase::Dormant { epoch: spec.epoch }),
                    _ => Some(Phase::Inactive),
                },
                Phase::Restoring { spec, .. } => Some(Phase::Dormant { epoch: spec.epoch }),
                _ => None,
            };
            if let Some(fallback) = fallback {
                if let Some(stale) = phase_op(&cell.phase) {
                    self.cell_ops.remove(&stale);
                }
                self.finish_requests(&id, &mut cell, Err(RequestError::NodeFenced), effects);
                set_phase(&mut self.occupied, &mut cell, fallback);
                cell.waiting_for = None;
                cell.waiting_activation = None;
                cell.alarm_wake = false;
                cell.resume_demand = false;
                self.resuming_cells.remove(&id);
            }
            self.cells.insert(id, cell);
        }
        if self.resuming_cells.is_empty() {
            self.resuming = false;
        }
        self.activation_permits.retain(|id| {
            self.cells
                .get(id)
                .is_some_and(|cell| phase_holds_activation(&cell.phase))
        });
        self.capacity_requests
            .retain(|request| self.request_cells.contains_key(request));
    }

    fn begin_capacity_lookup(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        let op = self.cell_op(id);
        set_phase(
            &mut self.occupied,
            cell,
            Phase::ReadingCapacity {
                op,
                claim: claim.clone(),
            },
        );
        effects.push(Effect::ReadCapacityPeers {
            op,
            cell: id.to_string(),
        });
    }

    /// Place a genuinely unowned cell. Ordinary ingress may look for fleet
    /// capacity before waiting locally; a capacity handoff must either reserve
    /// a real local slot now or explicitly refuse so its caller can traverse.
    fn place_unowned(
        &mut self,
        id: &str,
        cell: &mut Cell,
        claim: Claim,
        effects: &mut Vec<Effect>,
    ) {
        if self.has_capacity() {
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
            return;
        }

        let handoffs: Vec<RequestId> = cell
            .requests
            .iter()
            .copied()
            .filter(|request| self.capacity_requests.contains(request))
            .collect();
        for request in handoffs {
            cell.requests.remove(&request);
            self.request_cells.remove(&request);
            self.complete_request(id, request, Err(RequestError::CapacityExhausted), effects);
        }

        if cell.requests.is_empty() || !self.config.require_node_lease {
            // Alarm wakes cannot be proxied as an HTTP capacity handoff. They
            // retain the old local wait semantics until alarm dispatch itself
            // has a fleet transport effect. Lease-disabled mode likewise has
            // no authoritative fleet membership to enumerate.
            self.activate_or_wait(id, cell, Activation::Claim(claim), effects);
        } else {
            self.begin_capacity_lookup(id, cell, claim, effects);
        }
    }

    fn owner_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Option<OwnerRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(
                cell.phase,
                Phase::ReadingOwner { op: current }
                    | Phase::ReconcilingAcquire { op: current, .. }
                    if current == op
            )
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let reconciling_claim = match &cell.phase {
            Phase::ReconcilingAcquire { claim, .. } => Some(claim.clone()),
            _ => None,
        };
        // A reconcile re-reads and then acquires again: one attempt
        // continuing, not a new one. The count has to survive the round trip
        // or the bound below can never be reached.
        let reconciles = reconciling_claim
            .as_ref()
            .map_or(0, |claim| claim.reconciles);
        match result {
            Ok(Some(record))
                if reconciling_claim.as_ref().is_some_and(|claim| {
                    record.node.as_deref() == Some(self.node.as_str())
                        && record.epoch == claim.epoch
                }) =>
            {
                let spec = RestoreSpec {
                    epoch: record.epoch,
                    fresh: matches!(
                        reconciling_claim.as_ref().map(|claim| &claim.guard),
                        Some(CasGuard::Absent)
                    ),
                    took_over: reconciling_claim
                        .as_ref()
                        .is_some_and(|claim| claim.takeover),
                    resume_local: false,
                };
                self.record_acquisition(&spec);
                self.activate_or_wait(&id, &mut cell, Activation::Restore(spec), effects);
            }
            Ok(Some(record)) if record.node.as_deref() == Some(self.node.as_str()) => {
                let epoch = record.epoch.saturating_add(1);
                self.activate_or_wait(
                    &id,
                    &mut cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: false,
                        reconciles,
                    }),
                    effects,
                );
            }
            Ok(Some(record)) if record.node.is_none() => {
                let epoch = record.epoch.saturating_add(1);
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        reconciles,
                    },
                    effects,
                );
            }
            Ok(Some(record)) => {
                let owner = record.node.clone().expect("foreign owner checked above");
                let cached = self
                    .node_lease_cache
                    .get(&owner)
                    .filter(|lease| lease.expires_ms > now_ms && !lease.addr.is_empty())
                    .cloned();
                if let Some(lease) = cached {
                    self.apply_node_lease_result(
                        &id,
                        &mut cell,
                        record,
                        now_ms,
                        Ok(Some(lease)),
                        effects,
                    );
                } else {
                    self.node_lease_cache.remove(&owner);
                    let next = self.cell_op(&id);
                    set_phase(
                        &mut self.occupied,
                        &mut cell,
                        Phase::ReadingNodeLease {
                            op: next,
                            owner: record,
                        },
                    );
                    effects.push(Effect::ReadNodeLease {
                        op: next,
                        cell: id.clone(),
                        owner,
                    });
                }
            }
            Ok(None) => {
                self.place_unowned(
                    &id,
                    &mut cell,
                    Claim {
                        guard: CasGuard::Absent,
                        epoch: 1,
                        takeover: false,
                        reconciles,
                    },
                    effects,
                );
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::ResolveFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        if reconciling_claim.is_some() {
            self.pump_capacity(effects);
        }
    }

    fn node_lease_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::ReadingNodeLease { op: current, .. } if current == op)
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingNodeLease { owner: record, .. } = &cell.phase else {
            unreachable!()
        };
        let record = record.clone();
        if let Ok(Some(lease)) = &result {
            if record.node.as_deref() == Some(lease.node.as_str())
                && lease.expires_ms > now_ms
                && !lease.addr.is_empty()
            {
                self.node_lease_cache
                    .insert(lease.node.clone(), lease.clone());
            }
        }
        self.apply_node_lease_result(&id, &mut cell, record, now_ms, result, effects);
        self.cells.insert(id, cell);
    }

    fn capacity_peers_read(
        &mut self,
        op: OpId,
        now_ms: u64,
        result: Result<Vec<CapacityPeer>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::ReadingCapacity { op: current, .. } if current == op)
        }) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::ReadingCapacity { claim, .. } = &cell.phase else {
            unreachable!()
        };
        let claim = claim.clone();
        let peers = match result {
            Ok(peers) => peers,
            Err(_) => {
                self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
                self.cells.insert(id, cell);
                return;
            }
        };

        // A newer load sample supersedes both reservations made against and
        // refusals of the prior sample. Equal samples deliberately retain
        // both, which is what makes concurrent read completions compose.
        for peer in &peers {
            let prior = self.capacity_samples.get(&peer.node).copied().unwrap_or(0);
            if peer.sampled_ms > prior {
                self.capacity_samples
                    .insert(peer.node.clone(), peer.sampled_ms);
                self.capacity_reservations.remove(&peer.node);
                self.capacity_rejections.remove(&peer.node);
            }
        }

        let selected = peers
            .into_iter()
            .filter(|peer| {
                peer.node != self.node
                    && peer.expires_ms > now_ms
                    && !peer.addr.is_empty()
                    && peer.peer_protocol == self.config.peer_protocol
                    && peer.sampled_ms != 0
                    && !peer.pressured
                    && self
                        .capacity_rejections
                        .get(&peer.node)
                        .is_none_or(|sample| peer.sampled_ms > *sample)
            })
            .map(|peer| {
                let projected = peer.resident_cells.saturating_add(
                    self.capacity_reservations
                        .get(&peer.node)
                        .copied()
                        .unwrap_or(0),
                );
                (peer, projected)
            })
            .min_by_key(|(peer, projected)| {
                // The third key is the memory the peer holds, which is what
                // the peer decides its own pressure on. A peer from before that
                // field existed reports nothing, and its resident set size is
                // the only number available -- which reads as more loaded than
                // it is, and is the conservative direction for a tiebreak.
                (
                    *projected,
                    peer.host_websockets,
                    peer.in_use_bytes.unwrap_or(peer.rss_bytes),
                    peer.node.clone(),
                )
            });

        if let Some((peer, _)) = selected {
            *self
                .capacity_reservations
                .entry(peer.node.clone())
                .or_default() += 1;
            set_phase(
                &mut self.occupied,
                &mut cell,
                Phase::Remote {
                    node: peer.node.clone(),
                    addr: peer.addr.clone(),
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                    capacity_sampled_ms: Some(peer.sampled_ms),
                },
            );
            self.finish_requests(
                &id,
                &mut cell,
                Ok(Route::Remote {
                    node: peer.node,
                    addr: peer.addr,
                    epoch: 0,
                    peer_protocol: peer.peer_protocol,
                }),
                effects,
            );
        } else {
            self.activate_or_wait(&id, &mut cell, Activation::Claim(claim), effects);
        }
        self.cells.insert(id, cell);
    }

    fn apply_node_lease_result(
        &mut self,
        id: &str,
        cell: &mut Cell,
        record: OwnerRecord,
        now_ms: u64,
        result: Result<Option<NodeLeaseRecord>, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        match result {
            Ok(Some(lease))
                if lease.expires_ms > now_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol != self.config.peer_protocol =>
            {
                set_phase(&mut self.occupied, cell, Phase::Inactive);
                self.finish_requests(id, cell, Err(RequestError::PeerIncompatible), effects);
            }
            Ok(Some(lease))
                if lease.expires_ms > now_ms
                    && !lease.addr.is_empty()
                    && record.node.as_deref() == Some(lease.node.as_str())
                    && lease.peer_protocol == self.config.peer_protocol =>
            {
                let node = lease.node;
                let addr = lease.addr;
                let epoch = record.epoch;
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Remote {
                        node: node.clone(),
                        addr: addr.clone(),
                        epoch,
                        peer_protocol: lease.peer_protocol,
                        capacity_sampled_ms: None,
                    },
                );
                self.finish_requests(
                    id,
                    cell,
                    Ok(Route::Remote {
                        node,
                        addr,
                        epoch,
                        peer_protocol: lease.peer_protocol,
                    }),
                    effects,
                );
            }
            Ok(_) => {
                let epoch = record.epoch.saturating_add(1);
                self.activate_or_wait(
                    id,
                    cell,
                    Activation::Claim(Claim {
                        guard: CasGuard::Match(record.etag),
                        epoch,
                        takeover: true,
                        reconciles: 0,
                    }),
                    effects,
                );
            }
            Err(_) => {
                set_phase(&mut self.occupied, cell, Phase::Inactive);
                self.finish_requests(id, cell, Err(RequestError::ResolveFailed), effects);
            }
        }
    }

    fn owner_cas_completed(
        &mut self,
        op: OpId,
        result: Result<CasOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Acquiring { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Acquiring { claim, .. } = &cell.phase else {
            unreachable!()
        };
        let claim = claim.clone();
        match result {
            Ok(CasOutcome::Applied) => {
                let next = self.cell_op(&id);
                let spec = RestoreSpec {
                    epoch: claim.epoch,
                    fresh: matches!(claim.guard, CasGuard::Absent),
                    took_over: claim.takeover,
                    resume_local: false,
                };
                self.record_acquisition(&spec);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Restoring {
                        op: next,
                        spec: spec.clone(),
                    },
                );
                effects.push(Effect::Restore {
                    op: next,
                    cell: id.clone(),
                    spec,
                });
            }
            Ok(CasOutcome::Rejected) => {
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::ReadingOwner { op: next },
                );
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) if claim.reconciles < MAX_ACQUIRE_RECONCILES => {
                let next = self.cell_op(&id);
                let claim = Claim {
                    reconciles: claim.reconciles + 1,
                    ..claim
                };
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::ReconcilingAcquire { op: next, claim },
                );
                effects.push(Effect::ReadOwner {
                    op: next,
                    cell: id.clone(),
                });
            }
            Err(Failure::Ambiguous) => {
                // Out of reconciles. The claim may or may not have applied, so
                // this cell is left dormant for a later request to resolve
                // from the record rather than guessed at here.
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
            Err(Failure::Definite) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                self.finish_requests(&id, &mut cell, Err(RequestError::AcquireFailed), effects);
            }
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
    }

    fn restore_completed(
        &mut self,
        op: OpId,
        result: Result<RestoreOutcome, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Restoring { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Restoring { spec, .. } = &cell.phase else {
            unreachable!()
        };
        let epoch = spec.epoch;
        let mut resume_failed = false;
        match result {
            Ok(outcome) => {
                if outcome.restored {
                    self.activity.restored = self.activity.restored.saturating_add(1);
                }
                // Seed the mirror from the durable truth the restore loaded,
                // before the isolate opens the same database and long before
                // it would re-arm anything.
                if let Some(alarm) = outcome.alarm.filter(|alarm| alarm.at_ms >= 0) {
                    let generation = self.next_timer_generation;
                    self.next_timer_generation = self
                        .next_timer_generation
                        .checked_add(1)
                        .expect("timer generation exhausted");
                    let (now_ms, now_mono_ms) = (self.now_ms, self.now_mono_ms);
                    self.schedule_alarm_timer(
                        &id,
                        generation,
                        alarm.at_ms,
                        now_ms,
                        now_mono_ms,
                        effects,
                    );
                    cell.alarm = Some(AlarmState::Armed {
                        at_ms: alarm.at_ms,
                        generation,
                        covered: alarm.covered,
                    });
                }
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Starting { op: next, epoch },
                );
                effects.push(Effect::StartRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::RestoreFailed), effects);
                resume_failed = cell.resume_demand;
            }
        }
        self.cells.insert(id.clone(), cell);
        if resume_failed {
            self.settle_local_resume(&id);
        }
        self.pump_capacity(effects);
    }

    fn runtime_started(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Starting { op: current, .. } if current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Starting { epoch, .. } = cell.phase else {
            unreachable!()
        };
        let mut resume_failed = false;
        match result {
            Ok(()) => {
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Publishing { op: next, epoch },
                );
                effects.push(Effect::Publish {
                    op: next,
                    cell: id.clone(),
                    epoch,
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::RuntimeFailed), effects);
                resume_failed = cell.resume_demand;
            }
        }
        self.cells.insert(id.clone(), cell);
        if resume_failed {
            self.settle_local_resume(&id);
        }
        self.pump_capacity(effects);
    }

    fn published(&mut self, op: OpId, result: Result<(), Failure>, effects: &mut Vec<Effect>) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Publishing { op: current, .. } if current == op),
        ) else {
            self.compensate_retired_runtime(op, result, effects);
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Publishing { epoch, .. } = cell.phase else {
            unreachable!()
        };
        let completing_resume = cell.resume_demand;
        match result {
            Ok(()) => {
                set_phase(&mut self.occupied, &mut cell, Phase::Resident { epoch });
                self.finish_requests(&id, &mut cell, Ok(Route::Local), effects);
            }
            Err(_) => {
                let next = self.cell_op(&id);
                set_phase(
                    &mut self.occupied,
                    &mut cell,
                    Phase::Cleaning {
                        op: next,
                        epoch,
                        cause: StopCause::Cleanup,
                    },
                );
                effects.push(Effect::StopRuntime {
                    op: next,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Cleanup,
                });
            }
        }
        self.cells.insert(id.clone(), cell);
        if completing_resume {
            self.settle_local_resume(&id);
        }
    }

    /// A release either published the cell as unowned or did not. Ownership
    /// is the bucket's answer, not this node's, so a rejected or failed write
    /// simply leaves the record naming this node -- correct, if less useful,
    /// and the next eviction gets another chance.
    fn owner_released(&mut self, op: OpId, result: Result<CasOutcome, Failure>) {
        let Some(id) = self.take_cell_op(op, |cell| cell.releasing == Some(op)) else {
            return;
        };
        let cell = self.cells.get_mut(&id).expect("cell found above");
        cell.releasing = None;
        // Only a cell still sitting where the eviction left it may be
        // forgotten. Anything else means it was wanted again while the write
        // was in flight, and that claim outranks a release decided earlier.
        // On a draining node a rejection settles the cell too: the record no
        // longer names this node at this epoch, so nothing here blocks a
        // successor, and the pump must not re-read a record it can never
        // blank. An indefinite failure stays `Dormant`, which is what makes
        // the pump retry it while the drain window is open.
        let settled = matches!(result, Ok(CasOutcome::Applied))
            || (self.draining && matches!(result, Ok(CasOutcome::Rejected)));
        if settled && matches!(cell.phase, Phase::Dormant { .. }) {
            set_phase(&mut self.occupied, cell, Phase::Inactive);
        }
    }

    fn runtime_stopped(&mut self, op: OpId, effects: &mut Vec<Effect>) {
        let Some(id) = self.take_cell_op(
            op,
            |cell| matches!(cell.phase, Phase::Cleaning { op: current, .. } if current == op),
        ) else {
            return;
        };
        let mut cell = self.cells.remove(&id).expect("cell found above");
        let Phase::Cleaning { epoch, cause, .. } = cell.phase else {
            unreachable!()
        };
        match cause {
            StopCause::Cleanup => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.finish_requests(&id, &mut cell, Err(RequestError::PublishFailed), effects);
            }
            // `Inactive`, not `Dormant`: the next request re-reads the
            // ownership record. A proof that could not be completed is exactly
            // the moment to stop assuming this node still holds the cell, and
            // the re-read costs one GET on a path that was already failing.
            StopCause::Reset => {
                set_phase(&mut self.occupied, &mut cell, Phase::Inactive);
                if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                    self.cell_ops.remove(&op);
                }
                cell.alarm = None;
                cell.alarm_wake = false;
                cell.websockets.clear();
                self.finish_requests(
                    &id,
                    &mut cell,
                    Err(RequestError::DurabilityUnproven),
                    effects,
                );
            }
            StopCause::Evict { rebalance } if cell.requests.is_empty() => {
                set_phase(&mut self.occupied, &mut cell, Phase::Dormant { epoch });
                self.eviction_permits.remove(&id);
                // The record still names this node, which is the whole cost of
                // stopping here: every later request for the cell routes to a
                // node that has already decided it has no room. Publishing it
                // as unowned is what turns an eviction into shed load.
                if rebalance {
                    let op = self.cell_op(&id);
                    cell.releasing = Some(op);
                    effects.push(Effect::ReleaseOwner {
                        op,
                        cell: id.clone(),
                        epoch,
                    });
                }
            }
            StopCause::Evict { .. } => {
                // A request arrived mid-eviction, so the cell turns straight
                // back around. The eviction is over either way.
                self.eviction_permits.remove(&id);
                self.admit_or_queue_activation(
                    &id,
                    &mut cell,
                    ColdStart::Restore(RestoreSpec {
                        epoch,
                        fresh: false,
                        took_over: false,
                        resume_local: false,
                    }),
                    effects,
                );
            }
            StopCause::Fence => unreachable!("fenced cells do not wait for runtime shutdown"),
        }
        self.cells.insert(id, cell);
        self.pump_capacity(effects);
        self.shed_toward_floor(effects);
    }

    fn durability_checked(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let now = self.now_mono_ms;
        let Some(id) = self.take_cell_op(op, |cell| {
            matches!(cell.phase, Phase::EnsuringDurability { op: current, .. } if current == op)
        }) else {
            return;
        };
        let cell = self.cells.get_mut(&id).expect("cell found above");
        let Phase::EnsuringDurability { epoch, .. } = cell.phase else {
            unreachable!()
        };
        match result {
            Ok(()) => {
                // Minted inline: `cell` still borrows the map, and field
                // accesses split where a method call cannot.
                let stop = self.next_op;
                self.next_op = self.next_op.checked_add(1).expect("operation id exhausted");
                self.cell_ops.insert(stop, id.clone());
                let rebalance = cell.evict_rebalance;
                set_phase(
                    &mut self.occupied,
                    cell,
                    Phase::Cleaning {
                        op: stop,
                        epoch,
                        cause: StopCause::Evict { rebalance },
                    },
                );
                effects.push(Effect::StopRuntime {
                    op: stop,
                    cell: id,
                    epoch,
                    cause: StopCause::Evict { rebalance },
                });
            }
            Err(_) => {
                set_phase(&mut self.occupied, cell, Phase::Resident { epoch });
                cell.eviction_refused_mono_ms = Some(now);
                self.eviction_permits.remove(&id);
            }
        }
    }

    /// Evict on demand. Local, like an idle eviction: the caller asked
    /// this node to drop the cell, not to give it away.
    fn evict(&mut self, id: &str, effects: &mut Vec<Effect>) {
        self.begin_eviction(id, false, effects);
    }

    /// Shutdown handoff: give every resident cell away by releasing its
    /// owner record, so peers take over at once instead of waiting out the
    /// node lease. `rebalance` is forced true, so this releases even when
    /// `ownership_on_evict` keeps ownership on an ordinary evict.
    ///
    /// Only marks the node draining; `pump_release` does the work, at most
    /// `max_releases` at a time. An unbounded walk here would start a
    /// durability proof per resident cell at once -- a node holding
    /// thousands turns its own shutdown into the restore storm the eviction
    /// bound exists to prevent, and trips the `validate` ceiling besides.
    fn release_all(&mut self, effects: &mut Vec<Effect>) {
        self.draining = true;
        self.pump_release(effects);
    }

    /// The drain pump, run after every event once draining: keep
    /// `max_releases` handoffs in flight while any owned cell remains.
    /// A cell that refuses eviction now -- active, or holding an uncovered
    /// alarm -- is skipped and retried on a later pump, so an active cell
    /// is handed off the moment its activity finishes. Releases in flight
    /// count against the same ceiling as the proofs: the bound exists to
    /// keep shutdown from flooding the store, and a release is one more
    /// write in that flood.
    fn pump_release(&mut self, effects: &mut Vec<Effect>) {
        if !self.draining {
            return;
        }
        let mut in_flight = self.eviction_permits.len() + self.releasing();
        for id in self.residents() {
            if in_flight >= self.config.max_releases {
                return;
            }
            if self.begin_eviction(&id, true, effects) {
                in_flight += 1;
            }
        }
        // A cell evicted before the drain kept its record on this node --
        // a sticky eviction, hibernation, or a release whose write failed
        // -- so a later request could resume locally. On a leaving node
        // that record only makes a successor wait out the node lease. The
        // runtime is already stopped, so the drain releases the record
        // directly.
        let dormant: Vec<(CellId, Epoch)> = self
            .cells
            .iter()
            .filter_map(|(id, cell)| match cell.phase {
                Phase::Dormant { epoch }
                    if cell.releasing.is_none() && cell.requests.is_empty() =>
                {
                    Some((id.clone(), epoch))
                }
                _ => None,
            })
            .collect();
        for (id, epoch) in dormant {
            if in_flight >= self.config.max_releases {
                return;
            }
            let op = self.cell_op(&id);
            let cell = self.cells.get_mut(&id).expect("dormant cell listed above");
            cell.releasing = Some(op);
            effects.push(Effect::ReleaseOwner {
                op,
                cell: id,
                epoch,
            });
            in_flight += 1;
        }
    }

    /// Does an eviction made for room hand the cell away?
    fn rebalances(&self) -> bool {
        self.config.ownership_on_evict == OwnershipOnEvict::Release
    }

    fn begin_eviction(&mut self, id: &str, rebalance: bool, effects: &mut Vec<Effect>) -> bool {
        // An alarm about to fire is not worth an eviction: the wake costs
        // more than the residency it would save, so the cell is held even
        // though its entry is perfectly durable. Coverage says the alarm can
        // survive the eviction; this says it is not worth surviving it.
        let alarm_is_imminent = self.cells.get(id).is_some_and(|cell| match cell.alarm {
            Some(AlarmState::Armed { at_ms, .. }) | Some(AlarmState::Firing { at_ms, .. }) => {
                at_ms >= 0
                    && (at_ms as u64).saturating_sub(self.now_ms) <= self.config.alarm_resident_ms
            }
            None => false,
        });
        // Only while there is room to spare. Holding a cell to save a wake is
        // worth it on an idle node and indefensible on a full one: the window
        // defaults to an hour, so on an alarm-driven workload this would hold
        // most of the node and pin it at its ceiling -- trading a real
        // admission failure for a saved activation. Under pressure the node
        // takes the wake.
        if alarm_is_imminent && !self.shedding && !self.draining {
            return false;
        }
        let alarm_is_safe = self.cells.get(id).is_some_and(|cell| {
            cell.alarm
                .as_ref()
                .is_none_or(|alarm| matches!(alarm, AlarmState::Armed { covered: true, .. }))
        });
        if !self.is_hibernatable(id) || !alarm_is_safe {
            return false;
        }
        let Some(Phase::Resident { epoch }) = self.cells.get(id).map(|cell| &cell.phase) else {
            return false;
        };
        let epoch = *epoch;
        let op = self.cell_op(id);
        let Some(cell) = self.cells.get_mut(id) else {
            return false;
        };
        cell.evict_rebalance = rebalance;
        set_phase(
            &mut self.occupied,
            cell,
            Phase::EnsuringDurability { op, epoch },
        );
        self.eviction_permits.insert(id.to_string());
        effects.push(Effect::EnsureDurable {
            op,
            cell: id.to_string(),
            epoch,
        });
        true
    }

    fn shed_one(&mut self, effects: &mut Vec<Effect>) {
        // Only when the blocker is headroom. A latched node is already walking
        // down on its own schedule, and admission stays closed until it
        // reaches the low watermark -- so shedding to make room for a waiter
        // that cannot be admitted has no stopping condition, and every
        // completed eviction re-enters here through `pump_capacity` and starts
        // another. That empties the node.
        //
        // Count the evictions already in flight against the waiters: this is
        // reachable from every activity finish and websocket close, and a
        // waiter whose eviction is already under way must not turn each of
        // those triggers into another victim. Spend the cut on commit, never
        // on nomination.
        if self.shedding
            || self.capacity_waiters.len() <= self.eviction_permits.len()
            || self.eviction_permits.len() >= self.config.max_evictions
        {
            return;
        }
        if let Some(victim) = self.shed_candidate() {
            self.begin_eviction(&victim, self.rebalances(), effects);
        }
    }

    /// The cell to shed right now: resident, idle, not holding an alarm the
    /// node still owes, and the least recently used of those. Demand shedding
    /// and pressure shedding both come through here, so they cannot disagree
    /// about what is safe to take or which one to take first.
    fn shed_candidate(&self) -> Option<CellId> {
        self.cells
            .iter()
            .filter(|(id, cell)| {
                self.is_hibernatable(id)
                    && cell.alarm.as_ref().is_none_or(|alarm| {
                        matches!(alarm, AlarmState::Armed { covered: true, .. })
                    })
            })
            // Cells that have never been refused first, then by how long ago
            // the refusal was, then least recently used, with the id as a
            // tiebreak so the choice stays a function of the state and not of
            // map iteration order. A refused cell is still reachable -- it
            // just stops being the answer every time.
            .min_by_key(|(id, cell)| {
                (
                    cell.eviction_refused_mono_ms,
                    cell.last_used_mono_ms,
                    (*id).clone(),
                )
            })
            .map(|(id, _)| id.clone())
    }

    /// Release one cell that has gone cold, with nothing asking for the room.
    ///
    /// Shedding answers "this node is in trouble"; this answers the ordinary
    /// question of a cell nobody has touched in a long time. Without it a node
    /// only ever gives a cell back under pressure or when another cell wants
    /// the slot, so a quiet node holds every cell it ever served until it
    /// reaches a watermark -- paying to keep runtimes alive for traffic that
    /// stopped hours ago.
    fn evict_idle(&mut self, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        let Some(idle_ms) = self.config.idle_evict_ms else {
            return;
        };
        let Some(candidate) = self.shed_candidate() else {
            return;
        };
        let cold = self
            .cells
            .get(&candidate)
            .is_some_and(|cell| now_mono_ms.saturating_sub(cell.last_used_mono_ms) >= idle_ms);
        if cold {
            // Idle eviction is a local residency decision, not a handoff.
            self.begin_eviction(&candidate, false, effects);
        }
    }

    /// Fold a resource sample into the shedding latch, then act on it.
    ///
    /// The latch is the whole point. Shedding on the instantaneous crossing
    /// alone flaps: the eviction relieves the pressure, admission resumes, the
    /// ceiling is crossed again. `PressureConfig` holds the node in shedding
    /// until every configured low watermark clears, so the node walks down to
    /// its target instead of oscillating around its ceiling.
    fn load_sampled(&mut self, load: pressure::Load, now_mono_ms: u64, effects: &mut Vec<Effect>) {
        // Both latches are folded, and the reason names the more serious of the
        // two. The latches are state of their own: derived from the reason
        // instead, one crossing holds the node against the other's watermark.
        let (latches, reason) = self.config.pressure.classify(load, self.latches);
        self.latches = latches;
        self.shed_reason = reason;
        self.shedding = reason.is_some();
        if !self.shedding {
            // Relieved. Whatever was queued for capacity may proceed.
            self.shed_cut = None;
            self.pump_capacity(effects);
            self.evict_idle(now_mono_ms, effects);
            return;
        }
        // Evicting only helps if it returns memory. When the last cut has
        // fully landed and this sample's in-use bytes sit within 5% of what
        // that cut measured, another cut is futile: the latch holds -- the node
        // genuinely is over its ceiling, so admission stays closed -- but the
        // walk down stops spending the working set. Without this stopping
        // condition an unsatisfiable ceiling (one below the process's memory
        // floor) evicts a proportion of whatever remains on every sample and
        // walks the node to zero -- a latched walk down with no stopping
        // condition, the same shape as demand shedding for a waiter that can
        // never be admitted. A
        // sample that moves either way re-arms the walk down.
        //
        // The comparison reads whichever measurement the walk down works
        // against. The ordinary ceiling wins whenever it is latched, even if
        // the cap is latched too: eviction relieves the ordinary ceiling and
        // may do nothing at all for the cap, so reading the cap's number while
        // the node is genuinely over its ordinary ceiling stops the walk down
        // exactly when it is needed. Under the cap alone the in-use figure is
        // small and moves freely, so reading it there would find every sample
        // "not flat" and drain the working set against a condition eviction
        // cannot relieve.
        //
        // A change of measurement discards the baseline. The two numbers are
        // not comparable, and on this fleet they sit within 2% of each other,
        // so comparing across them finds them flat by coincidence and
        // suppresses every further cut.
        let metric = pressure::PressureConfig::walk_metric(latches);
        let sample_bytes = match metric {
            pressure::Metric::InUse => load.in_use_bytes,
            pressure::Metric::Rss => load.rss_bytes,
        };
        if self
            .shed_cut
            .is_some_and(|(previous, _, _)| previous != metric)
        {
            self.shed_cut = None;
        }
        if let Some((_, cut_bytes, cut_cells)) = self.shed_cut {
            let cut_landed = self.occupied() <= self.shed_floor && self.eviction_permits.is_empty();
            // Did the last cut return anything? It is only evidence if it
            // actually vacated cells: a walk down that has already stopped
            // vacates nothing, and reading that as "no progress" would make the
            // stop hold because it held.
            let removed = cut_cells.saturating_sub(self.occupied());
            // A tolerance, not a threshold. It absorbs the jitter of a live
            // sample; it does not decide how much a cut must achieve. The old
            // condition used 5% of the sample as a threshold, and that is the
            // fault: a cut can help and still move a large sample by less.
            let tolerance = cut_bytes / 100;
            let fell = sample_bytes.saturating_add(tolerance) < cut_bytes;
            let rose = sample_bytes > cut_bytes.saturating_add(tolerance);
            if cut_landed && removed > 0 && !fell && !rose {
                return;
            }
        }
        // How far this resource sample asks the node to come down: a proportion
        // of what was just measured, because the effect of an eviction on the
        // sample is not visible until the next one.
        self.shed_floor = pressure::PressureConfig::release_target(load.resident_cells);
        self.shed_cut = Some((metric, sample_bytes, load.resident_cells));
        self.shed_toward_floor(effects);
    }

    // The walk down's stopping condition lives in `load_sampled` above: a cut
    // that vacated cells and did not lower the sample is futile, and the node
    // holds what it has rather than spending the working set on a ceiling that
    // eviction cannot satisfy.
    //
    // It asks whether the last cut *helped*, not whether it helped by some
    // percentage. The cut is a proportion of the cells and the sample includes
    // memory the cells do not own, so a cut that helps can still move the
    // sample by very little -- and stopping there leaves the node above its own
    // resume line with idle cells in hand, refusing every request for a cell.
    // That is issue #50, measured on a fleet as nine hours of exactly that, and
    // on one node as a walk down that stopped 14 MB short of its line.

    /// Continue a latched walk down as each eviction lands.
    ///
    /// `shed_one` stands down while the latch is hot, because its stopping
    /// condition -- the waiter got in -- cannot be met until the node is
    /// relieved. This is the other half: a stopping condition that can be met,
    /// so the node reaches its floor at the speed evictions complete rather
    /// than one per sampling period. Serialized like every other eviction
    /// path, so a walk down never puts the whole working set in flight.
    fn shed_toward_floor(&mut self, effects: &mut Vec<Effect>) {
        if !self.shedding || self.occupied() <= self.shed_floor {
            return;
        }
        // Fill the permits rather than starting one and waiting for it. Each
        // proof is a round trip, so a serialized drain costs the number of
        // cells times that latency; running the bound's worth at once is the
        // difference between a walk down measured in seconds and one measured
        // in minutes.
        // Count what is already leaving against the target. `occupied` still
        // includes a cell whose proof is in flight, so comparing it directly
        // nominates cells the evictions already under way will account for,
        // and the node settles below its floor by up to the whole bound --
        // the mistake celld's eviction budget documents: spend the cut on
        // commit, never on nomination.
        while self.eviction_permits.len() < self.config.max_evictions
            && self.occupied().saturating_sub(self.eviction_permits.len()) > self.shed_floor
        {
            let Some(victim) = self.shed_candidate() else {
                return;
            };
            if !self.begin_eviction(&victim, self.rebalances(), effects) {
                return;
            }
        }
    }

    fn activity_finished(&mut self, request: RequestId, effects: &mut Vec<Effect>) {
        // A write still on the output gate keeps its request pinned, so the
        // cell cannot be evicted before the write is proven durable. The unpin
        // moves to whichever path drains the last gate for this request.
        if self
            .gated_writes
            .values()
            .any(|gate| gate.request == request)
        {
            self.gate_pinned.insert(request);
            return;
        }
        if let Some(id) = self.deactivate_request(request) {
            let now = self.now_mono_ms;
            if let Some(cell) = self.cells.get_mut(&id) {
                cell.last_used_mono_ms = now;
            }
        }
        self.shed_one(effects);
    }

    /// Open the output gate for a local write: hold its response until the
    /// cell's committed `position` is proven replicated. The request must still
    /// be a live local activity on its cell, resident at the epoch that
    /// committed the write; otherwise durability cannot be proven for it and
    /// the response fails rather than falsely acknowledging the write.
    fn wrote(&mut self, request: RequestId, position: u64, effects: &mut Vec<Effect>) {
        let held = self.active_requests.get(&request).and_then(|id| {
            match self.cells.get(id).map(|cell| &cell.phase) {
                Some(Phase::Resident { epoch }) => Some((id.clone(), *epoch)),
                _ => None,
            }
        });
        let Some((cell, epoch)) = held else {
            effects.push(Effect::ReleaseResponse {
                request,
                result: Err(RequestError::DurabilityUnproven),
            });
            return;
        };
        let op = self.op();
        self.gated_writes.insert(
            op,
            GatedWrite {
                request,
                cell: cell.clone(),
                epoch,
                position,
            },
        );
        effects.push(Effect::AwaitDurable {
            op,
            cell,
            epoch,
            position,
        });
    }

    /// A gated write's durability proof completed. Acknowledge the write only
    /// when the replica proved a position that *covers* it — a shorter proof
    /// (a lagging or lying replicator) fails it rather than acknowledging a
    /// write the node cannot actually restore. Any error fails it. A completion
    /// for a gate already drained (fence or deadline) is ignored — the
    /// versioned-op discipline used throughout the core.
    fn durable_reached(
        &mut self,
        op: OpId,
        result: Result<u64, Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some(gate) = self.gated_writes.remove(&op) else {
            return;
        };
        let proven = matches!(result, Ok(durable) if durable >= gate.position);
        let result = if proven {
            Ok(())
        } else {
            Err(RequestError::DurabilityUnproven)
        };
        // Unpin before releasing: the cleanup this ends -- shedding, eviction --
        // is queued ahead of the response, so a caller that sees its write
        // acknowledged sees the residency it released too. `activity_finished`
        // re-checks the gate map, so a request holding a second gated write
        // simply re-pins itself here.
        if self.gate_pinned.remove(&gate.request) {
            self.activity_finished(gate.request, effects);
        }
        effects.push(Effect::ReleaseResponse {
            request: gate.request,
            result,
        });
        if !proven {
            let (id, epoch) = (gate.cell.clone(), gate.epoch);
            self.reset_cell(&id, epoch, effects);
        }
    }

    /// Discard a runtime whose durability could not be proved.
    ///
    /// The caller has been told its write failed. If the cell stays resident,
    /// the very next read is served from the same unreplicated memory and
    /// returns that write anyway -- the node disagreeing with what it just
    /// told a client. Stopping with [`StopCause::Reset`] keeps no local
    /// snapshot, so the next activation restores from the bucket.
    ///
    /// Note this is a stop, not a truncation. The restored database is not
    /// invalidated by a failed proof; only this node's unreplicated additions
    /// to it are in doubt, and re-reading durable state is how they are
    /// discarded.
    fn reset_cell(&mut self, id: &str, epoch: Epoch, effects: &mut Vec<Effect>) {
        if !matches!(
            self.cells.get(id).map(|cell| &cell.phase),
            Some(Phase::Resident { epoch: current }) if *current == epoch
        ) {
            return;
        }
        // Every other write still on the gate for this cell loses its runtime
        // here, so none of them may be acknowledged either.
        let doomed: Vec<OpId> = self
            .gated_writes
            .iter()
            .filter(|(_, gate)| gate.cell == id && gate.epoch == epoch)
            .map(|(op, _)| *op)
            .collect();
        for op in doomed {
            let Some(gate) = self.gated_writes.remove(&op) else {
                continue;
            };
            self.gate_pinned.remove(&gate.request);
            effects.push(Effect::ReleaseResponse {
                request: gate.request,
                result: Err(RequestError::DurabilityUnproven),
            });
        }
        // Requests already running against this runtime lose it here, exactly
        // as a fence does. Leaving them in `active_requests` would leave the
        // core believing a stopped cell is still serving, which its own
        // invariant catches.
        let running: Vec<RequestId> = self
            .active_requests
            .iter()
            .filter(|(_, cell)| cell.as_str() == id)
            .map(|(request, _)| *request)
            .collect();
        for request in running {
            self.deactivate_request(request);
            self.gate_pinned.remove(&request);
        }
        let op = self.cell_op(id);
        if let Some(cell) = self.cells.get_mut(id) {
            // The phase being replaced may hold an op that will now never
            // complete against it; its index entry must not outlive it.
            if let Some(stale) = phase_op(&cell.phase) {
                self.cell_ops.remove(&stale);
            }
            set_phase(
                &mut self.occupied,
                cell,
                Phase::Cleaning {
                    op,
                    epoch,
                    cause: StopCause::Reset,
                },
            );
        }
        effects.push(Effect::StopRuntime {
            op,
            cell: id.to_string(),
            epoch,
            cause: StopCause::Reset,
        });
    }

    /// How many cells are currently held resident by a non-hibernatable
    /// transport.
    fn outbound_pinned(&self) -> usize {
        self.cells
            .values()
            .filter(|cell| {
                cell.websockets
                    .values()
                    .any(|kind| *kind == WebSocketKind::Outbound)
            })
            .count()
    }

    fn websocket_opened(
        &mut self,
        id: &str,
        websocket: WebSocketId,
        kind: WebSocketKind,
        effects: &mut Vec<Effect>,
    ) {
        if self.fenced {
            return;
        }
        // A non-hibernatable transport holds its cell resident for as long as
        // it is open, so every one of them is a cell the node can never shed.
        // Pin the whole ceiling and there is nothing left to nominate:
        // residency cannot fall, admission waits on capacity that will never
        // be freed, and the node is wedged by its own applications. Cells that
        // already hold one are not counted again -- the budget is on how much
        // of the node is held, not on how many sockets exist.
        let already_pinned = self.cells.get(id).is_some_and(|cell| {
            cell.websockets
                .values()
                .any(|kind| *kind == WebSocketKind::Outbound)
        });
        let cell_outbound = self.cells.get(id).map_or(0, |cell| {
            cell.websockets
                .values()
                .filter(|kind| **kind == WebSocketKind::Outbound)
                .count()
        });
        if kind == WebSocketKind::Outbound
            && (cell_outbound >= self.config.max_outbound_websockets
                || (!already_pinned
                    && !pressure::may_pin_outbound(
                        self.outbound_pinned(),
                        Some(self.config.max_resident),
                    )))
        {
            effects.push(Effect::CloseWebSocket {
                cell: id.to_string(),
                websocket,
            });
            return;
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        cell.websockets.insert(websocket, kind);
    }

    fn websocket_closed(&mut self, id: &str, websocket: WebSocketId, effects: &mut Vec<Effect>) {
        if let Some(cell) = self.cells.get_mut(id) {
            cell.websockets.remove(&websocket);
        }
        self.shed_one(effects);
    }

    fn invalidate_remote(&mut self, id: &str, node: &str, epoch: Epoch) {
        self.node_lease_cache.remove(node);
        let rejected_sample = self.cells.get(id).and_then(|cell| match &cell.phase {
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                capacity_sampled_ms,
                ..
            } if current == node && *current_epoch == epoch => *capacity_sampled_ms,
            _ => None,
        });
        if let Some(sample) = rejected_sample {
            self.capacity_rejections.insert(node.to_string(), sample);
            self.capacity_reservations.remove(node);
        }
        let Some(cell) = self.cells.get_mut(id) else {
            return;
        };
        if matches!(
            &cell.phase,
            Phase::Remote {
                node: current,
                epoch: current_epoch,
                ..
            } if current == node && *current_epoch == epoch
        ) {
            set_phase(&mut self.occupied, cell, Phase::Inactive);
        }
    }

    fn fence(&mut self, effects: &mut Vec<Effect>) {
        if self.fenced {
            return;
        }
        self.fenced = true;
        self.activation_waiters.clear();
        self.activation_permits.clear();
        self.capacity_waiters.clear();
        self.active_requests.clear();
        self.active_cells.clear();
        // Any write still waiting on the output gate loses its cell here, so it
        // must fail rather than be acknowledged — the fence and the fail are
        // atomic. A late DurableReached for a drained op is ignored.
        self.gate_pinned.clear();
        for (_, gate) in std::mem::take(&mut self.gated_writes) {
            effects.push(Effect::ReleaseResponse {
                request: gate.request,
                result: Err(RequestError::NodeFenced),
            });
        }
        for (request, _) in std::mem::take(&mut self.node_lease_waiters) {
            effects.push(Effect::Complete {
                request,
                result: Err(RequestError::NodeFenced),
            });
        }
        self.node_wake_waiters.clear();
        let ids: Vec<CellId> = self.cells.keys().cloned().collect();
        for id in ids {
            let mut cell = self.cells.remove(&id).expect("id came from map");
            match &cell.phase {
                Phase::Starting { op, epoch } | Phase::Publishing { op, epoch } => {
                    self.retired_runtime_ops.insert(*op, (id.clone(), *epoch));
                }
                _ => {}
            }
            if let Some(epoch) = runtime_epoch(&cell.phase) {
                let op = self.op();
                effects.push(Effect::StopRuntime {
                    op,
                    cell: id.clone(),
                    epoch,
                    cause: StopCause::Fence,
                });
            }
            self.finish_requests(&id, &mut cell, Err(RequestError::NodeFenced), effects);
            // The phase and alarm die here without completing; their ops can
            // never match a lookup again. The release op survives on purpose:
            // `owner_released` still consumes it after a fence, as it did
            // when resolution was a scan.
            if let Some(stale) = phase_op(&cell.phase) {
                self.cell_ops.remove(&stale);
            }
            if let Some(AlarmState::Firing { op, .. }) = cell.alarm {
                self.cell_ops.remove(&op);
            }
            set_phase(&mut self.occupied, &mut cell, Phase::Fenced);
            cell.waiting_for = None;
            cell.waiting_activation = None;
            cell.alarm = None;
            cell.alarm_wake = false;
            cell.websockets.clear();
            self.cells.insert(id, cell);
        }
    }

    fn compensate_retired_runtime(
        &mut self,
        op: OpId,
        result: Result<(), Failure>,
        effects: &mut Vec<Effect>,
    ) {
        let Some((cell, epoch)) = self.retired_runtime_ops.remove(&op) else {
            return;
        };
        // Definite failure created nothing. Success or ambiguity may have
        // created/published a runtime after authority was revoked, so cleanup
        // is mandatory and idempotent.
        if result != Err(Failure::Definite) {
            let cleanup = self.op();
            effects.push(Effect::StopRuntime {
                op: cleanup,
                cell,
                epoch,
                cause: StopCause::Cleanup,
            });
        }
    }
}

/// The sole state transition entry point.
/// The monotonic instant an event carries, if it carries one. Events without
/// a timestamp leave the remembered instant alone rather than resetting it.
/// The wall-clock reading an event carried, if it carried one.
fn event_now_ms(event: &Event) -> Option<u64> {
    match event {
        Event::StartNodeLease { now_ms, .. }
        | Event::SelfNodeLeaseRead { now_ms, .. }
        | Event::OwnerRead { now_ms, .. }
        | Event::NodeLeaseRead { now_ms, .. }
        | Event::TimerFired { now_ms, .. }
        | Event::AlarmObserved { now_ms, .. }
        | Event::AlarmFinished { now_ms, .. } => Some(*now_ms),
        _ => None,
    }
}

fn event_mono_ms(event: &Event) -> Option<u64> {
    match event {
        Event::SelfNodeLeaseRead { now_mono_ms, .. }
        | Event::NodeLeaseCasCompleted { now_mono_ms, .. }
        | Event::RequestAt { now_mono_ms, .. }
        | Event::CapacityRequestAt { now_mono_ms, .. }
        | Event::WakeHintAt { now_mono_ms, .. }
        | Event::TimerFired { now_mono_ms, .. }
        | Event::AlarmObserved { now_mono_ms, .. }
        | Event::AlarmFinished { now_mono_ms, .. }
        | Event::LoadSampled { now_mono_ms, .. } => Some(*now_mono_ms),
        _ => None,
    }
}

pub fn on_event(state: &mut State, event: Event) -> Vec<Effect> {
    let mut effects = Vec::new();
    if let Some(now_mono_ms) = event_mono_ms(&event) {
        state.now_mono_ms = state.now_mono_ms.max(now_mono_ms);
    }
    if let Some(now_ms) = event_now_ms(&event) {
        state.now_ms = state.now_ms.max(now_ms);
    }
    match event {
        Event::StartNodeLease { now_ms, spec } => {
            state.start_node_lease(now_ms, spec, &mut effects)
        }
        Event::SelfNodeLeaseRead {
            op,
            now_ms,
            now_mono_ms,
            result,
        } => state.read_self_node_lease(op, now_ms, now_mono_ms, result, &mut effects),
        Event::NodeLeaseCasCompleted {
            op,
            now_mono_ms,
            result,
        } => state.node_lease_cas_completed(op, now_mono_ms, result, &mut effects),
        Event::LocalCellsRead { result } => state.local_cells_read(result, &mut effects),
        Event::TimerFired {
            timer,
            now_ms,
            now_mono_ms,
        } => state.timer_fired(timer, now_ms, now_mono_ms, &mut effects),
        Event::Request { request, cell } => state.request(request, cell, 0, false, &mut effects),
        Event::RequestAt {
            request,
            cell,
            now_ms,
            ..
        } => state.request(request, cell, now_ms, false, &mut effects),
        Event::CapacityRequestAt {
            request,
            cell,
            now_ms,
            ..
        } => state.request(request, cell, now_ms, true, &mut effects),
        Event::WorkerRequest { request } => state.worker_request(request, &mut effects),
        Event::BeginPreserve => state.begin_preserve(&mut effects),
        Event::Cancel { request } => state.cancel(request),
        Event::ActivityFinished { request } => state.activity_finished(request, &mut effects),
        Event::Wrote { request, position } => state.wrote(request, position, &mut effects),
        Event::DurableReached { op, result } => state.durable_reached(op, result, &mut effects),
        Event::WebSocketOpened {
            cell,
            websocket,
            kind,
        } => state.websocket_opened(&cell, websocket, kind, &mut effects),
        Event::WebSocketClosed { cell, websocket } => {
            state.websocket_closed(&cell, websocket, &mut effects)
        }
        Event::AlarmObserved {
            cell,
            at_ms,
            covered,
            now_ms,
            now_mono_ms,
        } => state.alarm_observed(&cell, at_ms, covered, now_ms, now_mono_ms, &mut effects),
        Event::AlarmFinished {
            op,
            now_ms,
            now_mono_ms,
            result,
        } => state.alarm_finished(op, now_ms, now_mono_ms, result, &mut effects),
        Event::WakeHint { cell } => state.wake_hint(cell, 0, &mut effects),
        Event::WakeHintAt { cell, now_ms, .. } => state.wake_hint(cell, now_ms, &mut effects),
        Event::OwnerRead { op, now_ms, result } => {
            state.owner_read(op, now_ms, result, &mut effects)
        }
        Event::NodeLeaseRead { op, now_ms, result } => {
            state.node_lease_read(op, now_ms, result, &mut effects)
        }
        Event::CapacityPeersRead { op, now_ms, result } => {
            state.capacity_peers_read(op, now_ms, result, &mut effects)
        }
        Event::OwnerCasCompleted { op, result } => {
            state.owner_cas_completed(op, result, &mut effects)
        }
        Event::OwnerReleased { op, result } => state.owner_released(op, result),
        Event::RestoreCompleted { op, result } => state.restore_completed(op, result, &mut effects),
        Event::RuntimeStarted { op, result } => state.runtime_started(op, result, &mut effects),
        Event::Published { op, result } => state.published(op, result, &mut effects),
        Event::DurabilityChecked { op, result } => {
            state.durability_checked(op, result, &mut effects)
        }
        Event::RuntimeStopped { op } => state.runtime_stopped(op, &mut effects),
        Event::Evict { cell } => state.evict(&cell, &mut effects),
        Event::LoadSampled { load, now_mono_ms } => {
            state.load_sampled(load, now_mono_ms, &mut effects)
        }
        Event::InvalidateRemote { cell, node, epoch } => {
            state.invalidate_remote(&cell, &node, epoch)
        }
        Event::NodeFenced => state.fence_node(&mut effects),
        Event::ReleaseAll => state.release_all(&mut effects),
    }
    state.pump_activations(&mut effects);
    state.pump_release(&mut effects);
    state.update_node_lease_dependencies(state.now_mono_ms);
    if cfg!(debug_assertions) {
        state.validate().expect("state invariant");
    }
    state.arm_operation_deadlines(&mut effects);
    effects
}

fn same_node_lease(left: &NodeLeaseRecord, right: &NodeLeaseRecord) -> bool {
    left.node == right.node
        && left.addr == right.addr
        && left.expires_ms == right.expires_ms
        && left.peer_protocol == right.peer_protocol
        && left.generation == right.generation
}

fn phase_occupies_capacity(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::EnsuringDurability { .. }
            | Phase::Cleaning { .. }
            | Phase::Resident { .. }
    )
}

fn phase_holds_activation(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::Cleaning { .. }
    )
}

fn phase_depends_on_node_lease(phase: &Phase) -> bool {
    matches!(
        phase,
        Phase::WaitingActivation
            | Phase::ReadingOwner { .. }
            | Phase::ReadingNodeLease { .. }
            | Phase::ReadingCapacity { .. }
            | Phase::WaitingCapacity
            | Phase::Acquiring { .. }
            | Phase::ReconcilingAcquire { .. }
            | Phase::Restoring { .. }
            | Phase::Starting { .. }
            | Phase::Publishing { .. }
            | Phase::EnsuringDurability { .. }
            | Phase::Cleaning { .. }
            | Phase::Resident { .. }
    )
}

/// The one gate every phase change passes through: it keeps `occupied`
/// equal to the number of cells in a capacity-occupying phase, so nothing
/// ever has to count them. `validate` asserts the equality by walking.
fn set_phase(occupied: &mut usize, cell: &mut Cell, phase: Phase) {
    match (
        phase_occupies_capacity(&cell.phase),
        phase_occupies_capacity(&phase),
    ) {
        (false, true) => *occupied += 1,
        (true, false) => *occupied -= 1,
        _ => {}
    }
    cell.phase = phase;
}

/// The in-flight operation a phase is waiting on, if it is waiting on one.
fn phase_op(phase: &Phase) -> Option<OpId> {
    match phase {
        Phase::ReadingOwner { op }
        | Phase::ReadingNodeLease { op, .. }
        | Phase::ReadingCapacity { op, .. }
        | Phase::Acquiring { op, .. }
        | Phase::ReconcilingAcquire { op, .. }
        | Phase::Restoring { op, .. }
        | Phase::Starting { op, .. }
        | Phase::Publishing { op, .. }
        | Phase::EnsuringDurability { op, .. }
        | Phase::Cleaning { op, .. } => Some(*op),
        _ => None,
    }
}

fn runtime_epoch(phase: &Phase) -> Option<Epoch> {
    match phase {
        Phase::Publishing { epoch, .. }
        | Phase::EnsuringDurability { epoch, .. }
        | Phase::Cleaning { epoch, .. }
        | Phase::Resident { epoch } => Some(*epoch),
        _ => None,
    }
}

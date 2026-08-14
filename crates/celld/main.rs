// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Runnable celld vertical slice.
//!
//! One actor serializes every event through `celld-logic`; the actor polls its
//! mailbox, timers, and in-flight effect futures together. This is the
//! execution shape required for monotonic lease ticks to fence the node even
//! when a storage operation remains hung, without spawning a task per effect.

use anyhow::Context as _;
use base64::Engine as _;
use celld::assets::AssetResolver;
use celld::fleet;
use celld::js::{
    ArmGate, AssetCallReq, Compat, DoCallReq, HttpResponse, RpcCallReq, SvcCallReq, SvcRpcReq,
    WorkerConfigOptions, WsOut,
};
use celld::ownership_store::{now_ms, BucketOwnership};
use celld::peer_auth::{self, PeerAuth};
use celld::runtime::{CohostedWorker, Replication, RuntimeFetch, RuntimeManager, RuntimeOptions};
use celld_logic::{
    on_event, CasGuard, CasOutcome, Config, Effect, Event, Failure, LeaseCasOutcome, NodeLeaseMode,
    NodeLeaseRecord, NodeLeaseSpec, OpId, OwnerRecord, OwnershipOnEvict, Phase, RequestError,
    Route, State, StopCause, Timer, WebSocketKind, WorkerRoute,
};
use futures_util::stream::{FuturesUnordered, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use rand::RngCore;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch, Mutex};
use tokio_util::time::{delay_queue, DelayQueue};

// glibc's malloc serializes its arenas behind futexes, and under load the
// sixteen worker threads spent up to half a millisecond blocked per
// acquisition. On a 16-core host jemalloc measured 20% more hello-world
// throughput than glibc (mimalloc 11%), and returned the ~7% of the machine
// that arena-lock sleeps reported as idle.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Process-wide request identities shared by the HTTP shell and the actor.
/// The shell allocates an identity before it submits a cold route, so dropping
/// that route can cancel the exact core request without waiting for routing to
/// finish first.
static NEXT_CORE_REQUEST: AtomicU64 = AtomicU64::new(1);

/// Let an admitted HTTP request finish, then close the transport even when a
/// response stream or a client keep-alive does not settle. The semantic drain
/// continues after this bound, so durability and resident activity still use
/// the complete shutdown grace.
const CONNECTION_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// The lossy stdout writer's flush handle. Every exit path uses
/// `std::process::exit`, which skips destructors — but the last lines before
/// an exit are the fence forensics, exactly the lines that must survive.
static LOG_GUARD: std::sync::Mutex<Option<tracing_appender::non_blocking::WorkerGuard>> =
    std::sync::Mutex::new(None);

fn exit_flushed(code: i32) -> ! {
    drop(LOG_GUARD.lock().unwrap().take());
    std::process::exit(code);
}

fn next_core_request() -> u64 {
    NEXT_CORE_REQUEST.fetch_add(1, Ordering::Relaxed)
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum TimerSlot {
    NodeLeaseRenew,
    NodeLeaseFence,
    CellAlarm(String),
    /// Keyed by operation, deliberately. Every other timer here coalesces
    /// because only its newest arming matters; a deadline is the opposite --
    /// each watches a different outstanding operation, and a shared slot
    /// would let arming one silently cancel another, leaving every activation
    /// but the most recent with nothing watching it.
    OperationDeadline(OpId),
}

impl TimerSlot {
    fn of(timer: &Timer) -> Self {
        match timer {
            Timer::NodeLeaseRenew { .. } => Self::NodeLeaseRenew,
            Timer::NodeLeaseFence { .. } => Self::NodeLeaseFence,
            Timer::CellAlarm { cell, .. } => Self::CellAlarm(cell.clone()),
            Timer::OperationDeadline { op } => Self::OperationDeadline(*op),
        }
    }
}

struct MemoryOwnership {
    node: String,
    owners: BTreeMap<String, OwnerRecord>,
    leases: BTreeMap<String, NodeLeaseRecord>,
    next_etag: u64,
}

#[derive(Clone)]
enum Ownership {
    Memory(Arc<Mutex<MemoryOwnership>>),
    Bucket(Arc<BucketOwnership>),
}

impl Ownership {
    async fn read_owner(&self, cell: &str) -> Result<Option<OwnerRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.owners.get(cell).cloned()),
            Self::Bucket(bucket) => bucket.read_owner(cell).await.map_err(|error| {
                eprintln!("celld ownership read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_node_lease(&self, owner: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(owner).cloned()),
            Self::Bucket(bucket) => bucket.read_node_lease(owner).await.map_err(|error| {
                eprintln!("celld node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_capacity_peers(&self) -> Result<Vec<celld_logic::CapacityPeer>, Failure> {
        match self {
            // The in-memory adapter is a single-node development mode. It has
            // no external membership enumeration to offer.
            Self::Memory(_) => Ok(Vec::new()),
            Self::Bucket(bucket) => bucket.read_capacity_peers().await.map_err(|error| {
                eprintln!("celld capacity peer read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn read_self_node_lease(&self, node: &str) -> Result<Option<NodeLeaseRecord>, Failure> {
        match self {
            Self::Memory(memory) => Ok(memory.lock().await.leases.get(node).cloned()),
            Self::Bucket(bucket) => bucket.read_self_node_lease(node).await.map_err(|error| {
                eprintln!("celld self node lease read failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_owner(
        &self,
        cell: &str,
        guard: CasGuard,
        epoch: u64,
    ) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let allowed = match guard {
                    CasGuard::Absent => !memory.owners.contains_key(cell),
                    CasGuard::Match(expected) => memory
                        .owners
                        .get(cell)
                        .is_some_and(|owner| owner.etag == expected),
                };
                if allowed {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    let node = memory.node.clone();
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: Some(node),
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if allowed {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            Self::Bucket(bucket) => bucket.cas_owner(cell, guard, epoch).await.map_err(|error| {
                // Any transport or 5xx failure may have happened after the
                // store committed. The core reconciles by reading the owner
                // again.
                eprintln!("celld ownership CAS ambiguous: {error:#}");
                Failure::Ambiguous
            }),
        }
    }

    async fn release_owner(&self, cell: &str, epoch: u64) -> Result<CasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let node = memory.node.clone();
                let releasable = memory.owners.get(cell).is_some_and(|owner| {
                    owner.node.as_deref() == Some(node.as_str()) && owner.epoch == epoch
                });
                if releasable {
                    let etag = format!("e{}", memory.next_etag);
                    memory.next_etag += 1;
                    memory.owners.insert(
                        cell.into(),
                        OwnerRecord {
                            node: None,
                            epoch,
                            etag,
                        },
                    );
                }
                Ok(if releasable {
                    CasOutcome::Applied
                } else {
                    CasOutcome::Rejected
                })
            }
            // A release that may or may not have committed needs no
            // reconciliation: the record either still names this node, and the
            // next eviction releases it again, or it does not, and the cell is
            // already free. Either way nothing is owed.
            Self::Bucket(bucket) => bucket.release_owner(cell, epoch).await.map_err(|error| {
                eprintln!("celld ownership release failed: {error:#}");
                Failure::Definite
            }),
        }
    }

    async fn cas_node_lease(
        &self,
        guard: CasGuard,
        mut record: NodeLeaseRecord,
    ) -> Result<LeaseCasOutcome, Failure> {
        match self {
            Self::Memory(memory) => {
                let mut memory = memory.lock().await;
                let current = memory.leases.get(&record.node);
                let allowed = match guard {
                    CasGuard::Absent => current.is_none(),
                    CasGuard::Match(expected) => {
                        current.is_some_and(|lease| lease.etag == expected)
                    }
                };
                if !allowed {
                    return Ok(LeaseCasOutcome::Rejected);
                }
                let etag = format!("e{}", memory.next_etag);
                memory.next_etag += 1;
                record.etag = etag.clone();
                memory.leases.insert(record.node.clone(), record);
                Ok(LeaseCasOutcome::Applied { etag })
            }
            Self::Bucket(bucket) => bucket
                .cas_node_lease(guard, &record)
                .await
                .map_err(|error| {
                    eprintln!("celld node-lease CAS ambiguous: {error:#}");
                    Failure::Ambiguous
                }),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Self::Memory(_) => "memory",
            Self::Bucket(bucket) => bucket.storage_scheme(),
        }
    }
}

enum Message {
    /// A periodic resource sample. The measuring is the shell's job; every
    /// decision that follows belongs to the core.
    SampleLoad,
    /// Stop new internal routing while a clean same-node reload drains. The
    /// request shell has already stopped admission; this closes wake and
    /// service paths that do not pass through the HTTP gate.
    BeginPreserve,
    /// Graceful shutdown: release every owned cell's record so peers take
    /// over immediately. The release writes run as effects inside the
    /// shutdown drain window, and the drain waits for them to complete —
    /// bounded by the drain deadline, so a wedged store still cannot hold
    /// the exit hostage.
    ReleaseAll,
    /// Has the shutdown handoff finished? True once no cell occupies
    /// capacity: nothing resident, restoring, or mid-eviction. The drain
    /// loop polls this to exit as soon as the handoff completes instead of
    /// sitting out the full drain window.
    Drained {
        reply: oneshot::Sender<DrainStatus>,
    },
    Request {
        request: u64,
        cell: String,
        capacity_handoff: bool,
        reply: oneshot::Sender<Result<Routed, RequestError>>,
    },
    /// A caller disappeared before routing finished. The request identity is
    /// allocated by the shell, so the actor can remove it from either cold
    /// admission queue immediately.
    CancelRoute {
        request: u64,
    },
    WorkerRequest {
        reply: oneshot::Sender<WorkerRouted>,
    },
    ActivityFinished {
        request: u64,
        // The activity drop also observes the cell's alarm. Folded in here so a
        // request's completion costs the serial core one message, not two — the
        // hot path for every routed DO call.
        cell: String,
        alarm_at_ms: Option<i64>,
        alarm_covered: bool,
    },
    /// A local request whose handler advanced its cell's committed-write
    /// position wrote: open the output gate and hold the response until durable.
    GateWrite {
        request: u64,
        position: u64,
        reply: oneshot::Sender<Result<(), RequestError>>,
    },
    /// A `webSocketMessage` finished; hand its captured outbound frames to the
    /// cell's output gate. `write_position` present means the handler wrote, so
    /// the frames are held behind that write until it is durable; absent means
    /// flush them (behind any earlier still-pending write, else immediately).
    WsOutput {
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
        reply: oneshot::Sender<()>,
    },
    WebSocketOpened {
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
        reply: oneshot::Sender<bool>,
    },
    WebSocketClosed {
        cell: String,
        websocket: u64,
    },
    AlarmObserved {
        cell: String,
        at_ms: Option<i64>,
        covered: bool,
    },
    WakeHint {
        cell: String,
    },
    Evict {
        cell: String,
        reply: oneshot::Sender<()>,
    },
    InvalidateRemote {
        cell: String,
        node: String,
        epoch: u64,
        reply: oneshot::Sender<()>,
    },
    Snapshot {
        reply: oneshot::Sender<String>,
    },
    Health {
        reply: oneshot::Sender<bool>,
    },
    Presence {
        reply: oneshot::Sender<celld_logic::PresenceSnapshot>,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShutdownMode {
    /// The logical node is leaving. Release every owner record so another
    /// node can take over without waiting for the node lease to expire.
    Handoff,
    /// The same logical node will start again at the same address. Drain the
    /// request shell, but keep ownership and the local replica cache intact.
    Preserve,
}

struct Routed {
    request: u64,
    route: Route,
}

#[derive(Clone, Copy, Default)]
struct DrainStatus {
    occupied: usize,
    activating: usize,
    evicting: usize,
    /// Ownership releases still in flight. A handoff drain waits for zero:
    /// a released cell leaves `occupied` before its record write commits,
    /// so exiting on occupancy alone can abort the write mid-flight and
    /// leave a record the successor waits out the node lease for.
    releasing: usize,
}

// Read only by the orphaned `worker_route`; removed with the landing-cell
// machinery in the DO-dispatch refactor ([[designs/do-fast-path]]).
#[allow(dead_code)]
struct WorkerRouted {
    request: u64,
    route: Option<WorkerRoute>,
}

/// Worker requests owned by one HTTP connection. Hyper can finish a failed
/// connection without dropping its service future, so the connection also
/// aborts these requests explicitly when its transport ends.
#[derive(Clone, Default)]
struct ConnectionWorkerRequests(Arc<std::sync::Mutex<BTreeSet<celld::js::RequestId>>>);

impl ConnectionWorkerRequests {
    fn register(&self, request: celld::js::RequestId) {
        self.0.lock().unwrap().insert(request);
    }

    fn complete(&self, request: celld::js::RequestId) {
        self.0.lock().unwrap().remove(&request);
    }

    fn abort_all(&self) {
        let requests = std::mem::take(&mut *self.0.lock().unwrap());
        for request in requests {
            celld::js::abort_request(request);
        }
    }
}

struct IngressAbortGuard {
    request: Option<celld::js::RequestId>,
    connection: ConnectionWorkerRequests,
}

impl IngressAbortGuard {
    fn new(request: celld::js::RequestId, connection: ConnectionWorkerRequests) -> Self {
        connection.register(request);
        Self {
            request: Some(request),
            connection,
        }
    }

    fn disarm(&mut self) {
        if let Some(request) = self.request.take() {
            self.connection.complete(request);
        }
    }
}

/// Cancels a core route when the future awaiting it is dropped. A normal
/// route disarms the guard before returning its result.
struct RouteCancelGuard {
    tx: mpsc::UnboundedSender<Message>,
    request: Option<u64>,
}

impl RouteCancelGuard {
    fn new(tx: mpsc::UnboundedSender<Message>, request: u64) -> Self {
        Self {
            tx,
            request: Some(request),
        }
    }

    fn disarm(&mut self) {
        self.request = None;
    }
}

impl Drop for RouteCancelGuard {
    fn drop(&mut self) {
        if let Some(request) = self.request {
            let _ = self.tx.send(Message::CancelRoute { request });
        }
    }
}

impl Drop for IngressAbortGuard {
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            self.connection.complete(request);
            celld::js::abort_request(request);
        }
    }
}

struct ActivityGuard {
    tx: mpsc::UnboundedSender<Message>,
    request: u64,
    runtime: Option<RuntimeManager>,
    cell: String,
}

impl Drop for ActivityGuard {
    fn drop(&mut self) {
        // Read and send under the registry lock (`with_alarm`), so this
        // report cannot carry an alarm older than one the reporter already
        // sent — a stale fold arriving later would unarm the core and
        // delete the wake entry (`alarm_reporter` documents the ordering).
        let send = |tx: &mpsc::UnboundedSender<Message>, at_ms, covered| {
            let _ = tx.send(Message::ActivityFinished {
                request: self.request,
                cell: self.cell.clone(),
                alarm_at_ms: at_ms,
                alarm_covered: covered,
            });
        };
        match &self.runtime {
            Some(runtime) => runtime.with_alarm(&self.cell, |at_ms| {
                let covered = runtime.alarm_covered(&self.cell, at_ms);
                send(&self.tx, at_ms, covered);
            }),
            None => send(&self.tx, None, false),
        }
    }
}

#[derive(Clone, Copy)]
enum RouteStage {
    OwnershipRead,
    NodeLeaseLookup,
    CapacityLookup,
    OwnershipAcquire,
    Restore,
    IsolateStartup,
    RegistryInsert,
}

struct EffectTiming {
    cell: String,
    stage: RouteStage,
    elapsed_us: u64,
}

struct CompletedEffect {
    event: Event,
    timing: Option<EffectTiming>,
}

impl CompletedEffect {
    fn plain(event: Event) -> Self {
        Self {
            event,
            timing: None,
        }
    }

    fn timed(event: Event, cell: String, stage: RouteStage, started: Instant) -> Self {
        Self {
            event,
            timing: Some(EffectTiming {
                cell,
                stage,
                elapsed_us: started.elapsed().as_micros() as u64,
            }),
        }
    }
}

struct CellRouteTiming {
    started: Instant,
    activation_started: bool,
    capacity_wait_started: Option<Instant>,
    latch_wait_us: u64,
    ownership_read_us: u64,
    node_lease_lookup_us: u64,
    capacity_lookup_us: u64,
    capacity_wait_us: u64,
    activation_slot_wait_us: u64,
    lease_permit_us: u64,
    ownership_acquire_us: u64,
    replica_discovery_us: u64,
    restore_us: u64,
    isolate_startup_us: u64,
    registry_insert_us: u64,
    fresh: Option<bool>,
}

impl CellRouteTiming {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            activation_started: false,
            capacity_wait_started: None,
            latch_wait_us: 0,
            ownership_read_us: 0,
            node_lease_lookup_us: 0,
            capacity_lookup_us: 0,
            capacity_wait_us: 0,
            activation_slot_wait_us: 0,
            lease_permit_us: 0,
            ownership_acquire_us: 0,
            replica_discovery_us: 0,
            restore_us: 0,
            isolate_startup_us: 0,
            registry_insert_us: 0,
            fresh: None,
        }
    }

    fn effect_started(&mut self) {
        if !self.activation_started {
            self.activation_started = true;
            self.activation_slot_wait_us = self.started.elapsed().as_micros() as u64;
        }
        if let Some(started) = self.capacity_wait_started.take() {
            self.capacity_wait_us = self
                .capacity_wait_us
                .saturating_add(started.elapsed().as_micros() as u64);
        }
    }

    fn record(&mut self, stage: RouteStage, elapsed_us: u64) {
        let field = match stage {
            RouteStage::OwnershipRead => &mut self.ownership_read_us,
            RouteStage::NodeLeaseLookup => &mut self.node_lease_lookup_us,
            RouteStage::CapacityLookup => &mut self.capacity_lookup_us,
            RouteStage::OwnershipAcquire => &mut self.ownership_acquire_us,
            RouteStage::Restore => &mut self.restore_us,
            RouteStage::IsolateStartup => &mut self.isolate_startup_us,
            RouteStage::RegistryInsert => &mut self.registry_insert_us,
        };
        *field = field.saturating_add(elapsed_us);
    }
}

type EffectFuture = Pin<Box<dyn Future<Output = CompletedEffect> + Send>>;
type ConnectionFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type DoCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type AssetCallFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type WebSocketFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type CachePruneFuture = Pin<
    Box<dyn Future<Output = (u64, Result<(usize, usize, u64), tokio::task::JoinError>)> + Send>,
>;

#[derive(Clone)]
struct AppHandle {
    tx: mpsc::UnboundedSender<Message>,
    runtime: Option<RuntimeManager>,
    assets: Arc<HashMap<String, AssetResolver>>,
    asset_script: Option<Arc<str>>,
    peer_http: reqwest::Client,
    peer_auth: Arc<PeerAuth>,
    advertise: String,
    websockets: mpsc::UnboundedSender<WebSocketFuture>,
    /// Whether the RPO=0 output gate is armed: hold a local write's response
    /// until its cell is proven durable. On by default; set `CELLD_OUTPUT_GATE=0`
    /// to acknowledge writes without proving them durable. The core and its DST
    /// are unconditional.
    output_gate: bool,
    /// Concurrent outbound WebSockets one cell may hold, for the refusal
    /// message; the core enforces it.
    max_outbound_websockets: usize,
    /// Set the instant a graceful shutdown begins, so `/__celld/health` reports
    /// unhealthy and a load balancer stops routing here before teardown.
    draining: Arc<std::sync::atomic::AtomicBool>,
    /// Whether forwarded scheme and host headers can set `request.url`.
    /// The default is false because a direct client controls both headers.
    trust_forwarded_headers: bool,
}

impl AppHandle {
    async fn request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, false).await
    }

    async fn capacity_request(&self, cell: String) -> Result<Routed, RequestError> {
        self.request_with_mode(cell, true).await
    }

    async fn request_with_mode(
        &self,
        cell: String,
        capacity_handoff: bool,
    ) -> Result<Routed, RequestError> {
        let request = next_core_request();
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::Request {
                request,
                cell,
                capacity_handoff,
                reply,
            })
            .is_err()
        {
            return Err(RequestError::NodeFenced);
        }
        let mut cancel = RouteCancelGuard::new(self.tx.clone(), request);
        let result = receive.await.unwrap_or(Err(RequestError::NodeFenced));
        cancel.disarm();
        result
    }

    // Orphaned by the always-pool Worker entry below. The whole landing-cell
    // machinery (this, `Message::WorkerRequest`, `WorkerRouted`,
    // `fetch_worker_on_cell`, `CellJob::WorkerFetch`, the logic `worker_request`,
    // and the inline co-hosted-write gate) is removed together in the DO-dispatch
    // refactor ([[designs/do-fast-path]]) so the RPO=0 co-hosted path is reworked
    // coherently rather than unpicked here.
    #[allow(dead_code)]
    async fn worker_route(&self) -> anyhow::Result<WorkerRouted> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WorkerRequest { reply })
            .map_err(|_| anyhow::anyhow!("core stopped before Worker routing"))?;
        receive
            .await
            .context("core stopped while routing Worker request")
    }

    async fn fetch_worker(
        &self,
        url: String,
        method: String,
        body: Vec<u8>,
        headers: Vec<(String, String)>,
        connection: ConnectionWorkerRequests,
    ) -> anyhow::Result<HttpResponse> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Worker runtime unavailable"))?;
        // Admission happens in the runtime, against the pool that actually
        // holds the requests. It used to be duplicated here against an empty
        // `PoolLoad` — a snapshot with no isolates and no affiliations, which
        // could only ever answer "yes" once pressure stopped refusing
        // outright. A decision made against a fake reading is not a fast path.
        let js_request = celld::js::next_request_id();
        // The Worker entry is stateless — run it in the pool, always. Routing it
        // to a "landing cell" existed only to make an `env.NS.get(id)` call
        // resolve inline; but a cell runs one fetch at a time, so under any real
        // concurrency the entry can't be on the cell it calls, and the routing
        // round-trip is paid for nothing — on top of the DO call's own routing.
        // A DO call reaches its owning cell (with fencing and the output gate) on
        // the host path, so one routing round-trip per request, never two.
        let mut abort = IngressAbortGuard::new(js_request, connection);
        let result = runtime
            .fetch_worker_pool(url, method, body, headers, js_request)
            .await;
        abort.disarm();
        result
    }

    fn activity(&self, request: u64, cell: String) -> ActivityGuard {
        ActivityGuard {
            tx: self.tx.clone(),
            request,
            runtime: self.runtime.clone(),
            cell,
        }
    }

    /// The output gate. The caller invokes this only for a request whose
    /// handler advanced the cell's committed position, so the response is held
    /// until the core proves the cell durable; `Ok(())` releases it, `Err`
    /// fails it. Called before the activity guard drops, so the request stays
    /// pinned across the wait.
    async fn gate_write(&self, request: u64, position: u64) -> Result<(), RequestError> {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::GateWrite {
                request,
                position,
                reply,
            })
            .is_err()
        {
            return Err(RequestError::NodeFenced);
        }
        receive.await.unwrap_or(Err(RequestError::NodeFenced))
    }

    /// Hand a finished `webSocketMessage`'s frames to the cell's output gate.
    /// Awaited so the request stays pinned until the actor has registered the
    /// gate (the core reads the still-active request when it opens); frames flush
    /// or fail asynchronously as durability resolves, not here.
    async fn ws_output(
        &self,
        request: u64,
        scope: String,
        frames: Vec<(u64, WsOut)>,
        write_position: Option<u64>,
    ) {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                reply,
            })
            .is_ok()
        {
            let _ = receive.await;
        }
    }

    async fn websocket_opened(
        &self,
        cell: String,
        websocket: u64,
        kind: WebSocketKind,
    ) -> anyhow::Result<()> {
        let (reply, receive) = oneshot::channel();
        self.tx
            .send(Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            })
            .map_err(|_| anyhow::anyhow!("core stopped before WebSocket opened"))?;
        let held = receive
            .await
            .context("core stopped while opening WebSocket")?;
        anyhow::ensure!(
            held,
            "outbound WebSocket refused: a cell may hold at most {}, and a node \
             may pin at most {}% of its residency ceiling",
            self.max_outbound_websockets,
            celld_logic::pressure::MAX_OUTBOUND_PIN_PERCENT,
        );
        Ok(())
    }

    fn websocket_closed(&self, cell: String, websocket: u64) {
        let _ = self.tx.send(Message::WebSocketClosed { cell, websocket });
    }

    async fn evict(&self, cell: String) {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Evict { cell, reply }).is_ok() {
            let _ = receive.await;
        }
    }

    async fn invalidate_remote(&self, cell: String, node: String, epoch: u64) {
        let (reply, receive) = oneshot::channel();
        if self
            .tx
            .send(Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            })
            .is_ok()
        {
            let _ = receive.await;
        }
    }

    async fn snapshot(&self) -> String {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Snapshot { reply }).is_err() {
            return "{\"error\":\"actor_stopped\"}".into();
        }
        receive
            .await
            .unwrap_or_else(|_| "{\"error\":\"actor_stopped\"}".into())
    }

    fn is_draining(&self) -> bool {
        self.draining.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// A dead actor has nothing left to hand off, so channel failure
    /// reports drained rather than wedging shutdown.
    async fn drain_status(&self) -> DrainStatus {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Drained { reply }).is_err() {
            return DrainStatus::default();
        }
        receive.await.unwrap_or_default()
    }

    async fn healthy(&self) -> bool {
        let (reply, receive) = oneshot::channel();
        if self.tx.send(Message::Health { reply }).is_err() {
            return false;
        }
        receive.await.unwrap_or(false)
    }

    async fn presence(&self) -> Option<celld_logic::PresenceSnapshot> {
        let (reply, receive) = oneshot::channel();
        self.tx.send(Message::Presence { reply }).ok()?;
        receive.await.ok()
    }
}

/// One cell's WebSocket output gate: an ordered queue of write barriers.
#[derive(Default)]
struct WsGate {
    barriers: VecDeque<WsBarrier>,
}

/// A gated write and the outbound frames held behind it. `settled` is `None`
/// while the write's durability is unproven, `Some(true)` once proven (its
/// frames may flush when it reaches the front), `Some(false)` on failure (the
/// gate breaks). Frames from later non-writing messages accrete onto the
/// newest barrier's `frames`, so they never overtake the write they trail.
struct WsBarrier {
    request: u64,
    settled: Option<bool>,
    frames: Vec<(u64, WsOut)>,
}

struct Actor {
    state: State,
    ownership: Ownership,
    runtime: Option<RuntimeManager>,
    region: String,
    pending: BTreeMap<u64, oneshot::Sender<Result<Routed, RequestError>>>,
    request_cells: BTreeMap<u64, String>,
    route_timings: BTreeMap<String, CellRouteTiming>,
    pending_workers: BTreeMap<u64, oneshot::Sender<WorkerRouted>>,
    eviction_waiters: BTreeMap<String, Vec<oneshot::Sender<()>>>,
    durability_waiters: BTreeMap<u64, (String, Vec<oneshot::Sender<()>>)>,
    eviction_stops: BTreeMap<u64, Vec<oneshot::Sender<()>>>,
    /// Local write responses held open by the output gate, keyed by request,
    /// released when the core emits `ReleaseResponse`.
    gated_responses: BTreeMap<u64, oneshot::Sender<Result<(), RequestError>>>,
    /// Per-cell WebSocket output gate: a FIFO of write barriers, each holding
    /// the outbound frames produced up to it. The front drains in write order as
    /// durability proves, so no client ever sees a frame that trails an
    /// unproven write (the Cloudflare per-object output gate).
    ws_gates: BTreeMap<String, WsGate>,
    /// Gated `webSocketMessage` requests, mapping each to its cell so a
    /// `ReleaseResponse` routes to the WebSocket gate rather than an HTTP reply.
    ws_gated: BTreeMap<u64, String>,
    published: BTreeSet<String>,
    fail_publish_once: bool,
    publishes: u64,
    stops: u64,
    lease_spec: NodeLeaseSpec,
    /// Whether to re-check every core invariant after every event.
    ///
    /// The check is a full scan of the cell table, so its cost grows with
    /// residency: roughly 800us per event at ten thousand resident cells,
    /// which would cap a busy node at about a thousand events a second in
    /// assertion code alone. It is a model-checking assertion, and the place
    /// it earns its keep is simulation, which can afford to run it after
    /// every event over far more schedules than production will ever see.
    /// Debug builds keep it. Release builds rely on the deterministic model,
    /// because this full-table scan is too expensive for production traffic.
    validate_invariants: bool,
    /// Carries the previous CPU tick reading; a rate needs two samples.
    load_sampler: ProcessLoadSampler,
    /// The counters peers rank this node by, when there is a bucket to
    /// publish them to.
    live_load: Option<Arc<celld::ownership_store::LiveLoad>>,
    /// The shed reason last reported to the log, so a latch that holds for
    /// minutes is reported once rather than on every sample.
    logged_shed_reason: Option<&'static str>,
    started_at: Instant,
    fence: mpsc::UnboundedSender<i32>,
    timers: DelayQueue<Timer>,
    timer_keys: BTreeMap<TimerSlot, delay_queue::Key>,
    preserving: bool,
}

struct AdmissionLimits {
    resident: usize,
    activations: usize,
    evictions: usize,
    releases: usize,
}

struct ActorIdentity {
    node: String,
    advertise: String,
    region: String,
    managed: bool,
}

impl Actor {
    async fn from_environment(
        limits: AdmissionLimits,
        fail_publish_once: bool,
        fence: mpsc::UnboundedSender<i32>,
        runtime: Option<RuntimeManager>,
        ownership: Option<Ownership>,
        identity: ActorIdentity,
        resume_generation: Option<String>,
    ) -> anyhow::Result<Self> {
        let ActorIdentity {
            node,
            advertise,
            region,
            managed,
        } = identity;
        let ownership = if let Some(ownership) = ownership {
            ownership
        } else {
            Ownership::Memory(Arc::new(Mutex::new(MemoryOwnership {
                node: node.clone(),
                owners: BTreeMap::new(),
                leases: BTreeMap::new(),
                next_etag: 1,
            })))
        };
        let live_load = match &ownership {
            Ownership::Bucket(bucket) => Some(bucket.live()),
            Ownership::Memory(_) => None,
        };
        if let Some(live) = &live_load {
            celld::ownership_store::set_node_load(live.clone());
        }
        let process_generation = match &ownership {
            Ownership::Bucket(bucket) => bucket
                .process_generation()
                .map(str::to_owned)
                .unwrap_or_else(random_process_generation),
            Ownership::Memory(_) => random_process_generation(),
        };
        let ttl_ms = celld::env_vars::positive_or("CELLD_TTL_MS", 10_000)?;
        let lease_mode_value = celld::env_vars::value("CELLD_LAZY_NODE_LEASE")?;
        let lease_mode = match lease_mode_value.as_deref() {
            Some("lazy") => NodeLeaseMode::Lazy,
            Some("shadow") => NodeLeaseMode::Shadow,
            Some("continuous") | None => NodeLeaseMode::Continuous,
            Some(value) => anyhow::bail!(
                "CELLD_LAZY_NODE_LEASE must be continuous, lazy, or shadow, not {value:?}"
            ),
        };
        let lease_linger_ms = celld::env_vars::with_default(
            "CELLD_LEASE_LINGER_MS",
            if managed { 0 } else { ttl_ms },
        )?;
        let lease_spec = NodeLeaseSpec {
            // Startup has already resolved the environment and CLI settings
            // into this validated internal address. Reading the environment
            // again would let CELLD_ADVERTISE override a later CLI option and
            // publish an address that the bound listener did not validate.
            addr: advertise,
            peer_protocol: peer_auth::PROTOCOL_VERSION,
            generation: std::env::var("CELLD_TEST_GENERATION").unwrap_or(process_generation),
            resume_generation,
            ttl_ms,
            mode: lease_mode,
            linger_ms: lease_linger_ms,
        };
        Ok(Self {
            state: State::new(
                node,
                Config {
                    max_resident: limits.resident,
                    max_activations: limits.activations,
                    max_evictions: limits.evictions,
                    max_releases: limits.releases,
                    alarm_resident_ms: celld::wake::resident_ms().max(0) as u64,
                    require_node_lease: true,
                    peer_protocol: celld::peer_auth::PROTOCOL_VERSION,
                    operation_deadline_ms: Some(celld::env_vars::positive_or(
                        "CELLD_OPERATION_DEADLINE_MS",
                        DEFAULT_OPERATION_DEADLINE_MS,
                    )?),
                    idle_evict_ms: celld::env_vars::positive::<u64>("CELLD_IDLE_EVICT_S")?
                        .map(|seconds| seconds.saturating_mul(1_000)),
                    pressure: pressure_config_from_environment()?,
                    max_outbound_websockets: celld::env_vars::positive_or(
                        "CELLD_MAX_OUTBOUND_WEBSOCKETS",
                        DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
                    )?,
                    ownership_on_evict: match &ownership {
                        // Releasing a cell says "any node may take this
                        // one", which is only true if some other node could
                        // restore it. Without a bucket the sole copy is this
                        // node's disk, keyed to an epoch a re-acquire will
                        // step past, so releasing would lose the cell.
                        Ownership::Memory(_) => OwnershipOnEvict::Sticky,
                        Ownership::Bucket(_) => ownership_on_evict_from_environment()?,
                    },
                },
            ),
            ownership,
            runtime,
            region,
            pending: BTreeMap::new(),
            request_cells: BTreeMap::new(),
            route_timings: BTreeMap::new(),
            pending_workers: BTreeMap::new(),
            eviction_waiters: BTreeMap::new(),
            durability_waiters: BTreeMap::new(),
            gated_responses: BTreeMap::new(),
            ws_gates: BTreeMap::new(),
            ws_gated: BTreeMap::new(),
            eviction_stops: BTreeMap::new(),
            published: BTreeSet::new(),
            fail_publish_once,
            publishes: 0,
            stops: 0,
            lease_spec,
            live_load,
            logged_shed_reason: None,
            load_sampler: ProcessLoadSampler::default(),
            validate_invariants: cfg!(debug_assertions),
            started_at: Instant::now(),
            fence,
            timers: DelayQueue::new(),
            timer_keys: BTreeMap::new(),
            preserving: false,
        })
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Message>) {
        let mut in_flight = FuturesUnordered::new();
        self.drive(
            Event::StartNodeLease {
                now_ms: now_ms(),
                spec: self.lease_spec.clone(),
            },
            &mut in_flight,
        );
        loop {
            tokio::select! {
                message = rx.recv() => {
                    let Some(message) = message else {
                        break;
                    };
                    self.handle_message(message, &mut in_flight);
                }
                Some(completed) = in_flight.next(), if !in_flight.is_empty() => {
                    let route_cell = completed
                        .timing
                        .as_ref()
                        .map(|timing| timing.cell.clone());
                    if let Some(timing) = completed.timing {
                        self.record_effect_timing(timing);
                    }
                    self.drive(completed.event, &mut in_flight);
                    if let Some(cell) = route_cell {
                        self.observe_capacity_wait(&cell);
                    }
                }
                Some(expired) = self.timers.next(), if !self.timers.is_empty() => {
                    let timer = expired.into_inner();
                    self.timer_keys.remove(&TimerSlot::of(&timer));
                    if self.preserving && matches!(timer, Timer::CellAlarm { .. }) {
                        continue;
                    }
                    self.drive(Event::TimerFired {
                        timer,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    }, &mut in_flight);
                }
            }
        }
    }

    fn schedule_timer(&mut self, timer: Timer, at_mono_ms: u64) {
        let slot = TimerSlot::of(&timer);
        let displaced = self.timer_keys.remove(&slot);
        // Displacing is the point for the coalescing slots and a bug for the
        // per-operation ones: two live deadlines sharing a slot means one
        // operation is no longer watched, which is invisible until something
        // hangs forever in production.
        debug_assert!(
            displaced.is_none() || !matches!(slot, TimerSlot::OperationDeadline(_)),
            "an operation deadline displaced another: {slot:?}"
        );
        if let Some(key) = displaced {
            self.timers.remove(&key);
        }
        let delay = std::time::Duration::from_millis(
            at_mono_ms.saturating_sub(self.started_at.elapsed().as_millis() as u64),
        );
        let key = self.timers.insert(timer, delay);
        self.timer_keys.insert(slot, key);
    }

    fn handle_message(&mut self, message: Message, in_flight: &mut FuturesUnordered<EffectFuture>) {
        match message {
            Message::BeginPreserve => {
                self.preserving = true;
                self.drive(Event::BeginPreserve, in_flight);
            }
            Message::Request { reply, .. } if self.preserving => {
                let _ = reply.send(Err(RequestError::NodeFenced));
            }
            Message::Request {
                request,
                cell,
                capacity_handoff,
                reply,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.pending.insert(request, reply);
                self.begin_route_if_cold(&cell);
                self.request_cells.insert(request, cell.clone());
                self.drive(
                    if capacity_handoff {
                        Event::CapacityRequestAt {
                            request,
                            cell: cell.clone(),
                            now_ms: now_ms(),
                            now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                        }
                    } else {
                        Event::RequestAt {
                            request,
                            cell: cell.clone(),
                            now_ms: now_ms(),
                            now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                        }
                    },
                    in_flight,
                );
                self.observe_capacity_wait(&cell);
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::CancelRoute { request } => {
                self.pending.remove(&request);
                if let Some(cell) = self.request_cells.remove(&request) {
                    self.finish_route(&cell, "cancelled", "caller_disconnected", None);
                }
                self.drive(Event::Cancel { request }, in_flight);
            }
            Message::WorkerRequest { reply } if self.preserving => {
                let _ = reply.send(WorkerRouted {
                    request: next_core_request(),
                    route: None,
                });
            }
            Message::WorkerRequest { reply } => {
                let request = next_core_request();
                self.pending_workers.insert(request, reply);
                self.drive(Event::WorkerRequest { request }, in_flight);
            }
            Message::ReleaseAll => self.drive(Event::ReleaseAll, in_flight),
            Message::Drained { reply } => {
                let _ = reply.send(DrainStatus {
                    occupied: self.state.occupied(),
                    activating: self.state.activating(),
                    evicting: self.state.evicting(),
                    releasing: self.state.releasing(),
                });
            }
            Message::SampleLoad if self.preserving => {}
            Message::SampleLoad => {
                // Counted once: it is a walk of every cell, and the latch and
                // the number peers rank this node by must agree anyway.
                let occupied = self.state.occupied();
                let cpu = self.load_sampler.sample_cpu_percent_x100();
                let memory = celld::memory::sample();
                let load = celld_logic::pressure::Load {
                    resident_cells: occupied,
                    rss_bytes: memory.rss_bytes,
                    in_use_bytes: memory.in_use_bytes,
                };
                let now_mono_ms = self.started_at.elapsed().as_millis() as u64;
                self.drive(Event::LoadSampled { load, now_mono_ms }, in_flight);
                // Report a change of shed reason once. `rss-hard` is the one
                // that needs an operator: it says the resident set size crossed
                // the absolute cap, so the allocator is holding memory that
                // shedding may not return, and the process is near a kill.
                let reason = self.state.shed_reason();
                if reason != self.logged_shed_reason {
                    self.logged_shed_reason = reason;
                    match reason {
                        Some(celld_logic::pressure::SHED_RSS_HARD) => tracing::warn!(
                            rss_bytes = load.rss_bytes,
                            in_use_bytes = load.in_use_bytes,
                            "shedding on the absolute resident-set cap: the \
                             allocator holds memory that shedding cannot return"
                        ),
                        Some(reason) => tracing::info!(
                            reason,
                            rss_bytes = load.rss_bytes,
                            in_use_bytes = load.in_use_bytes,
                            "shedding"
                        ),
                        None => tracing::info!("no longer shedding"),
                    }
                }
                // Republish what peers rank this node by. The same numbers the
                // latch just saw: a node that reports last tick's residency
                // attracts work it has already refused.
                if let Some(live) = &self.live_load {
                    live.resident_cells.store(occupied, Ordering::Relaxed);
                    live.host_websockets
                        .store(self.state.host_websockets(), Ordering::Relaxed);
                    live.pressured
                        .store(self.state.shedding(), Ordering::Relaxed);
                    live.cpu_percent_x100.store(cpu, Ordering::Relaxed);
                    live.restoring
                        .store(self.state.activation_backlog() as u64, Ordering::Relaxed);
                }
            }
            Message::GateWrite {
                request,
                position,
                reply,
            } => {
                // The dispatch path only asks to gate a request whose handler
                // advanced the cell's committed position, so this is always a
                // write: hold the response until the core proves it durable.
                self.gated_responses.insert(request, reply);
                self.drive(Event::Wrote { request, position }, in_flight);
            }
            Message::WsOutput {
                request,
                scope,
                frames,
                write_position,
                reply,
            } => {
                match write_position {
                    // The handler wrote: open a barrier holding these frames and
                    // gate on durability. A later `ReleaseResponse` settles it.
                    Some(position) => {
                        self.ws_gates
                            .entry(scope.clone())
                            .or_default()
                            .barriers
                            .push_back(WsBarrier {
                                request,
                                settled: None,
                                frames,
                            });
                        self.ws_gated.insert(request, scope);
                        self.drive(Event::Wrote { request, position }, in_flight);
                    }
                    // No write: trail the newest pending write if one is open,
                    // else the gate is open — flush now.
                    None => match self.ws_gates.get_mut(&scope) {
                        Some(gate) if !gate.barriers.is_empty() => {
                            gate.barriers.back_mut().unwrap().frames.extend(frames);
                        }
                        _ => celld::js::ws_emit_batch(frames),
                    },
                }
                let _ = reply.send(());
            }
            Message::ActivityFinished {
                request,
                cell,
                alarm_at_ms,
                alarm_covered,
            } => {
                // Folded from the old AlarmObserved-then-ActivityFinished pair the
                // activity drop sent: observe the alarm and release any retired
                // durability waiters, then finish the activity — same events, same
                // order, one message instead of two.
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms: alarm_at_ms,
                        covered: alarm_covered,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                self.drive(Event::ActivityFinished { request }, in_flight);
            }
            Message::WebSocketOpened {
                cell,
                websocket,
                kind,
                reply,
            } => {
                self.drive(
                    Event::WebSocketOpened {
                        cell: cell.clone(),
                        websocket,
                        kind,
                    },
                    in_flight,
                );
                // The core may decline to hold the transport. Say so, rather
                // than acknowledging an open and closing the socket a moment
                // later: an application that hit a ceiling deserves to be
                // told which one, not left watching a socket disappear.
                // Only an outbound socket can be declined, and only for a
                // cell the core knows: an inbound transport is never refused,
                // and reporting one as refused fails opens that succeeded.
                let refused = kind == WebSocketKind::Outbound
                    && !self.state.holds_websocket(&cell, websocket);
                let _ = reply.send(!refused);
            }
            Message::WebSocketClosed { cell, websocket } => {
                self.drive(Event::WebSocketClosed { cell, websocket }, in_flight);
            }
            Message::AlarmObserved {
                cell,
                at_ms,
                covered,
            } => {
                let retired_durability = match self.state.phase(&cell) {
                    Some(Phase::EnsuringDurability { op, .. }) => Some(*op),
                    _ => None,
                };
                self.drive(
                    Event::AlarmObserved {
                        cell,
                        at_ms,
                        covered,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
                if let Some(op) = retired_durability {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            Message::WakeHint { .. } if self.preserving => {}
            Message::WakeHint { cell } => {
                self.drive(
                    Event::WakeHintAt {
                        cell,
                        now_ms: now_ms(),
                        now_mono_ms: self.started_at.elapsed().as_millis() as u64,
                    },
                    in_flight,
                );
            }
            Message::Evict { reply, .. } if self.preserving => {
                let _ = reply.send(());
            }
            Message::Evict { cell, reply } if self.state.is_active(&cell) => {
                let _ = reply.send(());
            }
            Message::Evict { cell, reply } => match self.state.phase(&cell) {
                Some(Phase::Resident { .. }) => {
                    self.eviction_waiters
                        .entry(cell.clone())
                        .or_default()
                        .push(reply);
                    self.drive(Event::Evict { cell }, in_flight);
                }
                Some(Phase::EnsuringDurability { op, .. }) => {
                    self.durability_waiters
                        .entry(*op)
                        .or_insert_with(|| (cell, Vec::new()))
                        .1
                        .push(reply);
                }
                Some(Phase::Cleaning {
                    op,
                    cause: StopCause::Evict { .. },
                    ..
                }) => {
                    self.eviction_stops.entry(*op).or_default().push(reply);
                }
                _ => {
                    let _ = reply.send(());
                }
            },
            Message::InvalidateRemote {
                cell,
                node,
                epoch,
                reply,
            } => {
                self.drive(Event::InvalidateRemote { cell, node, epoch }, in_flight);
                let _ = reply.send(());
            }
            Message::Snapshot { reply } => {
                let _ = reply.send(self.state_json());
            }
            Message::Health { reply } => {
                let _ = reply.send(self.state.ready_to_serve());
            }
            Message::Presence { reply } => {
                let _ = reply.send(self.state.presence_snapshot());
            }
        }
    }

    /// Settle a gated `webSocketMessage`'s durability. On success mark its
    /// barrier durable and flush the durable prefix in write order; on failure
    /// break the whole cell gate — drop every held frame and reset its sockets,
    /// since an unproven write must never leave an acknowledged trace.
    fn ws_release(&mut self, request: u64, ok: bool) {
        let Some(scope) = self.ws_gated.remove(&request) else {
            return;
        };
        let mut flush = Vec::new();
        let broke = {
            let Some(gate) = self.ws_gates.get_mut(&scope) else {
                return;
            };
            if let Some(barrier) = gate.barriers.iter_mut().find(|b| b.request == request) {
                barrier.settled = Some(ok);
            }
            if gate.barriers.iter().any(|b| b.settled == Some(false)) {
                true
            } else {
                while gate
                    .barriers
                    .front()
                    .is_some_and(|b| b.settled == Some(true))
                {
                    flush.push(gate.barriers.pop_front().unwrap().frames);
                }
                false
            }
        };
        if broke {
            self.ws_gates.remove(&scope);
            self.ws_gated.retain(|_, s| *s != scope);
            celld::js::ws_close_scope(&scope, 1011, "durability unproven");
            return;
        }
        for frames in flush {
            celld::js::ws_emit_batch(frames);
        }
        if self
            .ws_gates
            .get(&scope)
            .is_some_and(|g| g.barriers.is_empty())
        {
            self.ws_gates.remove(&scope);
        }
    }

    fn drive(&mut self, first: Event, in_flight: &mut FuturesUnordered<EffectFuture>) {
        let mut events = VecDeque::from([first]);
        while let Some(event) = events.pop_front() {
            let durability = match &event {
                Event::DurabilityChecked { op, .. } => Some(*op),
                // A deadline resolves the same operation the proof would
                // have, so it has to release the same waiters. Without this
                // the core abandons the eviction and the caller that asked
                // for it stays blocked on a proof that is no longer coming.
                Event::TimerFired {
                    timer: Timer::OperationDeadline { op },
                    ..
                } => Some(*op),
                _ => None,
            };
            let stopped = match &event {
                Event::RuntimeStopped { op } => Some(*op),
                _ => None,
            };
            let effects = on_event(&mut self.state, event);
            if let Some(op) = durability {
                if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                    let stop = effects.iter().find_map(|effect| match effect {
                        Effect::StopRuntime {
                            op,
                            cause: StopCause::Evict { .. },
                            ..
                        } => Some(*op),
                        _ => None,
                    });
                    if let Some(stop) = stop {
                        self.eviction_stops.entry(stop).or_default().extend(waiters);
                    } else {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
            }
            for effect in effects {
                self.execute(effect, &mut events, in_flight);
            }
            if let Some(op) = stopped {
                if let Some(waiters) = self.eviction_stops.remove(&op) {
                    for waiter in waiters {
                        let _ = waiter.send(());
                    }
                }
            }
            if self.validate_invariants {
                self.state.validate().expect("celld core invariant");
            }
        }
    }

    fn execute(
        &mut self,
        effect: Effect,
        immediate: &mut VecDeque<Event>,
        in_flight: &mut FuturesUnordered<EffectFuture>,
    ) {
        match effect {
            Effect::ScheduleTimer { timer, at_mono_ms } => {
                self.schedule_timer(timer, at_mono_ms);
            }
            Effect::ReadSelfNodeLease { op } => {
                let ownership = self.ownership.clone();
                let node = self.state.node().to_string();
                let started_at = self.started_at;
                in_flight.push(Box::pin(async move {
                    let result = ownership.read_self_node_lease(&node).await;
                    CompletedEffect::plain(Event::SelfNodeLeaseRead {
                        op,
                        now_ms: now_ms(),
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result,
                    })
                }));
            }
            Effect::CasNodeLease {
                op,
                guard,
                record,
                authority_expires_ms,
            } => {
                let ownership = self.ownership.clone();
                let started_at = self.started_at;
                in_flight.push(Box::pin(async move {
                    let attempt_started = Instant::now();
                    let node = record.node.clone();
                    let candidate_expires_ms = record.expires_ms;
                    // Logged before the CAS: with only the completion line, a
                    // renewal hung on a storage tail is indistinguishable from
                    // a timer that never fired (the n6 fence, 2026-08-11).
                    tracing::info!(
                        event = "node_lease_attempt_started",
                        %node,
                        attempt = if authority_expires_ms.is_some() {
                            "renew"
                        } else {
                            "acquire"
                        },
                        prior_authority_headroom_ms = authority_expires_ms
                            .map(|expires_ms| expires_ms.saturating_sub(now_ms()))
                            .unwrap_or(0),
                        "node lease attempt started"
                    );
                    // A renewal must return while proven authority remains,
                    // because only a returned attempt lets the ambiguity
                    // read-back run before the watchdog. The 10:15Z R2
                    // brownout fenced 9 nodes whose sole hung attempt was
                    // still inside the transport's 15 s timeout when the
                    // 10 s TTL expired. Bound the attempt to half the
                    // remaining authority (capped, floored) and map timeout
                    // to Ambiguous — the same conservative outcome a lost
                    // response already produces, so safety is unchanged.
                    let result = match authority_expires_ms {
                        Some(expires_ms) => {
                            let remaining = expires_ms.saturating_sub(now_ms());
                            let bound =
                                std::time::Duration::from_millis((remaining / 2).clamp(250, 2_500));
                            match tokio::time::timeout(
                                bound,
                                ownership.cas_node_lease(guard, record),
                            )
                            .await
                            {
                                Ok(result) => result,
                                Err(_) => Err(Failure::Ambiguous),
                            }
                        }
                        None => ownership.cas_node_lease(guard, record).await,
                    };
                    let completed_ms = now_ms();
                    let elapsed_ms = attempt_started.elapsed().as_millis() as u64;
                    let prior_authority_headroom_ms = authority_expires_ms
                        .map(|expires_ms| expires_ms.saturating_sub(completed_ms))
                        .unwrap_or(0);
                    let candidate_headroom_ms = candidate_expires_ms.saturating_sub(completed_ms);
                    let attempt = if authority_expires_ms.is_some() {
                        "renew"
                    } else {
                        "acquire"
                    };
                    let outcome = match &result {
                        Ok(LeaseCasOutcome::Applied { .. }) => "applied",
                        Ok(LeaseCasOutcome::Rejected) => "rejected",
                        Err(Failure::Ambiguous) => "ambiguous",
                        Err(Failure::Definite) => "definite_failure",
                    };
                    if matches!(&result, Ok(LeaseCasOutcome::Applied { .. })) {
                        tracing::info!(
                            event = "node_lease_attempt",
                            %node,
                            attempt,
                            outcome,
                            elapsed_ms,
                            prior_authority_headroom_ms,
                            candidate_headroom_ms,
                            "node lease attempt completed"
                        );
                    } else {
                        tracing::warn!(
                            event = "node_lease_attempt",
                            %node,
                            attempt,
                            outcome,
                            elapsed_ms,
                            prior_authority_headroom_ms,
                            candidate_headroom_ms,
                            "node lease attempt did not apply"
                        );
                    }
                    CompletedEffect::plain(Event::NodeLeaseCasCompleted {
                        op,
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result,
                    })
                }));
            }
            Effect::ReadLocalCells => {
                let runtime = self.runtime.clone();
                in_flight.push(Box::pin(async move {
                    let result = match runtime {
                        // The host runtime's blocking pool, not a second
                        // pool on the core's current-thread runtime.
                        Some(runtime) => celld::asyncrt::op_handle()
                            .spawn_blocking(move || {
                                runtime.local_reload_cells().map_err(|error| {
                                    eprintln!("celld local reload scan failed: {error:#}");
                                    Failure::Definite
                                })
                            })
                            .await
                            .unwrap_or(Err(Failure::Definite)),
                        None => Err(Failure::Definite),
                    };
                    CompletedEffect::plain(Event::LocalCellsRead { result })
                }));
            }
            Effect::ObserveNodeLeaseShadowRelease { sequence } => {
                tracing::info!(
                    event = "node_lease_shadow_release",
                    node = %self.state.node(),
                    sequence,
                    "lazy lease shadow mode would stop renewal"
                );
            }
            Effect::ObserveNodeLeaseReleased => {
                tracing::info!(
                    event = "node_lease_released",
                    node = %self.state.node(),
                    "no locally dependent cells; stopping node lease renewal"
                );
            }
            Effect::ReadOwner { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.read_owner(&cell).await;
                    CompletedEffect::timed(
                        Event::OwnerRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::OwnershipRead,
                        started,
                    )
                }));
            }
            Effect::ReadNodeLease { op, cell, owner } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell;
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.read_node_lease(&owner).await;
                    CompletedEffect::timed(
                        Event::NodeLeaseRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::NodeLeaseLookup,
                        started,
                    )
                }));
            }
            Effect::ReadCapacityPeers { op, cell } => {
                self.route_effect_started(&cell);
                let ownership = self.ownership.clone();
                let timing_cell = cell;
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.read_capacity_peers().await;
                    CompletedEffect::timed(
                        Event::CapacityPeersRead {
                            op,
                            now_ms: now_ms(),
                            result,
                        },
                        timing_cell,
                        RouteStage::CapacityLookup,
                        started,
                    )
                }));
            }
            Effect::CasOwner {
                op,
                cell,
                guard,
                epoch,
                takeover,
            } => {
                self.route_effect_started(&cell);
                if let Some(timing) = self.route_timings.get_mut(&cell) {
                    timing
                        .fresh
                        .get_or_insert(!takeover && matches!(guard, CasGuard::Absent));
                }
                let ownership = self.ownership.clone();
                let timing_cell = cell.clone();
                in_flight.push(Box::pin(async move {
                    let started = Instant::now();
                    let result = ownership.cas_owner(&cell, guard, epoch).await;
                    CompletedEffect::timed(
                        Event::OwnerCasCompleted { op, result },
                        timing_cell,
                        RouteStage::OwnershipAcquire,
                        started,
                    )
                }));
            }
            Effect::ReconcileWakeEntry {
                cell,
                next_alarm_ms,
            } => {
                if self.runtime.is_some() {
                    // An ARMED alarm must always be reconcilable, tracked or
                    // not: belief can be lost while the alarm stands (a
                    // failed arm-time PUT, an entry retired under a racing
                    // arm), and skipping here would mean it is never
                    // re-asserted — entryless until the alarm fires or the
                    // cell moves. Gating the CONSUME side on tracking is
                    // what keeps alarm-less cells op-quiescent: untracked
                    // with no alarm, there is nothing to delete.
                    if next_alarm_ms >= 0 || celld::js::wake_entry_tracked(&cell) {
                        // On the host runtime: this arm runs on the core
                        // thread, and a bare spawn would schedule the S3
                        // round trip there too.
                        celld::asyncrt::op_handle().spawn(async move {
                            // A consume-delete only ever follows a firing, and
                            // the FireAlarm path now proves the consuming
                            // commit durable (and this node still the owner)
                            // before the core settles the alarm — so by the
                            // time the core orders this delete, the proof
                            // already happened. The old sync_refused probe
                            // asked the same question a second way and was
                            // wrong more often: it refused for any database
                            // the replicator never registered, leaving the
                            // entry to outlive its alarm forever.
                            celld::js::reconcile_wake_entry(&cell, next_alarm_ms, true).await;
                        });
                    }
                }
            }
            Effect::ReleaseOwner { op, cell, epoch } => {
                let ownership = self.ownership.clone();
                in_flight.push(Box::pin(async move {
                    let result = ownership.release_owner(&cell, epoch).await;
                    CompletedEffect::plain(Event::OwnerReleased { op, result })
                }));
            }
            Effect::Restore { op, cell, spec } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    let timing_cell = cell.clone();
                    // A restore downloads, merges, and fsyncs a whole
                    // database. Poll it on the host runtime: this future
                    // lives in `in_flight`, which the core thread drives,
                    // and the core owns the node lease timer.
                    let task = celld::asyncrt::op_handle().spawn(async move {
                        let started = Instant::now();
                        let result = runtime.restore_cell(&cell, &spec).await.map_err(|error| {
                            eprintln!("celld restore failed for {cell}: {error:#}");
                            Failure::Definite
                        });
                        CompletedEffect::timed(
                            Event::RestoreCompleted { op, result },
                            timing_cell,
                            RouteStage::Restore,
                            started,
                        )
                    });
                    in_flight.push(Box::pin(async move {
                        task.await.expect("restore task panicked")
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::Restore,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RestoreCompleted {
                        op,
                        result: Ok(celld_logic::RestoreOutcome {
                            restored: false,
                            alarm: None,
                        }),
                    });
                }
            }
            Effect::StartRuntime { op, cell, epoch } => {
                self.route_effect_started(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    let fresh = self
                        .route_timings
                        .get(&cell)
                        .and_then(|timing| timing.fresh)
                        .unwrap_or(false);
                    let timing_cell = cell.clone();
                    in_flight.push(Box::pin(async move {
                        let started = Instant::now();
                        let result = runtime
                            .start_cell(cell.clone(), epoch, fresh)
                            .await
                            .map_err(|error| {
                                eprintln!("celld runtime start failed for {cell}: {error:#}");
                                Failure::Definite
                            });
                        CompletedEffect::timed(
                            Event::RuntimeStarted { op, result },
                            timing_cell,
                            RouteStage::IsolateStartup,
                            started,
                        )
                    }));
                } else {
                    self.record_effect_timing(EffectTiming {
                        cell,
                        stage: RouteStage::IsolateStartup,
                        elapsed_us: 0,
                    });
                    immediate.push_back(Event::RuntimeStarted { op, result: Ok(()) });
                }
            }
            Effect::Publish { op, cell, epoch } => {
                self.route_effect_started(&cell);
                let publish_started = Instant::now();
                self.publishes += 1;
                let result = if self.fail_publish_once {
                    self.fail_publish_once = false;
                    Err(Failure::Ambiguous)
                } else {
                    self.runtime
                        .as_ref()
                        .map_or(Ok(()), |runtime| runtime.publish_cell(&cell, epoch))
                        .map_err(|error| {
                            eprintln!("celld runtime publication failed for {cell}: {error:#}");
                            Failure::Definite
                        })
                };
                if result.is_ok() {
                    self.published.insert(cell.clone());
                }
                self.record_effect_timing(EffectTiming {
                    cell: cell.clone(),
                    stage: RouteStage::RegistryInsert,
                    elapsed_us: publish_started.elapsed().as_micros() as u64,
                });
                if result.is_ok() {
                    let node = self.state.node().to_string();
                    self.finish_route(&cell, "activated", "", Some((&node, epoch)));
                }
                immediate.push_back(Event::Published { op, result });
            }
            Effect::EnsureDurable { op, cell, epoch } => {
                let waiters = self.eviction_waiters.remove(&cell).unwrap_or_default();
                self.durability_waiters.insert(op, (cell.clone(), waiters));
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        let result = runtime.ensure_durable(&cell, epoch).await.map_err(|error| {
                            eprintln!(
                                "celld durability proof failed for {cell} epoch {epoch}: {error:#}"
                            );
                            Failure::Ambiguous
                        });
                        CompletedEffect::plain(Event::DurabilityChecked { op, result })
                    }));
                } else {
                    immediate.push_back(Event::DurabilityChecked { op, result: Ok(()) });
                }
            }
            // The output gate: prove the cell durable, then let the core release
            // the held write response. Reuses the same recency-proving primitive
            // as EnsureDurable — a proof issued after the write covers it.
            Effect::AwaitDurable {
                op,
                cell,
                epoch,
                position,
            } => {
                if let Some(runtime) = self.runtime.clone() {
                    let ownership = self.ownership.clone();
                    let node = self.state.node().to_string();
                    in_flight.push(Box::pin(async move {
                        // The replicator reports the position it actually proved
                        // durable; the core acks only if it covers this write.
                        let result = match runtime.await_durable(&cell, epoch, position).await {
                            // Durable in `e<epoch>/` is not the same as durable.
                            // If the cell has been taken over, that prefix is
                            // orphaned: the next owner restores a higher epoch
                            // and this write is gone. Re-read the ownership
                            // record before letting anything out.
                            //
                            // A read is enough, and is why this is one GET
                            // rather than a compare-and-swap. If the record
                            // still names us, no takeover linearised before
                            // this read; the LTX went up before it; so any
                            // later takeover restores from a lineage that
                            // already contains this position.
                            Ok(durable) => match ownership.read_owner(&cell).await {
                                Ok(Some(record))
                                    if record.node.as_deref() == Some(node.as_str())
                                        && record.epoch == epoch =>
                                {
                                    Ok(durable)
                                }
                                Ok(record) => {
                                    eprintln!(
                                        "celld output gate: {cell} epoch {epoch} is no longer \
                                         ours (record: {record:?}); refusing to acknowledge a \
                                         write in an orphaned epoch"
                                    );
                                    Err(Failure::Definite)
                                }
                                Err(failure) => Err(failure),
                            },
                            Err(error) => {
                                eprintln!(
                                    "celld output-gate durability proof failed for {cell} \
                                     epoch {epoch}: {error:#}"
                                );
                                Err(Failure::Ambiguous)
                            }
                        };
                        CompletedEffect::plain(Event::DurableReached { op, result })
                    }));
                } else {
                    immediate.push_back(Event::DurableReached {
                        op,
                        result: Ok(position),
                    });
                }
            }
            Effect::ReleaseResponse { request, result } => {
                if self.ws_gated.contains_key(&request) {
                    self.ws_release(request, result.is_ok());
                } else if let Some(reply) = self.gated_responses.remove(&request) {
                    let _ = reply.send(result);
                }
            }
            Effect::StopRuntime {
                op,
                cell,
                epoch,
                cause,
            } => {
                self.stops += 1;
                let evicting = matches!(cause, StopCause::Evict { .. });
                if matches!(cause, StopCause::Fence) {
                    // A fenced cell's wake entry belongs to whoever takes the
                    // cell over; a retained local belief would collide with
                    // the new owner's arm/consume traffic on the same key.
                    celld::js::forget_wake_entry(&cell);
                }
                // Handing the cell away makes the local snapshot dead weight;
                // keeping it is the whole point of an idle eviction. A
                // reset must keep nothing: the local database is precisely
                // what could not be proved durable, so the next activation has
                // to come from the bucket.
                let preserve_local = !matches!(
                    cause,
                    StopCause::Evict { rebalance: true } | StopCause::Reset
                );
                if evicting {
                    if let Some(live) = &self.live_load {
                        live.shed_cells.fetch_add(1, Ordering::Relaxed);
                    }
                }
                self.published.remove(&cell);
                if let Some(runtime) = self.runtime.clone() {
                    in_flight.push(Box::pin(async move {
                        runtime
                            .stop_cell(&cell, epoch, evicting, preserve_local)
                            .await;
                        CompletedEffect::plain(Event::RuntimeStopped { op })
                    }));
                } else {
                    immediate.push_back(Event::RuntimeStopped { op });
                }
            }
            Effect::FireAlarm {
                op,
                cell,
                epoch,
                scheduled_ms,
            } => {
                let started_at = self.started_at;
                if let Some(runtime) = self.runtime.clone() {
                    let ownership = self.ownership.clone();
                    let node = self.state.node().to_string();
                    in_flight.push(Box::pin(async move {
                        let mut result = runtime
                            .fire_alarm(cell.clone(), scheduled_ms)
                            .await
                            .map_err(|error| {
                                eprintln!("celld alarm dispatch failed: {error:#}");
                                Failure::Definite
                            });
                        // An alarm handler's write is gated like a request's:
                        // prove the consuming commit durable — and this node
                        // still the owner — before the core learns the alarm
                        // settled. The consume-side wake-entry delete the core
                        // then orders always follows a proven commit, which is
                        // what lets the shell drop the old sync_refused probe.
                        // On failure the core re-arms the alarm: at-least-once
                        // holds and the entry stays discoverable.
                        if let Ok((_, _, Some(position))) = result {
                            let proven = match runtime.await_durable(&cell, epoch, position).await {
                                Ok(_) => match ownership.read_owner(&cell).await {
                                    Ok(Some(record)) => {
                                        record.node.as_deref() == Some(node.as_str())
                                            && record.epoch == epoch
                                    }
                                    _ => false,
                                },
                                Err(error) => {
                                    eprintln!(
                                        "celld alarm durability proof failed for \
                                         {cell} epoch {epoch}: {error:#}"
                                    );
                                    false
                                }
                            };
                            if !proven {
                                result = Err(Failure::Ambiguous);
                            }
                        }
                        CompletedEffect::plain(Event::AlarmFinished {
                            op,
                            now_ms: now_ms(),
                            now_mono_ms: started_at.elapsed().as_millis() as u64,
                            result: result.map(|(at_ms, covered, _)| (at_ms, covered)),
                        })
                    }));
                } else {
                    immediate.push_back(Event::AlarmFinished {
                        op,
                        now_ms: now_ms(),
                        now_mono_ms: started_at.elapsed().as_millis() as u64,
                        result: Ok((None, true)),
                    });
                }
            }
            Effect::Complete { request, result } => {
                if let Some(cell) = self.request_cells.remove(&request) {
                    match &result {
                        Ok(Route::Remote { node, epoch, .. }) => {
                            self.finish_route(&cell, "remote_owner", "", Some((node, *epoch)));
                            // The cell lives elsewhere: its wake entry is no
                            // longer this node's to track, and a stale local
                            // belief would collide with the owner's own
                            // arm/consume traffic on the same key.
                            celld::js::forget_wake_entry(&cell);
                        }
                        Ok(Route::Local) => {
                            self.finish_route(&cell, "resident_after_wait", "", None);
                        }
                        Err(error) => {
                            self.finish_route(
                                &cell,
                                "route_error",
                                request_error_phase(*error),
                                None,
                            );
                        }
                    }
                }
                if let Some(reply) = self.pending.remove(&request) {
                    let local = result == Ok(Route::Local);
                    if reply
                        .send(result.map(|route| Routed { request, route }))
                        .is_err()
                        && local
                    {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CompleteWorker { request, route } => {
                if let Some(op) = route.as_ref().and_then(|route| route.retired_durability) {
                    if let Some((_, waiters)) = self.durability_waiters.remove(&op) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }
                }
                if let Some(reply) = self.pending_workers.remove(&request) {
                    let reserved = route.is_some();
                    if reply.send(WorkerRouted { request, route }).is_err() && reserved {
                        immediate.push_back(Event::ActivityFinished { request });
                    }
                }
            }
            Effect::CloseWebSocket { cell, websocket } => {
                // The core declined to hold this transport. Drop it and tell
                // the core it is gone, so the cell is not left believing it
                // has a socket the node already closed.
                eprintln!(
                    "celld refused an outbound WebSocket for {cell}: the node's \
                     outbound pin budget is spent"
                );
                celld::js::ws_unregister(websocket);
                immediate.push_back(Event::WebSocketClosed { cell, websocket });
            }
            Effect::Halt { code, reason } => {
                // Say why before going. Self-fencing is the most drastic thing
                // this process does, and an exit code on its own leaves an
                // operator to guess between a lease it could not renew, a
                // replicator that died, and a crash.
                match reason {
                    celld_logic::HaltReason::NodeLeaseExpired => tracing::warn!(
                        event = "node_lease_watchdog_fence",
                        code,
                        "SELF-FENCE: node lease not renewed within TTL — halting"
                    ),
                }
                let _ = self.fence.send(code);
            }
        }
    }

    fn state_json(&self) -> String {
        let residents = self
            .state
            .residents()
            .into_iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        let published = self
            .published
            .iter()
            .map(|cell| format!("{cell:?}"))
            .collect::<Vec<_>>()
            .join(",");
        // Both numbers: a gap between them is memory the allocator kept, which
        // no eviction returns. One sample, so they cannot disagree.
        let memory = celld::memory::sample();
        // `restoring` is a sum, and a node that refuses cells for a quarter of
        // an hour needs its parts. `activating` holds a permit and is doing
        // work; `activation_waiting` is queued behind the activation ceiling;
        // `capacity_waiting` is queued behind residency. The census says where
        // every cell is, which `occupied` cannot: it counts residency, so a
        // node part-way through thousands of cold starts reports almost none.
        // Issue #50 is open because none of this was recorded at the time.
        let phases = self
            .state
            .phase_census()
            .into_iter()
            .map(|(phase, count)| format!("{phase:?}:{count}"))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"ownership\":{:?},\"occupied\":{},\"evicting\":{},\"restoring\":{},\"activating\":{},\"activation_waiting\":{},\"capacity_waiting\":{},\"phases\":{{{}}},\"shedding\":{},\"rss_bytes\":{},\"in_use_bytes\":{},\"residents\":[{}],\"published\":[{}],\"publishes\":{},\"stops\":{}}}",
            self.ownership.name(),
            self.state.occupied(),
            self.state.evicting(),
            self.state.activation_backlog(),
            self.state.activating(),
            self.state.activation_waiting().len(),
            self.state.waiting().len(),
            phases,
            self.state
                .shed_reason()
                .map_or_else(|| "null".to_string(), |reason| format!("{reason:?}")),
            memory.rss_bytes,
            memory.in_use_bytes,
            residents,
            published,
            self.publishes,
            self.stops
        )
    }

    fn begin_route_if_cold(&mut self, cell: &str) {
        if !matches!(
            self.state.phase(cell),
            Some(Phase::Resident { .. } | Phase::EnsuringDurability { .. } | Phase::Remote { .. })
        ) {
            self.route_timings
                .entry(cell.to_string())
                .or_insert_with(CellRouteTiming::new);
        }
    }

    fn route_effect_started(&mut self, cell: &str) {
        if let Some(timing) = self.route_timings.get_mut(cell) {
            timing.effect_started();
        }
    }

    fn observe_capacity_wait(&mut self, cell: &str) {
        if matches!(self.state.phase(cell), Some(Phase::WaitingCapacity)) {
            if let Some(timing) = self.route_timings.get_mut(cell) {
                timing
                    .capacity_wait_started
                    .get_or_insert_with(Instant::now);
            }
        }
    }

    fn record_effect_timing(&mut self, completed: EffectTiming) {
        if let Some(timing) = self.route_timings.get_mut(&completed.cell) {
            timing.record(completed.stage, completed.elapsed_us);
        }
    }

    fn finish_route(
        &mut self,
        cell: &str,
        outcome: &str,
        failure_phase: &str,
        owner: Option<(&str, u64)>,
    ) {
        let Some(mut timing) = self.route_timings.remove(cell) else {
            return;
        };
        if let Some(started) = timing.capacity_wait_started.take() {
            timing.capacity_wait_us = timing
                .capacity_wait_us
                .saturating_add(started.elapsed().as_micros() as u64);
        }
        let (owner_node, epoch) = owner.unwrap_or(("", 0));
        tracing::debug!(
            target: "timing",
            event = "cell_route_timing",
            outcome,
            failure_phase,
            scope = %cell,
            node = %self.state.node(),
            region = %self.region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            owner_node,
            epoch,
            fresh = timing.fresh.unwrap_or(false),
            total_us = timing.started.elapsed().as_micros() as u64,
            latch_wait_us = timing.latch_wait_us,
            ownership_read_us = timing.ownership_read_us,
            node_lease_lookup_us = timing.node_lease_lookup_us,
            capacity_lookup_us = timing.capacity_lookup_us,
            capacity_wait_us = timing.capacity_wait_us,
            activation_slot_wait_us = timing.activation_slot_wait_us,
            lease_permit_us = timing.lease_permit_us,
            ownership_acquire_us = timing.ownership_acquire_us,
            replica_discovery_us = timing.replica_discovery_us,
            restore_us = timing.restore_us,
            isolate_startup_us = timing.isolate_startup_us,
            registry_insert_us = timing.registry_insert_us,
            "cell route resolved"
        );
    }
}

fn request_error_phase(error: RequestError) -> &'static str {
    match error {
        RequestError::NodeUnavailable | RequestError::NodeFenced => "node_authority",
        RequestError::ResolveFailed | RequestError::PeerIncompatible => "ownership_lookup",
        RequestError::CapacityExhausted => "capacity_wait",
        RequestError::AcquireFailed => "ownership_acquire",
        RequestError::RestoreFailed => "restore",
        RequestError::RuntimeFailed => "isolate_startup",
        RequestError::PublishFailed => "registry_insert",
        RequestError::DurabilityUnproven => "output_gate",
    }
}

type HttpReply = Response<UnsyncBoxBody<Bytes, std::io::Error>>;

const STALE_ROUTE_HEADER: &str = "x-cells-route-error";
const STALE_ROUTE_VALUE: &str = "stale-owner";
const DURABLE_OBJECT_ROUTING_ERROR_MARKER: &str = "__CELLD_DO_ROUTING_ERROR__:";

fn owner_unreachable(scope: &str, owner: &str, source: anyhow::Error) -> anyhow::Error {
    // Record how the attempt failed, not just that it did. `connect` is the
    // one that decides whether a retry is safe -- a request that never left
    // this node may be re-sent, a truncated read may not, because the owner
    // already ran it. Without these an operator cannot tell an unreachable
    // peer from one that answered badly, and neither can a bug report.
    let transport = source.downcast_ref::<reqwest::Error>();
    let cause = source
        .source()
        .map(ToString::to_string)
        .unwrap_or_else(|| source.to_string());
    tracing::warn!(
        %scope,
        %owner,
        error = %source,
        %cause,
        connect = transport.is_some_and(reqwest::Error::is_connect),
        timeout = transport.is_some_and(reqwest::Error::is_timeout),
        request = transport.is_some_and(reqwest::Error::is_request),
        body = transport.is_some_and(reqwest::Error::is_body),
        decode = transport.is_some_and(reqwest::Error::is_decode),
        "peer owner unreachable"
    );
    let detail = serde_json::json!({
        "scope": scope,
        "owner": owner,
    });
    source.context(format!("{DURABLE_OBJECT_ROUTING_ERROR_MARKER}{detail}"))
}

fn response(status: StatusCode, body: impl Into<Bytes>) -> HttpReply {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .expect("static HTTP response")
}

fn asset_response(response: axum::response::Response) -> HttpReply {
    response.map(|body| body.map_err(std::io::Error::other).boxed_unsync())
}

fn peer_response(mut response: HttpReply) -> HttpReply {
    response.headers_mut().insert(
        hyper::header::HeaderName::from_static(peer_auth::RESPONSE_VERSION_HEADER),
        hyper::header::HeaderValue::from_static(peer_auth::PROTOCOL_VERSION_TEXT),
    );
    response
}

fn runtime_response(worker_response: celld::js::HttpResponse) -> HttpReply {
    let Ok(status) = StatusCode::from_u16(worker_response.status) else {
        return response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker status");
    };
    let mut builder = Response::builder().status(status);
    for (name, value) in worker_response.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        builder = builder.header(name, value);
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let chunks = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(chunks).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    builder
        .body(body)
        .unwrap_or_else(|_| response(StatusCode::INTERNAL_SERVER_ERROR, "invalid Worker headers"))
}

fn peer_runtime_response(worker_response: celld::js::HttpResponse) -> HttpReply {
    let wire_status = if worker_response.status == 101 && worker_response.ws.is_some() {
        StatusCode::OK
    } else {
        let Ok(status) = StatusCode::from_u16(worker_response.status) else {
            return peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid Worker status",
            ));
        };
        status
    };
    let mut builder = Response::builder().status(wire_status);
    for (name, value) in worker_response.headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "connection" | "content-length" | "transfer-encoding"
        ) {
            continue;
        }
        builder = builder.header(name, value);
    }
    if let Some(target) = worker_response.ws {
        if let Ok(value) = serde_json::to_string(&target) {
            builder = builder.header("x-celld-ws-target", value);
        }
    }
    // Say whether this body is streamed. The peer reads an unmarked body
    // rather than handing it on, so without this every response looked
    // buffered and streaming was off across the hop.
    if worker_response.stream.is_some() {
        builder = builder.header("x-celld-body-stream", "1");
    }
    let body = match worker_response.stream {
        Some(stream) => {
            let stream = stream.map(|chunk| {
                chunk
                    .map(|bytes| Frame::data(Bytes::from(bytes)))
                    .map_err(std::io::Error::other)
            });
            StreamBody::new(stream).boxed_unsync()
        }
        None => Full::new(Bytes::from(worker_response.body))
            .map_err(|never| match never {})
            .boxed_unsync(),
    };
    peer_response(builder.body(body).expect("Worker peer response"))
}

#[derive(Debug)]
struct StalePeerRoute {
    scope: String,
}

impl std::fmt::Display for StalePeerRoute {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "peer no longer owns {}", self.scope)
    }
}

impl std::error::Error for StalePeerRoute {}

#[derive(Debug)]
struct RoutedRequestError(RequestError);

impl std::fmt::Display for RoutedRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "route failed: {:?}", self.0)
    }
}

impl std::error::Error for RoutedRequestError {}

fn classify_remote_attempt(error: &anyhow::Error) -> celld_logic::routing::Attempt {
    if error.downcast_ref::<StalePeerRoute>().is_some() {
        celld_logic::routing::Attempt::NotOwner
    } else if error
        .downcast_ref::<reqwest::Error>()
        .is_some_and(reqwest::Error::is_connect)
    {
        celld_logic::routing::Attempt::NeverConnected
    } else {
        celld_logic::routing::Attempt::Ambiguous
    }
}

struct WebSocketRouteTiming {
    started: Instant,
    route_resolution_us: u64,
    dispatch_us: u64,
    attempts: u8,
}

impl WebSocketRouteTiming {
    fn emit(
        &self,
        app: &AppHandle,
        scope: &str,
        request_id: Option<celld::js::RequestId>,
        outcome: &str,
        route: &str,
        peer_node: &str,
    ) {
        let request_id = request_id
            .map(celld::js::request_id_string)
            .unwrap_or_default();
        let (node, region) = app
            .runtime
            .as_ref()
            .map_or(("", ""), |runtime| (runtime.node(), runtime.region()));
        tracing::debug!(
            target: "timing",
            event = "websocket_route_timing",
            outcome,
            route,
            peer_node,
            scope,
            request_id,
            node,
            region,
            runtime_version = env!("CARGO_PKG_VERSION"),
            attempts = self.attempts,
            total_us = self.started.elapsed().as_micros() as u64,
            route_resolution_us = self.route_resolution_us,
            dispatch_us = self.dispatch_us,
            "WebSocket cell request resolved"
        );
    }
}

/// The output gate for the co-hosted (same-isolate) Durable Object fast path.
/// That path runs a resident DO's `fetch` inline, bypassing `dispatch_do_call`;
/// when it writes, the inline handler calls here to hold its response until the
/// cell is durable. Reuses the routed machinery: `request` pins the cell (no
/// eviction mid-wait) and `gate_write` drives the core gate. `Ok` releases the
/// inline response; `Err` breaks the call, as a routed gate failure would.
async fn dispatch_gate(app: AppHandle, req: celld::js::GateReq) {
    if !app.output_gate {
        let _ = req.reply.send(Ok(()));
        return;
    }
    let routed = match app.request(req.scope.clone()).await {
        Ok(routed) => routed,
        Err(error) => {
            let _ = req.reply.send(Err(error));
            return;
        }
    };
    // The guard pins the cell and releases the request on drop, so the else
    // branch does not leak the just-acquired request.
    let _activity = app.activity(routed.request, req.scope.clone());
    let result = if routed.route == Route::Local {
        app.gate_write(routed.request, req.position).await
    } else {
        // The owning isolate should route the cell locally; if it moved off the
        // node mid-call, fail closed rather than acknowledge an unproven write.
        Err(RequestError::NodeFenced)
    };
    let _ = req.reply.send(result);
}

async fn dispatch_do_call(app: AppHandle, call: DoCallReq) {
    let DoCallReq {
        request_id,
        cancel,
        deliver_abort_to_handler,
        scope,
        name,
        url,
        method,
        body,
        headers,
        reply,
        order,
        parent,
    } = call;
    let mut cancel = cancel;
    let mut order = order;
    let mut websocket_timing = headers
        .iter()
        .any(|(name, value)| {
            name.eq_ignore_ascii_case("upgrade") && value.eq_ignore_ascii_case("websocket")
        })
        .then(|| WebSocketRouteTiming {
            started: Instant::now(),
            route_resolution_us: 0,
            dispatch_us: 0,
            attempts: 0,
        });
    let operation = async {
        let mut dispatcher = celld_logic::routing::Dispatcher::default();
        loop {
            if let Some(timing) = websocket_timing.as_mut() {
                timing.attempts = timing.attempts.saturating_add(1);
            }
            let route_started = Instant::now();
            // A disconnect before routing completes has executed no handler,
            // so cancel the core request and release its activation admission.
            // Once routing completes, the same signal moves into the local or
            // remote dispatch below and aborts work that did start.
            let route = app.request(scope.clone());
            let routed = if deliver_abort_to_handler {
                // Workerd delivers an explicit JavaScript AbortSignal to the
                // target request. Resolve the route first, then give the
                // already-fired receiver to fetch_cell so the handler sees
                // request.signal and its waitUntil work can continue.
                route.await
            } else {
                match cancel.as_mut() {
                    Some(cancel) => tokio::select! {
                        routed = route => routed,
                        _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                    },
                    None => route.await,
                }
            };
            let routed = match routed {
                Ok(routed) => routed,
                Err(error) => {
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.route_resolution_us = timing
                            .route_resolution_us
                            .saturating_add(route_started.elapsed().as_micros() as u64);
                        timing.emit(&app, &scope, request_id, "route_error", "", "");
                    }
                    break Err(anyhow::Error::new(RoutedRequestError(error)));
                }
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.route_resolution_us = timing
                    .route_resolution_us
                    .saturating_add(route_started.elapsed().as_micros() as u64);
            }
            let Routed { request, route } = routed;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let dispatch_started = Instant::now();
                    let _activity = app.activity(request, scope.clone());
                    let result = async {
                        let runtime = app.runtime.as_ref().context("no cell runtime")?;
                        let response = runtime
                            .fetch_cell(
                                scope.clone(),
                                name,
                                RuntimeFetch {
                                    url,
                                    method,
                                    body,
                                    headers,
                                    request_id,
                                    // Moved on the first attempt and gone on
                                    // a retry, which is right: a retry is a
                                    // second delivery of a call whose place
                                    // in the order was already taken.
                                    order: order.take(),
                                    parent,
                                },
                                cancel.take(),
                            )
                            .await?;
                        if let Some(target) = &response.ws {
                            let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                                WebSocketKind::Hibernatable
                            } else {
                                WebSocketKind::Regular
                            };
                            app.websocket_opened(target.scope.clone(), target.id, kind)
                                .await?;
                        }
                        Ok(response)
                    }
                    .await;
                    // Output gate (RPO=0): a handler that advanced the cell's
                    // write position has its response held until the core proves
                    // the cell durable. The request is still pinned (the
                    // activity guard has not dropped), so it fails rather than
                    // acknowledges a write the node cannot prove durable.
                    let result = match result {
                        Ok(response) => match response.write_position.filter(|_| app.output_gate) {
                            Some(position) => match app.gate_write(request, position).await {
                                Ok(()) => Ok(response),
                                Err(error) => Err(anyhow::Error::new(RoutedRequestError(error))),
                            },
                            None => Ok(response),
                        },
                        Err(error) => Err(error),
                    };
                    if let Some(timing) = websocket_timing.as_mut() {
                        timing.dispatch_us = timing
                            .dispatch_us
                            .saturating_add(dispatch_started.elapsed().as_micros() as u64);
                        timing.emit(
                            &app,
                            &scope,
                            request_id,
                            if result.is_ok() { "ok" } else { "error" },
                            "local",
                            app.runtime.as_ref().map_or("", RuntimeManager::node),
                        );
                    }
                    break result;
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            let dispatch_started = Instant::now();
            let remote_call = async {
                anyhow::ensure!(
                    peer_protocol == peer_auth::PROTOCOL_VERSION,
                    "peer {node} speaks incompatible protocol {peer_protocol}"
                );
                let encoded = serde_json::json!({
                    "name": &name,
                    "url": &url,
                    "method": &method,
                    "bodyBase64": base64::engine::general_purpose::STANDARD.encode(&body),
                    "headers": &headers,
                    "requestId": request_id.map(celld::js::request_id_string),
                    "capacityHandoff": epoch == 0,
                });
                let encoded = serde_json::to_vec(&encoded)?;
                let path = format!("/__do/{scope}");
                let request = app.peer_auth.sign(
                    app.peer_http.post(format!("http://{addr}{path}")),
                    "POST",
                    &path,
                    &encoded,
                    &node,
                )?;
                let response =
                    request.body(encoded).send().await.map_err(|error| {
                        owner_unreachable(&scope, &addr, anyhow::Error::new(error))
                    })?;
                peer_auth::validate_response(response.headers())?;
                if response
                    .headers()
                    .get(STALE_ROUTE_HEADER)
                    .is_some_and(|value| value == STALE_ROUTE_VALUE)
                {
                    return Err(owner_unreachable(
                        &scope,
                        &addr,
                        anyhow::Error::new(StalePeerRoute {
                            scope: scope.clone(),
                        }),
                    ));
                }
                let mut websocket: Option<celld::js::WsTarget> = response
                    .headers()
                    .get("x-celld-ws-target")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| serde_json::from_str(value).ok());
                if let Some(target) = websocket.as_mut() {
                    target.peer_node = Some(node.clone());
                    target.peer_addr = Some(addr.clone());
                    target.peer_epoch = Some(epoch);
                }
                let status = if websocket.is_some() && response.status() == StatusCode::OK {
                    101
                } else {
                    response.status().as_u16()
                };
                let headers = response
                    .headers()
                    .iter()
                    .filter(|(name, _)| {
                        name.as_str() != "x-celld-ws-target"
                            && name.as_str() != "x-celld-body-stream"
                    })
                    .filter_map(|(name, value)| {
                        value
                            .to_str()
                            .ok()
                            .map(|value| (name.to_string(), value.to_string()))
                    })
                    .collect();
                let (body, stream) = if websocket.is_some() {
                    (Vec::new(), None)
                } else {
                    // Every proxied body streams through, marked or not. The
                    // owner already ran the request, so a failure mid-body is
                    // ambiguous and must surface without a redispatch -- and
                    // streaming makes that structural: the 200 head has gone
                    // out before the failure is known, a chunk error aborts
                    // the client's read instead of ending it cleanly, and
                    // nothing upstream of a sent head can re-send. Buffering
                    // here was the old shape; it turned a truncated body into
                    // a whole-response failure after the owner had committed.
                    (
                        Vec::new(),
                        Some(celld::js::reqwest_response_stream(response)),
                    )
                };
                Ok(HttpResponse {
                    status,
                    headers,
                    body,
                    ws: websocket,
                    stream,
                    // A proxied remote response wrote on the owner, not here.
                    write_position: None,
                })
            };
            let remote = match cancel.as_mut() {
                Some(cancel) => tokio::select! {
                    remote = remote_call => remote,
                    _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                },
                None => remote_call.await,
            };
            if let Some(timing) = websocket_timing.as_mut() {
                timing.dispatch_us = timing
                    .dispatch_us
                    .saturating_add(dispatch_started.elapsed().as_micros() as u64);
            }
            match remote {
                Ok(response) => {
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "ok", "remote", &node);
                    }
                    break Ok(response);
                }
                Err(error) => {
                    // Epoch zero is a candidate, not an owner. A signed peer
                    // refusal proves this attempt did not execute and should
                    // not consume the ordinary one-owner stale-route budget;
                    // the core excludes that exact load sample before the
                    // next deterministic placement decision.
                    let capacity_refused = epoch == 0
                        && error
                            .chain()
                            .any(|cause| cause.downcast_ref::<StalePeerRoute>().is_some());
                    if capacity_refused {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    let attempt = classify_remote_attempt(&error);
                    if dispatcher.redispatch(attempt) {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    if let Some(timing) = websocket_timing.as_ref() {
                        timing.emit(&app, &scope, request_id, "error", "remote", &node);
                    }
                    break Err(error);
                }
            }
        }
    };
    let result = operation.await;
    let _ = reply.send(result);
}

async fn dispatch_rpc_call(app: AppHandle, call: RpcCallReq) {
    let RpcCallReq {
        scope,
        name,
        method,
        args,
        reply,
    } = call;
    let result = async {
        let mut dispatcher = celld_logic::routing::Dispatcher::default();
        loop {
            let Routed { request, route } = app
                .request(scope.clone())
                .await
                .map_err(|error| anyhow::anyhow!("route RPC {scope}: {error:?}"))?;
            let (node, addr, epoch, peer_protocol) = match route {
                Route::Local => {
                    let _activity = app.activity(request, scope.clone());
                    let outcome = app
                        .runtime
                        .as_ref()
                        .context("no cell runtime")?
                        .rpc(scope, name, method, args)
                        .await?;
                    // Output gate (RPO=0): an RPC method that advanced the
                    // cell's write position has its reply held until the core
                    // proves the cell durable, exactly as fetch does. The
                    // activity guard is still alive, so the cell stays pinned
                    // across the wait.
                    if let Some(position) = outcome.write_position.filter(|_| app.output_gate) {
                        app.gate_write(request, position)
                            .await
                            .map_err(|error| anyhow::Error::new(RoutedRequestError(error)))?;
                    }
                    return Ok(outcome.data);
                }
                Route::Remote {
                    node,
                    addr,
                    epoch,
                    peer_protocol,
                } => (node, addr, epoch, peer_protocol),
            };
            anyhow::ensure!(
                peer_protocol == peer_auth::PROTOCOL_VERSION,
                "peer {node} speaks incompatible protocol {peer_protocol}"
            );
            let structured = matches!(args, celld::js::RpcData::V8(_));
            let envelope = match &args {
                celld::js::RpcData::Json(json) => serde_json::json!({
                    "name": &name,
                    "method": &method,
                    "args": serde_json::from_str::<serde_json::Value>(json)
                        .unwrap_or_else(|_| serde_json::json!([])),
                }),
                celld::js::RpcData::V8(bytes) => serde_json::json!({
                    "name": &name,
                    "method": &method,
                    "sc": base64::engine::general_purpose::STANDARD.encode(bytes),
                }),
            };
            let encoded = serde_json::to_vec(&envelope)?;
            let path = format!("/__rpc/{scope}");
            let request = app.peer_auth.sign(
                app.peer_http.post(format!("http://{addr}{path}")),
                "POST",
                &path,
                &encoded,
                &node,
            )?;
            let response = request.body(encoded).send().await;
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    let attempt = classify_remote_attempt(&anyhow::Error::new(error));
                    if dispatcher.redispatch(attempt) {
                        app.invalidate_remote(scope.clone(), node, epoch).await;
                        continue;
                    }
                    return Err(anyhow::anyhow!("remote RPC transport failed"));
                }
            };
            peer_auth::validate_response(response.headers())?;
            if response
                .headers()
                .get(STALE_ROUTE_HEADER)
                .is_some_and(|value| value == STALE_ROUTE_VALUE)
            {
                if dispatcher.redispatch(celld_logic::routing::Attempt::NotOwner) {
                    app.invalidate_remote(scope.clone(), node, epoch).await;
                    continue;
                }
                anyhow::bail!("remote RPC owner was stale");
            }
            anyhow::ensure!(
                response.status().is_success(),
                "remote RPC failed with {}",
                response.status()
            );
            return Ok(if structured {
                celld::js::RpcData::V8(response.bytes().await?.to_vec())
            } else {
                celld::js::RpcData::Json(response.text().await?)
            });
        }
    }
    .await;
    let _ = reply.send(result);
}

async fn request_payload(
    request: Request<Incoming>,
    trust_forwarded_headers: bool,
) -> Result<(String, String, Vec<u8>, Vec<(String, String)>), HttpReply> {
    let (parts, body) = request.into_parts();
    let body = body.collect().await.map_err(|error| {
        response(
            StatusCode::BAD_REQUEST,
            format!("request body failed: {error}"),
        )
    })?;
    let headers = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    Ok((
        request_url(&parts, trust_forwarded_headers),
        parts.method.to_string(),
        body.to_bytes().to_vec(),
        headers,
    ))
}

/// The refusal every path arm gives a cell scope that fails the charset gate.
///
/// A scope taken from a URL segment reaches `db_path`, which joins it under the
/// data directory, and the replication client, which builds a bucket key from
/// it, so a scope carrying its own path segments walks out of both. The gate
/// itself is reified in `celld_logic::cell`, next to the peer-identity gate it
/// mirrors.
fn malformed_scope() -> HttpReply {
    response(
        StatusCode::BAD_REQUEST,
        "{\"error\":\"malformed_cell_scope\"}",
    )
}

/// `request.url` controls application routing and absolute links, so celld
/// does not let an untrusted forwarding header or request-target authority set
/// its scheme or host. The path and query always come from the request target.
/// The host comes from `Host`, and the scheme is `http` because celld does not
/// terminate TLS.
///
/// An operator can set `--trust-forwarded-headers` when a trusted proxy
/// replaces both forwarded headers. The trusted read takes the last value
/// because a proxy can append its value after a client-supplied value.
fn request_url(parts: &hyper::http::request::Parts, trust_forwarded_headers: bool) -> String {
    let header = |name: &str, take_last: bool| {
        parts
            .headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| {
                if take_last {
                    value.split(',').next_back()
                } else {
                    value.split(',').next()
                }
            })
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let forwarded = |name: &str| {
        trust_forwarded_headers
            .then(|| header(name, true))
            .flatten()
    };
    let host = forwarded("x-forwarded-host")
        .or_else(|| header("host", false))
        .unwrap_or("celld.local");
    let scheme = forwarded("x-forwarded-proto").unwrap_or("http");
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", hyper::http::uri::PathAndQuery::as_str);
    format!("{scheme}://{host}{path_and_query}")
}

async fn internal_do(request: Request<Incoming>, app: AppHandle, scope: String) -> HttpReply {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
    let request_headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid body: {error}"),
            ));
        }
    };
    if let Err(error) = app.peer_auth.verify(
        &method,
        &path_and_query,
        &request_headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON: {error}"),
            ));
        }
    };
    let url = value
        .get("url")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("http://cell/")
        .to_string();
    let method = value
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("GET")
        .to_string();
    let name = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let request_body = value
        .get("bodyBase64")
        .and_then(serde_json::Value::as_str)
        .and_then(|body| base64::engine::general_purpose::STANDARD.decode(body).ok())
        .unwrap_or_default();
    let headers = serde_json::from_value(
        value
            .get("headers")
            .cloned()
            .unwrap_or_else(|| serde_json::json!([])),
    )
    .unwrap_or_default();
    let request_id = value
        .get("requestId")
        .and_then(serde_json::Value::as_str)
        .and_then(celld::js::parse_request_id);
    let capacity_handoff = value
        .get("capacityHandoff")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let routed = if capacity_handoff {
        app.capacity_request(scope.clone()).await
    } else {
        app.request(scope.clone()).await
    };
    let result = match routed {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            let Some(runtime) = &app.runtime else {
                return response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime");
            };
            let abort_scope = scope.clone();
            // The forwarding node hangs up when its own client does, so this
            // connection going away is the cancellation signal reaching the
            // owner -- and it arrives as a drop, which is why it is a guard
            // rather than the channel `fetch_cell` takes. Without it a handler
            // keeps running on the owner for a client that left the node it
            // dialled.
            let mut abort = AbortPeerFetchOnHangUp {
                runtime: runtime.clone(),
                scope: abort_scope,
                request_id,
            };
            match runtime
                .fetch_cell(
                    scope,
                    name,
                    RuntimeFetch {
                        url,
                        method,
                        body: request_body,
                        headers,
                        request_id,
                        // A peer's call has no caller in this process; its
                        // trace context crossing nodes is phase 2.
                        order: None,
                        parent: None,
                    },
                    None,
                )
                .await
            {
                Ok(worker_response) => {
                    abort.request_id = None;
                    // Output gate (RPO=0): a peer-served handler that advanced
                    // the cell's committed position holds its reply until the
                    // cell is proven durable, exactly as the local dispatch
                    // path does. This path used to acknowledge unproven writes
                    // — the loss the takeover tests catch. The activity guard
                    // is still alive, so the request stays pinned across the
                    // wait.
                    if let Some(position) =
                        worker_response.write_position.filter(|_| app.output_gate)
                    {
                        if let Err(error) = app.gate_write(request, position).await {
                            return peer_response(response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("durability unproven: {error:?}"),
                            ));
                        }
                    }
                    if let Some(target) = &worker_response.ws {
                        let kind = if celld::js::ws_hibernatable(target.id).unwrap_or(false) {
                            WebSocketKind::Hibernatable
                        } else {
                            WebSocketKind::Regular
                        };
                        if let Err(error) = app
                            .websocket_opened(target.scope.clone(), target.id, kind)
                            .await
                        {
                            return peer_response(response(
                                StatusCode::SERVICE_UNAVAILABLE,
                                format!("WebSocket core registration failed: {error:#}"),
                            ));
                        }
                    }
                    peer_runtime_response(worker_response)
                }
                Err(error) => response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("cell Worker failed: {error:#}"),
                ),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(RequestError::CapacityExhausted) => {
            let mut stale = response(StatusCode::CONFLICT, "capacity exhausted");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("route failed: {error:?}"),
        ),
    };
    peer_response(result)
}

async fn internal_abort(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid abort body: {error}"),
            ));
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some((encoded_scope, encoded_request)) = path
        .strip_prefix("/__abort/")
        .and_then(|rest| rest.rsplit_once('/'))
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort target"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid abort scope")),
    };
    if !celld_logic::cell::valid_cell_scope(&scope) {
        return peer_response(malformed_scope());
    }
    let Some(request_id) = celld::js::parse_request_id(encoded_request) else {
        return peer_response(response(StatusCode::BAD_REQUEST, "invalid request id"));
    };
    let result = match app.request(scope.clone()).await {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            match &app.runtime {
                Some(runtime) => {
                    runtime.abort_fetch(&scope, request_id);
                    response(StatusCode::NO_CONTENT, Bytes::new())
                }
                None => response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            stale
        }
        Err(error) => response(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("abort route failed: {error:?}"),
        ),
    };
    peer_response(result)
}

async fn internal_rpc(request: Request<Incoming>, app: AppHandle, scope: String) -> HttpReply {
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
    let request_headers = request.headers().clone();
    let body = match request.into_body().collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid body: {error}"),
            ));
        }
    };
    if let Err(error) = app.peer_auth.verify(
        &method,
        &path_and_query,
        &request_headers,
        &body,
        app.peer_auth.source(),
    ) {
        let mut denied = response(error.status(), error.message());
        if matches!(error, peer_auth::VerifyError::WrongTarget) {
            denied.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
        }
        return peer_response(denied);
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid JSON: {error}"),
            ));
        }
    };
    let name = value
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    let rpc_method = value
        .get("method")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    let args = match value.get("sc").and_then(serde_json::Value::as_str) {
        Some(bytes) => celld::js::RpcData::V8(
            base64::engine::general_purpose::STANDARD
                .decode(bytes)
                .unwrap_or_default(),
        ),
        None => celld::js::RpcData::Json(
            value
                .get("args")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([]))
                .to_string(),
        ),
    };
    let result = match app.request(scope.clone()).await {
        Ok(Routed {
            request,
            route: Route::Local,
        }) => {
            let _activity = app.activity(request, scope.clone());
            let Some(runtime) = &app.runtime else {
                return peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
            };
            // Output gate on the owner side, so a proxied RPC write is durable
            // before the calling node sees the reply -- the same rule the peer
            // fetch path follows.
            match runtime.rpc(scope, name, rpc_method, args).await {
                Ok(outcome) => {
                    if let Some(position) = outcome.write_position.filter(|_| app.output_gate) {
                        if let Err(error) = app.gate_write(request, position).await {
                            return peer_response(response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                format!("durability unproven: {error:?}"),
                            ));
                        }
                    }
                    Ok(outcome.data)
                }
                Err(error) => Err(error),
            }
        }
        Ok(Routed {
            route: Route::Remote { .. },
            ..
        }) => {
            let mut stale = response(StatusCode::CONFLICT, "stale route");
            stale.headers_mut().insert(
                hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
                hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
            );
            return peer_response(stale);
        }
        Err(error) => Err(anyhow::anyhow!("route failed: {error:?}")),
    };
    match result {
        Ok(celld::js::RpcData::Json(json)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/json")
                .body(
                    Full::new(Bytes::from(json))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC JSON response"),
        ),
        Ok(celld::js::RpcData::V8(bytes)) => peer_response(
            Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/octet-stream")
                .body(
                    Full::new(Bytes::from(bytes))
                        .map_err(|never| match never {})
                        .boxed_unsync(),
                )
                .expect("RPC clone response"),
        ),
        Err(error) => peer_response(response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("RPC failed: {error:#}"),
        )),
    }
}

#[path = "main/websocket.rs"]
mod websocket;
use websocket::{handle_peer_websocket, handle_websocket, outbound_websocket_task};

async fn handle_ingress(
    request: Request<Incoming>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> HttpReply {
    if matches!(*request.method(), hyper::Method::GET | hyper::Method::HEAD) {
        if let Some(resolver) = app
            .asset_script
            .as_deref()
            .and_then(|script| app.assets.get(script))
        {
            let path = request.uri().path();
            if !resolver.should_run_worker_first(path) {
                let head = request.method() == hyper::Method::HEAD;
                match resolver
                    .ingress_response(path, request.uri().query(), head, request.headers())
                    .await
                {
                    Ok(Some(response)) => return asset_response(response),
                    Ok(None) if resolver.asset_only() => {
                        return response(StatusCode::NOT_FOUND, "Not found");
                    }
                    Ok(None) => {}
                    Err(error) => {
                        eprintln!("celld asset response failed for {path}: {error:#}");
                        return response(
                            StatusCode::BAD_GATEWAY,
                            "Active deployment asset is unavailable",
                        );
                    }
                }
            }
        }
    }

    let (url, method, body, headers) =
        match request_payload(request, app.trust_forwarded_headers).await {
            Ok(payload) => payload,
            Err(response) => return response,
        };
    match app
        .fetch_worker(url, method, body, headers, connection)
        .await
    {
        Ok(worker_response) => runtime_response(worker_response),
        Err(error) => match error.downcast_ref::<celld::pool::AdmitError>() {
            // Saturation is not a failure of the request. Answering it now
            // lets the caller retry or shed; holding the connection until its
            // own deadline is what a node with no capacity used to do.
            Some(refused @ celld::pool::AdmitError::Refused(_)) => response(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("Worker refused: {refused}"),
            ),
            // A build failure is a fault, not saturation.
            Some(celld::pool::AdmitError::Build(_)) | None => response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            ),
        },
    }
}

async fn dispatch_asset_call(app: AppHandle, call: AssetCallReq) {
    let response = match app.assets.get(&call.script) {
        Some(resolver) => {
            resolver
                .binding_response(&call.url, &call.method, &call.headers)
                .await
        }
        None => Err(anyhow::anyhow!(
            "no asset resolver for script {}",
            call.script
        )),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_call(app: AppHandle, call: SvcCallReq) {
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .fetch_service(
                    &call.script,
                    call.url,
                    call.method,
                    call.body,
                    call.headers,
                    call.cancel,
                )
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

async fn dispatch_service_rpc(app: AppHandle, call: SvcRpcReq) {
    let response = match &app.runtime {
        Some(runtime) => {
            runtime
                .rpc_service(&call.script, call.entrypoint, call.method, call.args)
                .await
        }
        None => Err(anyhow::anyhow!("no Worker runtime")),
    };
    let _ = call.reply.send(response);
}

async fn internal_probe(request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid probe body: {error}"),
            ));
        }
    };
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or_else(|| parts.uri.path().to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        &parts.method,
        &path_and_query,
        &parts.headers,
        &body,
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    if parts.method != hyper::Method::GET {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let Some(challenge) = parts
        .headers
        .get("x-cells-probe-challenge")
        .and_then(|value| value.to_str().ok())
    else {
        return peer_response(response(StatusCode::BAD_REQUEST, "missing probe challenge"));
    };
    match celld::peer_probe::respond(app.peer_auth.source(), &app.advertise, challenge) {
        Ok(probe) => match serde_json::to_vec(&probe) {
            Ok(body) => peer_response(response(StatusCode::OK, body)),
            Err(_) => peer_response(response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "encode probe response",
            )),
        },
        Err(_) => peer_response(response(StatusCode::BAD_REQUEST, "invalid probe challenge")),
    }
}

async fn handle_public(
    request: Request<Incoming>,
    app: AppHandle,
    connection: ConnectionWorkerRequests,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    if draining && path != "/__celld/health" {
        let mut refused = response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"ok\":false,\"draining\":true}",
        );
        refused.headers_mut().insert(
            hyper::header::RETRY_AFTER,
            hyper::header::HeaderValue::from_static("1"),
        );
        refused.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
        return Ok(refused);
    }
    if path != "/__celld/health"
        && app.runtime.is_some()
        && fastwebsockets::upgrade::is_upgrade_request(&request)
    {
        return Ok(handle_websocket(request, app).await);
    }
    let mut result = match path.as_str() {
        "/__celld/health" if !app.is_draining() && app.healthy().await => {
            response(StatusCode::OK, "{\"ok\":true}")
        }
        "/__celld/health" => response(StatusCode::SERVICE_UNAVAILABLE, "{\"ok\":false}"),
        _ if app.runtime.is_some() => handle_ingress(request, app, connection).await,
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    if draining {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

async fn handle_internal(
    request: Request<Incoming>,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
) -> Result<HttpReply, Infallible> {
    let path = request.uri().path().to_string();
    let draining = app.is_draining();
    // A draining node accepts no new work: a request for a cell it just
    // released would re-claim the cell and undo the handoff, so everything
    // but diagnostics is refused, and `Connection: close` tears the
    // keep-alive down so the drain loop can finish instead of holding every
    // idle connection open until the deadline.
    if draining && !matches!(path.as_str(), "/__celld/probe" | "/state") {
        let mut refused = response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"ok\":false,\"draining\":true}",
        );
        refused.headers_mut().insert(
            hyper::header::RETRY_AFTER,
            hyper::header::HeaderValue::from_static("1"),
        );
        refused.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
        return Ok(refused);
    }
    if path.starts_with("/__ws/") && fastwebsockets::upgrade::is_upgrade_request(&request) {
        return Ok(handle_peer_websocket(request, app, &path).await);
    }
    let result = match path.as_str() {
        "/__celld/probe" => internal_probe(request, app).await,
        "/state" => response(StatusCode::OK, app.snapshot().await),
        "/shutdown" if request.method() != hyper::Method::POST => {
            response(StatusCode::METHOD_NOT_ALLOWED, "method not allowed")
        }
        "/shutdown" => {
            let preserve_ownership = request
                .uri()
                .query()
                .is_some_and(|query| query.split('&').any(|part| part == "handoff=preserve"));
            let mode = if preserve_ownership {
                ShutdownMode::Preserve
            } else {
                ShutdownMode::Handoff
            };
            let _ = shutdown.send(mode);
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ if path.starts_with("/__abort/") && app.runtime.is_some() => {
            internal_abort(request, app, path).await
        }
        _ if path.starts_with("/__do/") && app.runtime.is_some() => {
            if celld_logic::cell::valid_cell_scope(&path[6..]) {
                internal_do(request, app, path[6..].to_string()).await
            } else {
                peer_response(malformed_scope())
            }
        }
        _ if path.starts_with("/__rpc/") && app.runtime.is_some() => {
            if celld_logic::cell::valid_cell_scope(&path[7..]) {
                internal_rpc(request, app, path[7..].to_string()).await
            } else {
                peer_response(malformed_scope())
            }
        }
        _ if path.starts_with("/do/") && app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let cell = match runtime.cell_scope(&path[4..]) {
                Ok(cell) => cell,
                Err(error) => {
                    return Ok(response(StatusCode::BAD_REQUEST, format!("{error:#}")));
                }
            };
            let (url, method, body, headers) =
                match request_payload(request, app.trust_forwarded_headers).await {
                    Ok(payload) => payload,
                    Err(response) => return Ok(response),
                };
            // The same dispatcher a Durable Object call from inside a Worker
            // goes through. Public ingress used to resolve the route itself
            // and, on finding another owner, answer with a 307 and a JSON
            // description of where the cell lived -- with no Location header,
            // so nothing could follow it. A fleet behind a load balancer
            // serves a cell only from the node that happens to own it, which
            // is to say it does not serve a fleet at all.
            //
            // Going through `dispatch_do_call` also inherits the redispatch
            // policy and the cancellation channel, so a client that hangs up
            // reaches the owner rather than only the node it connected to.
            let (reply, receive) = oneshot::channel();
            let (cancel_tx, cancel) = oneshot::channel();
            let accepted = celld::js::submit_do_call(celld::js::DoCallReq {
                // Named, and named here: the abort fires only for a call that
                // carries both an id and a cancel signal, so leaving this None
                // silently costs the cancellation rather than failing.
                request_id: Some(celld::js::next_request_id()),
                cancel: Some(cancel),
                deliver_abort_to_handler: false,
                scope: cell,
                name: None,
                url,
                method,
                body,
                headers,
                reply,
                // An ingress call has no caller in this process to be
                // ordered against.
                order: None,
                // A direct-DO ingress caller's traceparent joins with the
                // cross-node propagation work (otel.md phase 2), which is
                // where remote parents of cell spans get their sampling
                // decision.
                parent: None,
            });
            if !accepted {
                return Ok(response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{\"error\":\"dispatcher unavailable\"}",
                ));
            }
            let _hangup = HangUp(Some(cancel_tx));
            match receive.await {
                Ok(Ok(worker_response)) => runtime_response(worker_response),
                Ok(Err(error)) => match error.downcast_ref::<RoutedRequestError>() {
                    Some(error) => response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("{{\"error\":\"{:?}\"}}", error.0),
                    ),
                    None => response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        format!("cell Worker failed: {error:#}"),
                    ),
                },
                Err(_) => response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "{\"error\":\"dispatcher dropped the call\"}",
                ),
            }
        }
        _ if path.starts_with("/cell/") && !celld_logic::cell::valid_cell_scope(&path[6..]) => {
            malformed_scope()
        }
        _ if path.starts_with("/cell/") => {
            let cell = path[6..].to_string();
            match app.request(cell.clone()).await {
                Ok(Routed {
                    request,
                    route: Route::Local,
                }) => {
                    let _activity = app.activity(request, cell.clone());
                    response(
                        StatusCode::OK,
                        format!("{{\"route\":\"local\",\"cell\":{cell:?}}}"),
                    )
                }
                Ok(Routed {
                    route:
                        Route::Remote {
                            node,
                            addr,
                            epoch,
                            peer_protocol,
                        },
                    ..
                }) => response(
                    StatusCode::TEMPORARY_REDIRECT,
                    format!(
                        "{{\"route\":\"remote\",\"node\":{node:?},\"addr\":{addr:?},\"epoch\":{epoch},\"peer_protocol\":{peer_protocol}}}"
                    ),
                ),
                Err(error) => response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("{{\"error\":\"{error:?}\"}}"),
                ),
            }
        }
        _ if path.starts_with("/evict/") && !celld_logic::cell::valid_cell_scope(&path[7..]) => {
            malformed_scope()
        }
        _ if path.starts_with("/evict/") => {
            app.evict(path[7..].to_string()).await;
            response(StatusCode::OK, "{\"ok\":true}")
        }
        _ => response(StatusCode::NOT_FOUND, "{\"error\":\"not_found\"}"),
    };
    // Close the connection behind any response sent while draining, so
    // keep-alive clients reconnect to a healthy node and the drain loop
    // can finish. A request that raced the drain flag converges on its
    // next request, which hits the gate above.
    let mut result = result;
    if draining {
        result.headers_mut().insert(
            hyper::header::CONNECTION,
            hyper::header::HeaderValue::from_static("close"),
        );
    }
    Ok(result)
}

#[derive(Clone, Copy)]
enum HttpSurface {
    Public,
    Internal,
}

fn serve_http_connection(
    stream: tokio::net::TcpStream,
    surface: HttpSurface,
    app: AppHandle,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
    mut connection_drain: watch::Receiver<bool>,
    connection_grace: std::time::Duration,
) -> ConnectionFuture {
    Box::pin(async move {
        // Serve on the runtime, not on this task. `main` drives its loop with
        // `block_on`, so serving there put every connection on one core.
        // Awaiting the spawned task keeps shutdown tracking unchanged.
        let served = tokio::spawn(async move {
            let connection_requests = ConnectionWorkerRequests::default();
            let service_requests = connection_requests.clone();
            let service = service_fn(move |request| {
                let app = app.clone();
                let shutdown = shutdown.clone();
                let service_requests = service_requests.clone();
                async move {
                    match surface {
                        HttpSurface::Public => handle_public(request, app, service_requests).await,
                        HttpSurface::Internal => handle_internal(request, app, shutdown).await,
                    }
                }
            });
            let connection = http1::Builder::new()
                // Reclaim a connection that never sends a complete request
                // head. The timeout also bounds an idle keep-alive waiting
                // for its next request.
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(std::time::Duration::from_secs(30))
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades();
            tokio::pin!(connection);
            let result = tokio::select! {
                result = &mut connection => Some(result),
                _ = connection_drain.changed() => {
                    connection.as_mut().graceful_shutdown();
                    tokio::time::timeout(connection_grace, &mut connection)
                        .await
                        .ok()
                }
            };
            connection_requests.abort_all();
            match result {
                Some(Err(error)) => eprintln!("celld connection failed: {error}"),
                None => tracing::warn!(
                    event = "connection_drain_forced",
                    grace_ms = connection_grace.as_millis(),
                    "forced an HTTP connection closed after its graceful drain"
                ),
                Some(Ok(())) => {}
            }
        });
        let _ = served.await;
    })
}

#[path = "main/cli.rs"]
mod cli;
use cli::{action_from_process, print_help, worker_loader_binding, Action};

/// An activation effect that has not answered by now is not going to help the
/// request that is waiting on it. celld had no such bound and parked requests
/// past ninety seconds.
const DEFAULT_OPERATION_DEADLINE_MS: u64 = 15_000;
/// Cold routes are I/O concurrency, but must stay below the point where they
/// can starve the node lease heartbeat or object store.
const DEFAULT_MAX_CONCURRENT_ACTIVATIONS: usize = 128;
/// celld's own default. Evictions are bounded far tighter than activations
/// because each one carries a durability proof, and a node that lets its whole
/// working set prove durability at once turns a walk down into a thundering
/// herd against the bucket.
const DEFAULT_MAX_CONCURRENT_EVICTIONS: usize = 4;
/// The shutdown handoff bound. Wider than the eviction bound because a
/// draining node has no live traffic left to protect and a stop grace to
/// beat, but still bounded so a node-wide handoff cannot thundering-herd
/// the bucket.
const DEFAULT_MAX_CONCURRENT_RELEASES: usize = 128;
/// Preserved SQLite snapshots make a same-node wake a rename instead of a
/// remote restore, but must not grow with the lifetime population of a node.
const DEFAULT_LOCAL_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
/// The walk is O(cached cells), so keep it off the hot maintenance cadence.
const LOCAL_CACHE_PRUNE_PERIOD: std::time::Duration = std::time::Duration::from_secs(60);

fn main() -> anyhow::Result<()> {
    celld::env_vars::validate()?;
    // Parse the telemetry group once, before any command or runtime work.
    // Its specialized values share the strict scalar parsers in env_vars.
    let telemetry_config = celld::telemetry::Config::from_env()?;
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();
    // Before the runtime exists, so every worker thread inherits a PKRU that
    // grants access to V8's pointer-table protection key.
    celld::runtime::init_v8();
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if let Some(workers) = celld::env_vars::positive::<usize>("CELLD_TOKIO_THREADS")? {
        builder.worker_threads(workers);
    }
    builder.build()?.block_on(async_main(telemetry_config))
}

async fn async_main(telemetry_config: Option<celld::telemetry::Config>) -> anyhow::Result<()> {
    celld::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    // Docker and journald can stop consuming the process pipe during a log
    // burst. Logging must lose diagnostics under that backpressure rather
    // than block the Tokio workers that route requests and renew authority.
    let (log_writer, log_guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(8_192)
        .lossy(true)
        .finish(std::io::stdout());
    *LOG_GUARD.lock().unwrap() = Some(log_guard);
    tracing_subscriber::fmt()
        .with_writer(log_writer)
        // The custom writer defeats fmt's own TTY detection, and journald
        // must not receive ANSI escapes.
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stdout()))
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // After the subscriber, because this reports whether the allocator agreed
    // to return freed pages on a timer. A node without that thread holds
    // retention until a thread allocates again, which is the condition behind
    // issue #36, so the operator has to be able to read the answer.
    celld::memory::tune_allocator();
    let mut settings = match action_from_process()? {
        Action::Deploy(arguments) => return fleet::run_deploy(arguments).await,
        Action::Connect(arguments) => {
            return celld::control_plane::handle_connect_command(arguments).await
        }
        Action::Credentials(arguments) => {
            return celld::control_plane::handle_credentials_command(arguments).await
        }
        Action::Token(arguments) => {
            return celld::control_plane::handle_token_command(arguments).await
        }
        Action::Disconnect(arguments) => {
            return celld::control_plane::handle_disconnect_command(arguments).await
        }
        Action::Help => {
            print_help();
            return Ok(());
        }
        Action::Version => {
            let profile = if cfg!(debug_assertions) {
                " (debug)"
            } else {
                ""
            };
            println!("celld {}{profile}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        Action::Diagnose {
            mut settings,
            peers,
            read_only,
        } => {
            let ingress = celld::startup::bind_ingress_listener(&settings.listen).await?;
            let internal =
                celld::startup::bind_internal_listener(&celld::startup::InternalListenerSettings {
                    listen: settings.internal_listen.clone(),
                    advertise: settings.advertise.clone(),
                    unsafe_public_advertise: settings.unsafe_public_advertise,
                })
                .await?;
            // The bind proves the address is free; diagnose never serves on it,
            // and an operator should not read this line as a running listener.
            println!(
                "ok listen {} (bind check; diagnose does not serve)",
                ingress.listen
            );
            println!(
                "ok internal listen {} (bind check; diagnose does not serve)",
                internal.listen
            );
            println!(
                "ok advertise {} ({}; direct reachability is not inferred)",
                internal.advertise,
                internal.advertise.scope()
            );
            let managed_storage = if settings.control_plane {
                match celld::control_plane::installation_storage().context(
                    "managed diagnostics require an existing enrollment; run `celld --control-plane` first",
                )? {
                    celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                        settings.bucket = Some(storage.bucket.clone());
                        settings.endpoint = Some(storage.endpoint.clone());
                        settings.region = storage.region.clone();
                        Some(storage)
                    }
                    celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                        settings.bucket = Some(storage.bucket);
                        settings.endpoint = storage.endpoint;
                        settings.region = storage.region;
                        None
                    }
                }
            } else {
                None
            };
            let bucket = settings
                .bucket
                .ok_or_else(|| anyhow::anyhow!("celld diagnose requires --bucket"))?;
            let client = fleet::bucket_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            return fleet::diagnose(&client, peers, settings.unsafe_public_advertise, read_only)
                .await;
        }
        Action::Run(settings) => settings,
    };
    celld::startup::raise_file_limit();
    let max_resident = celld::env_vars::optional("CELLD_MAX_RESIDENT_CELLS")?
        // celld has no resident ceiling unless the operator configures one.
        // The clean-sheet prototype originally defaulted to eight, which
        // introduced eviction churn in otherwise unconstrained workloads and
        // made cancellation semantics depend on cold-reactivation latency.
        .unwrap_or(usize::MAX);
    let local_cache_max_bytes = local_cache_max_bytes_from_environment()?;
    let fail_publish_once = std::env::var_os("CELLD_TEST_FAIL_PUBLISH_ONCE").is_some();
    let ingress = celld::startup::bind_ingress_listener(&settings.listen).await?;
    let internal =
        celld::startup::bind_internal_listener(&celld::startup::InternalListenerSettings {
            listen: settings.internal_listen.clone(),
            advertise: settings.advertise.clone(),
            unsafe_public_advertise: settings.unsafe_public_advertise,
        })
        .await?;
    let advertise = internal.advertise.to_string();
    let listen = ingress.listen.to_string();
    let listener = ingress.listener;
    let internal_listener = internal.listener;
    let mut adapter_credential_version = None;
    let managed_storage = if settings.control_plane {
        // The control plane issues and validates S3-compatible storage
        // only; celld's GCS client authenticates with OAuth, which the
        // control plane's S3-shaped credentials cannot provide.
        if settings
            .bucket
            .as_deref()
            .is_some_and(|b| b.starts_with("gs://"))
        {
            anyhow::bail!(
                "--control-plane storage is S3-compatible; a gs:// bucket runs without it"
            );
        }
        // The control plane issues one bucket per fleet and its enrollment
        // API rejects a bucket name holding a slash, so a prefix has neither
        // a purpose nor a path through. Say so instead of failing enrollment.
        if settings.bucket.as_deref().is_some_and(|b| b.contains('/')) {
            anyhow::bail!("--control-plane does not accept a --bucket prefix");
        }
        let requested_byo =
            settings
                .bucket
                .as_ref()
                .map(|bucket| celld::control_plane::ByoStorageConfig {
                    bucket: bucket.clone(),
                    endpoint: settings.endpoint.clone(),
                    region: settings.region.clone(),
                });
        celld::control_plane::connect_on_startup_with_storage(requested_byo).await?;
        settings.load_deployment = true;
        let (storage, credential_version) =
            celld::control_plane::installation_storage_with_version()?;
        adapter_credential_version = Some(credential_version);
        match storage {
            celld::control_plane::InstallationStorageConfig::Managed(storage) => {
                settings.bucket = Some(storage.bucket.clone());
                settings.endpoint = Some(storage.endpoint.clone());
                settings.region = storage.region.clone();
                Some(storage)
            }
            celld::control_plane::InstallationStorageConfig::Byo(storage) => {
                settings.bucket = Some(storage.bucket);
                settings.endpoint = storage.endpoint;
                settings.region = storage.region;
                None
            }
        }
    } else {
        None
    };
    let storage_credentials =
        managed_storage
            .as_ref()
            .map(|storage| celld::replication::StorageCredentials {
                access_key_id: storage.access_key_id.clone(),
                secret_access_key: storage.secret_access_key.clone(),
                session_token: storage.session_token.clone(),
            });
    let (tx, rx) = mpsc::unbounded_channel();
    let sample_tx = tx.clone();
    let alarm_tx = tx.clone();
    let alarm_observer: celld::runtime::AlarmObserver = Arc::new(move |cell, at_ms| {
        let _ = alarm_tx.send(Message::AlarmObserved {
            cell,
            at_ms,
            covered: false,
        });
    });
    let (fence_tx, mut fence_rx) = mpsc::unbounded_channel();
    let node = std::env::var("CELLD_NODE").unwrap_or_else(|_| random_node_session_id());
    let clean_reload_node = node.clone();
    celld::control_plane::install_reexec_node_session_id(&node)?;
    let probe_public_key = celld::peer_probe::install_signer()?;
    let max_activations =
        celld::env_vars::positive::<usize>("CELLD_ACTIVATIONS")?.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(DEFAULT_MAX_CONCURRENT_ACTIVATIONS)
        });
    let max_evictions = celld::env_vars::positive::<usize>("CELLD_EVICTIONS")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_EVICTIONS);
    let max_releases = celld::env_vars::positive::<usize>("CELLD_RELEASES")?
        .unwrap_or(DEFAULT_MAX_CONCURRENT_RELEASES);
    let data_dir = std::env::var_os("CELLD_TEST_DATA_DIR")
        .or_else(|| std::env::var_os("CELLD_WATCH"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("celld-{}", std::process::id())));
    let mut deploy_agent = None;
    let (runtime, ownership, peer_key, wake_scan, assets, asset_script) =
        if let Some(bucket) = settings.bucket.clone().filter(|_| settings.load_deployment) {
            let client = fleet::bucket_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            if settings.control_plane {
                fleet::validate_managed_bucket(&client).await?;
            } else {
                fleet::validate_bucket(&client).await?;
            }
            // The list above proves the bucket answers; it does not prove
            // the store enforces the conditional write this node fences
            // with. A store that ignores it makes the node self-fence in
            // a restart loop, so test it once, here, before serving.
            if settings.storage_probe {
                fleet::probe_storage_before_serving(&client, settings.control_plane).await?;
            }
            let lease_client = fleet::lease_bucket_client_with_credentials(
                &bucket,
                settings.endpoint.as_deref(),
                &settings.region,
                managed_storage.as_ref(),
            )?;
            if settings.control_plane {
                celld::control_plane::wait_for_initial_deployment(&client).await?;
                deploy_agent = Some(client.clone());
            }
            let peer_key = peer_auth::load_or_create(&client).await?;
            let mut deployment = fleet::load_current_worker(&client, node.clone()).await?;
            let primary_script = deployment.script_name.clone();
            let mut asset_resolvers = HashMap::new();
            if let Some(resolver) = deployment.assets.take() {
                asset_resolvers.insert(primary_script.clone(), resolver);
            }
            let mut visited = BTreeSet::from([primary_script.clone()]);
            let mut queue = deployment
                .services
                .iter()
                .map(|(_, script, _)| script.clone())
                .collect::<VecDeque<_>>();
            let mut cohosted = Vec::new();
            while let Some(target) = queue.pop_front() {
                if target == primary_script || !visited.insert(target.clone()) {
                    continue;
                }
                let mut loaded = fleet::load_named_worker(&client, &target, node.clone())
                    .await
                    .with_context(|| format!("load service binding target {target}"))?;
                if loaded.script_name != target {
                    anyhow::bail!(
                        "service pointer {target} resolved script {}",
                        loaded.script_name
                    );
                }
                queue.extend(loaded.services.iter().map(|(_, script, _)| script.clone()));
                if let Some(resolver) = loaded.assets.take() {
                    asset_resolvers.insert(target, resolver);
                }
                cohosted.push(CohostedWorker {
                    options: loaded.options,
                    services: loaded.services,
                    asset_binding: loaded.asset_binding,
                });
            }
            let wake = Arc::new(celld::wake::WakeFlusher::new());
            celld::js::set_arm_gate(ArmGate {
                bucket: client.clone(),
                flusher: wake.clone(),
            });
            // celld treats replication as a node service, not as a property
            // of today's manifest. Start it even for a stateless deployment
            // so a later deployment can introduce cells without changing the
            // durability contract underneath the node.
            let replication = Some(Replication::start(
                client.clone(),
                &data_dir,
                settings.endpoint.clone(),
                settings.region.clone(),
                storage_credentials.clone(),
            )?);
            let asset_script = Some(Arc::<str>::from(primary_script));
            let assets = Arc::new(asset_resolvers);
            let runtime = RuntimeManager::start(RuntimeOptions {
                worker: deployment.options,
                services: deployment.services,
                asset_binding: deployment.asset_binding,
                loader_binding: worker_loader_binding(),
                cohosted,
                data_dir: data_dir.clone(),
                replication,
                wake: Some(wake.clone()),
                alarm_observer: alarm_observer.clone(),
                node: node.clone(),
                region: settings.region.clone(),
            })?;
            let wake_scan = Some((client.clone(), wake.clone()));
            let ownership = Ownership::Bucket(Arc::new(
                BucketOwnership::new(client, lease_client, node.clone(), probe_public_key.clone())
                    .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
            ));
            (
                Some(runtime),
                Some(ownership),
                peer_key,
                wake_scan,
                assets,
                asset_script,
            )
        } else if let Ok(script_path) = std::env::var("CELLD_TEST_SCRIPT_PATH") {
            let source = std::fs::read_to_string(&script_path)?;
            let do_classes = std::env::var("CELLD_TEST_DO_CLASSES")
                .unwrap_or_default()
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .collect();
            let bindings = std::env::var("CELLD_TEST_DO_BINDINGS")
                .unwrap_or_default()
                .split(',')
                .filter_map(|value| value.split_once('='))
                .map(|(name, class)| (name.trim().to_string(), class.trim().to_string()))
                .filter(|(name, class)| !name.is_empty() && !class.is_empty())
                .collect();
            let options = WorkerConfigOptions {
                src: source,
                script_name: "celld-local".to_string(),
                do_classes,
                bindings,
                r2_bindings: Vec::new(),
                ai_binding: fleet::configured_ai_binding(None),
                vars: Vec::new(),
                node: node.clone(),
                modules: Vec::new(),
                compat: Compat::default(),
            };
            let (ownership, peer_key, wake, wake_scan) = match settings.bucket.clone() {
                Some(bucket) => {
                    let client = fleet::bucket_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let lease_client = fleet::lease_bucket_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let peer_key = peer_auth::load_or_create(&client).await?;
                    let wake = Arc::new(celld::wake::WakeFlusher::new());
                    celld::js::set_arm_gate(ArmGate {
                        bucket: client.clone(),
                        flusher: wake.clone(),
                    });
                    let wake_scan = Some((client.clone(), wake.clone()));
                    (
                        Some(Ownership::Bucket(Arc::new(
                            BucketOwnership::new(
                                client,
                                lease_client,
                                node.clone(),
                                probe_public_key.clone(),
                            )
                            .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
                        ))),
                        peer_key,
                        Some(wake),
                        wake_scan,
                    )
                }
                None => (None, random_peer_key(), None, None),
            };
            (
                Some(RuntimeManager::start(RuntimeOptions {
                    worker: options,
                    services: Vec::new(),
                    asset_binding: None,
                    loader_binding: worker_loader_binding(),
                    cohosted: Vec::new(),
                    data_dir: data_dir.clone(),
                    replication: None,
                    wake: wake.clone(),
                    alarm_observer: alarm_observer.clone(),
                    node: node.clone(),
                    region: settings.region.clone(),
                })?),
                ownership,
                peer_key,
                wake_scan,
                Arc::new(HashMap::new()),
                None,
            )
        } else {
            let (ownership, peer_key) = match settings.bucket.clone() {
                Some(bucket) => {
                    let client = fleet::bucket_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let lease_client = fleet::lease_bucket_client_with_credentials(
                        &bucket,
                        settings.endpoint.as_deref(),
                        &settings.region,
                        managed_storage.as_ref(),
                    )?;
                    let peer_key = peer_auth::load_or_create(&client).await?;
                    (
                        Some(Ownership::Bucket(Arc::new(
                            BucketOwnership::new(
                                client,
                                lease_client,
                                node.clone(),
                                probe_public_key.clone(),
                            )
                            .with_lease_ttl_ms(lease_ttl_ms_from_environment()),
                        ))),
                        peer_key,
                    )
                }
                None => (None, random_peer_key()),
            };
            (
                None,
                ownership,
                peer_key,
                None,
                Arc::new(HashMap::new()),
                None,
            )
        };
    if let Some(config) = &telemetry_config {
        let sink_bucket = match config.sink {
            celld::telemetry::SinkChoice::Bucket => {
                let Some(bucket) = settings.bucket.clone() else {
                    anyhow::bail!(
                        "CELLD_OTEL=1 but this node has no bucket; the \
                         bucket sink needs one (CELLD_BUCKET), or choose \
                         CELLD_OTEL_SINK=otlp"
                    );
                };
                // Its own client even for the fleet bucket: each open is its
                // own transport (bucket.rs), so telemetry PUT bursts never
                // share a connection pool with ownership traffic.
                Some(fleet::bucket_client_with_credentials(
                    config.bucket_override.as_deref().unwrap_or(&bucket),
                    settings.endpoint.as_deref(),
                    &settings.region,
                    managed_storage.as_ref(),
                )?)
            }
            // The collector path needs no bucket at all.
            celld::telemetry::SinkChoice::Otlp => None,
        };
        celld::telemetry::init(config, sink_bucket, node.clone(), settings.region.clone())?;
    }
    let peer_auth = Arc::new(PeerAuth::new(peer_key, node.clone())?);
    let resume_generation = celld::runtime::take_clean_reload_generation(&data_dir, &node);
    let clean_reload_candidate = resume_generation.is_some();
    let actor = Actor::from_environment(
        AdmissionLimits {
            resident: max_resident,
            activations: max_activations,
            evictions: max_evictions,
            releases: max_releases,
        },
        fail_publish_once,
        fence_tx,
        runtime.clone(),
        ownership,
        ActorIdentity {
            node: node.clone(),
            advertise: advertise.clone(),
            region: settings.region.clone(),
            managed: settings.control_plane,
        },
        resume_generation,
    )
    .await?;
    let process_generation = actor.lease_spec.generation.clone();
    let ownership_name = actor.ownership.name();
    let explorer_replication = runtime.as_ref().and_then(RuntimeManager::replication);
    let local_cache_replication = explorer_replication.clone();
    let (websocket_tx, mut websocket_rx) = mpsc::unbounded_channel();
    let app = AppHandle {
        tx,
        runtime,
        assets,
        asset_script,
        // Connect-only timeout: a peer request may legitimately run long, but
        // a handshake that never completes provably ran nothing, so failing it
        // fast lets the caller re-resolve the owner and redispatch.
        peer_http: reqwest::Client::builder()
            .connect_timeout(PEER_CONNECT_TIMEOUT)
            .build()
            .unwrap(),
        peer_auth,
        advertise: advertise.clone(),
        websockets: websocket_tx,
        draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        trust_forwarded_headers: settings.trust_forwarded_headers,
        // RPO=0 is the default. An operator can disable the output gate to
        // remove object-store replication latency from the write response,
        // explicitly accepting that an acknowledged write can be lost.
        output_gate: celld::env_vars::flag("CELLD_OUTPUT_GATE", true)?,
        max_outbound_websockets: celld::env_vars::positive_or(
            "CELLD_MAX_OUTBOUND_WEBSOCKETS",
            DEFAULT_MAX_OUTBOUND_WEBSOCKETS,
        )?,
    };
    let (do_call_tx, mut do_call_rx) = mpsc::unbounded_channel();
    celld::js::set_do_call_tx(do_call_tx);
    let (gate_tx, mut gate_rx) = mpsc::unbounded_channel();
    celld::js::set_gate_tx(gate_tx);
    let (rpc_call_tx, mut rpc_call_rx) = mpsc::unbounded_channel();
    celld::js::set_rpc_call_tx(rpc_call_tx);
    let (service_call_tx, mut service_call_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_call_tx(service_call_tx);
    let (service_rpc_tx, mut service_rpc_rx) = mpsc::unbounded_channel();
    celld::js::set_svc_rpc_tx(service_rpc_tx);
    let (asset_call_tx, mut asset_call_rx) = mpsc::unbounded_channel();
    celld::js::set_asset_call_tx(asset_call_tx);
    let (outbound_ws_tx, mut outbound_ws_rx) = mpsc::unbounded_channel();
    celld::js::set_outbound_ws_tx(outbound_ws_tx);
    // The core is a serial ownership actor, not a Worker executor. It owns the
    // node lease timer, so ingress, proxy retries, and restore completions must
    // not consume every scheduler turn it needs. Its isolated single-thread
    // runtime also keeps state transitions ordered exactly as the deterministic
    // executor models them. Request work, restores, and blocking scans stay on
    // the shared runtime and report their results back as messages.
    let (actor_exit_tx, mut actor_exit_rx) = mpsc::unbounded_channel();
    std::thread::Builder::new()
        .name("celld-core".into())
        .spawn(move || {
            let result = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string())
                .map(|runtime| runtime.block_on(actor.run(rx)));
            let _ = actor_exit_tx.send(result);
        })?;

    // The sampler is a plain ticker: it measures and posts, and decides
    // nothing. Everything downstream of the numbers -- the latch, the target,
    // which cell goes -- is in the core, so a sample sequence replays.
    {
        const LOAD_SAMPLE_PERIOD: std::time::Duration = std::time::Duration::from_secs(1);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(LOAD_SAMPLE_PERIOD);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if sample_tx.send(Message::SampleLoad).is_err() {
                    return;
                }
            }
        });
    }

    if let Some((client, wake)) = wake_scan {
        let authority_wait_seconds = if clean_reload_candidate { 60 } else { 10 };
        let deadline = Instant::now() + std::time::Duration::from_secs(authority_wait_seconds);
        while !app.healthy().await {
            if Instant::now() >= deadline {
                anyhow::bail!("node authority was not established before wake scan");
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        for (cell, due_ms) in celld::wake::due_scan(&client, now_ms() as i64).await {
            wake.adopt(&cell, due_ms);
            let _ = app.tx.send(Message::WakeHint { cell });
        }

        // And again, on a timer, for the rest of the process's life.
        //
        // The boot scan alone only covers alarms that came due while nothing
        // was watching *before this node started*. A node that dies while
        // this one is already running leaves its cells with armed alarms and
        // no owner, and nothing would look at them again until this process
        // restarted -- an alarm silently not firing, which is the one thing a
        // Durable Object is not allowed to do.
        //
        // Every decision about a due entry stays in the core: a hint for a
        // cell this node already serves is ignored, and one for a cell with a
        // live owner elsewhere resolves to that owner rather than stealing
        // it. The scan only reports what the bucket says is due.
        let scan_app = app.clone();
        let waker_node = node.clone();
        let tick_ms = celld::env_vars::positive::<u64>("CELLD_WAKER_TICK_MS")?.unwrap_or(60_000);
        let period = std::time::Duration::from_millis(tick_ms);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await;
            let mut dead_node_gc = celld::dead_node_gc::DeadNodeGc::default();
            loop {
                tick.tick().await;
                dead_node_gc
                    .run_elected_pass(&client, &waker_node, tick_ms)
                    .await;
                for (cell, due_ms) in celld::wake::due_scan(&client, now_ms() as i64).await {
                    wake.adopt(&cell, due_ms);
                    if scan_app.tx.send(Message::WakeHint { cell }).is_err() {
                        return;
                    }
                }
            }
        });
    }

    if let Some(client) = deploy_agent {
        celld::control_plane::start_deploy_agent(client.clone(), Arc::new(AtomicBool::new(true)));
        let presence_app = app.clone();
        celld::control_plane::start_presence_agent(celld::control_plane::PresenceRuntime {
            s3: client,
            replication: explorer_replication,
            node_session_id: node,
            advertise,
            listen,
            credential_version: adapter_credential_version
                .expect("managed adapters have a credential version"),
            snapshot: Arc::new(move || {
                let app = presence_app.clone();
                Box::pin(async move { app.presence().await })
            }),
        });
    }

    println!(
        "celld listening on {} (ownership={ownership_name})",
        listener.local_addr()?
    );
    println!(
        "celld internal listening on {} (advertise={})",
        internal_listener.local_addr()?,
        app.advertise
    );
    let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
    // A SIGTERM (systemd stop, `docker stop`, a Kubernetes pod delete) or a
    // SIGINT begins the same graceful shutdown as `POST /shutdown`, so the
    // orchestrator's ordinary stop drains and hands off instead of killing.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut sigint = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let (connection_drain_tx, connection_drain) = watch::channel(false);
    let drain_ms: u64 = celld::env_vars::positive("CELLD_SHUTDOWN_DRAIN_MS")?.unwrap_or(25_000);
    // A hung connection must not consume the whole preserve budget: the
    // semantic drain and the clean-reload certificate come out of the same
    // deadline.
    let connection_grace =
        CONNECTION_DRAIN_GRACE.min(std::time::Duration::from_millis(drain_ms / 4));
    let mut connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    let mut do_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut gate_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut service_calls: FuturesUnordered<DoCallFuture> = FuturesUnordered::new();
    let mut asset_calls: FuturesUnordered<AssetCallFuture> = FuturesUnordered::new();
    let mut websockets: FuturesUnordered<WebSocketFuture> = FuturesUnordered::new();
    let mut cache_prunes: FuturesUnordered<CachePruneFuture> = FuturesUnordered::new();
    let mut replication_health = tokio::time::interval(std::time::Duration::from_millis(250));
    let mut local_cache_prune = tokio::time::interval_at(
        tokio::time::Instant::now() + LOCAL_CACHE_PRUNE_PERIOD,
        LOCAL_CACHE_PRUNE_PERIOD,
    );
    local_cache_prune.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let shutdown_mode = loop {
        tokio::select! {
            connection = listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Public,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            connection = internal_listener.accept() => {
                let (stream, _) = connection?;
                connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Internal,
                    app.clone(),
                    shutdown_tx.clone(),
                    connection_drain.clone(),
                    connection_grace,
                ));
            }
            Some(()) = connections.next(), if !connections.is_empty() => {}
            call = do_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object call channel closed");
                };
                do_calls.push(Box::pin(dispatch_do_call(app.clone(), call)));
            }
            Some(()) = do_calls.next(), if !do_calls.is_empty() => {}
            req = gate_rx.recv() => {
                let Some(req) = req else {
                    anyhow::bail!("output-gate channel closed");
                };
                gate_calls.push(Box::pin(dispatch_gate(app.clone(), req)));
            }
            Some(()) = gate_calls.next(), if !gate_calls.is_empty() => {}
            call = service_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service call channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_call(app.clone(), call)));
            }
            call = service_rpc_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("service RPC channel closed");
                };
                service_calls.push(Box::pin(dispatch_service_rpc(app.clone(), call)));
            }
            Some(()) = service_calls.next(), if !service_calls.is_empty() => {}
            call = rpc_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("Durable Object RPC channel closed");
                };
                do_calls.push(Box::pin(dispatch_rpc_call(app.clone(), call)));
            }
            call = asset_call_rx.recv() => {
                let Some(call) = call else {
                    anyhow::bail!("asset call channel closed");
                };
                asset_calls.push(Box::pin(dispatch_asset_call(app.clone(), call)));
            }
            Some(()) = asset_calls.next(), if !asset_calls.is_empty() => {}
            socket = websocket_rx.recv() => {
                let Some(socket) = socket else {
                    anyhow::bail!("WebSocket channel closed");
                };
                websockets.push(socket);
            }
            Some(()) = websockets.next(), if !websockets.is_empty() => {}
            _ = local_cache_prune.tick(), if local_cache_replication.is_some()
                && local_cache_max_bytes.is_some() && cache_prunes.is_empty() => {
                let replication = local_cache_replication.clone().unwrap();
                let max_bytes = local_cache_max_bytes.unwrap();
                cache_prunes.push(Box::pin(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        replication.prune_local_cache(max_bytes)
                    }).await;
                    (max_bytes, result)
                }));
            }
            Some((max_bytes, result)) = cache_prunes.next(), if !cache_prunes.is_empty() => {
                match result {
                    Ok((kept, evicted, bytes)) if evicted > 0 => {
                        tracing::info!(
                            event = "local_cache_pruned",
                            kept,
                            evicted,
                            bytes,
                            max_bytes,
                            "pruned least-recently-used eviction snapshots"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(%error, "local cache pruning task failed");
                    }
                }
            }
            outbound = outbound_ws_rx.recv() => {
                let Some(outbound) = outbound else {
                    anyhow::bail!("outbound WebSocket channel closed");
                };
                let app = app.clone();
                websockets.push(Box::pin(async move {
                    if let Err(error) = outbound_websocket_task(app, outbound).await {
                        eprintln!("celld outbound WebSocket failed: {error:#}");
                    }
                }));
            }
            mode = shutdown_rx.recv() => break mode.unwrap_or(ShutdownMode::Handoff),
            _ = sigterm.recv() => break ShutdownMode::Handoff,
            _ = sigint.recv() => break ShutdownMode::Handoff,
            code = fence_rx.recv() => {
                exit_flushed(code.unwrap_or(3));
            }
            actor_exit = actor_exit_rx.recv() => {
                let error = match actor_exit {
                    Some(Err(error)) => error,
                    Some(Ok(())) => "the core actor stopped unexpectedly".to_string(),
                    None => "the core actor thread panicked".to_string(),
                };
                tracing::error!(
                    event = "core_actor_exit",
                    %error,
                    "SELF-FENCE: the core actor exited unexpectedly"
                );
                exit_flushed(3);
            }
            _ = replication_health.tick() => {
                if let Some(runtime) = &app.runtime {
                    match runtime.replication_status() {
                        Ok(None) => {}
                        Ok(Some(status)) => {
                            eprintln!("SELF-FENCE: replication process exited unexpectedly: {status}");
                            exit_flushed(3);
                        }
                        Err(error) => {
                            eprintln!("SELF-FENCE: replication process health check failed: {error}");
                            exit_flushed(3);
                        }
                    }
                }
            }
        }
    };
    // Graceful shutdown. Report unhealthy so a load balancer sheds this node,
    // and refuse new work. Keep accepting bounded health and diagnostic
    // requests while the semantic drain runs. A node removal hands every
    // resident cell to a peer. A planned same-node replacement preserves
    // ownership and its local replica cache, so the replacement does not
    // create a fleet-wide cold handoff or lasting skew.
    app.draining
        .store(true, std::sync::atomic::Ordering::Relaxed);
    let _ = connection_drain_tx.send(true);
    // Receivers cloned from `connection_drain` have already observed an old
    // version, so `changed()` would close each newly accepted connection
    // before it could send a diagnostic response. Give drain-time connections
    // their own signal. They are deliberately absent from `shell_drained`: an
    // incomplete health request must not prevent a clean reload certificate.
    let (drain_connection_tx, drain_connection) = watch::channel(false);
    let mut drain_connections: FuturesUnordered<ConnectionFuture> = FuturesUnordered::new();
    if shutdown_mode == ShutdownMode::Handoff {
        let _ = app.tx.send(Message::ReleaseAll);
    } else {
        let _ = app.tx.send(Message::BeginPreserve);
    }
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(drain_ms);
    let mut handoff = tokio::time::interval(std::time::Duration::from_millis(50));
    let drained = loop {
        let shell_drained = connections.is_empty()
            && do_calls.is_empty()
            && gate_calls.is_empty()
            && service_calls.is_empty()
            && asset_calls.is_empty()
            && websockets.is_empty();
        // The actor can be busy driving an immediate-effect failure loop, so
        // a status request is not itself allowed to bypass the drain deadline.
        let core_drained = if shell_drained {
            tokio::time::timeout(std::time::Duration::from_millis(50), app.drain_status())
                .await
                .is_ok_and(|status| match shutdown_mode {
                    ShutdownMode::Handoff => status.occupied == 0 && status.releasing == 0,
                    ShutdownMode::Preserve => status.activating == 0 && status.evicting == 0,
                })
        } else {
            false
        };
        if shell_drained && core_drained {
            break true;
        }
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break false,
            _ = handoff.tick() => {}
            connection = listener.accept() => {
                let (stream, _) = connection?;
                drain_connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Public,
                    app.clone(),
                    shutdown_tx.clone(),
                    drain_connection.clone(),
                    connection_grace,
                ));
            }
            connection = internal_listener.accept() => {
                let (stream, _) = connection?;
                drain_connections.push(serve_http_connection(
                    stream,
                    HttpSurface::Internal,
                    app.clone(),
                    shutdown_tx.clone(),
                    drain_connection.clone(),
                    connection_grace,
                ));
            }
            Some(_) = drain_connections.next(), if !drain_connections.is_empty() => {}
            Some(_) = connections.next(), if !connections.is_empty() => {}
            Some(_) = do_calls.next(), if !do_calls.is_empty() => {}
            Some(_) = gate_calls.next(), if !gate_calls.is_empty() => {}
            Some(_) = service_calls.next(), if !service_calls.is_empty() => {}
            Some(_) = asset_calls.next(), if !asset_calls.is_empty() => {}
            Some(_) = websockets.next(), if !websockets.is_empty() => {}
        }
    };
    let _ = drain_connection_tx.send(true);
    if !drained && shutdown_mode == ShutdownMode::Handoff {
        match tokio::time::timeout(
            std::time::Duration::from_millis(50),
            app.drain_status(),
        )
        .await
        {
            Ok(status) => eprintln!(
                "celld shutdown drain reached its {drain_ms}ms deadline: occupied={} activating={} evicting={} releasing={}",
                status.occupied, status.activating, status.evicting, status.releasing
            ),
            Err(_) => eprintln!(
                "celld shutdown drain reached its {drain_ms}ms deadline: core status unavailable"
            ),
        }
    } else if !drained {
        eprintln!(
            "celld preserve drain reached its {drain_ms}ms deadline: connections={} do_calls={} gate_calls={} service_calls={} asset_calls={} websockets={}",
            connections.len(),
            do_calls.len(),
            gate_calls.len(),
            service_calls.len(),
            asset_calls.len(),
            websockets.len(),
        );
    }
    if drained && shutdown_mode == ShutdownMode::Preserve {
        let prepared = match (&app.runtime, app.presence().await) {
            (Some(runtime), Some(presence)) => {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                tokio::time::timeout(remaining, runtime.prepare_clean_reload(&presence.cells)).await
            }
            _ => Ok(Err(anyhow::anyhow!(
                "clean reload requires a runtime and a resident snapshot"
            ))),
        };
        match prepared {
            Ok(Ok(pruned)) if app.healthy().await => {
                match celld::runtime::write_clean_reload_marker(
                    &data_dir,
                    &clean_reload_node,
                    &process_generation,
                ) {
                    Ok(()) => tracing::info!(
                        event = "clean_reload_prepared",
                        stale_live_databases_pruned = pruned,
                        "prepared local cells for an exact-generation reload"
                    ),
                    Err(error) => tracing::warn!(
                        event = "clean_reload_abandoned",
                        %error,
                        "could not publish the clean local reload certificate"
                    ),
                }
            }
            Ok(Ok(_)) => tracing::warn!(
                event = "clean_reload_abandoned",
                "node authority was lost while local cells were closing"
            ),
            Ok(Err(error)) => tracing::warn!(
                event = "clean_reload_abandoned",
                %error,
                "local reload preparation failed; replacement will use normal recovery"
            ),
            Err(_) => tracing::warn!(
                event = "clean_reload_abandoned",
                "local reload preparation exceeded the shutdown deadline"
            ),
        }
    }
    // Exit without unwinding. Returning from here drops the tokio runtime
    // and the V8 platform underneath tasks and isolates that are still
    // alive -- on a deadline-cut drain that teardown segfaults (status 139
    // observed fleet-wide, 2026-08-10). Nothing below needs a destructor:
    // every release the drain completed proved durability first, and a
    // cell the deadline cut off keeps its owner record exactly as a kill
    // would have left it.
    exit_flushed(0);
}

#[path = "main/machine.rs"]
mod machine;
use machine::{
    lease_ttl_ms_from_environment, local_cache_max_bytes_from_environment,
    ownership_on_evict_from_environment, pressure_config_from_environment, random_node_session_id,
    random_peer_key, random_process_generation, ProcessLoadSampler,
    DEFAULT_MAX_OUTBOUND_WEBSOCKETS, PEER_CONNECT_TIMEOUT,
};

/// Signals cancellation when the connection handling this request goes away.
struct HangUp(Option<oneshot::Sender<()>>);

impl Drop for HangUp {
    fn drop(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}

/// Abandons a forwarded fetch on the owner when the peer connection carrying
/// it goes away. Disarmed by clearing the id once the fetch has answered.
struct AbortPeerFetchOnHangUp {
    runtime: RuntimeManager,
    scope: String,
    request_id: Option<celld::js::RequestId>,
}

impl Drop for AbortPeerFetchOnHangUp {
    fn drop(&mut self) {
        if let Some(request_id) = self.request_id {
            self.runtime.abort_fetch(&self.scope, request_id);
        }
    }
}

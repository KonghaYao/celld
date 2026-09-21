use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use tokio::sync::Notify;
use wasmtime::component::{Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder, UpdateDeadline};

const DEADLINE_BOUND: Duration = Duration::from_millis(500);
const CANCEL_BOUND: Duration = Duration::from_millis(250);
const EPOCH_TICK: Duration = Duration::from_millis(10);
/// How long the fixture may take to reach its guest call before the run is a
/// failure rather than a hang: instantiation plus the first poll.
const START_BOUND: Duration = Duration::from_secs(5);
/// How long the guest may take to enter the hostcall it is cancelled in.
const HOSTCALL_BOUND: Duration = Duration::from_secs(5);
/// How long a guest-CPU fixture is left running before it is cancelled.
const CANCEL_DELAY: Duration = Duration::from_millis(40);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    DeadlineExceeded,
    ResourceExhausted,
    TrapDiscarded,
    Cancelled,
}

pub struct Observation {
    pub outcome: Outcome,
    pub elapsed: Duration,
    pub hostcall_started: bool,
}

struct HostState {
    limits: StoreLimits,
    hostcall: Option<Arc<Notify>>,
    started: Option<Arc<AtomicBool>>,
}

fn engine() -> Result<Engine> {
    let mut config = Config::new();
    config.async_support(true);
    config.epoch_interruption(true);
    config.wasm_component_model(true);
    // `wasmtime::Error` implements neither `std::error::Error` nor anyhow's
    // `Context` seam (wasmtime 48 keeps the type opaque), so the conversion is
    // explicit.
    Engine::new(&config).map_err(|error| anyhow!("qualification engine: {error}"))
}

fn start_epoch(engine: &Engine) -> Arc<AtomicBool> {
    let stop = Arc::new(AtomicBool::new(false));
    let ticking = stop.clone();
    let engine = engine.clone();
    std::thread::spawn(move || {
        while !ticking.load(Ordering::Relaxed) {
            std::thread::sleep(EPOCH_TICK);
            engine.increment_epoch();
        }
    });
    stop
}

fn limits() -> StoreLimits {
    StoreLimitsBuilder::new()
        .memory_size(65_536)
        .trap_on_grow_failure(true)
        .build()
}

/// One prepared guest: the store and the instance whose imports were resolved
/// by the linker in [`prepare`].
///
/// The instance is kept, and not just the store, because it is what holds the
/// resolved imports. Rebuilding a second linker for the call — as this fixture
/// did — leaves `pending` unregistered, so the guest fails at import
/// resolution, the hostcall never runs, and an observer waiting for it waits
/// forever.
struct Invocation {
    store: Store<HostState>,
    instance: wasmtime::component::Instance,
}

async fn prepare(
    engine: &Engine,
    wat: &str,
    hostcall: Option<Arc<Notify>>,
    started: Option<Arc<AtomicBool>>,
) -> Result<Invocation> {
    let component = Component::new(engine, wat)
        .map_err(|error| anyhow!("compile qualification fixture: {error}"))?;
    // The host state is the linker's context type, and it is what the
    // `func_wrap_async` closure reads through `caller.data()`.
    let mut linker: Linker<HostState> = Linker::new(engine);
    if hostcall.is_some() {
        linker
            .root()
            .func_wrap_async("pending", move |caller, (): ()| {
                Box::new(async move {
                    if let Some(flag) = caller.data().started.as_ref() {
                        flag.store(true, Ordering::SeqCst);
                    }
                    if let Some(notify) = caller.data().hostcall.clone() {
                        // Entry signal, and only the observer ever waits on it:
                        // `notify_one` stores a permit, so an observer that
                        // polls later still wakes. The hostcall must NOT await
                        // this same `Notify` — doing so consumes its own permit
                        // and hands the guest an immediately fulfilled call,
                        // which is precisely the "hostcall never pends" defect
                        // this fixture exists to exclude.
                        notify.notify_one();
                    }
                    // Stay inside the hostcall until the invocation is
                    // cancelled: aborting the caller's future is what ends it,
                    // and nothing else here can complete it.
                    std::future::pending::<()>().await;
                    Ok(())
                })
            })?;
    }
    let mut store = Store::new(
        engine,
        HostState {
            limits: limits(),
            hostcall,
            started,
        },
    );
    store.limiter(|state| &mut state.limits);
    store.epoch_deadline_async_yield_and_update(1);
    store.set_epoch_deadline(1);
    let instance = linker.instantiate_async(&mut store, &component).await?;
    Ok(Invocation { store, instance })
}

impl Invocation {
    /// Call an export on the instance this fixture prepared. Compiling the
    /// component again for the call would need its own linker, and a linker
    /// without `pending` cannot instantiate the hostcall fixture at all.
    async fn call(&mut self, export: &str) -> Result<()> {
        let func = self
            .instance
            .get_typed_func::<(), ()>(&mut self.store, export)?;
        func.call_async(&mut self.store, ()).await?;
        Ok(())
    }
}

/// Why a fixture task ended when the scenario expected it to still be working.
fn task_error(
    joined: std::result::Result<Result<()>, tokio::task::JoinError>,
    message: &str,
) -> anyhow::Error {
    match joined {
        Ok(Ok(())) => anyhow!("{message}: the fixture completed"),
        Ok(Err(error)) => anyhow!("{message}: {error:#}"),
        Err(join) => anyhow!("{message}: {join}"),
    }
}

pub async fn infinite_loop_deadline() -> Result<Observation> {
    let wat = r#"(component
  (core module $m
    (func (export "run")
      (loop $again (br $again))))
  (core instance $i (instantiate $m))
  (func (export "run") (canon lift (core func $i "run"))))"#;
    run_bounded(wat, "run", Outcome::DeadlineExceeded, DEADLINE_BOUND, None).await
}

pub async fn memory_growth() -> Result<Observation> {
    let wat = r#"(component
  (core module $m
    (memory (export "mem") 1)
    (func (export "run")
      (drop (memory.grow (i32.const 1)))))
  (core instance $i (instantiate $m))
  (func (export "run") (canon lift (core func $i "run"))))"#;
    run_bounded(wat, "run", Outcome::ResourceExhausted, DEADLINE_BOUND, None).await
}

pub async fn trap_then_recover(engine: &Engine) -> Result<Observation> {
    let trap_wat = r#"(component
  (core module $m
    (func (export "run") unreachable))
  (core instance $i (instantiate $m))
  (func (export "run") (canon lift (core func $i "run"))))"#;
    let healthy_wat = r#"(component
  (core module $m
    (func (export "echo") (param i32) (result i32) local.get 0))
  (core instance $i (instantiate $m))
  (func (export "echo") (param "value" u32) (result u32)
    (canon lift (core func $i "echo"))))"#;
    let started = Instant::now();
    let stop = start_epoch(engine);
    let mut trapping = prepare(engine, trap_wat, None, None).await?;
    let trapped = trapping.call("run").await;
    drop(trapping);
    anyhow::ensure!(trapped.is_err(), "trap fixture must trap");
    let mut recovered = prepare(engine, healthy_wat, None, None).await?;
    let echo = recovered
        .instance
        .get_typed_func::<(u32,), (u32,)>(&mut recovered.store, "echo")?;
    let (value,) = echo.call_async(&mut recovered.store, (7,)).await?;
    stop.store(true, Ordering::Relaxed);
    anyhow::ensure!(value == 7, "recovered invocation must return 7");
    Ok(Observation {
        outcome: Outcome::TrapDiscarded,
        elapsed: started.elapsed(),
        hostcall_started: false,
    })
}

pub async fn cancel_guest_cpu() -> Result<Observation> {
    let wat = r#"(component
  (core module $m
    (func (export "run")
      (loop $again (br $again))))
  (core instance $i (instantiate $m))
  (func (export "run") (canon lift (core func $i "run"))))"#;
    cancel_while(wat, false).await
}

pub async fn cancel_pending_hostcall() -> Result<Observation> {
    let wat = r#"(component
  (import "pending" (func $pending))
  (core func $lower (canon lower (func $pending)))
  (core module $m
    (import "" "pending" (func $pending))
    (func (export "run") call $pending))
  (core instance $i (instantiate $m (with "" (instance (export "pending" (func $lower))))))
  (func (export "run") (canon lift (core func $i "run"))))"#;
    cancel_while(wat, true).await
}

async fn run_bounded(
    wat: &str,
    export: &str,
    expected: Outcome,
    bound: Duration,
    hostcall: Option<Arc<Notify>>,
) -> Result<Observation> {
    let engine = engine()?;
    let stop = start_epoch(&engine);
    let started = Instant::now();
    let mut invocation = prepare(&engine, wat, hostcall, None).await?;
    let result = tokio::time::timeout(bound, invocation.call(export)).await;
    stop.store(true, Ordering::Relaxed);
    drop(invocation);
    let elapsed = started.elapsed();
    match result {
        Err(_) => Ok(Observation {
            outcome: Outcome::DeadlineExceeded,
            elapsed,
            hostcall_started: false,
        }),
        Ok(Err(error)) if expected == Outcome::ResourceExhausted => Ok(Observation {
            outcome: Outcome::ResourceExhausted,
            elapsed,
            hostcall_started: false,
        }),
        Ok(Err(_)) if expected == Outcome::DeadlineExceeded => Ok(Observation {
            outcome: Outcome::DeadlineExceeded,
            elapsed,
            hostcall_started: false,
        }),
        other => Err(anyhow!("unexpected qualification outcome: {other:?}")),
    }
}

async fn cancel_while(wat: &str, wait_hostcall: bool) -> Result<Observation> {
    let engine = engine()?;
    let stop = start_epoch(&engine);
    let notify = Arc::new(Notify::new());
    let started_flag = Arc::new(AtomicBool::new(false));
    let hostcall = wait_hostcall.then(|| notify.clone());
    // The task reports that it reached the guest call on its own channel. An
    // observer that waited on the hostcall notify instead would hang on a task
    // that failed to start, which is how a dropped import registration used to
    // read as "the guest never reached its hostcall".
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut handle = tokio::spawn({
        let engine = engine.clone();
        let wat = wat.to_string();
        let started_flag = started_flag.clone();
        async move {
            let mut invocation = prepare(&engine, &wat, hostcall, Some(started_flag)).await?;
            let _ = started_tx.send(());
            invocation.call("run").await
        }
    });
    let abort_handle = handle.abort_handle();

    // 1. The fixture must reach its guest call, or say why it could not.
    tokio::select! {
        biased;
        started = started_rx => {
            let _ = started;
        }
        joined = &mut handle => {
            stop.store(true, Ordering::Relaxed);
            return Err(task_error(joined, "the fixture task ended before it called the guest"));
        }
        () = tokio::time::sleep(START_BOUND) => {
            abort_handle.abort();
            stop.store(true, Ordering::Relaxed);
            return Err(anyhow!(
                "the fixture task did not reach its guest call within {START_BOUND:?}"
            ));
        }
    }

    // 2. Reach the point this scenario cancels from, still watching the task.
    if wait_hostcall {
        tokio::select! {
            biased;
            () = notify.notified() => {}
            joined = &mut handle => {
                stop.store(true, Ordering::Relaxed);
                return Err(task_error(joined, "the fixture task ended before its hostcall"));
            }
            () = tokio::time::sleep(HOSTCALL_BOUND) => {
                abort_handle.abort();
                stop.store(true, Ordering::Relaxed);
                return Err(anyhow!(
                    "the fixture never entered its hostcall within {HOSTCALL_BOUND:?}"
                ));
            }
        }
    } else {
        tokio::time::sleep(CANCEL_DELAY).await;
        anyhow::ensure!(
            !handle.is_finished(),
            "guest CPU work finished before cancel"
        );
    }

    let started = Instant::now();
    abort_handle.abort();
    let join = tokio::time::timeout(CANCEL_BOUND, &mut handle).await;
    stop.store(true, Ordering::Relaxed);
    match join {
        // `abort` ends the task with a `JoinError`, and the guest's own error
        // or completion is not a cancellation: only the abort is.
        Ok(Err(join_error)) if join_error.is_cancelled() => Ok(Observation {
            outcome: Outcome::Cancelled,
            elapsed: started.elapsed(),
            hostcall_started: wait_hostcall && started_flag.load(Ordering::SeqCst),
        }),
        Ok(joined) => Err(task_error(
            joined,
            "the fixture task ended on its own instead of being cancelled",
        )),
        Err(_) => {
            // The bound is the check. A task still running after `abort` is
            // not a cancellation, and reporting it as one would hide exactly
            // the failure this scenario exists to catch.
            abort_handle.abort();
            Err(anyhow!(
                "the fixture task was still running {CANCEL_BOUND:?} after abort"
            ))
        }
    }
}

#[allow(dead_code)]
static EPOCH: AtomicU64 = AtomicU64::new(0);
#[allow(dead_code)]
fn unused_table() -> ResourceTable {
    ResourceTable::new()
}
#[allow(dead_code)]
fn unused_deadline() -> UpdateDeadline {
    UpdateDeadline::Yield(1)
}

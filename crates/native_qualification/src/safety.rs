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
    Engine::new(&config).context("qualification engine")
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

async fn instantiate(
    engine: &Engine,
    wat: &str,
    hostcall: Option<Arc<Notify>>,
    started: Option<Arc<AtomicBool>>,
) -> Result<Store<HostState>> {
    let component = Component::new(engine, wat).context("compile qualification fixture")?;
    let mut linker = Linker::new(engine);
    if hostcall.is_some() {
        linker
            .root()
            .func_wrap_async("pending", move |caller, (): ()| {
                Box::new(async move {
                    if let Some(flag) = caller.data().started.as_ref() {
                        flag.store(true, Ordering::SeqCst);
                    }
                    if let Some(notify) = caller.data().hostcall.clone() {
                        notify.notify_waiters();
                        notify.notified().await;
                    }
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
    store.data_mut();
    let _ = instance;
    Ok(store)
}

async fn call_export(store: &mut Store<HostState>, wat: &str, export: &str) -> Result<()> {
    let component = Component::new(store.engine(), wat)?;
    let linker = Linker::new(store.engine());
    let instance = linker.instantiate_async(&mut *store, &component).await?;
    let func = instance.get_typed_func::<(), ()>(&mut *store, export)?;
    func.call_async(&mut *store, ()).await?;
    Ok(())
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
    let mut store = instantiate(engine, trap_wat, None, None).await?;
    let trapped = call_export(&mut store, trap_wat, "run").await;
    drop(store);
    anyhow::ensure!(trapped.is_err(), "trap fixture must trap");
    let mut recovered = instantiate(engine, healthy_wat, None, None).await?;
    let component = Component::new(engine, healthy_wat)?;
    let linker = Linker::<HostState>::new(engine);
    let instance = linker.instantiate_async(&mut recovered, &component).await?;
    let echo = instance.get_typed_func::<(u32,), (u32,)>(&mut recovered, "echo")?;
    let (value,) = echo.call_async(&mut recovered, (7,)).await?;
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
    let mut store = instantiate(&engine, wat, hostcall, None).await?;
    let result = tokio::time::timeout(bound, call_export(&mut store, wat, export)).await;
    stop.store(true, Ordering::Relaxed);
    drop(store);
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
    let handle = tokio::spawn({
        let engine = engine.clone();
        let wat = wat.to_string();
        let started_flag = started_flag.clone();
        async move {
            let mut store = instantiate(&engine, &wat, hostcall, Some(started_flag)).await?;
            call_export(&mut store, &wat, "run").await
        }
    });
    if wait_hostcall {
        notify.notified().await;
    } else {
        tokio::time::sleep(Duration::from_millis(40)).await;
        anyhow::ensure!(!handle.is_finished(), "guest CPU work finished before cancel");
    }
    let started = Instant::now();
    handle.abort();
    let join = tokio::time::timeout(CANCEL_BOUND, handle).await;
    stop.store(true, Ordering::Relaxed);
    match join {
        Ok(Err(_)) | Ok(Ok(Err(_))) | Err(_) => Ok(Observation {
            outcome: Outcome::Cancelled,
            elapsed: started.elapsed(),
            hostcall_started: wait_hostcall && started_flag.load(Ordering::SeqCst),
        }),
        Ok(Ok(Ok(()))) => Err(anyhow!("cancelled guest completed successfully")),
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

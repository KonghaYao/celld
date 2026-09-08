use native_qualification::safety::{
    cancel_guest_cpu, cancel_pending_hostcall, infinite_loop_deadline, memory_growth,
    trap_then_recover, Outcome,
};
use std::time::Duration;
use wasmtime::{Config, Engine};

fn report(label: &str, observation: &native_qualification::safety::Observation, bound: Duration) {
    println!(
        "Q2_OBSERVATION outcome={:?} elapsed_us={} bound_us={} hostcall_started={}",
        observation.outcome,
        observation.elapsed.as_micros(),
        bound.as_micros(),
        observation.hostcall_started
    );
    assert!(
        observation.elapsed <= bound,
        "{label} exceeded bound {:?}",
        observation.elapsed
    );
}

#[tokio::test]
async fn infinite_loop_is_preempted_by_deadline() {
    let observation = infinite_loop_deadline().await.expect("deadline");
    assert_eq!(observation.outcome, Outcome::DeadlineExceeded);
    report("deadline", &observation, Duration::from_millis(500));
}

#[tokio::test]
async fn memory_growth_is_rejected() {
    let observation = memory_growth().await.expect("memory");
    assert_eq!(observation.outcome, Outcome::ResourceExhausted);
    report("memory", &observation, Duration::from_millis(500));
}

#[tokio::test]
async fn trap_is_discarded_and_engine_recovers() {
    let mut config = Config::new();
    config.async_support(true);
    config.epoch_interruption(true);
    config.wasm_component_model(true);
    let engine = Engine::new(&config).expect("engine");
    let observation = trap_then_recover(&engine).await.expect("trap");
    assert_eq!(observation.outcome, Outcome::TrapDiscarded);
    report("trap", &observation, Duration::from_millis(500));
}

#[tokio::test]
async fn client_cancel_stops_guest_cpu() {
    let observation = cancel_guest_cpu().await.expect("cpu cancel");
    assert_eq!(observation.outcome, Outcome::Cancelled);
    report("cpu cancel", &observation, Duration::from_millis(250));
}

#[tokio::test]
async fn client_cancel_stops_pending_hostcall() {
    let observation = cancel_pending_hostcall().await.expect("hostcall cancel");
    assert_eq!(observation.outcome, Outcome::Cancelled);
    assert!(observation.hostcall_started);
    report("hostcall cancel", &observation, Duration::from_millis(250));
}

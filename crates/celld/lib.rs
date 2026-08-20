// Copyright 2026 Deno Land Inc. Apache-2.0 license.

#![warn(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::disallowed_macros)]

//! Effect adapters for the clean-sheet core.
//!
//! The executable owns one serial actor which is the only caller of
//! `celld_logic::on_event`. Adapter futures never borrow core state; they send
//! versioned completion events back through its mailbox.

#[macro_export]
macro_rules! __celld_domain_select {
    (
        $first_pattern:pat = $first_future:expr => $first_output:expr,
        $second_pattern:pat = $second_future:expr => $second_output:expr $(,)?
    ) => {{
        let mut first = std::pin::pin!($first_future);
        let mut second = std::pin::pin!($second_future);
        let selected = std::future::poll_fn(|context| {
            if let std::task::Poll::Ready(value) =
                std::future::Future::poll(first.as_mut(), context)
            {
                return std::task::Poll::Ready(futures_util::future::Either::Left(value));
            }
            if let std::task::Poll::Ready(value) =
                std::future::Future::poll(second.as_mut(), context)
            {
                return std::task::Poll::Ready(futures_util::future::Either::Right(value));
            }
            std::task::Poll::Pending
        })
        .await;
        match selected {
            futures_util::future::Either::Left($first_pattern) => $first_output,
            futures_util::future::Either::Right($second_pattern) => $second_output,
        }
    }};
}

pub mod actor;
pub mod assets;
#[cfg(not(all(test, celld_internal_tests)))]
pub mod asyncrt;
#[cfg(all(test, celld_internal_tests))]
#[allow(clippy::disallowed_methods, clippy::disallowed_types)]
#[warn(clippy::disallowed_macros)]
pub mod asyncrt {
    include!(env!("CELLD_INTERNAL_ASYNCRT"));
}
pub mod bucket;
pub mod control_plane;
pub mod d1_cli;
pub mod dead_node_gc;
pub mod deploy;
pub mod env_vars;
/// Test-only SQLite VFS fault and persistence instrumentation.
///
/// The external conformance build owns every caller, and the same gate covers
/// each test control. An ordinary build compiles none of this module and
/// offers no control seam.
#[cfg(all(test, celld_internal_tests))]
// The fault harness materializes private crash fixtures outside production.
#[allow(clippy::disallowed_methods)]
mod fault {
    include!(env!("CELLD_INTERNAL_SQLITE_FAULT"));
}
pub mod fleet;
pub mod host_services;
pub mod js;
pub mod ltx_repl;
pub mod machine;
pub mod memory;
pub mod node_log;
mod otlp;
pub mod ownership_store;
pub mod peer_auth;
pub mod peer_probe;
pub mod pool;
pub mod protocol;
pub mod replication;
pub mod runtime;
pub mod startup;
pub mod storage;
pub mod telemetry;
pub mod wake;
pub mod ws_client;

#[cfg(all(test, celld_internal_tests))]
mod conformance_main_tests {
    include!(env!("CELLD_CONFORMANCE_MAIN_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_world_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
pub(crate) mod conformance_sim_store {
    include!(env!("CELLD_CONFORMANCE_SIM_STORE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The private CellHost test inspects its materialized SQLite files.
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_sim_cell_host {
    include!(env!("CELLD_CONFORMANCE_SIM_CELL_HOST_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The private oracle reads and writes independent file-format fixtures.
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_o3_oracle {
    include!(env!("CELLD_CONFORMANCE_O3_ORACLE_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The World test persists explicit replay and SQLite oracle artifacts.
#[allow(clippy::disallowed_methods)]
pub(crate) mod conformance_world_s1_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S1_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The World test copies and inspects deliberate crash images.
#[allow(clippy::disallowed_methods)]
mod conformance_world_s2_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S2_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The World test copies and inspects deliberate fleet-disk images.
#[allow(clippy::disallowed_methods)]
mod conformance_world_s3_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S3_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The World test owns the materialized SQLite persistence oracle.
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5a_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S5A_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
// The World model owns and inspects its materialized filesystem images.
#[allow(clippy::disallowed_methods)]
mod conformance_world_s5b_tests {
    include!(env!("CELLD_CONFORMANCE_WORLD_S5B_TESTS"));
}

#[cfg(all(test, celld_internal_tests))]
mod conformance_ltx_eviction_tests {
    include!(env!("CELLD_CONFORMANCE_LTX_EVICTION_TESTS"));
}

/// Completion token for a resident-isolate reservation made by the decision
/// core. Dropping the queued/running job reports that the cell is idle again;
/// the token contains no selection or lifecycle policy of its own.
pub struct CellActivityGuard {
    finish: Option<Box<dyn FnOnce() + Send>>,
}

impl CellActivityGuard {
    pub fn new(finish: impl FnOnce() + Send + 'static) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }
}

impl Drop for CellActivityGuard {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            finish();
        }
    }
}

pub enum WorkerJob {
    Fetch {
        queued_at: std::time::Instant,
        url: String,
        method: String,
        body: js::RequestBody,
        headers: Vec<(String, String)>,
        request_id: Option<js::RequestId>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<js::HttpResponse>>,
    },
    Rpc {
        entrypoint: String,
        method: String,
        args: Vec<u8>,
        reply: tokio::sync::oneshot::Sender<anyhow::Result<Vec<u8>>>,
    },
}

/// Temporary host seam required by the verbatim JS adapter. The runtime
/// adapter will construct the real shared Worker queue; lifecycle policy does
/// not move into this type.
/// Compatibility switches copied with the JS adapter. These are runtime
/// semantics, not lifecycle decisions.
pub fn worker_compat(metadata: &serde_json::Value) -> js::Compat {
    let flags = metadata
        .get("compatibility_flags")
        .and_then(serde_json::Value::as_array);
    let has_flag = |name: &str| {
        flags.is_some_and(|flags| {
            flags
                .iter()
                .any(|flag| flag.as_str().is_some_and(|flag| flag == name))
        })
    };
    let date = metadata
        .get("compatibility_date")
        .and_then(serde_json::Value::as_str);
    let switch = |enable: &str, disable: &str, since: &str| {
        if has_flag(enable) {
            return true;
        }
        if has_flag(disable) {
            return false;
        }
        date.is_some_and(|date| date >= since)
    };
    js::Compat {
        delete_all_deletes_alarm: switch(
            "delete_all_deletes_alarm",
            "delete_all_preserves_alarm",
            "2026-02-24",
        ),
        js_rpc: has_flag("js_rpc"),
        fetcher_get_put_delete: !switch(
            "fetcher_no_get_put_delete",
            "fetcher_has_get_put_delete",
            "2024-03-26",
        ),
        sqlite_vec: has_flag("sqlite_vec"),
        websocket_standard_binary_type: has_flag("websocket_standard_binary_type"),
    }
}

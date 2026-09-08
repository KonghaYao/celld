//! Qualification-only Wasmtime experiments. Not linked into the celld binary.

pub mod adapter;
pub mod inventory;
pub mod safety;

#[allow(dead_code)]
fn link_smoke_types() {
    let _ = std::any::type_name::<wasmtime::Engine>();
    let _ = std::any::type_name::<wasmtime_wasi::WasiCtx>();
}

#[cfg(test)]
mod unit {
    use super::adapter::{AdapterErrorCode, AdapterRequest, invoke_echo};
    use super::inventory::pinned_versions;

    #[test]
    fn inventory_parses_workspace_lock() {
        let pins = pinned_versions().expect("lockfile pins");
        assert_eq!(pins.wasmtime, "48.0.1");
        assert_eq!(pins.wasmtime_wasi, "48.0.1");
        assert_eq!(pins.wasmtime_wasi_http, "48.0.1");
    }

    #[test]
    fn adapter_rejects_empty_world_without_engine_text() {
        let error = invoke_echo(b"not-a-component", "", AdapterRequest { value: 1 }).unwrap_err();
        assert_eq!(error.code, AdapterErrorCode::AbiUnsupported);
        let message = error.to_string();
        assert!(!message.to_ascii_lowercase().contains("wasmtime"));
        assert!(!message.to_ascii_lowercase().contains("webassembly"));
    }
}

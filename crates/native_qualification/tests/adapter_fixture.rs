use native_qualification::adapter::{
    echo_wat, import_missing_wat, AdapterErrorCode, AdapterRequest, invoke_echo,
};

const WORLD: &str = "cellp:qualification/echo@0.1.0";

#[test]
fn component_echo_round_trips_through_neutral_adapter() {
    let response = invoke_echo(
        echo_wat().as_bytes(),
        WORLD,
        AdapterRequest { value: 42 },
    )
    .expect("echo");
    assert_eq!(response.value, 42);
}

#[test]
fn invalid_world_is_rejected_without_engine_diagnostics() {
    let error = invoke_echo(echo_wat().as_bytes(), "wrong:world@1", AdapterRequest { value: 1 })
        .unwrap_err();
    assert_eq!(error.code, AdapterErrorCode::AbiUnsupported);
    let message = error.to_string().to_ascii_lowercase();
    assert!(!message.contains("wasmtime"));
    assert!(!message.contains("webassembly"));
}

#[test]
fn unavailable_import_is_normalized_without_engine_diagnostics() {
    let error = invoke_echo(
        import_missing_wat().as_bytes(),
        WORLD,
        AdapterRequest { value: 1 },
    )
    .unwrap_err();
    assert_eq!(error.code, AdapterErrorCode::ImportMissing);
    let message = error.to_string().to_ascii_lowercase();
    assert!(!message.contains("wasmtime"));
    assert!(!message.contains("webassembly"));
}

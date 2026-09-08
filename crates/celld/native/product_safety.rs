use super::contract::{NativeError, NativeErrorCode, RequestContext};
use super::engine::NativeHttpEngine;
use crate::protocol::{
    NativeComponentDescriptor, NATIVE_BASELINE_1, NATIVE_CAPABILITY_CONFIG, NATIVE_HTTP_WORLD,
};

#[test]
fn request_context_validation_is_fail_closed_at_invoke_boundary() {
    assert!(RequestContext::new(String::new(), "v".into(), 1).is_err());
    assert!(RequestContext::new("r".into(), String::new(), 1).is_err());
    assert!(RequestContext::new("r".into(), "v".into(), 0).is_err());
}

#[test]
fn invalid_component_redacts_engine_text() {
    let descriptor = NativeComponentDescriptor {
        path: "component.wasm".into(),
        sha256: "b".repeat(64),
        baseline: NATIVE_BASELINE_1.into(),
        world: NATIVE_HTTP_WORLD.into(),
        capabilities: vec![NATIVE_CAPABILITY_CONFIG.into()],
    };
    let outcome = NativeHttpEngine::compile(b"not-a-component", &descriptor);
    let error = match outcome {
        Err(error) => error,
        Ok(_) => panic!("expected compile to fail"),
    };
    assert_eq!(error.code, NativeErrorCode::ArtifactIntegrity);
    let message = error.to_string();
    assert!(!message.to_ascii_lowercase().contains("wasmtime"));
    assert!(!message.to_ascii_lowercase().contains("webassembly"));
}

#[test]
fn per_request_construction_is_recovery_compatible() {
    let first = RequestContext::new("a".into(), "v".into(), 1).unwrap();
    let second = RequestContext::new("b".into(), "v".into(), 1).unwrap();
    assert_ne!(first.request_id, second.request_id);
}

#[test]
fn public_error_bodies_are_stable() {
    for code in [
        NativeErrorCode::GuestTrap,
        NativeErrorCode::DeadlineExceeded,
        NativeErrorCode::ResourceExhausted,
        NativeErrorCode::HostcallFailed,
        NativeErrorCode::Cancelled,
    ] {
        let response = NativeError::new(code).into_http_response();
        let body = String::from_utf8(response.body).unwrap();
        assert!(body.contains(code.as_str()));
        assert!(!body.to_ascii_lowercase().contains("wasmtime"));
    }
}

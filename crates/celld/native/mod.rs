//! Experimental Native Component HTTP (`native-http-v1`) runtime.

pub mod contract;
pub mod engine;
pub mod invocation;
pub mod kv_service;
#[cfg(test)]
mod product_safety;

mod bindings {
    wasmtime::component::bindgen!({
        path: "native/wit",
        world: "native-http-v1",
        imports: { default: async },
        exports: { default: async },
        with: {
            "wasi:http/types": wasmtime_wasi_http::p2::bindings::http::types,
            "cellp:kv/kv.namespace": crate::native::engine::KvNamespaceRecord,
        },
    });
}

use std::sync::Arc;

use crate::js::{self, HttpResponse, RequestBody};
use crate::protocol::{
    NativeComponentDescriptor, NATIVE_CAPABILITY_CONFIG, NATIVE_CAPABILITY_KV,
};

use self::contract::{CapabilitySnapshot, NativeError, NativeLimits, RequestContext};
use self::engine::NativeHttpEngine;
use self::kv_service::KvHostService;

#[derive(Clone)]
pub struct NativeExecution {
    engine: Arc<NativeHttpEngine>,
    capabilities: CapabilitySnapshot,
    limits: NativeLimits,
    kv: KvHostService,
    version_id: String,
    generation: u64,
}

impl NativeExecution {
    pub fn build(
        component: &[u8],
        descriptor: &NativeComponentDescriptor,
        vars: Vec<(String, String)>,
        kv_bindings: Vec<(String, String)>,
        version_id: String,
        generation: u64,
    ) -> Result<Self, NativeError> {
        validate_runtime_capabilities(descriptor, &kv_bindings)?;
        let kv_admitted = descriptor
            .capabilities
            .iter()
            .any(|capability| capability == NATIVE_CAPABILITY_KV);
        let capabilities = CapabilitySnapshot::from_runtime(vars, kv_bindings, kv_admitted)?;
        let limits = NativeLimits::from_vars(capabilities.vars())?;
        let engine = NativeHttpEngine::compile(component, descriptor)?;
        Ok(Self {
            engine: Arc::new(engine),
            capabilities,
            limits,
            kv: KvHostService::new(),
            version_id,
            generation,
        })
    }

    pub fn with_generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    fn request_context(&self, request_id: js::RequestId) -> Result<RequestContext, NativeError> {
        RequestContext::new(
            js::request_id_string(request_id),
            self.version_id.clone(),
            self.generation,
        )
    }

    pub async fn fetch(
        &self,
        url: String,
        method: String,
        body: RequestBody,
        headers: Vec<(String, String)>,
        request_id: js::RequestId,
    ) -> anyhow::Result<HttpResponse> {
        let context = self.request_context(request_id)?;
        match self
            .engine
            .invoke(
                &self.capabilities,
                &self.limits,
                &self.kv,
                context,
                url,
                method,
                body,
                headers,
                request_id,
            )
            .await
        {
            Ok(response) => Ok(response),
            Err(error) => Ok(error.into_http_response()),
        }
    }
}

fn validate_runtime_capabilities(
    descriptor: &NativeComponentDescriptor,
    kv_bindings: &[(String, String)],
) -> Result<(), NativeError> {
    descriptor
        .validate()
        .map_err(|error| NativeError::invalid_contract(error.to_string()))?;
    let mut seen = std::collections::BTreeSet::new();
    let mut has_config = false;
    let mut has_kv = false;
    for capability in &descriptor.capabilities {
        if !seen.insert(capability.as_str()) {
            return Err(NativeError::invalid_contract(format!(
                "repeated capability {capability}"
            )));
        }
        match capability.as_str() {
            NATIVE_CAPABILITY_CONFIG => has_config = true,
            NATIVE_CAPABILITY_KV => has_kv = true,
            other => {
                return Err(NativeError::invalid_contract(format!(
                    "unknown capability {other}"
                )))
            }
        }
    }
    if !has_config {
        return Err(NativeError::invalid_contract(
            "missing cellp:config@0.1 capability",
        ));
    }
    if has_kv != !kv_bindings.is_empty() {
        return Err(NativeError::invalid_contract(
            "KV capability does not match runtime KV authority",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{NATIVE_BASELINE_1, NATIVE_HTTP_WORLD};

    fn descriptor(capabilities: &[&str]) -> NativeComponentDescriptor {
        NativeComponentDescriptor {
            path: "component.wasm".into(),
            sha256: "a".repeat(64),
            baseline: NATIVE_BASELINE_1.into(),
            world: NATIVE_HTTP_WORLD.into(),
            capabilities: capabilities.iter().map(|value| (*value).into()).collect(),
        }
    }

    #[test]
    fn runtime_rejects_unknown_and_repeated_capabilities() {
        let unknown = descriptor(&[NATIVE_CAPABILITY_CONFIG, "cellp:mystery@0.1"]);
        assert!(validate_runtime_capabilities(&unknown, &[]).is_err());
        let repeated = descriptor(&[NATIVE_CAPABILITY_CONFIG, NATIVE_CAPABILITY_CONFIG]);
        assert!(validate_runtime_capabilities(&repeated, &[]).is_err());
    }

    #[test]
    fn runtime_rejects_kv_authority_mismatch() {
        let desc = descriptor(&[NATIVE_CAPABILITY_CONFIG]);
        assert!(validate_runtime_capabilities(&desc, &[("VALUES".into(), "ns".into())]).is_err());
        let with_kv = descriptor(&[NATIVE_CAPABILITY_CONFIG, NATIVE_CAPABILITY_KV]);
        assert!(validate_runtime_capabilities(&with_kv, &[]).is_err());
        assert!(validate_runtime_capabilities(
            &with_kv,
            &[("VALUES".into(), "example-values".into())]
        )
        .is_ok());
    }
}

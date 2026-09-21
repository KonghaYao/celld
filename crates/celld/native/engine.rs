use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::BodyExt;
use http_body_util::Full;
use sha2::{Digest, Sha256};
use wasmtime::component::{Component, HasSelf, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store, StoreLimits, StoreLimitsBuilder};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};
use wasmtime_wasi_http::p2::bindings::http::types::Scheme;
use wasmtime_wasi_http::p2::body::HyperOutgoingBody;
use wasmtime_wasi_http::{WasiHttpCtx, WasiHttpCtxView, WasiHttpHooks, WasiHttpView};

use crate::js::{self, HttpResponse, RequestBody};
use crate::protocol::{NativeComponentDescriptor, NATIVE_HTTP_WORLD};

use super::bindings::cellp::config::config::Host as ConfigHost;
use super::bindings::cellp::kv::kv::{
    AccessError, Host as KvHost, HostNamespace, KvError, Namespace,
};
use super::contract::{
    CapabilitySnapshot, NativeError, NativeErrorCode, NativeLimits, RequestContext,
};
use super::invocation::{run_with_client_cancellation, InvocationCancel};
use super::kv_service::{KvHostService, KvOp};

/// Host state for the `cellp:kv/kv.namespace` resource (see `mod.rs` bindgen `with`).
#[derive(Clone)]
pub struct KvNamespaceRecord {
    pub id: String,
}

#[derive(Default)]
struct NativeHttpHooks;

impl WasiHttpHooks for NativeHttpHooks {}

pub struct NativeHttpEngine {
    engine: Engine,
    component: Component,
}

pub struct StoreState {
    table: ResourceTable,
    wasi: WasiCtx,
    http: WasiHttpCtx,
    http_hooks: NativeHttpHooks,
    limits: StoreLimits,
    capabilities: CapabilitySnapshot,
    kv: KvHostService,
    cancel: InvocationCancel,
    hostcalls: u32,
    max_hostcalls: u32,
    exhausted: bool,
    force_hostcall_fail: bool,
    hostcall_failed: bool,
    capability_denied: bool,
    request: RequestContext,
}

impl WasiView for StoreState {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.wasi,
            table: &mut self.table,
        }
    }
}

impl WasiHttpView for StoreState {
    fn http(&mut self) -> WasiHttpCtxView<'_> {
        WasiHttpCtxView {
            ctx: &mut self.http,
            table: &mut self.table,
            hooks: &mut self.http_hooks,
        }
    }
}

impl NativeHttpEngine {
    pub fn compile(
        bytes: &[u8],
        descriptor: &NativeComponentDescriptor,
    ) -> Result<Self, NativeError> {
        let digest = format!("{:x}", Sha256::digest(bytes));
        if digest != descriptor.sha256 {
            return Err(NativeError::artifact_integrity());
        }
        if descriptor.world != NATIVE_HTTP_WORLD {
            return Err(NativeError::invalid_contract("world"));
        }
        let mut config = Config::new();
        config.async_support(true);
        config.epoch_interruption(true);
        config.wasm_component_model(true);
        let engine = Engine::new(&config).map_err(|_| NativeError::instantiate_failed())?;
        let component =
            Component::new(&engine, bytes).map_err(|_| NativeError::instantiate_failed())?;
        Ok(Self { engine, component })
    }

    pub async fn invoke(
        &self,
        capabilities: &CapabilitySnapshot,
        limits: &NativeLimits,
        kv: &KvHostService,
        context: RequestContext,
        url: String,
        method: String,
        body: RequestBody,
        headers: Vec<(String, String)>,
        request_id: js::RequestId,
    ) -> Result<HttpResponse, NativeError> {
        validate_request_context(&context)?;
        let bytes = match body {
            RequestBody::Bytes(bytes) => bytes,
            RequestBody::Stream(_) => return Err(NativeError::invalid_request()),
        };
        if bytes.len() as u64 > limits.max_request_body_bytes {
            return Err(NativeError::resource_exhausted());
        }
        let cancel = InvocationCancel::new();
        let cancel_for_run = cancel.clone();
        let engine = self.engine.clone();
        let component = self.component.clone();
        let capabilities = capabilities.clone();
        let kv = kv.clone();
        let limits = *limits;
        let force_hostcall_fail = capabilities
            .var("CELLP_NATIVE_E2E_HOSTCALL_FAIL")
            .is_some_and(|value| value == "1");
        let work = async move {
            invoke_once(
                engine,
                component,
                capabilities,
                kv,
                limits,
                context,
                cancel,
                url,
                method,
                bytes.to_vec(),
                headers,
                force_hostcall_fail,
            )
            .await
        };
        run_with_client_cancellation(request_id, limits.cancellation_grace, cancel_for_run, work)
            .await
    }
}

fn validate_request_context(context: &RequestContext) -> Result<(), NativeError> {
    if context.request_id.is_empty() || context.version_id.is_empty() || context.generation == 0 {
        return Err(NativeError::invalid_request());
    }
    Ok(())
}

async fn invoke_once(
    engine: Engine,
    component: Component,
    capabilities: CapabilitySnapshot,
    kv: KvHostService,
    limits: NativeLimits,
    context: RequestContext,
    cancel: InvocationCancel,
    url: String,
    method: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    force_hostcall_fail: bool,
) -> Result<HttpResponse, NativeError> {
    let stop_epoch = start_epoch(&engine);
    let result = tokio::time::timeout(limits.request_deadline, async {
        let store_limits = StoreLimitsBuilder::new()
            .memory_size(limits.max_memory_bytes as usize)
            .trap_on_grow_failure(true)
            .build();
        let mut store = Store::new(
            &engine,
            StoreState {
                table: ResourceTable::new(),
                wasi: WasiCtx::builder().build(),
                http: WasiHttpCtx::new(),
                http_hooks: NativeHttpHooks::default(),
                limits: store_limits,
                capabilities,
                kv,
                cancel,
                hostcalls: 0,
                max_hostcalls: limits.max_hostcalls,
                exhausted: false,
                force_hostcall_fail,
                hostcall_failed: false,
                capability_denied: false,
                request: context,
            },
        );
        store.limiter(|state| &mut state.limits);
        store.set_epoch_deadline(1);
        store.epoch_deadline_async_yield_and_update(1);

        let mut linker = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)
            .map_err(|_| NativeError::instantiate_failed())?;
        wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)
            .map_err(|_| NativeError::instantiate_failed())?;
        super::bindings::cellp::config::config::add_to_linker::<StoreState, HasSelf<StoreState>>(
            &mut linker,
            |state: &mut StoreState| state,
        )
        .map_err(|_| NativeError::instantiate_failed())?;
        super::bindings::cellp::kv::kv::add_to_linker::<StoreState, HasSelf<StoreState>>(
            &mut linker,
            |state: &mut StoreState| state,
        )
        .map_err(|_| NativeError::instantiate_failed())?;

        let instance =
            super::bindings::NativeHttpV1::instantiate_async(&mut store, &component, &linker)
                .await
                .map_err(|_| NativeError::instantiate_failed())?;

        let mut builder = hyper::Request::builder().method(method.as_str()).uri(&url);
        for (name, value) in headers {
            builder = builder.header(name, value);
        }
        let request = builder
            .body(Full::new(Bytes::from(body)))
            .map_err(|_| NativeError::invalid_request())?;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        {
            let req = store
                .data_mut()
                .http()
                .new_incoming_request(Scheme::Http, request)
                .map_err(|_| NativeError::invalid_request())?;
            let out = store
                .data_mut()
                .http()
                .new_response_outparam(sender)
                .map_err(|_| NativeError::instantiate_failed())?;
            instance
                .wasi_http_incoming_handler()
                .call_handle(&mut store, req, out)
                .await
                .map_err(|error| {
                    if store.data().capability_denied {
                        NativeError::capability_denied()
                    } else if store.data().exhausted {
                        NativeError::resource_exhausted()
                    } else if store.data().hostcall_failed {
                        NativeError::hostcall_failed()
                    } else {
                        classify_trap_wasmtime(&error)
                    }
                })?;
        }
        let hostcall_failed = store.data().hostcall_failed;
        let exhausted = store.data().exhausted;
        let response = receiver
            .await
            .map_err(|_| response_outcome(exhausted, hostcall_failed))?
            .map_err(|_| response_outcome(exhausted, hostcall_failed))?;
        collect_response(response, limits.max_response_body_bytes).await
    })
    .await;
    stop_epoch.store(true, std::sync::atomic::Ordering::Relaxed);
    match result {
        Ok(Ok(response)) => Ok(response),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(NativeError::deadline_exceeded()),
    }
}

fn start_epoch(engine: &Engine) -> Arc<std::sync::atomic::AtomicBool> {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ticking = stop.clone();
    let engine = engine.clone();
    std::thread::spawn(move || {
        while !ticking.load(std::sync::atomic::Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(10));
            engine.increment_epoch();
        }
    });
    stop
}

fn response_outcome(exhausted: bool, hostcall_failed: bool) -> NativeError {
    if exhausted {
        NativeError::resource_exhausted()
    } else if hostcall_failed {
        NativeError::hostcall_failed()
    } else {
        NativeError::guest_trap()
    }
}

fn classify_trap_wasmtime(error: &wasmtime::Error) -> NativeError {
    classify_trap(&anyhow::anyhow!("{error:#}"))
}

fn classify_trap(error: &anyhow::Error) -> NativeError {
    let text = format!("{error:#}").to_ascii_lowercase();
    if text.contains("deadline") || text.contains("epoch") {
        NativeError::deadline_exceeded()
    } else if text.contains("memory") || text.contains("resource") || text.contains("limit") {
        NativeError::resource_exhausted()
    } else {
        NativeError::guest_trap()
    }
}

async fn collect_response(
    response: hyper::Response<HyperOutgoingBody>,
    max_bytes: u64,
) -> Result<HttpResponse, NativeError> {
    let status = response.status().as_u16();
    let headers = response
        .headers()
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_string(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect();
    let collected = response
        .into_body()
        .collect()
        .await
        .map_err(|_| NativeError::guest_trap())?;
    let body = collected.to_bytes();
    if body.len() as u64 > max_bytes {
        return Err(NativeError::resource_exhausted());
    }
    Ok(HttpResponse {
        status,
        body: body.to_vec(),
        stream: None,
        headers,
        websocket: None,
        write_position: None,
        // A native response is finished when it is returned; nothing downstream
        // observes a write position for it.
        observed_position: None,
    })
}

impl ConfigHost for StoreState {
    async fn get(&mut self, name: String) -> Option<String> {
        self.capabilities.var(&name).map(str::to_string)
    }
}

impl KvHost for StoreState {
    async fn open(
        &mut self,
        binding: String,
    ) -> Result<wasmtime::component::Resource<Namespace>, AccessError> {
        self.charge()?;
        let namespace = match self.capabilities.kv_namespace(&binding) {
            Ok(namespace) => namespace.to_string(),
            Err(_) => {
                self.capability_denied = true;
                return Err(AccessError::Denied);
            }
        };
        self.table
            .push(KvNamespaceRecord { id: namespace })
            .map_err(|_| AccessError::Denied)
    }
}

impl HostNamespace for StoreState {
    async fn get(
        &mut self,
        self_: wasmtime::component::Resource<Namespace>,
        key: String,
    ) -> Result<Option<Vec<u8>>, KvError> {
        self.charge_kv()?;
        let namespace = self
            .table
            .get(&self_)
            .map_err(|_| KvError::Unavailable)?
            .id
            .clone();
        self.kv
            .call(namespace, KvOp::Get { key }, &self.cancel)
            .await
    }

    async fn put(
        &mut self,
        self_: wasmtime::component::Resource<Namespace>,
        key: String,
        value: Vec<u8>,
    ) -> Result<(), KvError> {
        self.charge_kv()?;
        if self.force_hostcall_fail && key == "__e2e_probe_hostcall_fail" {
            self.hostcall_failed = true;
            return Err(KvError::Unavailable);
        }
        let namespace = self
            .table
            .get(&self_)
            .map_err(|_| KvError::Unavailable)?
            .id
            .clone();
        self.kv
            .call(namespace, KvOp::Put { key, value }, &self.cancel)
            .await
            .map(|_| ())
    }

    async fn delete(
        &mut self,
        self_: wasmtime::component::Resource<Namespace>,
        key: String,
    ) -> Result<(), KvError> {
        self.charge_kv()?;
        let namespace = self
            .table
            .get(&self_)
            .map_err(|_| KvError::Unavailable)?
            .id
            .clone();
        self.kv
            .call(namespace, KvOp::Delete { key }, &self.cancel)
            .await
            .map(|_| ())
    }

    async fn drop(
        &mut self,
        self_: wasmtime::component::Resource<Namespace>,
    ) -> wasmtime::Result<()> {
        self.table.delete(self_).map_err(wasmtime::Error::from)?;
        Ok(())
    }
}

impl StoreState {
    fn charge(&mut self) -> Result<(), AccessError> {
        self.hostcalls = self.hostcalls.saturating_add(1);
        if self.hostcalls > self.max_hostcalls {
            self.exhausted = true;
            return Err(AccessError::Denied);
        }
        Ok(())
    }

    fn charge_kv(&mut self) -> Result<(), KvError> {
        self.hostcalls = self.hostcalls.saturating_add(1);
        if self.hostcalls > self.max_hostcalls {
            self.exhausted = true;
            return Err(KvError::ResourceExhausted);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_context_validation_is_fail_closed_at_invoke_boundary() {
        assert!(validate_request_context(&RequestContext {
            request_id: String::new(),
            version_id: "v".into(),
            generation: 1,
        })
        .is_err());
        assert!(validate_request_context(&RequestContext {
            request_id: "r".into(),
            version_id: String::new(),
            generation: 1,
        })
        .is_err());
        assert!(validate_request_context(&RequestContext {
            request_id: "r".into(),
            version_id: "v".into(),
            generation: 0,
        })
        .is_err());
    }

    #[test]
    fn trap_families_map_to_stable_codes() {
        assert_eq!(
            classify_trap(&anyhow::anyhow!("wasm trap: unreachable")).code,
            NativeErrorCode::GuestTrap
        );
        assert_eq!(
            classify_trap(&anyhow::anyhow!("epoch deadline exceeded")).code,
            NativeErrorCode::DeadlineExceeded
        );
        assert_eq!(
            classify_trap(&anyhow::anyhow!("memory maximum exceeded")).code,
            NativeErrorCode::ResourceExhausted
        );
    }
}

// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// The binary is now connection, startup, shutdown, and V8 shell code. The
// World boundary lives in the library Actor and its domain-routed adapters.
#![allow(clippy::disallowed_macros, clippy::disallowed_methods)]

//! Runnable celld vertical slice.
//!
//! One actor serializes every event through `celld-logic`; the actor polls its
//! mailbox, timers, and in-flight effect futures together. This is the
//! execution shape required for monotonic lease ticks to fence the node even
//! when a storage operation remains hung, without spawning a task per effect.

use anyhow::Context as _;
use base64::Engine as _;
use celld::actor::*;
use celld::fleet;
use celld::js::{
    ArmGate, AssetCallReq, Compat, DoCallReq, HttpResponse, RpcCallReq, SvcCallReq, SvcRpcReq,
    WorkerConfigOptions,
};
use celld::ownership_store::{now_ms, BucketOwnership};
use celld::peer_auth::{self, PeerAuth};
use celld::runtime::{CohostedWorker, Replication, RuntimeFetch, RuntimeManager, RuntimeOptions};
use celld_logic::{RequestError, Route, WebSocketKind};
use futures_util::stream::{FuturesUnordered, StreamExt};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Bytes, Frame, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::convert::Infallible;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{mpsc, oneshot, watch};

// glibc's malloc serializes its arenas behind futexes, and under load the
// sixteen worker threads spent up to half a millisecond blocked per
// acquisition. On a 16-core host jemalloc measured 20% more hello-world
// throughput than glibc (mimalloc 11%), and returned the ~7% of the machine
// that arena-lock sleeps reported as idle.
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

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
        app.gate_output(routed.request, req.position).await
    } else {
        // The owning isolate should route the cell locally; if it moved off the
        // node mid-call, fail closed rather than acknowledge an unproven write.
        Err(RequestError::NodeFenced)
    };
    let _ = req.reply.send(result);
}

/// Call the reserved cron cell's arm endpoint, through the ordinary Durable
/// Object routing path so ownership resolution and remote forwarding are the
/// ones every other call gets.
async fn arm_cron_schedule(app: AppHandle, cell: String) -> anyhow::Result<()> {
    let (reply, receive) = tokio::sync::oneshot::channel();
    dispatch_do_call(
        app,
        DoCallReq {
            request_id: None,
            cancel: None,
            deliver_abort_to_handler: false,
            scope: cell,
            name: None,
            url: "https://cron.celld.internal/arm".to_string(),
            method: "POST".to_string(),
            body: Vec::new(),
            headers: Vec::new(),
            reply,
            order: None,
            parent: None,
        },
    )
    .await;
    let response = receive.await.context("cron arm dispatch dropped")??;
    if !(200..300).contains(&response.status) {
        anyhow::bail!("cron arm returned status {}", response.status);
    }
    Ok(())
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
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "cell scope is malformed or exceeds the fleet storage limit"
        );
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
                    Some(cancel) => celld::asyncrt::select! {
                        _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                        routed = route => routed,
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
                        Ok(response) if app.output_gate => {
                            match app.gate_output(request, response.write_position).await {
                                Ok(()) => Ok(response),
                                Err(error) => Err(anyhow::Error::new(RoutedRequestError(error))),
                            }
                        }
                        Ok(response) => Ok(response),
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
                Some(cancel) => celld::asyncrt::select! {
                    _ = cancel => break Err(anyhow::anyhow!("Durable Object call cancelled")),
                    remote = remote_call => remote,
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
        anyhow::ensure!(
            celld_logic::cell::valid_cell_scope(&scope),
            "cell scope is malformed or exceeds the fleet storage limit"
        );
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
                    if app.output_gate {
                        app.gate_output(request, outcome.write_position)
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

/// A body at or above this many bytes reaches the Worker as a stream.
///
/// A small body crosses as bytes. The host makes one copy, and the handler
/// reads that copy with no asynchronous operation. A large body crosses as
/// a stream, because one copy of a large body costs more than the
/// operations that read it in parts. The host also streams a body that
/// declares no length, because the host cannot know the size of that body.
const INGRESS_STREAM_THRESHOLD: u64 = 1 << 20;

/// Divide an ingress request into its metadata and a body that the Worker
/// can read. The function streams the body if one copy of the body costs
/// more than the operations that read it in parts.
fn ingress_payload(
    request: Request<Incoming>,
    trust_forwarded_headers: bool,
) -> (
    String,
    String,
    celld::js::RequestBody,
    Vec<(String, String)>,
    Option<Incoming>,
) {
    let (parts, body) = request.into_parts();
    let headers: Vec<(String, String)> = parts
        .headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.to_string(), value.to_string()))
        })
        .collect();
    let url = request_url(&parts, trust_forwarded_headers);
    let method = parts.method.to_string();
    let declared = parts
        .headers
        .get(hyper::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok());
    // A method that carries no body does not stream. There is nothing to
    // pull, and a stream costs the handler one operation to learn this.
    let bodyless =
        matches!(parts.method, hyper::Method::GET | hyper::Method::HEAD) || declared == Some(0);
    if bodyless || declared.is_some_and(|length| length < INGRESS_STREAM_THRESHOLD) {
        return (
            url,
            method,
            celld::js::RequestBody::Bytes(Vec::new()),
            headers,
            Some(body),
        );
    }
    let chunks = body.into_data_stream().map(|chunk| {
        chunk
            .map(|bytes| bytes.to_vec())
            .map_err(|error| error.to_string())
    });
    let stream_id = celld::js::register_body_stream(Box::pin(chunks));
    (
        url,
        method,
        celld::js::RequestBody::Stream(stream_id),
        headers,
        None,
    )
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

/// Send a fetch to a cell through the dispatcher a Durable Object call from
/// inside a Worker goes through.
///
/// Public ingress used to resolve the route itself and, on finding another
/// owner, answer with a 307 and a JSON description of where the cell lived --
/// with no Location header, so nothing could follow it. A fleet behind a load
/// balancer serves a cell only from the node that happens to own it, which is
/// to say it does not serve a fleet at all.
///
/// Going through `dispatch_do_call` also inherits the redispatch policy and
/// the cancellation channel, so a client that hangs up reaches the owner
/// rather than only the node it connected to. `/do/` and `/__d1/` share this:
/// they differ in what they authenticate, not in how they reach a cell.
async fn dispatch_cell_fetch(
    cell: String,
    url: String,
    method: String,
    body: Vec<u8>,
    headers: Vec<(String, String)>,
) -> HttpReply {
    let (reply, receive) = oneshot::channel();
    let (cancel_tx, cancel) = oneshot::channel();
    let accepted = celld::js::submit_do_call(celld::js::DoCallReq {
        // Named, and named here: the abort fires only for a call that carries
        // both an id and a cancel signal, so leaving this None silently costs
        // the cancellation rather than failing.
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
        // An ingress call has no caller in this process to be ordered against.
        order: None,
        // A direct-DO ingress caller's traceparent joins with the cross-node
        // propagation work (otel.md phase 2), which is where remote parents of
        // cell spans get their sampling decision.
        parent: None,
    });
    if !accepted {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "{\"error\":\"dispatcher unavailable\"}",
        );
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
                    if app.output_gate {
                        if let Err(error) = app
                            .gate_output(request, worker_response.write_position)
                            .await
                        {
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
                    if app.output_gate {
                        if let Err(error) = app.gate_output(request, outcome.write_position).await {
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

    let (url, method, body, headers, held) = ingress_payload(request, app.trust_forwarded_headers);
    // The host collects a small body here. A read failure at this point
    // can still return status 400. A streamed body has no such point,
    // because that body fails while the handler reads it. The failure
    // surfaces there instead.
    let body = match held {
        None => body,
        Some(held) => match held.collect().await {
            Ok(collected) => celld::js::RequestBody::Bytes(collected.to_bytes().to_vec()),
            Err(error) => {
                return response(
                    StatusCode::BAD_REQUEST,
                    format!("request body failed: {error}"),
                );
            }
        },
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

/// The log tier's follower endpoints (crate::node_log): append fsyncs
/// entries and answers the ack-all vote, seal persists the fence mark
/// before replying, tail hands recovery the held fragment. Fleet-HMAC
/// verified like every internal peer surface.
async fn internal_log(request: Request<Incoming>, app: AppHandle, path: String) -> HttpReply {
    let Some(follower) = app.follower.clone() else {
        return peer_response(response(StatusCode::NOT_FOUND, "no follower store"));
    };
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("invalid log body: {error}"),
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
    if parts.method != hyper::Method::POST {
        return peer_response(response(
            StatusCode::METHOD_NOT_ALLOWED,
            "method not allowed",
        ));
    }
    let result: anyhow::Result<Vec<u8>> = match path.as_str() {
        // The append body and the tail response are binary — the entries
        // dominate both — while every control message stays JSON.
        "/__log/append" => match celld::node_log::decode_append(&body) {
            Ok(req) => serde_json::to_vec(&follower.append(req).await).map_err(Into::into),
            Err(error) => Err(error),
        },
        "/__log/seal" => match serde_json::from_slice::<celld::node_log::SealReq>(&body) {
            Ok(req) => match follower.seal(&req).await {
                Ok(resp) => serde_json::to_vec(&resp).map_err(Into::into),
                Err(error) => Err(error),
            },
            Err(error) => Err(error.into()),
        },
        "/__log/tail" => serde_json::from_slice::<celld::node_log::TailReq>(&body)
            .map_err(anyhow::Error::from)
            .map(|req| celld::node_log::encode_tail_resp(&follower.tail(&req))),
        _ => Err(anyhow::anyhow!("unknown log endpoint")),
    };
    match result {
        Ok(body) => peer_response(response(StatusCode::OK, body)),
        Err(error) => peer_response(response(StatusCode::BAD_REQUEST, format!("{error:#}"))),
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
        _ if path.starts_with("/__log/") => internal_log(request, app, path.clone()).await,
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
        // `celld d1`'s way in: the same forwarding dispatch `/do/` uses, with
        // the peer authentication `/do/` does not have. A D1 database holds
        // application data and answers arbitrary SQL, so it must not be
        // reachable from an unauthenticated operator route; `/do/` refuses a
        // D1 scope below and sends the caller here.
        _ if path.starts_with("/__d1/") && app.runtime.is_some() => {
            let scope = path[6..].to_string();
            if !celld_logic::cell::valid_cell_scope(&scope) {
                return Ok(peer_response(malformed_scope()));
            }
            // The mirror of `/do/`'s refusal: that route serves everything
            // but D1, this one serves nothing but D1. Without the check, a
            // signed request could drive an ordinary Durable Object through
            // a route whose only documented contract is SQL to a database.
            if !celld::deploy::is_d1_scope(&scope) {
                return Ok(peer_response(response(
                    StatusCode::FORBIDDEN,
                    "{\"error\":\"only a D1 database is served on /__d1/; use /do/ for a Durable Object\"}",
                )));
            }
            let method = request.method().clone();
            let path_and_query = request
                .uri()
                .path_and_query()
                .map_or_else(|| request.uri().path().to_string(), ToString::to_string);
            let request_headers = request.headers().clone();
            let body = match request.into_body().collect().await {
                Ok(body) => body.to_bytes(),
                Err(error) => {
                    return Ok(peer_response(response(
                        StatusCode::BAD_REQUEST,
                        format!("invalid body: {error}"),
                    )));
                }
            };
            if let Err(error) = app.peer_auth.verify(
                &method,
                &path_and_query,
                &request_headers,
                &body,
                app.peer_auth.source(),
            ) {
                return Ok(peer_response(response(error.status(), error.message())));
            }
            dispatch_cell_fetch(
                scope,
                "http://cell/".to_string(),
                "POST".to_string(),
                body.to_vec(),
                vec![("content-type".to_string(), "application/json".to_string())],
            )
            .await
        }
        _ if path.starts_with("/do/") && app.runtime.is_some() => {
            let runtime = app.runtime.as_ref().expect("checked runtime");
            let cell = match runtime.cell_scope(&path[4..]) {
                Ok(cell) => cell,
                Err(error) => {
                    return Ok(response(StatusCode::BAD_REQUEST, format!("{error:#}")));
                }
            };
            // This route has no authentication, so it must not reach a D1
            // database: the cell answers arbitrary SQL, and its scope is an
            // HMAC over the script and database names, both of which sit in
            // the project's config. `/__d1/` serves D1 and is authenticated.
            if celld::deploy::is_d1_scope(&cell) {
                return Ok(response(
                    StatusCode::FORBIDDEN,
                    "{\"error\":\"a D1 database is not reachable over /do/; use `celld d1`\"}",
                ));
            }
            let (url, method, body, headers) =
                match request_payload(request, app.trust_forwarded_headers).await {
                    Ok(payload) => payload,
                    Err(response) => return Ok(response),
                };
            dispatch_cell_fetch(cell, url, method, body, headers).await
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
    // Nagle + delayed-ACK stalls small request/response exchanges by tens
    // of milliseconds; the log tier's append acks measured it directly (a
    // ~70 ms floor on a sub-millisecond VPC path). Responses must leave
    // when written, on every surface.
    let _ = stream.set_nodelay(true);
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
        Action::D1(arguments) => return celld::d1_cli::run(arguments).await,
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
        // only. celld's GCS client authenticates with OAuth and its Azure
        // client with an Azure identity or account key, and the control
        // plane's S3-shaped credentials provide neither.
        for scheme in ["gs://", "az://"] {
            if settings
                .bucket
                .as_deref()
                .is_some_and(|bucket| bucket.starts_with(scheme))
            {
                anyhow::bail!(
                    "--control-plane storage is S3-compatible; a {scheme} bucket runs without it"
                );
            }
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
    let (runtime, ownership, peer_key, wake_scan, assets, asset_script) = if let Some(bucket) =
        settings.bucket.clone().filter(|_| settings.load_deployment)
    {
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
            // A node runs the schedule of the deployment it was given and
            // of no other. The reserved class is one key, so a second
            // script's cron cell would resolve to the first script's
            // config and run the wrong `scheduled` handler. Dropping the
            // schedule is the safe half of that trade and this says so out
            // loud, because a trigger that never fires and says nothing is
            // the failure the whole feature is built to avoid. Deploy the
            // script as a node's own deployment to run its crons.
            if !loaded.crons.is_empty() {
                tracing::warn!(
                    script = %target,
                    crons = %loaded.crons.join(", "),
                    "a service binding target declares cron triggers; a node fires only its own deployment's schedule, so these never run here"
                );
            }
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
            crons: deployment.crons,
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
        // `BINDING=database` pairs, the local-script equivalent of
        // `d1_databases` in a deployed project.
        let d1_bindings: Vec<(String, String)> = std::env::var("CELLD_TEST_D1_BINDINGS")
            .unwrap_or_default()
            .split(',')
            .filter_map(|value| value.split_once('='))
            .map(|(name, database)| (name.trim().to_string(), database.trim().to_string()))
            .filter(|(name, database)| !name.is_empty() && !database.is_empty())
            .collect();
        let mut do_classes: Vec<String> = do_classes;
        if !d1_bindings.is_empty() {
            do_classes.push(celld::deploy::D1_CLASS.to_string());
        }
        let crons: Vec<String> = std::env::var("CELLD_TEST_CRONS")
            .unwrap_or_default()
            .split(';')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect();
        let options = WorkerConfigOptions {
            src: source,
            script_name: std::env::var("CELLD_TEST_SCRIPT_NAME")
                .unwrap_or_else(|_| "celld-local".to_string()),
            do_classes,
            bindings,
            r2_bindings: Vec::new(),
            d1_bindings,
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
                crons,
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
        },
        resume_generation,
    )
    .await?;
    let process_generation = actor.lease_spec.generation.clone();
    let ownership_name = actor.ownership.name();
    let explorer_replication = runtime.as_ref().and_then(RuntimeManager::replication);
    let local_cache_replication = explorer_replication.clone();
    let (websocket_tx, mut websocket_rx) = mpsc::unbounded_channel();
    // The log tier's follower store (crate::node_log): fragments other
    // leaders replicate here live under the node's data dir beside the cell
    // databases.
    let follower = match &actor.ownership {
        Ownership::Bucket(bucket_ownership) if settings.bucket.is_some() => {
            Some(Arc::new(celld::node_log::FollowerStore::new(
                &data_dir,
                Some(Arc::new(bucket_ownership.bucket_client())),
                &node,
            )))
        }
        _ => None,
    };
    if let Some(store) = &follower {
        celld::node_log::spawn_fragment_gc(store.clone());
    }
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
        follower,
    };

    // The in-fleet log tier, v0. The takeover interlock is installed in
    // every posture — a bucket-posture node can take over from a
    // fleet-posture one and must find a complete bucket — while shipping
    // requires the explicit fleet posture.
    // Fleet is the DEFAULT (decided 2026-08-14): a single node behaves
    // exactly like sync-to-bucket — no peers means no record, no shipper,
    // and bucket-proven acks — and the moment peers appear the
    // maintenance tick recruits them and fleet replication turns on. One
    // value serves the hobbyist's first node and the fleet it grows into;
    // CELLD_DURABILITY=bucket remains the explicit opt-out.
    let durability = std::env::var("CELLD_DURABILITY").unwrap_or_else(|_| "fleet".into());
    anyhow::ensure!(
        matches!(durability.as_str(), "bucket" | "fleet"),
        "CELLD_DURABILITY must be `bucket` or `fleet`"
    );
    let mut node_log_close: Option<Arc<celld::node_log::NodeLogManager>> = None;
    if let Ownership::Bucket(bucket_ownership) = &actor.ownership {
        if let (Some(replication), Some(spec)) = (
            app.runtime.as_ref().and_then(RuntimeManager::replication),
            settings.bucket.clone(),
        ) {
            let _ = spec;
            let log_bucket = Arc::new(bucket_ownership.bucket_client());
            let ltx = replication.ltx();
            // Bundle the paced tiering: one PUT per node-flush instead of
            // one per cell-transaction — the Class A collapse, measured at
            // 208x against per-transaction PUTs. On by default; 0 is the
            // opt-out.
            let bundle_mode = celld::env_vars::flag("CELLD_LOG_BUNDLE", true)?;
            let nudge_tx = app.tx.clone();
            let own_log = Arc::new(celld::node_log::OwnLog {
                ownership: bucket_ownership.clone(),
                nudge: Box::new(move || {
                    let _ = nudge_tx.send(Message::NudgeNodeLease);
                }),
                write_lock: tokio::sync::Mutex::new(()),
            });
            let manager = Arc::new(celld::node_log::NodeLogManager::new(
                &format!("{node}/{process_generation}"),
                log_bucket,
                own_log,
                ltx.clone(),
                app.peer_auth.clone(),
                bundle_mode,
                celld::node_log::eviction_policy_from_env()?,
            ));
            *actor.node_log.lock().unwrap() = Some(manager.clone());
            node_log_close = Some(manager.clone());
            // Recovery-before-install, EVERY posture, FATAL on failure
            // (cold reviews, B2/B3 and the second pass's finding 2): the
            // predecessor's folded state lives in the lease record this
            // session is about to replace, and the install writes a fresh
            // log — so an unrecovered predecessor must stop the boot, or
            // the install erases the only evidence recovery was needed.
            // The ladder waits out the predecessor's own lease (the S1
            // fence recheck refuses a live lease), retries with backoff,
            // and exits on exhaustion; systemd restarts while peers'
            // follower stores come back. No hostage problem — this node
            // has no lease yet, and peers may race the same
            // CAS-idempotent recovery.
            {
                let ttl_backoff =
                    std::time::Duration::from_millis(actor.lease_spec.ttl_ms.max(1_000));
                let mut recovered = Ok(());
                for attempt in 1..=4u32 {
                    recovered = match tokio::time::timeout(
                        std::time::Duration::from_secs(180),
                        manager.recover_self(),
                    )
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => Err(anyhow::anyhow!("predecessor recovery timed out")),
                    };
                    if recovered.is_ok() || attempt == 4 {
                        break;
                    }
                    if let Err(error) = recovered.as_ref() {
                        eprintln!("celld predecessor recovery attempt {attempt} failed: {error:#}");
                    }
                    // At least one full TTL between attempts, so a
                    // refused-because-live predecessor lease has expired
                    // by the retry.
                    tokio::time::sleep(ttl_backoff.saturating_mul(attempt)).await;
                }
                recovered.map_err(|error| {
                    anyhow::anyhow!(
                        "refusing to install a lease over an unrecovered \
                         predecessor log: {error:#}"
                    )
                })?;
            }
            if durability == "fleet" {
                ltx.set_shipper(manager.clone());
                if bundle_mode {
                    ltx.set_bundle_sink(manager.clone());
                }
                celld::node_log::spawn_maintenance(manager);
            }
        }
    }
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
        celld::asyncrt::spawn(async move {
            let mut tick = celld::asyncrt::interval(LOAD_SAMPLE_PERIOD);
            tick.set_missed_tick_behavior(celld::asyncrt::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if sample_tx.send(Message::SampleLoad).is_err() {
                    return;
                }
            }
        })
        .detach();
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
        celld::asyncrt::spawn(async move {
            let mut tick = celld::asyncrt::interval(period);
            tick.set_missed_tick_behavior(celld::asyncrt::MissedTickBehavior::Delay);
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
        })
        .detach();
    }

    // Arm this deployment's cron schedule. A cron cell has no client to wake
    // it, so somebody has to make the first call; every node makes it, and
    // ownership CAS decides which one keeps the cell while the others route to
    // that owner. No new election, no reserved node -- the same arbiter that
    // makes an alarm fire once per fleet makes a cron trigger fire once too.
    //
    // Nothing re-arms after this. Once the schedule is in the cell's alarm row
    // it is durable, its wake entry is in the bucket, and losing the owner is
    // the ordinary alarm-recovery path: the fleet waker finds the due entry
    // and another node takes the cell over.
    if let Some(cell) = app.runtime.as_ref().and_then(|runtime| runtime.cron_cell()) {
        let arm_app = app.clone();
        tokio::spawn(async move {
            // Routing needs node authority, exactly as the wake scan does.
            let deadline = Instant::now() + std::time::Duration::from_secs(30);
            while !arm_app.healthy().await {
                if Instant::now() >= deadline {
                    tracing::warn!(cell, "cron schedule not armed: no node authority");
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            // A failed arm leaves the schedule silent until the next restart,
            // so retry rather than log once. The backoff is bounded because a
            // fleet that cannot route for a minute has a larger problem.
            for attempt in 0..5 {
                match arm_cron_schedule(arm_app.clone(), cell.clone()).await {
                    Ok(()) => return,
                    Err(error) if attempt == 4 => {
                        tracing::error!(cell, %error, "cron schedule not armed");
                    }
                    Err(error) => {
                        tracing::warn!(cell, %error, attempt, "cron arm failed, retrying");
                        tokio::time::sleep(std::time::Duration::from_millis(500 << attempt.min(5)))
                            .await;
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
                    let result = celld::asyncrt::blocking(move || {
                        replication.prune_local_cache(max_bytes)
                    }).await;
                    (max_bytes, result)
                }));
            }
            Some((max_bytes, result)) = cache_prunes.next(), if !cache_prunes.is_empty() => {
                match result {
                    Ok(Ok((kept, evicted, bytes))) if evicted > 0 => {
                        tracing::info!(
                            event = "local_cache_pruned",
                            kept,
                            evicted,
                            bytes,
                            max_bytes,
                            "pruned least-recently-used eviction snapshots"
                        );
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(%error, "local cache inventory failed");
                    }
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
    // The graceful-shutdown drain point: seal our node-log record so the
    // next incarnation starts with no gather. Guarded inside — it seals
    // only when every shipped frame is bucket-covered.
    if let Some(manager) = &node_log_close {
        manager.close_gracefully().await;
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

use celld::machine::{
    lease_ttl_ms_from_environment, local_cache_max_bytes_from_environment, random_node_session_id,
    random_peer_key, DEFAULT_MAX_OUTBOUND_WEBSOCKETS, PEER_CONNECT_TIMEOUT,
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

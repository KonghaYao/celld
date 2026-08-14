// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! WebSockets on the ingress: accepting them, proxying them to the owner,
//! and the tasks that pump each one.
//!
//! A socket outlives the request that opened it, so each becomes its own
//! task. Three shapes exist — a local socket to a cell on this node, a
//! socket proxied to the owning peer, and a socket the worker opened
//! outbound — and they differ only in what sits on the far end.
use super::*;

async fn dispatch_ws_message(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    data: celld::js::WsIn,
) -> anyhow::Result<()> {
    // The auto-response short circuit: a matched text frame is answered here
    // in the shell and never becomes a `webSocketMessage`. No routing, no
    // activity, no wake — a hibernated cell stays hibernated, which is the
    // feature.
    if let celld::js::WsIn::Text(text) = &data {
        if let Some(response) = celld::js::ws_auto_response(scope, ws_id, text) {
            celld::js::ws_emit_batch(vec![(ws_id, celld::js::WsOut::Text(response))]);
            return Ok(());
        }
    }
    let Routed { request, route } = app
        .request(scope.to_string())
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let activity = app.activity(request, scope.to_string());
    let dispatch = app
        .runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_message(scope.to_string(), ws_id, data)
        .await?;
    // The gate captured the handler's outbound frames. With the gate armed, hand
    // them to the cell's barrier queue; else flush them as the handler produced
    // them. Either way the frames only reach a socket from here.
    if !app.output_gate {
        celld::js::ws_emit_batch(dispatch.frames);
    } else if !dispatch.frames.is_empty() || dispatch.write_position.is_some() {
        app.ws_output(
            request,
            scope.to_string(),
            dispatch.frames,
            dispatch.write_position,
        )
        .await;
    }
    drop(activity);
    Ok(())
}

async fn dispatch_ws_closed(
    app: &AppHandle,
    scope: &str,
    ws_id: u64,
    code: u16,
    reason: String,
    was_clean: bool,
) -> anyhow::Result<()> {
    let Routed { request, route } = app
        .request(scope.to_string())
        .await
        .map_err(|error| anyhow::anyhow!("route WebSocket close {scope}: {error:?}"))?;
    anyhow::ensure!(route == Route::Local, "WebSocket owner moved off node");
    let _activity = app.activity(request, scope.to_string());
    app.runtime
        .as_ref()
        .context("no cell runtime")?
        .ws_closed(scope.to_string(), ws_id, code, reason, was_clean)
        .await
}

async fn finish_websocket(
    app: &AppHandle,
    target: &celld::js::WsTarget,
    code: u16,
    reason: String,
    was_clean: bool,
) {
    let _ = dispatch_ws_closed(app, &target.scope, target.id, code, reason, was_clean).await;
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

enum OutboundWebSocketSink {
    Cell { app: Box<AppHandle>, scope: String },
    Isolate(celld::js::WsPullSender),
}

impl OutboundWebSocketSink {
    async fn open(&self, websocket: u64, protocol: String) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let Routed { request, route } = app
                    .request(scope.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("route outbound WebSocket: {error:?}"))?;
                anyhow::ensure!(
                    route == Route::Local,
                    "outbound WebSocket cell moved off node"
                );
                let _activity = app.activity(request, scope.clone());
                app.websocket_opened(scope.clone(), websocket, WebSocketKind::Outbound)
                    .await?;
                let result = app
                    .runtime
                    .as_ref()
                    .context("no cell runtime")?
                    .ws_open(scope.clone(), websocket, protocol)
                    .await;
                if result.is_err() {
                    app.websocket_closed(scope.clone(), websocket);
                }
                result
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Open(protocol))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn message(&self, websocket: u64, data: celld::js::WsIn) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => dispatch_ws_message(app, scope, websocket, data).await,
            Self::Isolate(tx) => tx
                .send(match data {
                    celld::js::WsIn::Text(text) => celld::js::WsPull::Text(text),
                    celld::js::WsIn::Binary(bytes) => celld::js::WsPull::Binary(bytes),
                })
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    async fn closed(
        &self,
        websocket: u64,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> anyhow::Result<()> {
        match self {
            Self::Cell { app, scope } => {
                let result =
                    dispatch_ws_closed(app, scope, websocket, code, reason, was_clean).await;
                app.websocket_closed(scope.clone(), websocket);
                result
            }
            Self::Isolate(tx) => tx
                .send(celld::js::WsPull::Close(code, reason, was_clean))
                .map_err(|_| anyhow::anyhow!("isolate stopped reading WebSocket")),
        }
    }

    fn scope(&self) -> &str {
        match self {
            Self::Cell { scope, .. } => scope,
            Self::Isolate(_) => "",
        }
    }
}

pub(crate) async fn outbound_websocket_task(
    app: AppHandle,
    request: celld::js::OutboundWsReq,
) -> anyhow::Result<()> {
    use hyper::header::{HeaderMap, HeaderName, HeaderValue, SEC_WEBSOCKET_PROTOCOL};

    let celld::js::OutboundWsReq {
        scope,
        id,
        url,
        protocols,
        pull,
        headers,
        want_response,
        reply,
    } = request;
    let sink = match pull {
        Some(pull) => OutboundWebSocketSink::Isolate(pull),
        None => OutboundWebSocketSink::Cell {
            app: Box::new(app),
            scope: scope.clone(),
        },
    };
    let mut handshake = HeaderMap::new();
    for (name, value) in &headers {
        if matches!(
            name.to_ascii_lowercase().as_str(),
            "upgrade"
                | "connection"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "host"
        ) {
            continue;
        }
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) else {
            continue;
        };
        handshake.insert(name, value);
    }
    if !protocols.is_empty() {
        handshake.insert(
            SEC_WEBSOCKET_PROTOCOL,
            HeaderValue::from_str(&protocols.join(", "))
                .context("invalid WebSocket subprotocol")?,
        );
    }
    let timeout = std::time::Duration::from_secs(10);
    let connected = tokio::time::timeout(timeout, celld::ws_client::connect(&url, handshake)).await;
    let connection = match connected {
        Ok(Ok(connection)) => connection,
        Ok(Err(celld::ws_client::Error::Declined(declined))) if want_response => {
            let declined = celld::js::DeclinedUpgrade {
                status: declined.status.as_u16(),
                headers: declined
                    .headers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            value.to_str().unwrap_or_default().to_string(),
                        )
                    })
                    .collect(),
                body: declined.body,
            };
            let _ = reply.send(Ok(celld::js::OutboundWsOpen {
                protocol: None,
                declined: Some(declined),
            }));
            return Ok(());
        }
        Ok(Err(error)) => {
            let _ = reply.send(Err(anyhow::anyhow!("{error}")));
            return Ok(());
        }
        Err(_) => {
            let _ = reply.send(Err(anyhow::anyhow!(
                "outbound WebSocket handshake timed out after {}ms",
                timeout.as_millis()
            )));
            return Ok(());
        }
    };
    let socket = connection.socket;
    let protocol = connection
        .headers
        .get(SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    if protocol
        .as_ref()
        .is_some_and(|selected| !protocols.iter().any(|offered| offered == selected))
    {
        let _ = reply.send(Err(anyhow::anyhow!(
            "server selected an unrequested WebSocket subprotocol"
        )));
        return Ok(());
    }

    let (outbound, mut outputs) = mpsc::unbounded_channel();
    if matches!(sink, OutboundWebSocketSink::Cell { .. }) {
        celld::js::ws_register_outbound(id, sink.scope());
    }
    celld::js::ws_register(id, outbound);
    if let Err(error) = sink
        .open(id, protocol.as_deref().unwrap_or_default().to_string())
        .await
    {
        celld::js::ws_unregister(id);
        let _ = reply.send(Err(error));
        return Ok(());
    }
    if reply
        .send(Ok(celld::js::OutboundWsOpen {
            protocol,
            declined: None,
        }))
        .is_err()
    {
        let _ = sink
            .closed(id, 1006, "opening event was cancelled".into(), false)
            .await;
        celld::js::ws_unregister(id);
        return Ok(());
    }

    // Ping and Close are answered by the reader's auto-pong and auto-close, as
    // the previous client library also did unprompted. The pump writes those
    // replies on the same socket, so nothing about that changes here.
    let (close, _writer) = {
        let sink = &sink;
        pump_cell_socket(socket, &mut outputs, true, move |data| {
            sink.message(id, data)
        })
        .await
    };
    let _ = sink.closed(id, close.0, close.1, close.2).await;
    celld::js::ws_unregister(id);
    Ok(())
}

fn websocket_close_details(payload: &[u8]) -> (u16, String, bool) {
    match payload {
        [] => (1005, String::new(), true),
        [_] => (1002, String::new(), false),
        [first, second, reason @ ..] => match std::str::from_utf8(reason) {
            Ok(reason) => (
                u16::from_be_bytes([*first, *second]),
                reason.to_string(),
                true,
            ),
            Err(_) => (1007, String::new(), false),
        },
    }
}

/// The write half of a cell socket, shared by the reader and the writer.
///
/// The reader needs it because auto-pong and auto-close hand their reply back to
/// the caller once a socket is split. The writer needs it for the cell's own
/// frames, and the close path needs it after both directions stop.
type SocketWriter<S> =
    std::sync::Arc<tokio::sync::Mutex<fastwebsockets::WebSocketWrite<tokio::io::WriteHalf<S>>>>;

/// Close details: the code, the reason, and whether the close was clean.
type CloseState = (u16, String, bool);

async fn write_ws_out<S>(ws: &SocketWriter<S>, out: celld::js::WsOut) -> bool
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::Frame;
    let keep_open = matches!(out, celld::js::WsOut::Text(_) | celld::js::WsOut::Binary(_));
    let frame = match out {
        celld::js::WsOut::Text(text) => Frame::text(text.into_bytes().into()),
        celld::js::WsOut::Binary(data) => Frame::binary(data.into()),
        celld::js::WsOut::Close(code, reason) => Frame::close(code, reason.as_bytes()),
    };
    ws.lock().await.write_frame(frame).await.is_ok() && keep_open
}

/// Carry frames between a socket and the cell behind it until one side stops.
///
/// The read is never cancelled. `fastwebsockets::read_frame` consumes header
/// bytes from its buffer and then awaits for the payload, holding what it parsed
/// in local variables, so dropping that future keeps the buffer advanced and
/// loses the header. The next read then treats a payload byte as a frame header
/// and the stream never realigns. Earlier versions of both callers read the
/// socket in the same `tokio::select!` as the cell's outbound queue, which drops
/// the losing future on every iteration; `tests/p0/runtime/socket_cancel.rs`
/// fails against that shape.
///
/// One async block therefore owns each direction. The halves are split so that
/// the writer can write while the reader reads, and the writer stops on a signal
/// instead of being dropped, because dropping it can leave a partial frame on
/// the socket.
///
/// The caller sets `auto_close` and `auto_pong` on the socket before the pump
/// runs, and the pump keeps that choice: an automatic reply is written on the
/// same socket, exactly as the unsplit collector wrote it.
///
/// `outbound_close_is_clean` reports a close that the cell sends as the close
/// state of the socket. The local path leaves that false, because it answers an
/// unclean end with its own protocol echo after the pump returns.
///
/// The returned writer is still live, so the caller can write the close frames
/// that follow.
async fn pump_cell_socket<S, F, Fut>(
    socket: fastwebsockets::WebSocket<S>,
    outputs: &mut mpsc::UnboundedReceiver<celld::js::WsOut>,
    outbound_close_is_clean: bool,
    mut inbound: F,
) -> (CloseState, SocketWriter<S>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnMut(celld::js::WsIn) -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    use fastwebsockets::{FragmentCollectorRead, Frame as WsFrame, OpCode, Payload};

    let (reader, writer) = socket.split(tokio::io::split);
    let mut reader = FragmentCollectorRead::new(reader);
    let writer: SocketWriter<S> = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    let mut close: CloseState = (1006, String::new(), false);
    let (stop_writer, mut stopped) = tokio::sync::oneshot::channel::<()>();
    {
        let obligated_writer = writer.clone();
        let mut obligated = move |frame: WsFrame<'_>| {
            let writer = obligated_writer.clone();
            // The reply borrows the reader's buffer, and the write outlives the
            // callback, so the payload is copied out. The reader builds a pong
            // or a close echo unmasked, and `write_frame` masks it if the role
            // needs it, exactly as the unsplit collector did.
            let reply = WsFrame::new(
                frame.fin,
                frame.opcode,
                None,
                Payload::Owned(frame.payload.to_vec()),
            );
            async move { writer.lock().await.write_frame(reply).await }
        };

        let read = async {
            loop {
                let Ok(frame) = reader.read_frame(&mut obligated).await else {
                    return None;
                };
                let delivered = match frame.opcode {
                    OpCode::Text => {
                        inbound(celld::js::WsIn::Text(
                            String::from_utf8_lossy(&frame.payload).into_owned(),
                        ))
                        .await
                    }
                    OpCode::Binary => {
                        inbound(celld::js::WsIn::Binary(frame.payload.to_vec())).await
                    }
                    OpCode::Close => return Some(websocket_close_details(&frame.payload)),
                    _ => Ok(()),
                };
                if delivered.is_err() {
                    return None;
                }
            }
        };

        // `recv` and the stop signal are both cancel-safe, and the write runs in
        // the branch body, so no write is ever cancelled either.
        let write = async {
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => return None,
                    output = outputs.recv() => {
                        let output = output?;
                        let closed = match &output {
                            celld::js::WsOut::Close(code, reason) if outbound_close_is_clean => {
                                Some((*code, reason.clone(), true))
                            }
                            _ => None,
                        };
                        if !write_ws_out(&writer, output).await { return closed; }
                    }
                }
            }
        };

        let mut read = std::pin::pin!(read);
        let mut write = std::pin::pin!(write);
        tokio::select! {
            result = &mut read => {
                if let Some(details) = result { close = details; }
                // Stop the writer between frames rather than dropping it, so a
                // frame it started reaches the socket whole.
                let _ = stop_writer.send(());
                let _ = write.await;
            }
            result = &mut write => {
                if let Some(details) = result { close = details; }
                // The read is dropped here. That is safe only because the pump
                // is over and the socket is never read again.
            }
        }
    }
    (close, writer)
}

async fn websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    mut socket: fastwebsockets::WebSocket<S>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    socket.set_auto_close(false);
    let (outbound, mut outputs) = mpsc::unbounded_channel();
    celld::js::ws_register(target.id, outbound);
    // A close the cell sends does not become the close state here: the echo
    // below decides what the peer observes, so the pump must not claim the
    // close was clean.
    let (close, writer) = {
        let app = &app;
        let scope = target.scope.as_str();
        let id = target.id;
        pump_cell_socket(socket, &mut outputs, false, move |data| {
            dispatch_ws_message(app, scope, id, data)
        })
        .await
    };
    if let Err(error) = dispatch_ws_closed(
        &app,
        &target.scope,
        target.id,
        close.0,
        close.1.clone(),
        close.2,
    )
    .await
    {
        tracing::warn!(
            %error,
            scope = %target.scope,
            websocket = target.id,
            "WebSocket close dispatch failed"
        );
    }

    // The close handler is allowed to choose the response code and reason.
    // Its output is queued while dispatch_ws_closed drives V8, so flush it
    // before unregistering the socket or considering a protocol-level echo.
    let mut handler_sent_close = false;
    while let Ok(output) = outputs.try_recv() {
        handler_sent_close |= matches!(output, celld::js::WsOut::Close(_, _));
        if !write_ws_out(&writer, output).await {
            break;
        }
    }
    if let Some(code) =
        celld_logic::schedule::websocket_echo_close(close.0, close.2, handler_sent_close)
    {
        let _ = write_ws_out(&writer, celld::js::WsOut::Close(code, close.1)).await;
    }
    app.websocket_closed(target.scope.clone(), target.id);
    celld::js::ws_unregister(target.id);
}

async fn remote_websocket_task<S>(
    app: AppHandle,
    target: celld::js::WsTarget,
    client: fastwebsockets::WebSocket<S>,
) -> anyhow::Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut node = target
        .peer_node
        .clone()
        .context("remote WebSocket target has no node")?;
    let mut addr = target
        .peer_addr
        .clone()
        .context("remote WebSocket target has no address")?;
    let mut epoch = target
        .peer_epoch
        .context("remote WebSocket target has no owner epoch")?;
    let mut dispatcher = celld_logic::routing::Dispatcher::default();
    let peer = loop {
        let path = format!("/__ws/{}?id={}&epoch={epoch}", target.scope, target.id);
        let signed = app.peer_auth.signed_headers("GET", &path, &[], &node)?;
        match celld::ws_client::connect(&format!("ws://{addr}{path}"), signed).await {
            Ok(peer) => {
                peer_auth::validate_response(&peer.headers)?;
                break peer.socket;
            }
            Err(celld::ws_client::Error::Declined(declined))
                if declined
                    .headers
                    .get(STALE_ROUTE_HEADER)
                    .is_some_and(|value| value == STALE_ROUTE_VALUE) =>
            {
                if !dispatcher.redispatch(celld_logic::routing::Attempt::NotOwner) {
                    anyhow::bail!("stale WebSocket route retry exhausted for {}", target.scope);
                }
                app.invalidate_remote(target.scope.clone(), node.clone(), epoch)
                    .await;
                let routed = app
                    .request(target.scope.clone())
                    .await
                    .map_err(|error| anyhow::anyhow!("refresh WebSocket route: {error:?}"))?;
                match routed.route {
                    Route::Remote {
                        node: fresh_node,
                        addr: fresh_addr,
                        epoch: fresh_epoch,
                        peer_protocol,
                    } => {
                        anyhow::ensure!(
                            peer_protocol == peer_auth::PROTOCOL_VERSION,
                            "peer {fresh_node} speaks incompatible protocol {peer_protocol}"
                        );
                        node = fresh_node;
                        addr = fresh_addr;
                        epoch = fresh_epoch;
                    }
                    Route::Local => {
                        drop(app.activity(routed.request, target.scope.clone()));
                        anyhow::bail!(
                            "WebSocket ownership moved local after remote target was created"
                        );
                    }
                }
            }
            Err(error) => {
                return Err(anyhow::anyhow!("{error}"))
                    .with_context(|| format!("connect WebSocket tunnel to peer {node} at {addr}"));
            }
        }
    };

    // Both halves speak the same implementation, so a frame crosses the hop
    // as itself. The one rewrite left is the close code: 1005 means "no code
    // was sent" and is not transmissible, so it becomes a plain 1000.
    pump_tunnel(client, peer).await
}

/// Carry frames both ways between a client and the node that owns its cell.
///
/// The read halves are never cancelled. `fastwebsockets::read_frame` consumes
/// header bytes from its buffer and then awaits for the payload, holding what
/// it parsed in local variables, so dropping that future keeps the buffer
/// advanced and loses the header. The next read then treats a payload byte as
/// a frame header and the stream never realigns. An earlier version of this
/// function ran both reads in one `tokio::select!`, which drops the losing
/// future on every iteration; `tests/p0/runtime/tunnel_cancel.rs` fails against
/// that shape.
///
/// One task therefore owns each direction. The halves are split so that a task
/// can write to a socket while the other task reads it, and neither read is
/// ever interrupted.
///
/// The two directions are not independent. Both can write to the client, so
/// they share one writer behind a mutex, and `write_frame` is awaited while
/// that mutex is held. A client that stops reading therefore blocks the other
/// direction as well. The cancelling `select!` had the same head-of-line
/// property, so this is not a regression, but the split shape must not be read
/// as two isolated pipes.
async fn pump_tunnel<C, P>(
    mut client: fastwebsockets::WebSocket<C>,
    mut peer: fastwebsockets::WebSocket<P>,
) -> anyhow::Result<()>
where
    C: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    P: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use fastwebsockets::{FragmentCollectorRead, Frame as WsFrame, OpCode, WebSocketError};
    use std::sync::Arc;

    // celld forwards a ping, a pong and a close as ordinary frames, so the
    // reader must not answer any of them itself. With both automatic replies
    // off, the obligated-send callback below never runs.
    client.set_auto_close(false);
    client.set_auto_pong(false);
    peer.set_auto_close(false);
    peer.set_auto_pong(false);

    let (client_rx, client_tx) = client.split(tokio::io::split);
    let (peer_rx, peer_tx) = peer.split(tokio::io::split);
    let mut client_rx = FragmentCollectorRead::new(client_rx);
    let mut peer_rx = FragmentCollectorRead::new(peer_rx);

    // Both directions can need to tell the client that the owner is gone, so
    // the client writer is shared. The peer writer has one user.
    let client_tx = Arc::new(tokio::sync::Mutex::new(client_tx));
    let peer_tx = Arc::new(tokio::sync::Mutex::new(peer_tx));

    let close_code = |payload: &[u8]| {
        let (code, reason, _) = websocket_close_details(payload);
        WsFrame::close(if code == 1005 { 1000 } else { code }, reason.as_bytes())
    };

    // Why the client direction ended. A close from the client is a half close:
    // the owner still has to answer it, and that answer carries the close code
    // the client must see. Ending the hop here would drop it.
    enum ClientEnd {
        Closed,
        OwnerUnavailable,
    }

    let to_peer_client_tx = client_tx.clone();
    let to_peer = async move {
        let mut obligated = |_: WsFrame| async { Ok::<(), WebSocketError>(()) };
        loop {
            let frame = client_rx
                .read_frame(&mut obligated)
                .await
                .context("read client WebSocket frame")?;
            let closing = frame.opcode == OpCode::Close;
            let frame = match frame.opcode {
                OpCode::Text | OpCode::Binary | OpCode::Ping | OpCode::Pong => frame,
                OpCode::Close => close_code(&frame.payload),
                _ => continue,
            };
            if peer_tx.lock().await.write_frame(frame).await.is_err() {
                let _ = to_peer_client_tx
                    .lock()
                    .await
                    .write_frame(WsFrame::close(1012, b"owner unavailable"))
                    .await;
                return anyhow::Ok(ClientEnd::OwnerUnavailable);
            }
            if closing {
                return anyhow::Ok(ClientEnd::Closed);
            }
        }
    };

    let to_client_tx = client_tx.clone();
    let to_client = async move {
        let mut obligated = |_: WsFrame| async { Ok::<(), WebSocketError>(()) };
        loop {
            let frame = match peer_rx.read_frame(&mut obligated).await {
                Ok(frame) => frame,
                Err(error) => {
                    let _ = to_client_tx
                        .lock()
                        .await
                        .write_frame(WsFrame::close(1012, b"owner unavailable"))
                        .await;
                    return Err(anyhow::anyhow!("{error}")).context("read owner WebSocket frame");
                }
            };
            let closing = frame.opcode == OpCode::Close;
            let frame = if closing {
                close_code(&frame.payload)
            } else {
                frame
            };
            to_client_tx
                .lock()
                .await
                .write_frame(frame)
                .await
                .context("write client WebSocket frame")?;
            if closing {
                return Ok(());
            }
        }
    };

    // The owner direction ends the hop. The client direction ends it only when
    // the owner is gone, because a client close still needs the owner's answer:
    // that answer carries the close code the client must observe.
    //
    // This select drops the direction that did not finish, which cancels a read
    // that may be in progress. That is safe only because the hop is over and
    // neither socket is read again. It must not be copied into a steady-state
    // loop.
    let mut to_peer = std::pin::pin!(to_peer);
    let mut to_client = std::pin::pin!(to_client);
    tokio::select! {
        result = &mut to_peer => match result? {
            ClientEnd::Closed => to_client.await,
            ClientEnd::OwnerUnavailable => Ok(()),
        },
        result = &mut to_client => result,
    }
}

pub(crate) async fn handle_peer_websocket(
    mut request: Request<Incoming>,
    app: AppHandle,
    path: &str,
) -> HttpReply {
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| path.to_string(), ToString::to_string);
    if let Err(error) = app.peer_auth.verify(
        request.method(),
        &path_and_query,
        request.headers(),
        &[],
        app.peer_auth.source(),
    ) {
        return peer_response(response(error.status(), error.message()));
    }
    let Some(encoded_scope) = path.strip_prefix("/__ws/") else {
        return peer_response(response(StatusCode::NOT_FOUND, "missing WebSocket scope"));
    };
    let scope = match percent_encoding::percent_decode_str(encoded_scope).decode_utf8() {
        Ok(scope) => scope.into_owned(),
        Err(_) => return peer_response(response(StatusCode::BAD_REQUEST, "invalid scope")),
    };
    let query: BTreeMap<String, String> = request
        .uri()
        .query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default();
    let websocket = query.get("id").and_then(|value| value.parse::<u64>().ok());
    let epoch = query
        .get("epoch")
        .and_then(|value| value.parse::<u64>().ok());
    let (Some(websocket), Some(epoch)) = (websocket, epoch) else {
        return peer_response(response(
            StatusCode::BAD_REQUEST,
            "invalid WebSocket target",
        ));
    };
    let Some(runtime) = &app.runtime else {
        return peer_response(response(StatusCode::SERVICE_UNAVAILABLE, "no cell runtime"));
    };
    if runtime.published_epoch(&scope) != Some(epoch) {
        let mut stale = peer_response(response(StatusCode::CONFLICT, "stale route"));
        stale.headers_mut().insert(
            hyper::header::HeaderName::from_static(STALE_ROUTE_HEADER),
            hyper::header::HeaderValue::from_static(STALE_ROUTE_VALUE),
        );
        return stale;
    }
    let (upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => {
            return peer_response(response(
                StatusCode::BAD_REQUEST,
                format!("ws upgrade: {error}"),
            ));
        }
    };
    let target = celld::js::WsTarget {
        id: websocket,
        scope,
        peer_node: None,
        peer_addr: None,
        peer_epoch: None,
    };
    let task_app = app.clone();
    let task = Box::pin(async move {
        match upgrade.await {
            Ok(socket) => websocket_task(task_app, target, socket).await,
            Err(error) => eprintln!("celld peer WebSocket upgrade failed: {error}"),
        }
    });
    if app.websockets.send(task).is_err() {
        return peer_response(response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        ));
    }
    peer_response(upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync()))
}

pub(crate) async fn handle_websocket(mut request: Request<Incoming>, app: AppHandle) -> HttpReply {
    let started = Instant::now();
    let request_id = celld::js::next_request_id();
    let (upgrade_response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => return response(StatusCode::BAD_REQUEST, format!("ws upgrade: {error}")),
    };
    let runtime = app.runtime.as_ref().expect("WebSocket runtime checked");
    let body_started = Instant::now();
    let (url, method, body, headers) =
        match request_payload(request, app.trust_forwarded_headers).await {
            Ok(payload) => payload,
            Err(response) => return response,
        };
    let body_read_us = body_started.elapsed().as_micros() as u64;
    let worker_started = Instant::now();
    let worker_response = match runtime
        .fetch_worker_pool(url, method, body, headers, request_id)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            emit_websocket_connection_timing(
                runtime,
                request_id,
                started,
                body_read_us,
                worker_started.elapsed().as_micros() as u64,
                WebSocketConnectionOutcome {
                    outcome: "worker_error",
                    route: "",
                    scope: "",
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                },
            );
            return response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Worker failed: {error:#}"),
            );
        }
    };
    let worker_dispatch_us = worker_started.elapsed().as_micros() as u64;
    let Some(target) = worker_response.ws else {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: "",
                status: worker_response.status,
            },
        );
        return runtime_response(worker_response);
    };
    if worker_response.status != 101 {
        emit_websocket_connection_timing(
            runtime,
            request_id,
            started,
            body_read_us,
            worker_dispatch_us,
            WebSocketConnectionOutcome {
                outcome: "rejected",
                route: "",
                scope: &target.scope,
                status: worker_response.status,
            },
        );
        return response(StatusCode::BAD_GATEWAY, "unsupported WebSocket route");
    }
    emit_websocket_connection_timing(
        runtime,
        request_id,
        started,
        body_read_us,
        worker_dispatch_us,
        WebSocketConnectionOutcome {
            outcome: "accepted",
            route: if target.peer_node.is_some() {
                "remote"
            } else {
                "local"
            },
            scope: &target.scope,
            status: 101,
        },
    );
    let task_app = app.clone();
    let task = Box::pin(async move {
        match upgrade.await {
            Ok(socket) if target.peer_node.is_some() => {
                if let Err(error) = remote_websocket_task(task_app, target, socket).await {
                    eprintln!("celld remote WebSocket tunnel failed: {error:#}");
                }
            }
            Ok(socket) => websocket_task(task_app, target, socket).await,
            Err(error) => {
                eprintln!("celld WebSocket upgrade failed: {error}");
                if target.peer_node.is_none() {
                    finish_websocket(&task_app, &target, 1006, String::new(), false).await;
                }
            }
        }
    });
    if app.websockets.send(task).is_err() {
        return response(
            StatusCode::SERVICE_UNAVAILABLE,
            "WebSocket executor stopped",
        );
    }
    upgrade_response.map(|body| body.map_err(|never| match never {}).boxed_unsync())
}

struct WebSocketConnectionOutcome<'a> {
    outcome: &'a str,
    route: &'a str,
    scope: &'a str,
    status: u16,
}

fn emit_websocket_connection_timing(
    runtime: &RuntimeManager,
    request_id: celld::js::RequestId,
    started: Instant,
    body_read_us: u64,
    worker_dispatch_us: u64,
    event_outcome: WebSocketConnectionOutcome<'_>,
) {
    let WebSocketConnectionOutcome {
        outcome,
        route,
        scope,
        status,
    } = event_outcome;
    tracing::debug!(
        target: "timing",
        event = "websocket_connection_timing",
        outcome,
        route,
        scope,
        request_id = %celld::js::request_id_string(request_id),
        node = runtime.node(),
        region = runtime.region(),
        runtime_version = env!("CARGO_PKG_VERSION"),
        status,
        total_us = started.elapsed().as_micros() as u64,
        body_read_us,
        worker_dispatch_us,
        "WebSocket connection resolved"
    );
}

// The tunnel reads both directions in one select!, so it drops a read future on
// every iteration. Proving the drop is not safe takes a test that watches the
// frames, because a workload oracle cannot see the fault: a lost frame keeps
// the ledger exact while delivery is not.
#[cfg(all(test, celld_internal_tests))]
mod tunnel_cancel_private {
    include!(env!("CELLD_CONFORMANCE_TUNNEL_CANCEL_TESTS"));
}

// Both cell socket loops had the same hazard as the tunnel: they read the socket
// in the same select! as the cell's outbound queue, so an outbound frame that
// won the race dropped a read in progress. A workload oracle cannot see the
// fault: a lost frame keeps the ledger exact while delivery is not, so the
// test has to watch the frames.
#[cfg(all(test, celld_internal_tests))]
mod socket_cancel_private {
    include!(env!("CELLD_CONFORMANCE_SOCKET_CANCEL_TESTS"));
}

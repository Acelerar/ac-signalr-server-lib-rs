//! SignalR server implementation using Axum and fastwebsockets

use crate::connection::Connection;
use crate::error::Result;
use crate::error::SignalRError;
use crate::hub::Hub;
use crate::hub::HubContext;
use crate::message::CloseMessage;
use crate::message::CompletionMessage;
use crate::message::HubMessage;
use crate::message::PingMessage;
use crate::protocol::parse_handshake;
use crate::protocol::parse_message;
use crate::protocol::parse_messages_messagepack;
use crate::protocol::serialize_handshake;
use crate::protocol::serialize_message;
use crate::protocol::serialize_message_messagepack;
use crate::protocol::HandshakeResponse;
use crate::protocol::Protocol;
use axum::extract::Query;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use fastwebsockets::FragmentCollector;
use fastwebsockets::FragmentCollectorRead;
use fastwebsockets::Frame;
use fastwebsockets::OpCode;
use fastwebsockets::Payload;
use fastwebsockets::WebSocket;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use serde_json::json;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::Notify;
use tokio::time::interval;
use tokio::time::timeout;
use tokio::time::Duration;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const CLOSE_ECHO_TIMEOUT: Duration = Duration::from_secs(5);

/// SignalR server adapter that exposes a hub through Axum routes.
///
/// The hub value is cloned into per-connection tasks, so shared application
/// state should be stored in cheap-to-clone handles such as `Arc<_>`,
/// `broadcast::Sender<_>`, or similar primitives.
#[derive(Clone)]
pub struct SignalRServer<H: Hub> {
    state: Arc<SignalRServerState<H>>,
}

struct SignalRServerState<H: Hub> {
    hub: H,
    shutdown: Arc<SignalRServerShutdownState>,
}

struct SignalRServerShutdownState {
    draining: AtomicBool,
    active_connections: AtomicUsize,
    drained: Notify,
    shutdown_tx: watch::Sender<bool>,
}

/// Shutdown handle for a [`SignalRServer`].
///
/// Use this handle when the enclosing application is shutting down so active
/// WebSocket connections can receive a SignalR close message and a WebSocket
/// close frame before the process exits.
#[derive(Clone)]
pub struct SignalRServerHandle {
    shutdown: Arc<SignalRServerShutdownState>,
}

struct ActiveConnectionGuard {
    shutdown: Arc<SignalRServerShutdownState>,
}

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        if self
            .shutdown
            .active_connections
            .fetch_sub(1, Ordering::AcqRel)
            == 1
        {
            self.shutdown.drained.notify_waiters();
        }
    }
}

impl SignalRServerShutdownState {
    fn new() -> Self {
        let (shutdown_tx, _) = watch::channel(false);

        Self {
            draining: AtomicBool::new(false),
            active_connections: AtomicUsize::new(0),
            drained: Notify::new(),
            shutdown_tx,
        }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn is_draining(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    fn try_register_connection(self: &Arc<Self>) -> Option<ActiveConnectionGuard> {
        loop {
            if self.is_draining() {
                return None;
            }

            let active_connections = self.active_connections.load(Ordering::Acquire);
            if self
                .active_connections
                .compare_exchange_weak(
                    active_connections,
                    active_connections + 1,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                return Some(ActiveConnectionGuard {
                    shutdown: Arc::clone(self),
                });
            }
        }
    }

    fn begin_shutdown(&self) {
        self.draining.store(true, Ordering::Release);
        self.shutdown_tx.send_replace(true);

        if self.active_connections.load(Ordering::Acquire) == 0 {
            self.drained.notify_waiters();
        }
    }

    async fn wait_for_shutdown(&self) {
        loop {
            let notified = self.drained.notified();
            if self.active_connections.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

impl<H: Hub> SignalRServer<H> {
    /// Creates a new SignalR server for the provided hub implementation.
    pub fn new(hub: H) -> Self {
        Self {
            state: Arc::new(SignalRServerState {
                hub,
                shutdown: Arc::new(SignalRServerShutdownState::new()),
            }),
        }
    }

    /// Returns a handle that can gracefully drain active WebSocket connections.
    pub fn shutdown_handle(&self) -> SignalRServerHandle {
        SignalRServerHandle {
            shutdown: Arc::clone(&self.state.shutdown),
        }
    }

    /// Converts the server into an Axum router.
    ///
    /// Nest the returned router beneath a hub path such as `/chat`. The router
    /// serves `GET|POST /negotiate` for SignalR negotiation and `GET /` for the
    /// WebSocket transport endpoint.
    pub fn into_router(self) -> Router {
        Router::new()
            .route("/negotiate", get(negotiate).post(negotiate))
            .route("/", get(websocket_handler::<H>))
            .with_state(self.state)
    }
}

impl SignalRServerHandle {
    /// Marks the server as draining and stops accepting new WebSocket upgrades.
    pub fn begin_shutdown(&self) {
        self.shutdown.begin_shutdown();
    }

    /// Waits until all tracked WebSocket connections have finished closing.
    pub async fn wait_for_shutdown(&self) {
        self.shutdown.wait_for_shutdown().await;
    }

    /// Starts shutdown and waits for all tracked WebSocket connections to drain.
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        self.wait_for_shutdown().await;
    }
}

/// Negotiation endpoint response
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct NegotiateResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    connection_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    negotiate_version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    available_transports: Vec<Transport>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct Transport {
    transport: String,
    transfer_formats: Vec<String>,
}

/// Query parameters for negotiation endpoint
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct NegotiateQuery {
    #[serde(default)]
    negotiate_version: Option<i32>,
}

/// Negotiation endpoint
async fn negotiate(Query(query): Query<NegotiateQuery>) -> axum::Json<NegotiateResponse> {
    let connection_token = uuid::Uuid::new_v4().to_string();

    // Default to version 1 if not specified
    let version = query.negotiate_version.unwrap_or(1);

    let response = match version {
        0 => {
            // Version 0: Use connectionId, no negotiateVersion field
            NegotiateResponse {
                connection_id: Some(connection_token),
                connection_token: None,
                negotiate_version: None,
                url: None,
                available_transports: vec![Transport {
                    transport: "WebSockets".to_string(),
                    transfer_formats: vec!["Text".to_string(), "Binary".to_string()],
                }],
            }
        }
        _ => {
            // Version 1 (default): Use connectionToken and include negotiateVersion
            // Note: .NET 9 client still requires connectionId even in version 1
            NegotiateResponse {
                connection_id: Some(connection_token.clone()),
                connection_token: Some(connection_token),
                negotiate_version: Some(1),
                url: None,
                available_transports: vec![Transport {
                    transport: "WebSockets".to_string(),
                    transfer_formats: vec!["Text".to_string(), "Binary".to_string()],
                }],
            }
        }
    };

    axum::Json(response)
}

/// WebSocket upgrade handler
async fn websocket_handler<H: Hub>(
    State(server): State<Arc<SignalRServerState<H>>>,
    ws: fastwebsockets::upgrade::IncomingUpgrade,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let Some(active_connection) = server.shutdown.try_register_connection() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "SignalR server is shutting down",
        )
            .into_response();
    };

    let (response, fut) = match ws.upgrade() {
        Ok((response, fut)) => (response, fut),
        Err(_) => {
            return (StatusCode::BAD_REQUEST, "Failed to upgrade WebSocket").into_response();
        }
    };

    let metadata = json!({
        "query": query,
        "access_token": query.get("access_token").cloned().or_else(|| query.get("token").cloned())
    });
    tokio::spawn(async move {
        let _active_connection = active_connection;
        match fut.await {
            Ok(ws) => {
                handle_socket(ws, server, metadata).await;
            }
            Err(e) => {
                error!("WebSocket upgrade error: {:?}", e);
            }
        }
    });

    response.map(axum::body::Body::new)
}

/// Handle a WebSocket connection
async fn handle_socket<H: Hub>(
    ws: WebSocket<TokioIo<Upgraded>>,
    server: Arc<SignalRServerState<H>>,
    metadata: Value,
) {
    let connection = Arc::new(Connection::new());
    let connection_id = connection.id();

    info!("New WebSocket connection: {}", connection_id);
    connection.set_metadata(metadata).await;

    // Perform SignalR handshake first (before splitting)
    let mut ws_handshake = FragmentCollector::new(ws);
    let protocol = match perform_handshake(&mut ws_handshake).await {
        Ok(proto) => proto,
        Err(e) => {
            error!("Handshake failed for {}: {}", connection_id, e);
            return;
        }
    };

    info!(
        "Handshake completed for {} using {:?} protocol",
        connection_id, protocol
    );

    // Get the inner stream and recreate WebSocket
    let stream = ws_handshake.into_inner();
    let ws = WebSocket::after_handshake(stream, fastwebsockets::Role::Server);

    // Split the WebSocket into read and write halves
    let (ws_read, ws_write) = ws.split(tokio::io::split);

    // Wrap the read half in FragmentCollectorRead
    let mut ws_read = FragmentCollectorRead::new(ws_read);

    // Create channels for bidirectional communication
    let (tx, mut rx) = mpsc::unbounded_channel::<HubMessage>();
    let (auto_frame_tx, mut auto_frame_rx) = mpsc::unbounded_channel::<Frame<'static>>();
    let (ws_close_sent_tx, mut ws_close_sent_rx) = watch::channel(false);
    let (reader_done_tx, mut reader_done_rx) = watch::channel(false);
    let ctx = HubContext::new(connection.clone(), tx);

    // Notify hub of connection
    server.hub.on_connected(&ctx).await;

    let connection_id_read = connection_id;
    let connection_id_write = connection_id;
    let hub = server.hub.clone();
    let ctx_read = ctx.clone();
    let mut server_shutdown_rx = server.shutdown.subscribe();

    let protocol_read = protocol;
    let read_task = tokio::spawn(async move {
        let auto_frame_tx = auto_frame_tx;
        let mut awaiting_close_echo = false;

        loop {
            if awaiting_close_echo {
                let mut send_fn =
                    |_frame: Frame<'static>| std::future::ready(Ok::<(), std::io::Error>(()));

                match timeout(CLOSE_ECHO_TIMEOUT, ws_read.read_frame(&mut send_fn)).await {
                    Ok(Ok(frame)) => {
                        if frame.opcode == OpCode::Close {
                            info!(
                                "Received close echo while shutting down connection: {}",
                                connection_id_read
                            );
                            break;
                        }

                        debug!(
                            "Ignoring frame {:?} while waiting for close echo on {}",
                            frame.opcode, connection_id_read
                        );
                    }
                    Ok(Err(error)) => {
                        error!(
                            "Error reading close echo for {}: {}",
                            connection_id_read, error
                        );
                        break;
                    }
                    Err(_) => {
                        warn!(
                            "Timed out waiting for close echo on connection: {}",
                            connection_id_read
                        );
                        break;
                    }
                }
                continue;
            }

            if *ws_close_sent_rx.borrow() {
                awaiting_close_echo = true;
                continue;
            }

            let auto_frame_tx = auto_frame_tx.clone();
            let mut send_fn = move |frame: Frame<'static>| {
                let _ = auto_frame_tx.send(frame);
                std::future::ready(Ok::<(), std::io::Error>(()))
            };

            tokio::select! {
                changed = ws_close_sent_rx.changed() => {
                    if changed.is_err() || *ws_close_sent_rx.borrow() {
                        awaiting_close_echo = true;
                    }
                }
                result = timeout(CLIENT_TIMEOUT, ws_read.read_frame(&mut send_fn)) => {
                    match result {
                        Ok(Ok(frame)) => {
                            match frame.opcode {
                                OpCode::Text => {
                                    let data = String::from_utf8_lossy(&frame.payload);
                                    debug!("Received text message: {}", data);

                                    // SignalR can batch multiple messages in one WebSocket frame,
                                    // each separated by the record separator (0x1E).
                                    for part in data.split('\u{001e}') {
                                        let part = part.trim();
                                        if part.is_empty() {
                                            continue;
                                        }
                                        if let Err(error) =
                                            handle_message(&hub, &ctx_read, part, protocol_read).await
                                        {
                                            error!("Error handling message: {}", error);
                                        }
                                    }
                                }
                                OpCode::Binary => {
                                    debug!("Received binary message with {} bytes", frame.payload.len());

                                    if let Err(error) =
                                        handle_message_binary(&hub, &ctx_read, &frame.payload).await
                                    {
                                        error!("Error handling binary message: {}", error);
                                    }
                                }
                                OpCode::Close => {
                                    info!("Client closed connection: {}", connection_id_read);
                                    break;
                                }
                                OpCode::Ping => {
                                    debug!("Received ping");
                                }
                                _ => {
                                    debug!("Received frame with opcode: {:?}", frame.opcode);
                                }
                            }
                        }
                        Ok(Err(error)) => {
                            error!("Error reading frame: {}", error);
                            break;
                        }
                        Err(_) => {
                            warn!(
                                "Client timed out without sending data: {}",
                                connection_id_read
                            );
                            break;
                        }
                    }
                }
            }
        }

        reader_done_tx.send_replace(true);
    });

    let protocol_write = protocol;
    let write_task = tokio::spawn(async move {
        let mut ws_write = ws_write;
        let mut keepalive_interval = interval(KEEPALIVE_INTERVAL);
        keepalive_interval.tick().await;

        loop {
            if *server_shutdown_rx.borrow() {
                let close_message = HubMessage::Close(CloseMessage {
                    error: None,
                    allow_reconnect: Some(false),
                });

                match outgoing_frame(protocol_write, &close_message) {
                    Ok(frame) => {
                        if let Err(error) = ws_write.write_frame(frame).await {
                            error!(
                                "Failed to send SignalR close message on {}: {}",
                                connection_id_write, error
                            );
                        }
                    }
                    Err(error) => {
                        error!(
                            "Failed to serialize SignalR close message on {}: {}",
                            connection_id_write, error
                        );
                    }
                }

                if let Err(error) = ws_write.write_frame(Frame::close(1001, &[])).await {
                    error!(
                        "Failed to send WebSocket close frame on {}: {}",
                        connection_id_write, error
                    );
                }

                ws_close_sent_tx.send_replace(true);

                if timeout(
                    CLOSE_ECHO_TIMEOUT + Duration::from_secs(1),
                    wait_for_flag(&mut reader_done_rx),
                )
                .await
                .is_err()
                {
                    warn!(
                        "Timed out waiting for reader task to finish after close handshake: {}",
                        connection_id_write
                    );
                }
                break;
            }

            if *reader_done_rx.borrow() {
                while let Some(frame) = auto_frame_rx.recv().await {
                    if let Err(error) = ws_write.write_frame(frame).await {
                        error!(
                            "Failed to flush automatic frame on {}: {}",
                            connection_id_write, error
                        );
                        break;
                    }
                }
                break;
            }

            tokio::select! {
                biased;

                changed = reader_done_rx.changed() => {
                    if changed.is_err() || *reader_done_rx.borrow() {
                        while let Some(frame) = auto_frame_rx.recv().await {
                            if let Err(error) = ws_write.write_frame(frame).await {
                                error!(
                                    "Failed to flush automatic frame on {}: {}",
                                    connection_id_write, error
                                );
                                break;
                            }
                        }
                        break;
                    }
                }
                changed = server_shutdown_rx.changed() => {
                    if changed.is_err() || *server_shutdown_rx.borrow() {
                        continue;
                    }
                }
                Some(frame) = auto_frame_rx.recv() => {
                    if let Err(error) = ws_write.write_frame(frame).await {
                        error!("Failed to send automatic frame on {}: {}", connection_id_write, error);
                        break;
                    }
                }
                Some(message) = rx.recv() => {
                    let frame = match outgoing_frame(protocol_write, &message) {
                        Ok(frame) => frame,
                        Err(error) => {
                            error!("Failed to serialize outgoing SignalR message: {}", error);
                            continue;
                        }
                    };

                    if let Err(error) = ws_write.write_frame(frame).await {
                        error!("Failed to send message: {}", error);
                        break;
                    }

                    keepalive_interval.reset();
                }
                _ = keepalive_interval.tick() => {
                    debug!("Sending keepalive ping to connection: {}", connection_id_write);
                    let ping_message = HubMessage::Ping(PingMessage {});
                    let frame = match outgoing_frame(protocol_write, &ping_message) {
                        Ok(frame) => frame,
                        Err(error) => {
                            error!("Failed to serialize keepalive ping: {}", error);
                            continue;
                        }
                    };

                    if let Err(error) = ws_write.write_frame(frame).await {
                        error!("Failed to send keepalive ping: {}", error);
                        break;
                    }
                }
            }
        }
    });

    // Wait for both tasks to complete
    let _ = tokio::join!(read_task, write_task);

    // Notify hub of disconnection
    server.hub.on_disconnected(&ctx).await;

    info!("Connection closed: {}", connection_id);
}

enum SerializedHubMessage {
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

fn serialize_outgoing_message(
    protocol: Protocol,
    message: &HubMessage,
) -> Result<SerializedHubMessage> {
    match protocol {
        Protocol::Json => Ok(SerializedHubMessage::Text(
            serialize_message(message)?.into_bytes(),
        )),
        Protocol::MessagePack => Ok(SerializedHubMessage::Binary(serialize_message_messagepack(
            message,
        )?)),
    }
}

fn outgoing_frame(protocol: Protocol, message: &HubMessage) -> Result<Frame<'static>> {
    match serialize_outgoing_message(protocol, message)? {
        SerializedHubMessage::Text(bytes) => Ok(Frame::text(Payload::Owned(bytes))),
        SerializedHubMessage::Binary(bytes) => Ok(Frame::binary(Payload::Owned(bytes))),
    }
}

async fn wait_for_flag(flag_rx: &mut watch::Receiver<bool>) {
    if *flag_rx.borrow() {
        return;
    }

    while flag_rx.changed().await.is_ok() {
        if *flag_rx.borrow() {
            return;
        }
    }
}

/// Perform the SignalR handshake
async fn perform_handshake(ws: &mut FragmentCollector<TokioIo<Upgraded>>) -> Result<Protocol> {
    // Read the handshake request
    let frame = match timeout(HANDSHAKE_TIMEOUT, ws.read_frame()).await {
        Ok(frame) => frame?,
        Err(_) => {
            let error = SignalRError::InvalidHandshake(
                "Timed out waiting for handshake request".to_string(),
            );
            send_handshake_error(ws, &error).await;
            return Err(error);
        }
    };

    if frame.opcode != OpCode::Text {
        let error = SignalRError::InvalidHandshake("Expected text frame for handshake".to_string());
        send_handshake_error(ws, &error).await;
        return Err(error);
    }

    let data = String::from_utf8_lossy(&frame.payload);
    debug!("Handshake request: {}", data);

    // Parse the handshake
    let handshake = match parse_handshake(&data) {
        Ok(handshake) => handshake,
        Err(error) => {
            send_handshake_error(ws, &error).await;
            return Err(error);
        }
    };

    // Validate protocol
    let protocol = match validate_handshake(&handshake) {
        Ok(protocol) => protocol,
        Err(error) => {
            send_handshake_error(ws, &error).await;
            return Err(error);
        }
    };

    // Send successful handshake response
    let response = HandshakeResponse::success();
    let json = serialize_handshake(&response)?;
    ws.write_frame(Frame::text(Payload::Owned(json.into_bytes())))
        .await?;

    Ok(protocol)
}

async fn send_handshake_error(ws: &mut FragmentCollector<TokioIo<Upgraded>>, error: &SignalRError) {
    let response = HandshakeResponse::error(error.to_string());
    let json = match serialize_handshake(&response) {
        Ok(json) => json,
        Err(error) => {
            error!("Failed to serialize handshake error response: {}", error);
            return;
        }
    };

    if let Err(error) = ws
        .write_frame(Frame::text(Payload::Owned(json.into_bytes())))
        .await
    {
        warn!("Failed to send handshake error response: {}", error);
    }
}

fn validate_handshake(handshake: &crate::protocol::HandshakeRequest) -> Result<Protocol> {
    if handshake.version != 1 {
        return Err(SignalRError::InvalidHandshake(format!(
            "Unsupported protocol version: {}",
            handshake.version
        )));
    }

    Protocol::parse(&handshake.protocol)
        .ok_or_else(|| SignalRError::UnsupportedProtocol(handshake.protocol.clone()))
}

/// Handle a SignalR message
async fn handle_message<H: Hub>(
    hub: &H,
    ctx: &HubContext,
    data: &str,
    protocol: Protocol,
) -> Result<()> {
    let message = match protocol {
        Protocol::Json => parse_message(data)?,
        Protocol::MessagePack => {
            // This shouldn't happen as MessagePack uses binary frames
            return Err(SignalRError::Protocol(
                "Expected binary frame for MessagePack".to_string(),
            ));
        }
    };

    debug!("Parsed message: {:?}", message);

    dispatch_hub_message(hub, ctx, message).await
}

/// Handle a SignalR binary message (MessagePack)
async fn handle_message_binary<H: Hub>(hub: &H, ctx: &HubContext, data: &[u8]) -> Result<()> {
    let messages = parse_messages_messagepack(data)?;

    for message in messages {
        debug!("Parsed MessagePack message: {:?}", message);
        dispatch_hub_message(hub, ctx, message).await?;
    }

    Ok(())
}

async fn dispatch_hub_message<H: Hub>(
    hub: &H,
    ctx: &HubContext,
    message: HubMessage,
) -> Result<()> {
    match message {
        HubMessage::Invocation(inv_msg) => {
            handle_invocation_message(hub, ctx, inv_msg).await?;
        }
        HubMessage::StreamInvocation(stream_inv_msg) => {
            handle_stream_invocation_message(hub, ctx, stream_inv_msg).await?;
        }
        HubMessage::Ping(_) => {
            // Pings are handled at the WebSocket level
        }
        HubMessage::Close(close_msg) => {
            if let Some(error) = close_msg.error {
                warn!("Client closing with error: {}", error);
            }
        }
        _ => {
            debug!("Unhandled message type: {:?}", message);
        }
    }

    Ok(())
}

async fn handle_invocation_message<H: Hub>(
    hub: &H,
    ctx: &HubContext,
    inv_msg: crate::message::InvocationMessage,
) -> Result<()> {
    let invocation_id = inv_msg.invocation_id.clone();

    match hub.on_invocation(ctx, &inv_msg).await {
        Ok(result) => {
            if let Some(invocation_id) = invocation_id {
                send_completion(ctx, invocation_id, result, None)?;
            }

            Ok(())
        }
        Err(error) => {
            if let Some(invocation_id) = invocation_id {
                send_completion(ctx, invocation_id, None, Some(error.to_string()))?;
                Ok(())
            } else {
                Err(error)
            }
        }
    }
}

async fn handle_stream_invocation_message<H: Hub>(
    hub: &H,
    ctx: &HubContext,
    stream_inv_msg: crate::message::StreamInvocationMessage,
) -> Result<()> {
    let invocation_id = stream_inv_msg.invocation_id.clone();

    match hub.on_stream_invocation(ctx, &stream_inv_msg).await {
        Ok(()) => Ok(()),
        Err(error) => {
            send_completion(ctx, invocation_id, None, Some(error.to_string()))?;
            Ok(())
        }
    }
}

fn send_completion(
    ctx: &HubContext,
    invocation_id: String,
    result: Option<Value>,
    error: Option<String>,
) -> Result<()> {
    ctx.send(HubMessage::Completion(CompletionMessage {
        invocation_id,
        result,
        error,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::parse_message_messagepack;
    use axum::body::Body;
    use axum::http::Request;
    use fastwebsockets::FragmentCollector;
    use fastwebsockets::OpCode;
    use http_body_util::BodyExt;
    use http_body_util::Empty;
    use hyper::body::Bytes;
    use hyper::header::CONNECTION;
    use hyper::header::UPGRADE;
    use hyper::upgrade::Upgraded;
    use hyper::Request as HyperRequest;
    use hyper_util::rt::TokioIo;
    use serde_json::json;
    use std::future::Future;
    use std::sync::Arc;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;
    use tower::Service;

    #[derive(Clone)]
    struct TestHub;

    #[async_trait::async_trait]
    impl Hub for TestHub {
        async fn on_connected(&self, _ctx: &HubContext) {}
        async fn on_disconnected(&self, _ctx: &HubContext) {}
        async fn on_invocation(
            &self,
            _ctx: &HubContext,
            _message: &crate::message::InvocationMessage,
        ) -> Result<Option<serde_json::Value>> {
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct EchoHub;

    #[async_trait::async_trait]
    impl Hub for EchoHub {
        async fn on_invocation(
            &self,
            _ctx: &HubContext,
            message: &crate::message::InvocationMessage,
        ) -> Result<Option<serde_json::Value>> {
            Ok(Some(json!({
                "target": message.target,
                "arguments": message.arguments,
            })))
        }
    }

    #[derive(Clone)]
    struct ErrorHub;

    #[async_trait::async_trait]
    impl Hub for ErrorHub {
        async fn on_invocation(
            &self,
            _ctx: &HubContext,
            _message: &crate::message::InvocationMessage,
        ) -> Result<Option<serde_json::Value>> {
            Err(SignalRError::Protocol("invocation failed".to_string()))
        }

        async fn on_stream_invocation(
            &self,
            _ctx: &HubContext,
            _message: &crate::message::StreamInvocationMessage,
        ) -> Result<()> {
            Err(SignalRError::Protocol("stream failed".to_string()))
        }
    }

    fn test_context() -> (HubContext, mpsc::UnboundedReceiver<HubMessage>) {
        let connection = Arc::new(Connection::new());
        let (tx, rx) = mpsc::unbounded_channel();
        (HubContext::new(connection, tx), rx)
    }

    struct SpawnExecutor;

    impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        fn execute(&self, fut: Fut) {
            tokio::spawn(fut);
        }
    }

    async fn spawn_test_server<H: Hub>(hub: H) -> (String, SignalRServerHandle, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = SignalRServer::new(hub);
        let shutdown_handle = server.shutdown_handle();
        let app = server.into_router();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (address.to_string(), shutdown_handle, handle)
    }

    async fn connect_websocket(address: &str, path: &str) -> FragmentCollector<TokioIo<Upgraded>> {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let request = HyperRequest::builder()
            .method("GET")
            .uri(format!("http://{}{}", address, path))
            .header("Host", address)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header(
                "Sec-WebSocket-Key",
                fastwebsockets::handshake::generate_key(),
            )
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())
            .unwrap();

        let (websocket, _) = fastwebsockets::handshake::client(&SpawnExecutor, request, stream)
            .await
            .unwrap();
        FragmentCollector::new(websocket)
    }

    #[tokio::test]
    async fn test_negotiate_version_0() {
        let server = SignalRServer::new(TestHub);
        let mut app = server.into_router();

        let request = Request::builder()
            .uri("/negotiate?negotiateVersion=0")
            .body(Body::empty())
            .unwrap();

        let response = app.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_str).unwrap();

        // Version 0 should have connectionId, no connectionToken, no negotiateVersion
        assert!(json.get("connectionId").is_some());
        assert!(json.get("connectionToken").is_none());
        assert!(json.get("negotiateVersion").is_none());
        assert!(json.get("availableTransports").is_some());
    }

    #[tokio::test]
    async fn test_negotiate_version_1() {
        let server = SignalRServer::new(TestHub);
        let mut app = server.into_router();

        let request = Request::builder()
            .uri("/negotiate?negotiateVersion=1")
            .body(Body::empty())
            .unwrap();

        let response = app.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_str).unwrap();

        // Version 1 should have connectionId, connectionToken, and negotiateVersion
        // Note: .NET 9 client requires connectionId even in version 1
        assert!(json.get("connectionId").is_some());
        assert!(json.get("connectionToken").is_some());
        assert_eq!(json.get("negotiateVersion").unwrap().as_i64().unwrap(), 1);
        assert!(json.get("availableTransports").is_some());
    }

    #[tokio::test]
    async fn test_negotiate_default_version() {
        let server = SignalRServer::new(TestHub);
        let mut app = server.into_router();

        let request = Request::builder()
            .uri("/negotiate")
            .body(Body::empty())
            .unwrap();

        let response = app.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_str).unwrap();

        // Default should be version 1 (which now includes connectionId for .NET 9 compatibility)
        assert!(json.get("connectionId").is_some());
        assert!(json.get("connectionToken").is_some());
        assert_eq!(json.get("negotiateVersion").unwrap().as_i64().unwrap(), 1);
        assert!(json.get("availableTransports").is_some());
    }

    #[tokio::test]
    async fn test_negotiate_available_transports() {
        let server = SignalRServer::new(TestHub);
        let mut app = server.into_router();

        let request = Request::builder()
            .uri("/negotiate")
            .body(Body::empty())
            .unwrap();

        let response = app.call(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        let json: serde_json::Value = serde_json::from_str(&body_str).unwrap();

        let transports = json.get("availableTransports").unwrap().as_array().unwrap();
        assert_eq!(transports.len(), 1);

        let transport = &transports[0];
        assert_eq!(
            transport.get("transport").unwrap().as_str().unwrap(),
            "WebSockets"
        );

        let formats = transport
            .get("transferFormats")
            .unwrap()
            .as_array()
            .unwrap();
        assert_eq!(formats.len(), 2);
        assert!(formats.iter().any(|f| f.as_str().unwrap() == "Text"));
        assert!(formats.iter().any(|f| f.as_str().unwrap() == "Binary"));
    }

    #[test]
    fn test_validate_handshake_accepts_supported_protocols() {
        let json_handshake = crate::protocol::HandshakeRequest {
            protocol: "json".to_string(),
            version: 1,
        };
        let messagepack_handshake = crate::protocol::HandshakeRequest {
            protocol: "messagepack".to_string(),
            version: 1,
        };

        assert_eq!(validate_handshake(&json_handshake).unwrap(), Protocol::Json);
        assert_eq!(
            validate_handshake(&messagepack_handshake).unwrap(),
            Protocol::MessagePack
        );
    }

    #[test]
    fn test_validate_handshake_rejects_unsupported_version() {
        let handshake = crate::protocol::HandshakeRequest {
            protocol: "json".to_string(),
            version: 2,
        };

        let result = validate_handshake(&handshake);
        assert!(matches!(result, Err(SignalRError::InvalidHandshake(_))));
    }

    #[test]
    fn test_validate_handshake_rejects_unsupported_protocol() {
        let handshake = crate::protocol::HandshakeRequest {
            protocol: "xml".to_string(),
            version: 1,
        };

        let result = validate_handshake(&handshake);
        assert!(matches!(result, Err(SignalRError::UnsupportedProtocol(_))));
    }

    #[tokio::test]
    async fn test_handle_message_sends_completion_result() {
        let (ctx, mut rx) = test_context();
        let message = r#"{"type":1,"invocationId":"1","target":"Echo","arguments":["hello"]}"#;

        handle_message(&EchoHub, &ctx, message, Protocol::Json)
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "1");
                assert_eq!(
                    completion.result.unwrap(),
                    json!({"target": "Echo", "arguments": ["hello"]})
                );
                assert_eq!(completion.error, None);
            }
            _ => panic!("Expected completion message"),
        }
    }

    #[tokio::test]
    async fn test_handle_message_sends_completion_error_on_invocation_error() {
        let (ctx, mut rx) = test_context();
        let message = r#"{"type":1,"invocationId":"1","target":"Fail","arguments":[]}"#;

        handle_message(&ErrorHub, &ctx, message, Protocol::Json)
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "1");
                assert_eq!(completion.result, None);
                assert_eq!(
                    completion.error,
                    Some("Protocol error: invocation failed".to_string())
                );
            }
            _ => panic!("Expected completion message"),
        }
    }

    #[tokio::test]
    async fn test_handle_message_nonblocking_invocation_error_propagates() {
        let (ctx, mut rx) = test_context();
        let message = r#"{"type":1,"target":"Fail","arguments":[]}"#;

        let result = handle_message(&ErrorHub, &ctx, message, Protocol::Json).await;

        assert!(matches!(result, Err(SignalRError::Protocol(_))));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_handle_message_stream_invocation_error_sends_completion() {
        let (ctx, mut rx) = test_context();
        let message = r#"{"type":4,"invocationId":"stream-1","target":"Stream","arguments":[]}"#;

        handle_message(&ErrorHub, &ctx, message, Protocol::Json)
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "stream-1");
                assert_eq!(completion.result, None);
                assert_eq!(
                    completion.error,
                    Some("Protocol error: stream failed".to_string())
                );
            }
            _ => panic!("Expected completion message"),
        }
    }

    #[tokio::test]
    async fn test_handle_message_binary_sends_completion_result() {
        let (ctx, mut rx) = test_context();
        let message = HubMessage::Invocation(crate::message::InvocationMessage {
            invocation_id: Some("binary-1".to_string()),
            target: "Echo".to_string(),
            arguments: vec![json!("hello")],
            stream_ids: None,
        });
        let payload = serialize_message_messagepack(&message).unwrap();

        handle_message_binary(&EchoHub, &ctx, &payload)
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "binary-1");
                assert_eq!(
                    completion.result.unwrap(),
                    json!({"target": "Echo", "arguments": ["hello"]})
                );
            }
            _ => panic!("Expected completion message"),
        }
    }

    #[tokio::test]
    async fn test_handle_message_binary_processes_multiple_messages() {
        let (ctx, mut rx) = test_context();
        let first = serialize_message_messagepack(&HubMessage::Invocation(
            crate::message::InvocationMessage {
                invocation_id: Some("first".to_string()),
                target: "One".to_string(),
                arguments: vec![],
                stream_ids: None,
            },
        ))
        .unwrap();
        let second = serialize_message_messagepack(&HubMessage::Invocation(
            crate::message::InvocationMessage {
                invocation_id: Some("second".to_string()),
                target: "Two".to_string(),
                arguments: vec![],
                stream_ids: None,
            },
        ))
        .unwrap();
        let mut payload = first;
        payload.extend(second);

        handle_message_binary(&EchoHub, &ctx, &payload)
            .await
            .unwrap();

        let first_completion = rx.recv().await.unwrap();
        let second_completion = rx.recv().await.unwrap();
        assert_eq!(first_completion.invocation_id(), Some("first"));
        assert_eq!(second_completion.invocation_id(), Some("second"));
    }

    #[tokio::test]
    async fn test_dispatch_hub_message_handles_ping_close_and_unhandled_messages() {
        let (ctx, mut rx) = test_context();

        dispatch_hub_message(&EchoHub, &ctx, HubMessage::Ping(PingMessage {}))
            .await
            .unwrap();
        dispatch_hub_message(
            &EchoHub,
            &ctx,
            HubMessage::Close(crate::message::CloseMessage {
                error: Some("client closed".to_string()),
                allow_reconnect: Some(false),
            }),
        )
        .await
        .unwrap();
        dispatch_hub_message(
            &EchoHub,
            &ctx,
            HubMessage::StreamItem(crate::message::StreamItemMessage {
                invocation_id: "orphan".to_string(),
                item: json!(1),
            }),
        )
        .await
        .unwrap();

        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_serialize_outgoing_message_json() {
        let serialized =
            serialize_outgoing_message(Protocol::Json, &HubMessage::Ping(PingMessage {})).unwrap();

        match serialized {
            SerializedHubMessage::Text(bytes) => {
                assert_eq!(String::from_utf8(bytes).unwrap(), "{\"type\":6}\u{001e}");
            }
            SerializedHubMessage::Binary(_) => panic!("Expected text message"),
        }
    }

    #[test]
    fn test_serialize_outgoing_message_messagepack() {
        let serialized =
            serialize_outgoing_message(Protocol::MessagePack, &HubMessage::Ping(PingMessage {}))
                .unwrap();

        match serialized {
            SerializedHubMessage::Binary(bytes) => {
                assert_eq!(bytes, vec![0x02, 0x91, 0x06]);
            }
            SerializedHubMessage::Text(_) => panic!("Expected binary message"),
        }
    }

    #[tokio::test]
    async fn test_websocket_json_handshake_invocation_and_close() {
        let (address, _shutdown_handle, server_handle) = spawn_test_server(EchoHub).await;
        let mut websocket = connect_websocket(&address, "/?access_token=test-token").await;

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"protocol\":\"json\",\"version\":1}\x1e".to_vec(),
            )))
            .await
            .unwrap();

        let handshake_response = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handshake_response.opcode, OpCode::Text);
        assert_eq!(
            String::from_utf8(handshake_response.payload.to_vec()).unwrap(),
            "{}\u{001e}"
        );

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"type\":1,\"invocationId\":\"1\",\"target\":\"Echo\",\"arguments\":[\"hello\"]}\x1e"
                    .to_vec(),
            )))
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completion.opcode, OpCode::Text);

        let completion = String::from_utf8(completion.payload.to_vec()).unwrap();
        let completion = parse_message(&completion).unwrap();
        match completion {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "1");
                assert_eq!(
                    completion.result.unwrap(),
                    json!({"target": "Echo", "arguments": ["hello"]})
                );
            }
            _ => panic!("Expected completion message"),
        }

        let _ = websocket.write_frame(Frame::close(1000, &[])).await;
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_websocket_messagepack_handshake_invocation_and_close() {
        let (address, _shutdown_handle, server_handle) = spawn_test_server(EchoHub).await;
        let mut websocket = connect_websocket(&address, "/").await;

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"protocol\":\"messagepack\",\"version\":1}\x1e".to_vec(),
            )))
            .await
            .unwrap();

        let handshake_response = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handshake_response.opcode, OpCode::Text);

        let invocation = HubMessage::Invocation(crate::message::InvocationMessage {
            invocation_id: Some("binary".to_string()),
            target: "Echo".to_string(),
            arguments: vec![json!(42)],
            stream_ids: None,
        });
        websocket
            .write_frame(Frame::binary(Payload::Owned(
                serialize_message_messagepack(&invocation).unwrap(),
            )))
            .await
            .unwrap();

        let completion = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completion.opcode, OpCode::Binary);

        let completion = parse_message_messagepack(&completion.payload).unwrap();
        match completion {
            HubMessage::Completion(completion) => {
                assert_eq!(completion.invocation_id, "binary");
                assert_eq!(
                    completion.result.unwrap(),
                    json!({"target": "Echo", "arguments": [42]})
                );
            }
            _ => panic!("Expected completion message"),
        }

        let _ = websocket.write_frame(Frame::close(1000, &[])).await;
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_websocket_client_close_receives_close_frame() {
        let (address, _shutdown_handle, server_handle) = spawn_test_server(EchoHub).await;
        let mut websocket = connect_websocket(&address, "/").await;

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"protocol\":\"json\",\"version\":1}\x1e".to_vec(),
            )))
            .await
            .unwrap();

        let _handshake_response = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();

        websocket
            .write_frame(Frame::close(1000, &[]))
            .await
            .unwrap();

        let close_frame = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(close_frame.opcode, OpCode::Close);

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_websocket_server_shutdown_sends_signalr_close_and_websocket_close() {
        let (address, shutdown_handle, server_handle) = spawn_test_server(EchoHub).await;
        let mut websocket = connect_websocket(&address, "/").await;

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"protocol\":\"json\",\"version\":1}\x1e".to_vec(),
            )))
            .await
            .unwrap();

        let _handshake_response = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();

        let shutdown_task = tokio::spawn(async move {
            shutdown_handle.shutdown().await;
        });

        let close_message_frame = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(close_message_frame.opcode, OpCode::Text);

        let close_message =
            parse_message(std::str::from_utf8(&close_message_frame.payload).unwrap()).unwrap();
        match close_message {
            HubMessage::Close(close_message) => {
                assert_eq!(close_message.error, None);
                assert_eq!(close_message.allow_reconnect, Some(false));
            }
            _ => panic!("Expected close message"),
        }

        let close_frame = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(close_frame.opcode, OpCode::Close);

        websocket
            .write_frame(Frame::close(1001, &[]))
            .await
            .unwrap();

        timeout(Duration::from_secs(2), shutdown_task)
            .await
            .unwrap()
            .unwrap();

        server_handle.abort();
    }

    #[tokio::test]
    async fn test_websocket_unsupported_handshake_version_returns_error() {
        let (address, _shutdown_handle, server_handle) = spawn_test_server(EchoHub).await;
        let mut websocket = connect_websocket(&address, "/").await;

        websocket
            .write_frame(Frame::text(Payload::Owned(
                b"{\"protocol\":\"json\",\"version\":2}\x1e".to_vec(),
            )))
            .await
            .unwrap();

        let handshake_response = timeout(Duration::from_secs(2), websocket.read_frame())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(handshake_response.opcode, OpCode::Text);

        let body = String::from_utf8(handshake_response.payload.to_vec()).unwrap();
        let response: crate::protocol::HandshakeResponse =
            serde_json::from_str(body.trim_end_matches('\u{001e}')).unwrap();
        assert_eq!(
            response.error,
            Some("Invalid handshake: Unsupported protocol version: 2".to_string())
        );

        server_handle.abort();
    }
}

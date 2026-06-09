#![warn(missing_docs, rustdoc::broken_intra_doc_links)]
//! Async `SignalR` server primitives for Axum applications.
//!
//! `ac-signalr-server` mounts a SignalR-compatible hub beneath an Axum route.
//! [`SignalRServer`] exposes the standard negotiation and WebSocket endpoints,
//! while [`Hub`] provides lifecycle, invocation, and streaming hooks for your
//! application logic.
//!
//! # Routing
//!
//! Nest the router returned by [`SignalRServer::into_router`] under a path such
//! as `/chat`. That mount exposes:
//!
//! - `GET|POST /chat/negotiate` for `SignalR` negotiation
//! - `GET /chat/` for the WebSocket transport endpoint
//!
//! # Quick start
//!
//! ```no_run
//! use async_trait::async_trait;
//! use axum::Router;
//! use serde_json::json;
//! use ac_signalr_server::Hub;
//! use ac_signalr_server::HubContext;
//! use ac_signalr_server::HubMessage;
//! use ac_signalr_server::InvocationMessage;
//! use ac_signalr_server::SignalRServer;
//!
//! #[derive(Clone)]
//! struct ChatHub;
//!
//! #[async_trait]
//! impl Hub for ChatHub {
//!     async fn on_connected(&self, ctx: &HubContext) {
//!         let _ = ctx.send(HubMessage::Invocation(InvocationMessage {
//!             invocation_id: None,
//!             target: "ReceiveMessage".to_string(),
//!             arguments: vec![
//!                 json!("Server"),
//!                 json!(format!("Welcome {}", ctx.connection_id())),
//!             ],
//!             stream_ids: None,
//!         }));
//!     }
//!
//!     async fn on_invocation(
//!         &self,
//!         _ctx: &HubContext,
//!         message: &InvocationMessage,
//!     ) -> ac_signalr_server::Result<Option<serde_json::Value>> {
//!         match message.target.as_str() {
//!             "Ping" => Ok(Some(json!("pong"))),
//!             _ => Ok(None),
//!         }
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let app = Router::new().nest("/chat", SignalRServer::new(ChatHub).into_router());
//!     let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
//!     axum::serve(listener, app).await?;
//!     Ok(())
//! }
//! ```
//!
//! # Protocol support
//!
//! The crate supports JSON and `MessagePack` hub payloads, `SignalR` handshakes,
//! request/response invocations, server-to-client messages via
//! [`HubContext::send`], and server streaming through
//! [`Hub::on_stream_invocation`].
//!
//! See the crate README and the `examples/` directory for end-to-end examples.

mod connection;
mod error;
mod hub;
mod message;
mod protocol;
mod server;

pub use connection::Connection;
pub use connection::ConnectionId;
pub use error::Result;
pub use error::SignalRError;
pub use hub::Hub;
pub use hub::HubContext;
pub use message::CancelInvocationMessage;
pub use message::CloseMessage;
pub use message::CompletionMessage;
pub use message::HubMessage;
pub use message::InvocationMessage;
pub use message::MessageType;
pub use message::PingMessage;
pub use message::StreamInvocationMessage;
pub use message::StreamItemMessage;
pub use protocol::parse_message;
pub use protocol::parse_message_messagepack;
pub use protocol::serialize_message;
pub use protocol::serialize_message_messagepack;
pub use protocol::HandshakeRequest;
pub use protocol::HandshakeResponse;
pub use protocol::Protocol;
pub use server::SignalRServer;
pub use server::SignalRServerHandle;

//! Simple chat server example using tt-signalr-server
//!
//! Run this example with:
//! ```sh
//! cargo run --example chat_server
//! ```
//!
//! Then connect with a SignalR client to http://localhost:3000/chat

use ac_signalr_server::CompletionMessage;
use ac_signalr_server::Hub;
use ac_signalr_server::HubContext;
use ac_signalr_server::HubMessage;
use ac_signalr_server::InvocationMessage;
use ac_signalr_server::SignalRServer;
use ac_signalr_server::StreamInvocationMessage;
use ac_signalr_server::StreamItemMessage;
use async_trait::async_trait;
use serde_json::json;
use tokio::sync::broadcast;
use tracing::info;

#[derive(Clone)]
struct ChatHub {
    // Broadcast channel for streaming messages to all connected clients
    tx: broadcast::Sender<(String, String)>,
}

#[async_trait]
impl Hub for ChatHub {
    async fn on_connected(&self, ctx: &HubContext) {
        info!("Client connected: {}", ctx.connection_id());

        // Send welcome message to the client
        let _ = ctx.send(HubMessage::Invocation(InvocationMessage {
            invocation_id: None,
            target: "ReceiveMessage".to_string(),
            arguments: vec![
                json!("Server"),
                json!(format!(
                    "Welcome! Your connection ID is {}",
                    ctx.connection_id()
                )),
            ],
            stream_ids: None,
        }));
    }

    async fn on_disconnected(&self, ctx: &HubContext) {
        info!("Client disconnected: {}", ctx.connection_id());
    }

    async fn on_invocation(
        &self,
        ctx: &HubContext,
        message: &InvocationMessage,
    ) -> ac_signalr_server::Result<Option<serde_json::Value>> {
        info!(
            "Received invocation from {}: {} with {} args",
            ctx.connection_id(),
            message.target,
            message.arguments.len()
        );

        match message.target.as_str() {
            "SendMessage" => {
                // Extract user and message from arguments
                if message.arguments.len() >= 2 {
                    let user = message.arguments[0].as_str().unwrap_or("Unknown");
                    let msg = message.arguments[1].as_str().unwrap_or("");
                    info!("Chat message from {}: {}", user, msg);

                    // Broadcast the message to all subscribers
                    let _ = self.tx.send((user.to_string(), msg.to_string()));

                    // Return confirmation instead of echoing the message
                    Ok(Some(json!({"status": "success"})))
                } else {
                    Ok(Some(
                        json!({"status": "error", "message": "Invalid arguments"}),
                    ))
                }
            }
            "GetConnectionId" => {
                // Return the connection ID
                Ok(Some(json!(ctx.connection_id().to_string())))
            }
            _ => {
                info!("Unknown method: {}", message.target);
                Ok(Some(
                    json!({"status": "error", "message": "Unknown method"}),
                ))
            }
        }
    }

    async fn on_stream_invocation(
        &self,
        ctx: &HubContext,
        message: &StreamInvocationMessage,
    ) -> ac_signalr_server::Result<()> {
        info!(
            "Received stream invocation from {}: {}",
            ctx.connection_id(),
            message.target
        );

        match message.target.as_str() {
            "ReceiveMessages" => {
                let invocation_id = message.invocation_id.clone();
                let ctx = ctx.clone();
                let mut rx = self.tx.subscribe();

                // Spawn a task to stream messages
                tokio::spawn(async move {
                    info!("Starting message stream for invocation: {}", invocation_id);

                    loop {
                        match rx.recv().await {
                            Ok((user, msg)) => {
                                // Send stream item
                                let stream_item = HubMessage::StreamItem(StreamItemMessage {
                                    invocation_id: invocation_id.clone(),
                                    item: json!({
                                        "user": user,
                                        "message": msg
                                    }),
                                });

                                if let Err(e) = ctx.send(stream_item) {
                                    info!("Failed to send stream item: {}", e);
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => {
                                // Client is too slow, continue
                                continue;
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                info!("Broadcast channel closed");
                                break;
                            }
                        }
                    }

                    // Send completion message
                    let completion = HubMessage::Completion(CompletionMessage {
                        invocation_id,
                        result: None,
                        error: None,
                    });
                    let _ = ctx.send(completion);
                    info!("Message stream completed");
                });

                Ok(())
            }
            _ => {
                info!("Unknown stream method: {}", message.target);
                Ok(())
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    let _ = tracing_log::LogTracer::init();
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    info!("Starting SignalR chat server...");

    // Create broadcast channel for chat messages
    let (tx, _rx) = broadcast::channel(100);

    // Create the SignalR server with our chat hub
    let server = SignalRServer::new(ChatHub { tx });

    // Create the Axum app
    let app = axum::Router::new().nest("/chat", server.into_router());

    // Start the server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("Failed to bind to port 3000");

    info!("Server listening on http://127.0.0.1:3000/chat");
    info!("Connect your SignalR client to ws://127.0.0.1:3000/chat");

    axum::serve(listener, app).await.expect("Server failed");
}

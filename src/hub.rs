//! Hub trait and context for `SignalR` hubs

use crate::connection::Connection;
use crate::connection::ConnectionId;
use crate::error::Result;
use crate::message::HubMessage;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;

/// Per-connection context passed to [`Hub`] callbacks.
///
/// The context exposes the current [`Connection`], its identifier, and an
/// outbound channel for sending server-to-client [`HubMessage`] values.
#[derive(Clone)]
pub struct HubContext {
    connection: Arc<Connection>,
    sender: OutboundSender,
    disconnect_tx: Option<watch::Sender<bool>>,
}

#[derive(Clone)]
enum OutboundSender {
    Unbounded(mpsc::UnboundedSender<HubMessage>),
    Bounded(mpsc::Sender<HubMessage>),
}

impl HubContext {
    /// Creates a new hub context from a connection and outbound sender.
    ///
    /// Most applications receive a `HubContext` from the runtime instead of
    /// constructing one directly. This constructor is mainly useful for tests
    /// and custom integration layers.
    #[must_use]
    pub fn new(connection: Arc<Connection>, sender: mpsc::UnboundedSender<HubMessage>) -> Self {
        Self {
            connection,
            sender: OutboundSender::Unbounded(sender),
            disconnect_tx: None,
        }
    }

    pub(crate) fn new_bounded(
        connection: Arc<Connection>,
        sender: mpsc::Sender<HubMessage>,
        disconnect_tx: watch::Sender<bool>,
    ) -> Self {
        Self {
            connection,
            sender: OutboundSender::Bounded(sender),
            disconnect_tx: Some(disconnect_tx),
        }
    }

    /// Returns the stable identifier for the current connection.
    #[must_use]
    pub fn connection_id(&self) -> ConnectionId {
        self.connection.id()
    }

    /// Returns the shared connection state for the current client.
    ///
    /// The [`Connection`] can be used to read or update per-connection metadata
    /// that should be available across hub callbacks.
    #[must_use]
    pub fn connection(&self) -> &Arc<Connection> {
        &self.connection
    }

    /// Queues a server-to-client `SignalR` message for the current connection.
    ///
    /// # Errors
    ///
    /// Returns [`crate::SignalRError::ConnectionClosed`] when the client has
    /// disconnected or the bounded outbound channel is full.
    pub fn send(&self, message: HubMessage) -> Result<()> {
        match &self.sender {
            OutboundSender::Unbounded(sender) => sender
                .send(message)
                .map_err(|_| crate::error::SignalRError::ConnectionClosed),
            OutboundSender::Bounded(sender) => match sender.try_send(message) {
                Ok(()) => Ok(()),
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    Err(crate::error::SignalRError::ConnectionClosed)
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if let Some(disconnect_tx) = &self.disconnect_tx {
                        disconnect_tx.send_replace(true);
                    }
                    Err(crate::error::SignalRError::ConnectionClosed)
                }
            },
        }
    }
}

/// Trait implemented by application hubs.
///
/// Hub values are cloned into connection-handling tasks, so shared state should
/// live behind cheap-to-clone handles such as `Arc<_>` or Tokio channel types.
#[async_trait]
pub trait Hub: Send + Sync + Clone + 'static {
    /// Called after a client successfully completes the `SignalR` handshake.
    async fn on_connected(&self, _ctx: &HubContext) {}

    /// Called after the connection loop finishes for a client.
    async fn on_disconnected(&self, _ctx: &HubContext) {}

    /// Handles a client-to-server invocation.
    ///
    /// If the client supplied an invocation ID, the returned value becomes the
    /// completion payload sent back to the client. Returning `Ok(None)` sends an
    /// empty completion for request/response calls and produces no reply for
    /// fire-and-forget invocations without an ID.
    async fn on_invocation(
        &self,
        _ctx: &HubContext,
        _message: &crate::message::InvocationMessage,
    ) -> Result<Option<serde_json::Value>> {
        Ok(None)
    }

    /// Handles a client stream invocation.
    ///
    /// Implementations typically spawn a task that emits one or more
    /// [`crate::StreamItemMessage`] values and then a final
    /// [`crate::CompletionMessage`] through [`HubContext::send`]. Returning an
    /// error automatically sends a failed completion for the invocation.
    async fn on_stream_invocation(
        &self,
        _ctx: &HubContext,
        _message: &crate::message::StreamInvocationMessage,
    ) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::pedantic, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;
    use crate::message::HubMessage;
    use crate::message::PingMessage;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_hub_context_creation() {
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let ctx = HubContext::new(connection.clone(), tx);
        let conn_id = ctx.connection_id();

        assert_eq!(conn_id, connection.id());
    }

    #[tokio::test]
    async fn test_hub_context_connection_id() {
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let ctx = HubContext::new(connection.clone(), tx);
        let id1 = ctx.connection_id();
        let id2 = ctx.connection_id();

        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn test_hub_context_connection() {
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let ctx = HubContext::new(connection.clone(), tx);
        let conn = ctx.connection();

        assert_eq!(conn.id(), connection.id());
    }

    #[tokio::test]
    async fn test_hub_context_send_message() {
        let connection = Arc::new(Connection::new());
        let (tx, mut rx) = mpsc::unbounded_channel();

        let ctx = HubContext::new(connection, tx);
        let msg = HubMessage::Ping(PingMessage {});

        let result = ctx.send(msg.clone());
        assert!(result.is_ok());

        // Verify message was sent
        let received = rx.recv().await;
        assert!(received.is_some());
    }

    #[tokio::test]
    async fn test_hub_context_send_closed_channel() {
        let connection = Arc::new(Connection::new());
        let (tx, rx) = mpsc::unbounded_channel();

        let ctx = HubContext::new(connection, tx);

        // Drop receiver to close channel
        drop(rx);

        let msg = HubMessage::Ping(PingMessage {});
        let result = ctx.send(msg);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            crate::error::SignalRError::ConnectionClosed
        ));
    }

    #[tokio::test]
    async fn test_bounded_hub_context_disconnects_when_full() {
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::channel(1);
        let (disconnect_tx, disconnect_rx) = watch::channel(false);
        let ctx = HubContext::new_bounded(connection, tx, disconnect_tx);

        assert!(ctx.send(HubMessage::Ping(PingMessage {})).is_ok());
        assert!(ctx.send(HubMessage::Ping(PingMessage {})).is_err());
        assert!(*disconnect_rx.borrow());
    }

    #[tokio::test]
    async fn test_hub_context_clone() {
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();

        let ctx1 = HubContext::new(connection, tx);
        let ctx2 = ctx1.clone();

        assert_eq!(ctx1.connection_id(), ctx2.connection_id());
    }

    // Test hub implementation
    #[derive(Clone)]
    struct TestHub;

    #[async_trait]
    impl Hub for TestHub {
        async fn on_connected(&self, _ctx: &HubContext) {
            // Default implementation test
        }

        async fn on_disconnected(&self, _ctx: &HubContext) {
            // Default implementation test
        }

        async fn on_invocation(
            &self,
            _ctx: &HubContext,
            message: &crate::message::InvocationMessage,
        ) -> Result<Option<serde_json::Value>> {
            if message.target == "Echo" {
                Ok(message.arguments.first().cloned())
            } else {
                Ok(None)
            }
        }
    }

    #[tokio::test]
    async fn test_hub_trait_implementation() {
        let hub = TestHub;
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = HubContext::new(connection, tx);

        // Test on_connected (should not panic)
        hub.on_connected(&ctx).await;

        // Test on_disconnected (should not panic)
        hub.on_disconnected(&ctx).await;

        // Test on_invocation
        let message = crate::message::InvocationMessage {
            invocation_id: Some("1".to_string()),
            target: "Echo".to_string(),
            arguments: vec![serde_json::json!("test")],
            stream_ids: None,
        };

        let result = hub.on_invocation(&ctx, &message).await;
        assert!(result.is_ok());
        let value = result.unwrap();
        assert!(value.is_some());
        assert_eq!(value.unwrap(), serde_json::json!("test"));
    }

    #[tokio::test]
    async fn test_hub_trait_unknown_method() {
        let hub = TestHub;
        let connection = Arc::new(Connection::new());
        let (tx, _rx) = mpsc::unbounded_channel();
        let ctx = HubContext::new(connection, tx);

        let message = crate::message::InvocationMessage {
            invocation_id: Some("1".to_string()),
            target: "UnknownMethod".to_string(),
            arguments: vec![],
            stream_ids: None,
        };

        let result = hub.on_invocation(&ctx, &message).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }
}

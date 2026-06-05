//! Connection management for SignalR

use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Unique identifier for a SignalR connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(Uuid);

impl ConnectionId {
    /// Creates a new random connection ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Returns the underlying UUID value.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for ConnectionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for ConnectionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Represents a connected SignalR client.
///
/// Each connection carries a unique ID and arbitrary JSON metadata that can be
/// shared across hub callbacks.
pub struct Connection {
    id: ConnectionId,
    metadata: Arc<RwLock<serde_json::Value>>,
}

impl Connection {
    /// Creates a new connection with an empty metadata payload.
    pub fn new() -> Self {
        Self {
            id: ConnectionId::new(),
            metadata: Arc::new(RwLock::new(serde_json::Value::Null)),
        }
    }

    /// Returns the connection ID.
    pub fn id(&self) -> ConnectionId {
        self.id
    }

    /// Clones the current metadata value for this connection.
    pub async fn metadata(&self) -> serde_json::Value {
        self.metadata.read().await.clone()
    }

    /// Replaces the metadata value for this connection.
    pub async fn set_metadata(&self, metadata: serde_json::Value) {
        *self.metadata.write().await = metadata;
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_id_creation() {
        let id1 = ConnectionId::new();
        let id2 = ConnectionId::new();
        assert_ne!(id1, id2, "Each connection ID should be unique");
    }

    #[test]
    fn test_connection_id_default() {
        let id1 = ConnectionId::default();
        let id2 = ConnectionId::default();
        assert_ne!(id1, id2, "Default connection IDs should be unique");
    }

    #[test]
    fn test_connection_id_display() {
        let id = ConnectionId::new();
        let display_str = format!("{}", id);
        assert!(!display_str.is_empty());
        // UUID format check (basic)
        assert!(display_str.contains('-'));
    }

    #[test]
    fn test_connection_id_as_uuid() {
        let id = ConnectionId::new();
        let uuid = id.as_uuid();
        assert_eq!(format!("{}", uuid), format!("{}", id));
    }

    #[test]
    fn test_connection_creation() {
        let conn = Connection::new();
        let id = conn.id();
        assert_eq!(format!("{}", id), format!("{}", conn.id()));
    }

    #[test]
    fn test_connection_default() {
        let conn1 = Connection::default();
        let conn2 = Connection::default();
        assert_ne!(
            conn1.id(),
            conn2.id(),
            "Each connection should have a unique ID"
        );
    }

    #[tokio::test]
    async fn test_connection_metadata() {
        let conn = Connection::new();

        // Initial metadata should be null
        let metadata = conn.metadata().await;
        assert_eq!(metadata, serde_json::Value::Null);

        // Set metadata
        let test_metadata = serde_json::json!({"key": "value", "count": 42});
        conn.set_metadata(test_metadata.clone()).await;

        // Retrieve and verify metadata
        let retrieved = conn.metadata().await;
        assert_eq!(retrieved, test_metadata);
    }

    #[tokio::test]
    async fn test_connection_metadata_update() {
        let conn = Connection::new();

        // Set initial metadata
        conn.set_metadata(serde_json::json!({"status": "initial"}))
            .await;

        // Update metadata
        conn.set_metadata(serde_json::json!({"status": "updated"}))
            .await;

        // Verify updated metadata
        let metadata = conn.metadata().await;
        assert_eq!(metadata["status"], "updated");
    }
}

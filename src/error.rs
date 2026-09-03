//! Error types for `SignalR` operations

use thiserror::Error;

/// Result type for `SignalR` operations
pub type Result<T> = std::result::Result<T, SignalRError>;

/// Errors that can occur during `SignalR` operations
#[derive(Debug, Error)]
pub enum SignalRError {
    /// WebSocket error
    #[error("WebSocket error: {0}")]
    WebSocket(#[from] fastwebsockets::WebSocketError),

    /// JSON serialization/deserialization error
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// `MessagePack` serialization/deserialization error
    #[error("MessagePack error: {0}")]
    MessagePack(#[from] rmp_serde::encode::Error),

    /// `MessagePack` deserialization error
    #[error("MessagePack decode error: {0}")]
    MessagePackDecode(#[from] rmp_serde::decode::Error),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Invalid server configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),

    /// Connection closed
    #[error("Connection closed")]
    ConnectionClosed,

    /// Invalid handshake
    #[error("Invalid handshake: {0}")]
    InvalidHandshake(String),

    /// Unsupported protocol
    #[error("Unsupported protocol: {0}")]
    UnsupportedProtocol(String),
}

#[cfg(test)]
#[allow(clippy::pedantic, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_error() {
        let error = SignalRError::Protocol("test error".to_string());
        assert_eq!(format!("{}", error), "Protocol error: test error");
    }

    #[test]
    fn test_invalid_configuration_error() {
        let error = SignalRError::InvalidConfiguration("capacity must be positive".to_string());
        assert_eq!(
            format!("{error}"),
            "Invalid configuration: capacity must be positive"
        );
    }

    #[test]
    fn test_connection_closed_error() {
        let error = SignalRError::ConnectionClosed;
        assert_eq!(format!("{}", error), "Connection closed");
    }

    #[test]
    fn test_invalid_handshake_error() {
        let error = SignalRError::InvalidHandshake("missing field".to_string());
        assert_eq!(format!("{}", error), "Invalid handshake: missing field");
    }

    #[test]
    fn test_unsupported_protocol_error() {
        let error = SignalRError::UnsupportedProtocol("msgpack".to_string());
        assert_eq!(format!("{}", error), "Unsupported protocol: msgpack");
    }

    #[test]
    fn test_json_error_conversion() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json");
        assert!(json_error.is_err());

        let signalr_error: SignalRError = json_error.unwrap_err().into();
        assert!(matches!(signalr_error, SignalRError::Json(_)));
    }

    #[test]
    fn test_io_error_conversion() {
        use std::io;
        let io_error = io::Error::other("test io error");
        let signalr_error: SignalRError = io_error.into();
        assert!(matches!(signalr_error, SignalRError::Io(_)));
    }
}

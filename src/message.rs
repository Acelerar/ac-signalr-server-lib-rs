//! SignalR protocol messages

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

/// The type of SignalR message
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Invocation message (type 1)
    #[serde(rename = "1")]
    Invocation = 1,
    /// Stream item message (type 2)
    #[serde(rename = "2")]
    StreamItem = 2,
    /// Completion message (type 3)
    #[serde(rename = "3")]
    Completion = 3,
    /// Stream invocation message (type 4)
    #[serde(rename = "4")]
    StreamInvocation = 4,
    /// Cancel invocation message (type 5)
    #[serde(rename = "5")]
    CancelInvocation = 5,
    /// Ping message (type 6)
    #[serde(rename = "6")]
    Ping = 6,
    /// Close message (type 7)
    #[serde(rename = "7")]
    Close = 7,
}

/// A SignalR hub message
#[derive(Debug, Clone)]
pub enum HubMessage {
    /// Invocation message
    Invocation(InvocationMessage),
    /// Stream item message
    StreamItem(StreamItemMessage),
    /// Completion message
    Completion(CompletionMessage),
    /// Stream invocation message
    StreamInvocation(StreamInvocationMessage),
    /// Cancel invocation message
    CancelInvocation(CancelInvocationMessage),
    /// Ping message
    Ping(PingMessage),
    /// Close message
    Close(CloseMessage),
}

/// Invocation message - calls a method on the hub
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InvocationMessage {
    /// Optional invocation ID for request-response pattern
    #[serde(skip_serializing_if = "Option::is_none")]
    pub invocation_id: Option<String>,
    /// The name of the target method
    pub target: String,
    /// The arguments to pass to the method
    #[serde(default)]
    pub arguments: Vec<Value>,
    /// Optional stream IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_ids: Option<Vec<String>>,
}

/// Stream item message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamItemMessage {
    /// The invocation ID
    pub invocation_id: String,
    /// The item being streamed
    pub item: Value,
}

/// Completion message - indicates the end of an invocation
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompletionMessage {
    /// The invocation ID
    pub invocation_id: String,
    /// The result of the invocation (if successful)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    /// The error message (if failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Stream invocation message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamInvocationMessage {
    /// The invocation ID
    pub invocation_id: String,
    /// The name of the target method
    pub target: String,
    /// The arguments to pass to the method
    #[serde(default)]
    pub arguments: Vec<Value>,
    /// Optional stream IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_ids: Option<Vec<String>>,
}

/// Cancel invocation message
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CancelInvocationMessage {
    /// The invocation ID to cancel
    pub invocation_id: String,
}

/// Ping message - keeps the connection alive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingMessage {}

/// Close message - gracefully closes the connection
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CloseMessage {
    /// Optional error message
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Whether to allow reconnection
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_reconnect: Option<bool>,
}

impl HubMessage {
    /// Get the invocation ID if present
    pub fn invocation_id(&self) -> Option<&str> {
        match self {
            HubMessage::Invocation(msg) => msg.invocation_id.as_deref(),
            HubMessage::StreamItem(msg) => Some(&msg.invocation_id),
            HubMessage::Completion(msg) => Some(&msg.invocation_id),
            HubMessage::StreamInvocation(msg) => Some(&msg.invocation_id),
            HubMessage::CancelInvocation(msg) => Some(&msg.invocation_id),
            _ => None,
        }
    }
}

// Custom serialization and deserialization for HubMessage to support both
// integer and string type values (for compatibility with different SignalR clients)
impl Serialize for HubMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;

        match self {
            HubMessage::Invocation(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &1)?;
                if let Some(ref inv_id) = msg.invocation_id {
                    map.serialize_entry("invocationId", inv_id)?;
                }
                map.serialize_entry("target", &msg.target)?;
                map.serialize_entry("arguments", &msg.arguments)?;
                if let Some(stream_ids) = &msg.stream_ids {
                    map.serialize_entry("streamIds", stream_ids)?;
                }
                map.end()
            }
            HubMessage::StreamItem(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &2)?;
                map.serialize_entry("invocationId", &msg.invocation_id)?;
                map.serialize_entry("item", &msg.item)?;
                map.end()
            }
            HubMessage::Completion(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &3)?;
                map.serialize_entry("invocationId", &msg.invocation_id)?;
                if let Some(error) = &msg.error {
                    map.serialize_entry("error", error)?;
                }
                if let Some(result) = &msg.result {
                    map.serialize_entry("result", result)?;
                }
                map.end()
            }
            HubMessage::StreamInvocation(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &4)?;
                map.serialize_entry("invocationId", &msg.invocation_id)?;
                map.serialize_entry("target", &msg.target)?;
                map.serialize_entry("arguments", &msg.arguments)?;
                if let Some(stream_ids) = &msg.stream_ids {
                    map.serialize_entry("streamIds", stream_ids)?;
                }
                map.end()
            }
            HubMessage::CancelInvocation(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &5)?;
                map.serialize_entry("invocationId", &msg.invocation_id)?;
                map.end()
            }
            HubMessage::Ping(_) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &6)?;
                map.end()
            }
            HubMessage::Close(msg) => {
                let mut map = serializer.serialize_map(None)?;
                map.serialize_entry("type", &7)?;
                if let Some(error) = &msg.error {
                    map.serialize_entry("error", error)?;
                }
                if msg.allow_reconnect.is_some() {
                    map.serialize_entry("allowReconnect", &msg.allow_reconnect)?;
                }
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for HubMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let obj = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("Expected object"))?;

        // Get the type field - support both integer and string
        let type_value = obj
            .get("type")
            .ok_or_else(|| serde::de::Error::missing_field("type"))?;
        let message_type = if let Some(n) = type_value.as_i64() {
            n as u8
        } else if let Some(s) = type_value.as_str() {
            s.parse::<u8>()
                .map_err(|_| serde::de::Error::custom("Invalid type value"))?
        } else {
            return Err(serde::de::Error::custom("Type must be a number or string"));
        };

        match message_type {
            1 => {
                let msg: InvocationMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::Invocation(msg))
            }
            2 => {
                let msg: StreamItemMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::StreamItem(msg))
            }
            3 => {
                let msg: CompletionMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::Completion(msg))
            }
            4 => {
                let msg: StreamInvocationMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::StreamInvocation(msg))
            }
            5 => {
                let msg: CancelInvocationMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::CancelInvocation(msg))
            }
            6 => {
                let msg: PingMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::Ping(msg))
            }
            7 => {
                let msg: CloseMessage =
                    serde_json::from_value(value).map_err(serde::de::Error::custom)?;
                Ok(HubMessage::Close(msg))
            }
            _ => Err(serde::de::Error::custom(format!(
                "Unknown message type: {}",
                message_type
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_invocation_message_serialization() {
        let msg = InvocationMessage {
            invocation_id: Some("1".to_string()),
            target: "SendMessage".to_string(),
            arguments: vec![json!("user"), json!("hello")],
            stream_ids: None,
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("SendMessage"));
        assert!(serialized.contains("invocationId"));
    }

    #[test]
    fn test_invocation_message_deserialization() {
        let json_str =
            r#"{"invocationId":"1","target":"SendMessage","arguments":["user","hello"]}"#;
        let msg: InvocationMessage = serde_json::from_str(json_str).unwrap();

        assert_eq!(msg.invocation_id, Some("1".to_string()));
        assert_eq!(msg.target, "SendMessage");
        assert_eq!(msg.arguments.len(), 2);
    }

    #[test]
    fn test_invocation_message_without_id() {
        let msg = InvocationMessage {
            invocation_id: None,
            target: "Notify".to_string(),
            arguments: vec![json!("notification")],
            stream_ids: None,
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(!serialized.contains("invocationId"));
    }

    #[test]
    fn test_completion_message_with_result() {
        let msg = CompletionMessage {
            invocation_id: "123".to_string(),
            result: Some(json!({"status": "success"})),
            error: None,
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("123"));
        assert!(serialized.contains("success"));
    }

    #[test]
    fn test_completion_message_with_error() {
        let msg = CompletionMessage {
            invocation_id: "123".to_string(),
            result: None,
            error: Some("Operation failed".to_string()),
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("Operation failed"));
    }

    #[test]
    fn test_ping_message() {
        let msg = PingMessage {};
        let serialized = serde_json::to_string(&msg).unwrap();
        assert_eq!(serialized, "{}");
    }

    #[test]
    fn test_close_message_with_error() {
        let msg = CloseMessage {
            error: Some("Server shutting down".to_string()),
            allow_reconnect: Some(true),
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("Server shutting down"));
    }

    #[test]
    fn test_close_message_without_error() {
        let msg = CloseMessage {
            error: None,
            allow_reconnect: None,
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        // Should not contain error field when None
        assert!(!serialized.contains("error") || serialized == "{}");
    }

    #[test]
    fn test_hub_message_invocation() {
        let inv = InvocationMessage {
            invocation_id: Some("1".to_string()),
            target: "Test".to_string(),
            arguments: vec![],
            stream_ids: None,
        };
        let msg = HubMessage::Invocation(inv);

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":1"#));
    }

    #[test]
    fn test_hub_message_completion() {
        let comp = CompletionMessage {
            invocation_id: "2".to_string(),
            result: Some(json!("ok")),
            error: None,
        };
        let msg = HubMessage::Completion(comp);

        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":3"#));
    }

    #[test]
    fn test_hub_message_ping() {
        let msg = HubMessage::Ping(PingMessage {});
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":6"#));
    }

    #[test]
    fn test_hub_message_close() {
        let msg = HubMessage::Close(CloseMessage {
            error: None,
            allow_reconnect: None,
        });
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains(r#""type":7"#));
    }

    #[test]
    fn test_hub_message_invocation_id_extraction() {
        let msg = HubMessage::Invocation(InvocationMessage {
            invocation_id: Some("test-id".to_string()),
            target: "Test".to_string(),
            arguments: vec![],
            stream_ids: None,
        });

        assert_eq!(msg.invocation_id(), Some("test-id"));
    }

    #[test]
    fn test_hub_message_invocation_id_none() {
        let msg = HubMessage::Ping(PingMessage {});
        assert_eq!(msg.invocation_id(), None);
    }

    #[test]
    fn test_stream_item_message() {
        let msg = StreamItemMessage {
            invocation_id: "stream-1".to_string(),
            item: json!({"data": "chunk"}),
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("stream-1"));
        assert!(serialized.contains("chunk"));
    }

    #[test]
    fn test_stream_invocation_message() {
        let msg = StreamInvocationMessage {
            invocation_id: "stream-inv-1".to_string(),
            target: "StreamData".to_string(),
            arguments: vec![json!(100)],
            stream_ids: Some(vec!["s1".to_string()]),
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("stream-inv-1"));
        assert!(serialized.contains("StreamData"));
    }

    #[test]
    fn test_cancel_invocation_message() {
        let msg = CancelInvocationMessage {
            invocation_id: "cancel-1".to_string(),
        };

        let serialized = serde_json::to_string(&msg).unwrap();
        assert!(serialized.contains("cancel-1"));
    }

    #[test]
    fn test_hub_message_roundtrip() {
        let original = HubMessage::Invocation(InvocationMessage {
            invocation_id: Some("roundtrip".to_string()),
            target: "TestMethod".to_string(),
            arguments: vec![json!(42), json!("test")],
            stream_ids: None,
        });

        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: HubMessage = serde_json::from_str(&serialized).unwrap();

        match deserialized {
            HubMessage::Invocation(msg) => {
                assert_eq!(msg.invocation_id, Some("roundtrip".to_string()));
                assert_eq!(msg.target, "TestMethod");
                assert_eq!(msg.arguments.len(), 2);
            }
            _ => panic!("Expected Invocation message"),
        }
    }

    #[test]
    fn test_message_type_values() {
        assert_eq!(MessageType::Invocation as i32, 1);
        assert_eq!(MessageType::StreamItem as i32, 2);
        assert_eq!(MessageType::Completion as i32, 3);
        assert_eq!(MessageType::StreamInvocation as i32, 4);
        assert_eq!(MessageType::CancelInvocation as i32, 5);
        assert_eq!(MessageType::Ping as i32, 6);
        assert_eq!(MessageType::Close as i32, 7);
    }
}

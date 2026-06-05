//! SignalR protocol handling (handshake, negotiation)

use crate::error::Result;
use crate::error::SignalRError;
use crate::message::CancelInvocationMessage;
use crate::message::CloseMessage;
use crate::message::CompletionMessage;
use crate::message::HubMessage;
use crate::message::InvocationMessage;
use crate::message::PingMessage;
use crate::message::StreamInvocationMessage;
use crate::message::StreamItemMessage;
use rmpv::Value as MessagePackValue;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Map;
use serde_json::Number;
use serde_json::Value;
use std::io::Cursor;
use std::str::FromStr;

const MESSAGEPACK_MAX_FRAME_LENGTH: usize = 0x7fff_ffff;
const MESSAGEPACK_MAX_PREFIX_BYTES: usize = 5;
const MESSAGEPACK_RESULT_KIND_ERROR: u64 = 1;
const MESSAGEPACK_RESULT_KIND_VOID: u64 = 2;
const MESSAGEPACK_RESULT_KIND_NON_VOID: u64 = 3;

/// Protocol type for SignalR communication
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// JSON protocol
    Json,
    /// MessagePack protocol
    MessagePack,
}

impl Protocol {
    /// Parse protocol from string (returns Option for convenience)
    pub fn parse(s: &str) -> Option<Self> {
        s.parse().ok()
    }

    /// Get protocol name as string
    pub fn as_str(&self) -> &str {
        match self {
            Protocol::Json => "json",
            Protocol::MessagePack => "messagepack",
        }
    }
}

impl FromStr for Protocol {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Protocol::Json),
            "messagepack" => Ok(Protocol::MessagePack),
            _ => Err(format!("Unknown protocol: {}", s)),
        }
    }
}

/// SignalR handshake request
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeRequest {
    /// The protocol to use (e.g., "json")
    pub protocol: String,
    /// The protocol version
    pub version: i32,
}

/// SignalR handshake response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HandshakeResponse {
    /// Error message if handshake failed
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl HandshakeResponse {
    /// Create a successful handshake response
    pub fn success() -> Self {
        Self { error: None }
    }

    /// Create a failed handshake response
    pub fn error(message: impl Into<String>) -> Self {
        Self {
            error: Some(message.into()),
        }
    }
}

/// Parse a handshake request from a JSON string
pub fn parse_handshake(data: &str) -> Result<HandshakeRequest> {
    // SignalR handshake messages end with the record separator (0x1E)
    let data = data.trim_end_matches('\u{001e}');
    serde_json::from_str(data).map_err(SignalRError::Json)
}

/// Serialize a handshake response to a JSON string with record separator
pub fn serialize_handshake(response: &HandshakeResponse) -> Result<String> {
    let json = serde_json::to_string(response)?;
    Ok(format!("{}\u{001e}", json))
}

/// Parse a SignalR message from JSON
pub fn parse_message(data: &str) -> Result<HubMessage> {
    // SignalR messages end with the record separator (0x1E)
    let data = data.trim_end_matches('\u{001e}');
    serde_json::from_str(data).map_err(SignalRError::Json)
}

/// Serialize a SignalR message to JSON with record separator
pub fn serialize_message(message: &HubMessage) -> Result<String> {
    let json = serde_json::to_string(message)?;
    Ok(format!("{}\u{001e}", json))
}

/// Parse a single SignalR message from MessagePack binary framing.
pub fn parse_message_messagepack(data: &[u8]) -> Result<HubMessage> {
    let messages = parse_messages_messagepack(data)?;
    let mut messages = messages.into_iter();
    let message = messages
        .next()
        .ok_or_else(|| SignalRError::Protocol("No MessagePack message found".to_string()))?;

    if messages.next().is_some() {
        return Err(SignalRError::Protocol(
            "Expected a single MessagePack message".to_string(),
        ));
    }

    Ok(message)
}

/// Serialize a SignalR message to MessagePack binary framing.
pub fn serialize_message_messagepack(message: &HubMessage) -> Result<Vec<u8>> {
    let body = serialize_messagepack_body(message)?;
    frame_messagepack_payload(&body)
}

pub(crate) fn parse_messages_messagepack(data: &[u8]) -> Result<Vec<HubMessage>> {
    split_messagepack_frames(data)?
        .into_iter()
        .map(parse_messagepack_body)
        .collect()
}

fn serialize_messagepack_body(message: &HubMessage) -> Result<Vec<u8>> {
    let value = message_to_messagepack_value(message)?;
    let mut bytes = Vec::new();
    rmpv::encode::write_value(&mut bytes, &value)
        .map_err(|error| SignalRError::Protocol(format!("MessagePack encode error: {}", error)))?;
    Ok(bytes)
}

fn parse_messagepack_body(data: &[u8]) -> Result<HubMessage> {
    let mut cursor = Cursor::new(data);
    let value = rmpv::decode::read_value(&mut cursor)
        .map_err(|error| SignalRError::Protocol(format!("MessagePack decode error: {}", error)))?;

    if cursor.position() != data.len() as u64 {
        return Err(SignalRError::Protocol(
            "MessagePack message contained trailing bytes".to_string(),
        ));
    }

    messagepack_value_to_message(value)
}

fn frame_messagepack_payload(payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MESSAGEPACK_MAX_FRAME_LENGTH {
        return Err(SignalRError::Protocol(
            "MessagePack payload exceeds maximum frame length".to_string(),
        ));
    }

    let mut framed = Vec::with_capacity(MESSAGEPACK_MAX_PREFIX_BYTES + payload.len());
    write_varint_length(payload.len(), &mut framed)?;
    framed.extend_from_slice(payload);
    Ok(framed)
}

fn split_messagepack_frames(data: &[u8]) -> Result<Vec<&[u8]>> {
    let mut frames = Vec::new();
    let mut offset = 0;

    while offset < data.len() {
        let (length, prefix_len) = read_varint_length(&data[offset..])?;
        offset += prefix_len;

        let end = offset.checked_add(length).ok_or_else(|| {
            SignalRError::Protocol("MessagePack frame length overflow".to_string())
        })?;

        if end > data.len() {
            return Err(SignalRError::Protocol(
                "MessagePack frame length exceeds available data".to_string(),
            ));
        }

        frames.push(&data[offset..end]);
        offset = end;
    }

    Ok(frames)
}

fn write_varint_length(mut length: usize, output: &mut Vec<u8>) -> Result<()> {
    if length > MESSAGEPACK_MAX_FRAME_LENGTH {
        return Err(SignalRError::Protocol(
            "MessagePack payload exceeds maximum frame length".to_string(),
        ));
    }

    loop {
        let mut byte = (length & 0x7f) as u8;
        length >>= 7;

        if length > 0 {
            byte |= 0x80;
        }

        output.push(byte);

        if length == 0 {
            return Ok(());
        }
    }
}

fn read_varint_length(data: &[u8]) -> Result<(usize, usize)> {
    let mut length = 0usize;

    for (index, byte) in data
        .iter()
        .copied()
        .enumerate()
        .take(MESSAGEPACK_MAX_PREFIX_BYTES)
    {
        let value = usize::from(byte & 0x7f);
        length |= value << (index * 7);

        if byte & 0x80 == 0 {
            if length > MESSAGEPACK_MAX_FRAME_LENGTH {
                return Err(SignalRError::Protocol(
                    "MessagePack frame length exceeds maximum".to_string(),
                ));
            }

            return Ok((length, index + 1));
        }
    }

    if data.len() < MESSAGEPACK_MAX_PREFIX_BYTES {
        return Err(SignalRError::Protocol(
            "Incomplete MessagePack frame length prefix".to_string(),
        ));
    }

    Err(SignalRError::Protocol(
        "MessagePack frame length prefix is too large".to_string(),
    ))
}

fn message_to_messagepack_value(message: &HubMessage) -> Result<MessagePackValue> {
    let value = match message {
        HubMessage::Invocation(message) => MessagePackValue::Array(vec![
            MessagePackValue::from(1),
            empty_messagepack_headers(),
            optional_string_to_messagepack(message.invocation_id.as_deref()),
            MessagePackValue::from(message.target.clone()),
            json_array_to_messagepack(&message.arguments)?,
            string_array_to_messagepack(message.stream_ids.as_deref()),
        ]),
        HubMessage::StreamItem(message) => MessagePackValue::Array(vec![
            MessagePackValue::from(2),
            empty_messagepack_headers(),
            MessagePackValue::from(message.invocation_id.clone()),
            json_to_messagepack_value(&message.item)?,
        ]),
        HubMessage::Completion(message) => completion_to_messagepack(message)?,
        HubMessage::StreamInvocation(message) => MessagePackValue::Array(vec![
            MessagePackValue::from(4),
            empty_messagepack_headers(),
            MessagePackValue::from(message.invocation_id.clone()),
            MessagePackValue::from(message.target.clone()),
            json_array_to_messagepack(&message.arguments)?,
            string_array_to_messagepack(message.stream_ids.as_deref()),
        ]),
        HubMessage::CancelInvocation(message) => MessagePackValue::Array(vec![
            MessagePackValue::from(5),
            empty_messagepack_headers(),
            MessagePackValue::from(message.invocation_id.clone()),
        ]),
        HubMessage::Ping(_) => MessagePackValue::Array(vec![MessagePackValue::from(6)]),
        HubMessage::Close(message) => close_to_messagepack(message),
    };

    Ok(value)
}

fn completion_to_messagepack(message: &CompletionMessage) -> Result<MessagePackValue> {
    if message.error.is_some() && message.result.is_some() {
        return Err(SignalRError::Protocol(
            "Completion message cannot contain both result and error".to_string(),
        ));
    }

    let mut items = vec![
        MessagePackValue::from(3),
        empty_messagepack_headers(),
        MessagePackValue::from(message.invocation_id.clone()),
    ];

    if let Some(error) = &message.error {
        items.push(MessagePackValue::from(MESSAGEPACK_RESULT_KIND_ERROR));
        items.push(MessagePackValue::from(error.clone()));
    } else if let Some(result) = &message.result {
        items.push(MessagePackValue::from(MESSAGEPACK_RESULT_KIND_NON_VOID));
        items.push(json_to_messagepack_value(result)?);
    } else {
        items.push(MessagePackValue::from(MESSAGEPACK_RESULT_KIND_VOID));
    }

    Ok(MessagePackValue::Array(items))
}

fn close_to_messagepack(message: &CloseMessage) -> MessagePackValue {
    let mut items = vec![
        MessagePackValue::from(7),
        optional_string_to_messagepack(message.error.as_deref()),
    ];

    if let Some(allow_reconnect) = message.allow_reconnect {
        items.push(MessagePackValue::from(allow_reconnect));
    }

    MessagePackValue::Array(items)
}

fn messagepack_value_to_message(value: MessagePackValue) -> Result<HubMessage> {
    let items = match value {
        MessagePackValue::Array(items) => items,
        _ => {
            return Err(SignalRError::Protocol(
                "MessagePack hub message must be an array".to_string(),
            ));
        }
    };

    let message_type = messagepack_item_as_u64(&items, 0, "message type")?;
    match message_type {
        1 => parse_messagepack_invocation(items),
        2 => parse_messagepack_stream_item(items),
        3 => parse_messagepack_completion(items),
        4 => parse_messagepack_stream_invocation(items),
        5 => parse_messagepack_cancel_invocation(items),
        6 => parse_messagepack_ping(items),
        7 => parse_messagepack_close(items),
        _ => Err(SignalRError::Protocol(format!(
            "Unknown MessagePack message type: {}",
            message_type
        ))),
    }
}

fn parse_messagepack_invocation(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 5, 6, "Invocation")?;
    validate_messagepack_headers(&items, 1)?;

    Ok(HubMessage::Invocation(InvocationMessage {
        invocation_id: optional_messagepack_string(&items[2], "invocationId")?,
        target: messagepack_item_as_string(&items, 3, "target")?,
        arguments: messagepack_item_as_json_array(&items, 4, "arguments")?,
        stream_ids: optional_messagepack_string_array(&items, 5, "streamIds")?,
    }))
}

fn parse_messagepack_stream_item(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 4, 4, "StreamItem")?;
    validate_messagepack_headers(&items, 1)?;

    Ok(HubMessage::StreamItem(StreamItemMessage {
        invocation_id: messagepack_item_as_string(&items, 2, "invocationId")?,
        item: messagepack_to_json_value(items[3].clone())?,
    }))
}

fn parse_messagepack_completion(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 4, 5, "Completion")?;
    validate_messagepack_headers(&items, 1)?;

    let invocation_id = messagepack_item_as_string(&items, 2, "invocationId")?;
    let result_kind = messagepack_item_as_u64(&items, 3, "resultKind")?;
    let (result, error) = match result_kind {
        MESSAGEPACK_RESULT_KIND_ERROR => {
            validate_messagepack_item_count(&items, 5, 5, "Completion error")?;
            (None, Some(messagepack_item_as_string(&items, 4, "error")?))
        }
        MESSAGEPACK_RESULT_KIND_VOID => {
            validate_messagepack_item_count(&items, 4, 4, "Completion void")?;
            (None, None)
        }
        MESSAGEPACK_RESULT_KIND_NON_VOID => {
            validate_messagepack_item_count(&items, 5, 5, "Completion result")?;
            (Some(messagepack_to_json_value(items[4].clone())?), None)
        }
        _ => {
            return Err(SignalRError::Protocol(format!(
                "Invalid MessagePack completion result kind: {}",
                result_kind
            )));
        }
    };

    Ok(HubMessage::Completion(CompletionMessage {
        invocation_id,
        result,
        error,
    }))
}

fn parse_messagepack_stream_invocation(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 5, 6, "StreamInvocation")?;
    validate_messagepack_headers(&items, 1)?;

    Ok(HubMessage::StreamInvocation(StreamInvocationMessage {
        invocation_id: messagepack_item_as_string(&items, 2, "invocationId")?,
        target: messagepack_item_as_string(&items, 3, "target")?,
        arguments: messagepack_item_as_json_array(&items, 4, "arguments")?,
        stream_ids: optional_messagepack_string_array(&items, 5, "streamIds")?,
    }))
}

fn parse_messagepack_cancel_invocation(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 3, 3, "CancelInvocation")?;
    validate_messagepack_headers(&items, 1)?;

    Ok(HubMessage::CancelInvocation(CancelInvocationMessage {
        invocation_id: messagepack_item_as_string(&items, 2, "invocationId")?,
    }))
}

fn parse_messagepack_ping(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 1, 1, "Ping")?;
    Ok(HubMessage::Ping(PingMessage {}))
}

fn parse_messagepack_close(items: Vec<MessagePackValue>) -> Result<HubMessage> {
    validate_messagepack_item_count(&items, 1, 3, "Close")?;

    Ok(HubMessage::Close(CloseMessage {
        error: optional_messagepack_item_as_string(&items, 1, "error")?,
        allow_reconnect: optional_messagepack_item_as_bool(&items, 2, "allowReconnect")?,
    }))
}

fn empty_messagepack_headers() -> MessagePackValue {
    MessagePackValue::Map(Vec::new())
}

fn optional_string_to_messagepack(value: Option<&str>) -> MessagePackValue {
    value
        .map(MessagePackValue::from)
        .unwrap_or(MessagePackValue::Nil)
}

fn string_array_to_messagepack(values: Option<&[String]>) -> MessagePackValue {
    let values = values
        .unwrap_or(&[])
        .iter()
        .map(|value| MessagePackValue::from(value.clone()))
        .collect();

    MessagePackValue::Array(values)
}

fn json_array_to_messagepack(values: &[Value]) -> Result<MessagePackValue> {
    values
        .iter()
        .map(json_to_messagepack_value)
        .collect::<Result<Vec<_>>>()
        .map(MessagePackValue::Array)
}

fn json_to_messagepack_value(value: &Value) -> Result<MessagePackValue> {
    let value = match value {
        Value::Null => MessagePackValue::Nil,
        Value::Bool(value) => MessagePackValue::from(*value),
        Value::Number(value) => json_number_to_messagepack(value)?,
        Value::String(value) => MessagePackValue::from(value.clone()),
        Value::Array(values) => json_array_to_messagepack(values)?,
        Value::Object(values) => {
            let values = values
                .iter()
                .map(|(key, value)| {
                    Ok((
                        MessagePackValue::from(key.clone()),
                        json_to_messagepack_value(value)?,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            MessagePackValue::Map(values)
        }
    };

    Ok(value)
}

fn json_number_to_messagepack(number: &Number) -> Result<MessagePackValue> {
    if let Some(value) = number.as_i64() {
        Ok(MessagePackValue::from(value))
    } else if let Some(value) = number.as_u64() {
        Ok(MessagePackValue::from(value))
    } else if let Some(value) = number.as_f64() {
        Ok(MessagePackValue::from(value))
    } else {
        Err(SignalRError::Protocol(
            "Unsupported JSON number for MessagePack encoding".to_string(),
        ))
    }
}

fn messagepack_to_json_value(value: MessagePackValue) -> Result<Value> {
    match value {
        MessagePackValue::Nil => Ok(Value::Null),
        MessagePackValue::Boolean(value) => Ok(Value::Bool(value)),
        MessagePackValue::Integer(value) => messagepack_integer_to_json(value),
        MessagePackValue::F32(value) => messagepack_float_to_json(f64::from(value)),
        MessagePackValue::F64(value) => messagepack_float_to_json(value),
        MessagePackValue::String(value) => value
            .as_str()
            .map(|value| Value::String(value.to_string()))
            .ok_or_else(|| SignalRError::Protocol("MessagePack string is not UTF-8".to_string())),
        MessagePackValue::Binary(values) => Ok(Value::Array(
            values
                .into_iter()
                .map(|value| Value::Number(Number::from(value)))
                .collect(),
        )),
        MessagePackValue::Array(values) => values
            .into_iter()
            .map(messagepack_to_json_value)
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        MessagePackValue::Map(values) => messagepack_map_to_json(values),
        MessagePackValue::Ext(_, _) => Err(SignalRError::Protocol(
            "MessagePack extension values are not supported".to_string(),
        )),
    }
}

fn messagepack_integer_to_json(value: rmpv::Integer) -> Result<Value> {
    if let Some(value) = value.as_i64() {
        Ok(Value::Number(Number::from(value)))
    } else if let Some(value) = value.as_u64() {
        Ok(Value::Number(Number::from(value)))
    } else {
        Err(SignalRError::Protocol(
            "MessagePack integer exceeds JSON number range".to_string(),
        ))
    }
}

fn messagepack_float_to_json(value: f64) -> Result<Value> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| SignalRError::Protocol("MessagePack float is not finite".to_string()))
}

fn messagepack_map_to_json(values: Vec<(MessagePackValue, MessagePackValue)>) -> Result<Value> {
    let mut map = Map::new();

    for (key, value) in values {
        let key = key.as_str().ok_or_else(|| {
            SignalRError::Protocol("MessagePack map keys must be UTF-8 strings".to_string())
        })?;
        map.insert(key.to_string(), messagepack_to_json_value(value)?);
    }

    Ok(Value::Object(map))
}

fn validate_messagepack_headers(items: &[MessagePackValue], index: usize) -> Result<()> {
    match items.get(index) {
        Some(MessagePackValue::Map(headers)) => {
            for (key, value) in headers {
                if key.as_str().is_none() || value.as_str().is_none() {
                    return Err(SignalRError::Protocol(
                        "MessagePack headers must be string key-value pairs".to_string(),
                    ));
                }
            }

            Ok(())
        }
        _ => Err(SignalRError::Protocol(
            "MessagePack headers must be a map".to_string(),
        )),
    }
}

fn validate_messagepack_item_count(
    items: &[MessagePackValue],
    min: usize,
    max: usize,
    message_name: &str,
) -> Result<()> {
    if items.len() < min || items.len() > max {
        return Err(SignalRError::Protocol(format!(
            "{} MessagePack message has {} items; expected {}{}",
            message_name,
            items.len(),
            min,
            if min == max {
                String::new()
            } else {
                format!("-{}", max)
            }
        )));
    }

    Ok(())
}

fn messagepack_item_as_u64(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<u64> {
    items
        .get(index)
        .and_then(MessagePackValue::as_u64)
        .ok_or_else(|| {
            SignalRError::Protocol(format!(
                "MessagePack field '{}' must be an integer",
                field_name
            ))
        })
}

fn messagepack_item_as_string(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<String> {
    items
        .get(index)
        .and_then(MessagePackValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            SignalRError::Protocol(format!(
                "MessagePack field '{}' must be a string",
                field_name
            ))
        })
}

fn optional_messagepack_item_as_string(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<Option<String>> {
    match items.get(index) {
        Some(MessagePackValue::Nil) | None => Ok(None),
        Some(_) => messagepack_item_as_string(items, index, field_name).map(Some),
    }
}

fn optional_messagepack_item_as_bool(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<Option<bool>> {
    match items.get(index) {
        Some(MessagePackValue::Nil) | None => Ok(None),
        Some(MessagePackValue::Boolean(value)) => Ok(Some(*value)),
        Some(_) => Err(SignalRError::Protocol(format!(
            "MessagePack field '{}' must be a boolean",
            field_name
        ))),
    }
}

fn optional_messagepack_string(
    value: &MessagePackValue,
    field_name: &str,
) -> Result<Option<String>> {
    match value {
        MessagePackValue::Nil => Ok(None),
        _ => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                SignalRError::Protocol(format!(
                    "MessagePack field '{}' must be a string",
                    field_name
                ))
            }),
    }
}

fn messagepack_item_as_json_array(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<Vec<Value>> {
    match items.get(index) {
        Some(MessagePackValue::Array(values)) => values
            .iter()
            .cloned()
            .map(messagepack_to_json_value)
            .collect::<Result<Vec<_>>>(),
        _ => Err(SignalRError::Protocol(format!(
            "MessagePack field '{}' must be an array",
            field_name
        ))),
    }
}

fn optional_messagepack_string_array(
    items: &[MessagePackValue],
    index: usize,
    field_name: &str,
) -> Result<Option<Vec<String>>> {
    match items.get(index) {
        None | Some(MessagePackValue::Nil) => Ok(None),
        Some(MessagePackValue::Array(values)) if values.is_empty() => Ok(None),
        Some(MessagePackValue::Array(values)) => values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    SignalRError::Protocol(format!(
                        "MessagePack field '{}' must contain only strings",
                        field_name
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        _ => Err(SignalRError::Protocol(format!(
            "MessagePack field '{}' must be an array",
            field_name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::CompletionMessage;
    use crate::message::HubMessage;
    use crate::message::InvocationMessage;
    use crate::message::PingMessage;
    use serde_json::json;

    #[test]
    fn test_parse_handshake() {
        let data = r#"{"protocol":"json","version":1}"#;
        let req = parse_handshake(&format!("{}\u{001e}", data)).unwrap();
        assert_eq!(req.protocol, "json");
        assert_eq!(req.version, 1);
    }

    #[test]
    fn test_parse_handshake_without_separator() {
        let data = r#"{"protocol":"json","version":1}"#;
        let req = parse_handshake(data).unwrap();
        assert_eq!(req.protocol, "json");
        assert_eq!(req.version, 1);
    }

    #[test]
    fn test_parse_handshake_invalid_json() {
        let data = r#"{"protocol":"json""#;
        let result = parse_handshake(data);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), SignalRError::Json(_)));
    }

    #[test]
    fn test_serialize_handshake() {
        let response = HandshakeResponse::success();
        let json = serialize_handshake(&response).unwrap();
        assert!(json.ends_with('\u{001e}'));
        assert!(json.contains(r#"{"error":null}"#) || json.contains(r#"{}"#));
    }

    #[test]
    fn test_serialize_handshake_with_error() {
        let response = HandshakeResponse::error("Protocol not supported");
        let json = serialize_handshake(&response).unwrap();
        assert!(json.ends_with('\u{001e}'));
        assert!(json.contains("Protocol not supported"));
    }

    #[test]
    fn test_handshake_response_success() {
        let response = HandshakeResponse::success();
        assert!(response.error.is_none());
    }

    #[test]
    fn test_handshake_response_error() {
        let response = HandshakeResponse::error("test error");
        assert_eq!(response.error, Some("test error".to_string()));
    }

    #[test]
    fn test_parse_message_invocation() {
        let data =
            r#"{"type":1,"invocationId":"1","target":"SendMessage","arguments":["user","hello"]}"#;
        let msg = parse_message(&format!("{}\u{001e}", data)).unwrap();

        match msg {
            HubMessage::Invocation(inv) => {
                assert_eq!(inv.invocation_id, Some("1".to_string()));
                assert_eq!(inv.target, "SendMessage");
                assert_eq!(inv.arguments.len(), 2);
            }
            _ => panic!("Expected Invocation message"),
        }
    }

    #[test]
    fn test_parse_message_completion() {
        let data = r#"{"type":3,"invocationId":"1","result":"success"}"#;
        let msg = parse_message(&format!("{}\u{001e}", data)).unwrap();

        match msg {
            HubMessage::Completion(comp) => {
                assert_eq!(comp.invocation_id, "1");
                assert!(comp.result.is_some());
            }
            _ => panic!("Expected Completion message"),
        }
    }

    #[test]
    fn test_parse_message_ping() {
        let data = r#"{"type":6}"#;
        let msg = parse_message(&format!("{}\u{001e}", data)).unwrap();
        assert!(matches!(msg, HubMessage::Ping(_)));
    }

    #[test]
    fn test_parse_message_without_separator() {
        let data = r#"{"type":6}"#;
        let msg = parse_message(data).unwrap();
        assert!(matches!(msg, HubMessage::Ping(_)));
    }

    #[test]
    fn test_parse_message_invalid_json() {
        let data = r#"{"type":1"#;
        let result = parse_message(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_message_invocation() {
        let msg = HubMessage::Invocation(InvocationMessage {
            invocation_id: Some("test".to_string()),
            target: "Method".to_string(),
            arguments: vec![json!("arg1")],
            stream_ids: None,
        });

        let serialized = serialize_message(&msg).unwrap();
        assert!(serialized.ends_with('\u{001e}'));
        assert!(serialized.contains("Method"));
        assert!(serialized.contains(r#""type":1"#));
    }

    #[test]
    fn test_serialize_message_completion() {
        let msg = HubMessage::Completion(CompletionMessage {
            invocation_id: "test".to_string(),
            result: Some(json!({"status": "ok"})),
            error: None,
        });

        let serialized = serialize_message(&msg).unwrap();
        assert!(serialized.ends_with('\u{001e}'));
        assert!(serialized.contains(r#""type":3"#));
    }

    #[test]
    fn test_serialize_message_ping() {
        let msg = HubMessage::Ping(PingMessage {});
        let serialized = serialize_message(&msg).unwrap();
        assert!(serialized.ends_with('\u{001e}'));
        assert!(serialized.contains(r#""type":6"#));
    }

    #[test]
    fn test_message_roundtrip() {
        let original = HubMessage::Invocation(InvocationMessage {
            invocation_id: Some("roundtrip".to_string()),
            target: "TestMethod".to_string(),
            arguments: vec![json!(1), json!("test")],
            stream_ids: None,
        });

        let serialized = serialize_message(&original).unwrap();
        let deserialized = parse_message(&serialized).unwrap();

        match deserialized {
            HubMessage::Invocation(msg) => {
                assert_eq!(msg.invocation_id, Some("roundtrip".to_string()));
                assert_eq!(msg.target, "TestMethod");
            }
            _ => panic!("Expected Invocation message"),
        }
    }

    #[test]
    fn test_handshake_request_serialization() {
        let req = HandshakeRequest {
            protocol: "json".to_string(),
            version: 1,
        };

        let serialized = serde_json::to_string(&req).unwrap();
        assert!(serialized.contains("json"));
        assert!(serialized.contains("1"));
    }

    #[test]
    fn test_handshake_request_deserialization() {
        let json_str = r#"{"protocol":"json","version":1}"#;
        let req: HandshakeRequest = serde_json::from_str(json_str).unwrap();

        assert_eq!(req.protocol, "json");
        assert_eq!(req.version, 1);
    }

    #[test]
    fn test_protocol_from_str() {
        assert_eq!(Protocol::parse("json"), Some(Protocol::Json));
        assert_eq!(Protocol::parse("JSON"), Some(Protocol::Json));
        assert_eq!(Protocol::parse("messagepack"), Some(Protocol::MessagePack));
        assert_eq!(Protocol::parse("MessagePack"), Some(Protocol::MessagePack));
        assert_eq!(Protocol::parse("unknown"), None);
    }

    #[test]
    fn test_protocol_as_str() {
        assert_eq!(Protocol::Json.as_str(), "json");
        assert_eq!(Protocol::MessagePack.as_str(), "messagepack");
    }

    #[test]
    fn test_parse_message_messagepack_invocation() {
        let body = vec![
            0x96, 0x01, 0x80, 0xa3, b'x', b'y', b'z', 0xa6, b'm', b'e', b't', b'h', b'o', b'd',
            0x91, 0x2a, 0x90,
        ];
        let serialized = frame_messagepack_payload(&body).unwrap();
        let deserialized = parse_message_messagepack(&serialized).unwrap();

        match deserialized {
            HubMessage::Invocation(inv) => {
                assert_eq!(inv.invocation_id, Some("xyz".to_string()));
                assert_eq!(inv.target, "method");
                assert_eq!(inv.arguments, vec![json!(42)]);
                assert_eq!(inv.stream_ids, None);
            }
            _ => panic!("Expected Invocation message"),
        }
    }

    #[test]
    fn test_serialize_message_messagepack_uses_binary_length_prefix() {
        let msg = HubMessage::Ping(PingMessage {});
        let serialized = serialize_message_messagepack(&msg).unwrap();
        assert_eq!(serialized, vec![0x02, 0x91, 0x06]);
    }

    #[test]
    fn test_messagepack_roundtrip() {
        let original = HubMessage::Completion(CompletionMessage {
            invocation_id: "test-123".to_string(),
            result: Some(json!({"status": "success", "value": 42})),
            error: None,
        });

        let serialized = serialize_message_messagepack(&original).unwrap();
        let deserialized = parse_message_messagepack(&serialized).unwrap();

        match deserialized {
            HubMessage::Completion(comp) => {
                assert_eq!(comp.invocation_id, "test-123");
                assert!(comp.result.is_some());
                assert!(comp.error.is_none());
            }
            _ => panic!("Expected Completion message"),
        }
    }

    #[test]
    fn test_messagepack_roundtrip_mixed_arguments() {
        let expected_arguments = vec![
            json!(42),
            json!(42.5),
            json!(true),
            json!(["nested", 7]),
            json!({"name": "value"}),
        ];
        let original = HubMessage::Invocation(InvocationMessage {
            invocation_id: Some("mixed".to_string()),
            target: "Send".to_string(),
            arguments: expected_arguments.clone(),
            stream_ids: Some(vec!["stream-1".to_string()]),
        });

        let serialized = serialize_message_messagepack(&original).unwrap();
        let deserialized = parse_message_messagepack(&serialized).unwrap();

        match deserialized {
            HubMessage::Invocation(message) => {
                assert_eq!(message.invocation_id, Some("mixed".to_string()));
                assert_eq!(message.target, "Send");
                assert_eq!(message.arguments, expected_arguments);
                assert_eq!(message.stream_ids, Some(vec!["stream-1".to_string()]));
            }
            _ => panic!("Expected Invocation message"),
        }
    }

    #[test]
    fn test_messagepack_completion_error_decoding() {
        let body = vec![
            0x95, 0x03, 0x80, 0xa3, b'x', b'y', b'z', 0x01, 0xa5, b'E', b'r', b'r', b'o', b'r',
        ];
        let serialized = frame_messagepack_payload(&body).unwrap();
        let deserialized = parse_message_messagepack(&serialized).unwrap();

        match deserialized {
            HubMessage::Completion(message) => {
                assert_eq!(message.invocation_id, "xyz");
                assert_eq!(message.result, None);
                assert_eq!(message.error, Some("Error".to_string()));
            }
            _ => panic!("Expected Completion message"),
        }
    }

    #[test]
    fn test_messagepack_multiple_frames() {
        let ping = serialize_message_messagepack(&HubMessage::Ping(PingMessage {})).unwrap();
        let completion =
            serialize_message_messagepack(&HubMessage::Completion(CompletionMessage {
                invocation_id: "done".to_string(),
                result: None,
                error: None,
            }))
            .unwrap();

        let mut combined = ping;
        combined.extend(completion);

        let messages = parse_messages_messagepack(&combined).unwrap();
        assert_eq!(messages.len(), 2);
        assert!(matches!(messages[0], HubMessage::Ping(_)));
        assert!(matches!(messages[1], HubMessage::Completion(_)));
    }

    #[test]
    fn test_parse_message_messagepack_rejects_multiple_frames() {
        let ping = serialize_message_messagepack(&HubMessage::Ping(PingMessage {})).unwrap();
        let mut combined = ping.clone();
        combined.extend(ping);

        let result = parse_message_messagepack(&combined);
        assert!(matches!(result, Err(SignalRError::Protocol(_))));
    }

    #[test]
    fn test_parse_message_messagepack_rejects_unframed_payload() {
        let result = parse_message_messagepack(&[0x91, 0x06]);
        assert!(matches!(result, Err(SignalRError::Protocol(_))));
    }

    #[test]
    fn test_messagepack_incomplete_frame_is_error() {
        let result = parse_message_messagepack(&[0x03, 0x91, 0x06]);
        assert!(matches!(result, Err(SignalRError::Protocol(_))));
    }

    #[test]
    fn test_messagepack_binary_value_maps_to_json_array() {
        let value = MessagePackValue::Array(vec![
            MessagePackValue::from(2),
            MessagePackValue::Map(Vec::new()),
            MessagePackValue::from("stream"),
            MessagePackValue::Binary(vec![1, 2, 3]),
        ]);
        let mut body = Vec::new();
        rmpv::encode::write_value(&mut body, &value).unwrap();
        let framed = frame_messagepack_payload(&body).unwrap();

        let deserialized = parse_message_messagepack(&framed).unwrap();
        match deserialized {
            HubMessage::StreamItem(message) => {
                assert_eq!(message.item, json!([1, 2, 3]));
            }
            _ => panic!("Expected StreamItem message"),
        }
    }
}

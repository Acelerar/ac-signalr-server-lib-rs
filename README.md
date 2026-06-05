# tt-signalr-server

`tt-signalr-server` is a Rust library for hosting SignalR-compatible hubs on top of
[Axum](https://github.com/tokio-rs/axum) and
[fastwebsockets](https://github.com/denoland/fastwebsockets). It handles the
SignalR negotiation endpoint, WebSocket transport, hub dispatch, and JSON or
MessagePack framing so application code can focus on hub behavior.

## Features

- Hub-oriented API through the `Hub` trait
- Axum router integration with a single `SignalRServer::into_router()` call
- JSON and MessagePack SignalR protocols
- Request/response hub invocations and server-to-client messages
- Server streaming support through `on_stream_invocation`
- Automatic keepalive pings for idle connections

## Installation

Add the crate and the usual async web dependencies:

```toml
[dependencies]
async-trait = "0.1"
axum = "0.8"
serde_json = "1"
tokio = { version = "1", features = ["macros", "net", "rt-multi-thread"] }
tt-signalr-server = "0.1.0"
```

## Documentation

- API reference: <https://docs.rs/tt-signalr-server>
- Runnable examples: `examples/chat_server.rs` and `examples/trade_server.rs`
- Interop client for manual testing: `clients/dotnet-chat-client/`

## Quick start

```rust,no_run
use async_trait::async_trait;
use axum::Router;
use serde_json::json;
use tt_signalr_server::Hub;
use tt_signalr_server::HubContext;
use tt_signalr_server::HubMessage;
use tt_signalr_server::InvocationMessage;
use tt_signalr_server::SignalRServer;

#[derive(Clone)]
struct ChatHub;

#[async_trait]
impl Hub for ChatHub {
    async fn on_connected(&self, ctx: &HubContext) {
        let _ = ctx.send(HubMessage::Invocation(InvocationMessage {
            invocation_id: None,
            target: "ReceiveMessage".to_string(),
            arguments: vec![
                json!("Server"),
                json!(format!("Welcome {}", ctx.connection_id())),
            ],
            stream_ids: None,
        }));
    }

    async fn on_invocation(
        &self,
        _ctx: &HubContext,
        message: &InvocationMessage,
    ) -> tt_signalr_server::Result<Option<serde_json::Value>> {
        match message.target.as_str() {
            "Ping" => Ok(Some(json!("pong"))),
            _ => Ok(None),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = Router::new().nest("/chat", SignalRServer::new(ChatHub).into_router());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;

    axum::serve(listener, app).await?;
    Ok(())
}
```

## Routing model

When you mount the router under a path such as `/chat`:

- `GET` and `POST /chat/negotiate` serve the SignalR negotiation endpoint
- `GET /chat/` serves the WebSocket transport endpoint

SignalR clients should connect to the hub base path (`/chat` in the example
above). The client library handles negotiation before opening the WebSocket.

## Protocol support

The crate currently supports:

- JSON hub messages
- MessagePack hub messages
- SignalR handshakes and completion messages
- Client-to-server invocations
- Server-to-client invocations
- Server streaming
- Connection lifecycle callbacks

## Observability

The server emits diagnostics via [`tracing`](https://docs.rs/tracing).  
Initialize your subscriber in the host application, for example:

```rust
let _ = tracing_log::LogTracer::init();
let _ = tracing_subscriber::fmt()
    .with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
    )
    .try_init();
```

## Examples

Run the included chat server:

```bash
cargo run --example chat_server
```

Run the multi-hub trade server:

```bash
cargo run --example trade_server
```

The repository also includes a .NET chat client under
`clients/dotnet-chat-client/` for end-to-end interoperability testing.

## License

Licensed under either `MIT` or `Apache-2.0`, at your option.

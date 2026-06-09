//! Trade server example with market data and notifications
//!
//! This example demonstrates a trade server with two hubs:
//! - MarketHub: Streams bars, trades, and symbols
//! - UserHub: Streams notifications
//!
//! Run this example with:
//! ```sh
//! cargo run --example trade_server
//! ```
//!
//! Then connect with SignalR clients:
//! - Market hub: http://localhost:5000/hubs/market
//! - User hub: http://localhost:5000/hubs/user
#![allow(clippy::pedantic, clippy::unwrap_used, clippy::indexing_slicing)]

use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use serde_json::json;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio::time::Duration;
use tower_http::cors::CorsLayer;
use tracing::info;

fn random_index(len: usize) -> usize {
    (rand::random::<u64>() % len as u64) as usize
}
use ac_signalr_server::Hub;
use ac_signalr_server::HubContext;
use ac_signalr_server::HubMessage;
use ac_signalr_server::InvocationMessage;
use ac_signalr_server::SignalRServer;

// Data structures matching .NET representations

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bar {
    pub timestamp: String, // ISO 8601 format for DateTime
    pub open: String,      // decimal as string for precision
    pub high: String,
    pub low: String,
    pub close: String,
    pub volume: i64,
    pub symbol: String,
    pub timeframe: i32, // in minutes
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    pub id: String,
    pub timestamp: String, // ISO 8601 format for DateTime
    pub symbol: String,
    pub side: String,     // "Buy" or "Sell"
    pub price: String,    // decimal as string for precision
    pub quantity: String, // decimal as string for precision
    pub is_our_trade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    pub id: String,
    pub timestamp: String, // ISO 8601 format for DateTime
    pub message: String,
    #[serde(rename = "Type")]
    pub notification_type: String, // "info", "success", "warning", "error"
}

// Market Hub - streams bars, trades, and symbols
#[derive(Clone)]
struct MarketHub {
    bar_tx: broadcast::Sender<Bar>,
    trade_tx: broadcast::Sender<Trade>,
    symbols_tx: broadcast::Sender<Vec<String>>,
}

#[async_trait]
impl Hub for MarketHub {
    async fn on_connected(&self, ctx: &HubContext) {
        info!("Market hub - Client connected: {}", ctx.connection_id());

        // Start streaming bars
        let ctx_bars = ctx.clone();
        let mut bar_rx = self.bar_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match bar_rx.recv().await {
                    Ok(bar) => {
                        let message = HubMessage::Invocation(InvocationMessage {
                            invocation_id: None,
                            target: "ReceiveBar".to_string(),
                            arguments: vec![json!(bar)],
                            stream_ids: None,
                        });
                        if ctx_bars.send(message).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Start streaming trades
        let ctx_trades = ctx.clone();
        let mut trade_rx = self.trade_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match trade_rx.recv().await {
                    Ok(trade) => {
                        let message = HubMessage::Invocation(InvocationMessage {
                            invocation_id: None,
                            target: "ReceiveTrade".to_string(),
                            arguments: vec![json!(trade)],
                            stream_ids: None,
                        });
                        if ctx_trades.send(message).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

        // Start streaming symbols
        let ctx_symbols = ctx.clone();
        let mut symbols_rx = self.symbols_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match symbols_rx.recv().await {
                    Ok(symbols) => {
                        let message = HubMessage::Invocation(InvocationMessage {
                            invocation_id: None,
                            target: "ReceiveSymbols".to_string(),
                            arguments: vec![json!(symbols)],
                            stream_ids: None,
                        });
                        if ctx_symbols.send(message).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn on_disconnected(&self, ctx: &HubContext) {
        info!("Market hub - Client disconnected: {}", ctx.connection_id());
    }
}

// User Hub - streams notifications
#[derive(Clone)]
struct UserHub {
    notification_tx: broadcast::Sender<Notification>,
}

#[async_trait]
impl Hub for UserHub {
    async fn on_connected(&self, ctx: &HubContext) {
        info!("User hub - Client connected: {}", ctx.connection_id());

        // Start streaming notifications
        let ctx_notifications = ctx.clone();
        let mut notification_rx = self.notification_tx.subscribe();
        tokio::spawn(async move {
            loop {
                match notification_rx.recv().await {
                    Ok(notification) => {
                        let message = HubMessage::Invocation(InvocationMessage {
                            invocation_id: None,
                            target: "ReceiveNotification".to_string(),
                            arguments: vec![json!(notification)],
                            stream_ids: None,
                        });
                        if ctx_notifications.send(message).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn on_disconnected(&self, ctx: &HubContext) {
        info!("User hub - Client disconnected: {}", ctx.connection_id());
    }
}

// Replay Hub - stub for future replay functionality
#[derive(Clone)]
struct ReplayHub;

#[async_trait]
impl Hub for ReplayHub {
    async fn on_connected(&self, ctx: &HubContext) {
        info!("Replay hub - Client connected: {}", ctx.connection_id());
    }

    async fn on_disconnected(&self, ctx: &HubContext) {
        info!("Replay hub - Client disconnected: {}", ctx.connection_id());
    }
}

// Backtesting Hub - stub for future backtesting functionality
#[derive(Clone)]
struct BacktestingHub;

#[async_trait]
impl Hub for BacktestingHub {
    async fn on_connected(&self, ctx: &HubContext) {
        info!(
            "Backtesting hub - Client connected: {}",
            ctx.connection_id()
        );
    }

    async fn on_disconnected(&self, ctx: &HubContext) {
        info!(
            "Backtesting hub - Client disconnected: {}",
            ctx.connection_id()
        );
    }
}

// Random data generator functions
fn generate_random_bar(symbol: &str) -> Bar {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    // Generate random price around 100.0
    let base_price = 100.0 + (rand::random::<f64>() - 0.5) * 20.0;
    let open = format!("{:.2}", base_price);
    let high = format!("{:.2}", base_price + rand::random::<f64>() * 2.0);
    let low = format!("{:.2}", base_price - rand::random::<f64>() * 2.0);
    let close = format!("{:.2}", base_price + (rand::random::<f64>() - 0.5) * 2.0);

    Bar {
        timestamp: chrono::DateTime::from_timestamp(now.as_secs() as i64, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string(),
        open,
        high,
        low,
        close,
        volume: (rand::random::<u32>() % 10000 + 1000) as i64,
        symbol: symbol.to_string(),
        timeframe: 1,
    }
}

fn generate_random_trade(symbol: &str) -> Trade {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    let price = 100.0 + (rand::random::<f64>() - 0.5) * 20.0;
    let quantity = rand::random::<f64>() * 10.0 + 0.1;
    let side = if rand::random::<bool>() {
        "Buy"
    } else {
        "Sell"
    };

    Trade {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::DateTime::from_timestamp(now.as_secs() as i64, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string(),
        symbol: symbol.to_string(),
        side: side.to_string(),
        price: format!("{:.2}", price),
        quantity: format!("{:.4}", quantity),
        is_our_trade: rand::random::<bool>(),
    }
}

fn generate_random_notification() -> Notification {
    use std::time::SystemTime;

    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap();

    let types = ["info", "success", "warning", "error"];
    let messages = [
        "Trade executed successfully",
        "Market data updated",
        "Connection established",
        "Warning: High volatility detected",
        "Error: Order rejected",
        "Information: New symbol added",
    ];

    let notification_type = types[random_index(types.len())];
    let message = messages[random_index(messages.len())];

    Notification {
        id: uuid::Uuid::new_v4().to_string(),
        timestamp: chrono::DateTime::from_timestamp(now.as_secs() as i64, 0)
            .unwrap()
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
            .to_string(),
        message: message.to_string(),
        notification_type: notification_type.to_string(),
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

    info!("Starting SignalR trade server...");

    // Create broadcast channels
    let (bar_tx, _) = broadcast::channel(100);
    let (trade_tx, _) = broadcast::channel(100);
    let (symbols_tx, _) = broadcast::channel(100);
    let (notification_tx, _) = broadcast::channel(100);

    // Clone senders for the data generator task
    let bar_tx_clone = bar_tx.clone();
    let trade_tx_clone = trade_tx.clone();
    let symbols_tx_clone = symbols_tx.clone();
    let notification_tx_clone = notification_tx.clone();

    // Spawn task to generate random data
    tokio::spawn(async move {
        let symbols = vec![
            "BTCUSD".to_string(),
            "ETHUSD".to_string(),
            "ADAUSD".to_string(),
        ];

        // Send initial symbols
        let _ = symbols_tx_clone.send(symbols.clone());

        let mut bar_interval = interval(Duration::from_secs(2));
        let mut trade_interval = interval(Duration::from_secs(1));
        let mut notification_interval = interval(Duration::from_secs(5));
        let mut symbols_interval = interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = bar_interval.tick() => {
                    // Generate a bar for a random symbol
                    let symbol = &symbols[random_index(symbols.len())];
                    let bar = generate_random_bar(symbol);
                    let _ = bar_tx_clone.send(bar);
                }
                _ = trade_interval.tick() => {
                    // Generate a trade for a random symbol
                    let symbol = &symbols[random_index(symbols.len())];
                    let trade = generate_random_trade(symbol);
                    let _ = trade_tx_clone.send(trade);
                }
                _ = notification_interval.tick() => {
                    // Generate a random notification
                    let notification = generate_random_notification();
                    let _ = notification_tx_clone.send(notification);
                }
                _ = symbols_interval.tick() => {
                    // Re-send symbols list periodically
                    let _ = symbols_tx_clone.send(symbols.clone());
                }
            }
        }
    });

    // Create the SignalR servers for each hub
    let market_server = SignalRServer::new(MarketHub {
        bar_tx,
        trade_tx,
        symbols_tx,
    });

    let user_server = SignalRServer::new(UserHub { notification_tx });

    let replay_server = SignalRServer::new(ReplayHub);
    let backtesting_server = SignalRServer::new(BacktestingHub);

    // Configure CORS to allow all origins, methods, and headers (permissive for development)
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::mirror_request())
        .allow_methods(tower_http::cors::AllowMethods::mirror_request())
        .allow_headers(tower_http::cors::AllowHeaders::mirror_request())
        .allow_credentials(true);

    // Create the Axum app with all hubs and CORS
    let app = axum::Router::new()
        .nest("/hubs/market", market_server.into_router())
        .nest("/hubs/user", user_server.into_router())
        .nest("/hubs/replay", replay_server.into_router())
        .nest("/hubs/backtesting", backtesting_server.into_router())
        .layer(cors);

    // Start the server on port 5000
    let listener = tokio::net::TcpListener::bind("127.0.0.1:5000")
        .await
        .expect("Failed to bind to port 5000");

    info!("Server listening on http://127.0.0.1:5000");
    info!("Connect your SignalR client to:");
    info!("  - Market hub: ws://127.0.0.1:5000/hubs/market");
    info!("  - User hub: ws://127.0.0.1:5000/hubs/user");
    info!("  - Replay hub: ws://127.0.0.1:5000/hubs/replay");
    info!("  - Backtesting hub: ws://127.0.0.1:5000/hubs/backtesting");

    axum::serve(listener, app).await.expect("Server failed");
}

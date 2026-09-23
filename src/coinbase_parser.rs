use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};

use crate::{
    NormalizedOrderBookUpdate, PriceLevel,
    ws_connect::{Client, ServerEvent},
};

// Internal Serde models for Coinbase JSON variants
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum CoinbaseMessage {
    // Initial Snapshot Frame
    Snapshot {
        product_id: String,
        bids: Vec<(String, String)>, // [price, qty]
        asks: Vec<(String, String)>, // [price, qty]
    },
    // Subsequent Delta Update Frame
    #[serde(rename = "l2update")]
    L2Update {
        product_id: String,
        changes: Vec<(String, String, String)>, // [side ("buy"/"sell"), price, qty]
        time: String,
    },
    // Subscription Acknowledgment
    Subscriptions {
        channels: Vec<Value>,
    },
}

pub async fn coinbase_parser_task(
    client: Client,
    mut raw_events: broadcast::Receiver<ServerEvent>,
    kafka_tx: mpsc::Sender<NormalizedOrderBookUpdate>,
    streams: Vec<String>,
) {
    while let Ok(event) = raw_events.recv().await {
        match event {
            ServerEvent::Connected => {
                tracing::info!(
                    "Connected to Coinbase WebSocket. Resetting state and subscribing..."
                );

                let sub_payload = serde_json::json!({
                    "type": "subscribe",
                    "product_ids": streams,
                    "channels": ["level2_batch"]
                });

                if let Err(e) = client.send(sub_payload.to_string()).await {
                    tracing::error!("Failed to send subscription payload: {}", e);
                }
            }

            ServerEvent::Message(text) => {
                // Attempt to deserialize into tagged enum
                let msg: CoinbaseMessage = match serde_json::from_str(&text) {
                    Ok(parsed) => parsed,
                    Err(_) => continue,
                };

                match msg {
                    CoinbaseMessage::Snapshot {
                        product_id,
                        bids,
                        asks,
                    } => {
                        let normalized_bids = parse_level_pairs(&bids);
                        let normalized_asks = parse_level_pairs(&asks);

                        let update = NormalizedOrderBookUpdate {
                            exchange: "Coinbase".to_string(),
                            symbol: product_id,
                            exchange_event_timestamp: Utc::now(),
                            received_timestamp: Utc::now(),
                            bids: normalized_bids,
                            asks: normalized_asks,
                            is_snapshot: true,
                        };

                        if let Err(e) = kafka_tx.send(update).await {
                            tracing::error!("Failed to dispatch snapshot update to OBM: {:?}", e);
                        }
                    }

                    CoinbaseMessage::L2Update {
                        product_id,
                        changes,
                        time,
                    } => {
                        let mut bids = Vec::new();
                        let mut asks = Vec::new();

                        // Separate Coinbase's mixed "changes" array into bids and asks
                        for (side, price_str, qty_str) in changes {
                            let price = match Decimal::from_str(&price_str) {
                                Ok(p) => p,
                                Err(_) => continue,
                            };
                            let qty = match Decimal::from_str(&qty_str) {
                                Ok(q) => q,
                                Err(_) => continue,
                            };

                            let level = PriceLevel { price, qty };

                            match side.as_str() {
                                "buy" => bids.push(level),
                                "sell" => asks.push(level),
                                _ => {}
                            }
                        }

                        let exchange_event_timestamp = DateTime::parse_from_rfc3339(&time)
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now());

                        let normalised = NormalizedOrderBookUpdate {
                            exchange: "Coinbase".to_string(),
                            symbol: product_id,
                            exchange_event_timestamp,
                            received_timestamp: Utc::now(),
                            bids,
                            asks,
                            is_snapshot: false,
                        };

                        if let Err(e) = kafka_tx.send(normalised).await {
                            tracing::error!("Failed to dispatch L2 update to OBM: {:?}", e);
                        }
                    }

                    CoinbaseMessage::Subscriptions { .. } => {
                        tracing::info!("Coinbase subscription acknowledged by server.");
                    }
                }
            }

            ServerEvent::Disconnected { reason } => {
                tracing::error!("Coinbase Disconnected: {}", reason);
            }
        }
    }
}

// Helper to convert string price/qty tuples into PriceLevel structs
fn parse_level_pairs(pairs: &[(String, String)]) -> Vec<PriceLevel> {
    pairs
        .iter()
        .filter_map(|(p, q)| {
            let price = Decimal::from_str(p).ok()?;
            let qty = Decimal::from_str(q).ok()?;
            Some(PriceLevel { price, qty })
        })
        .collect()
}

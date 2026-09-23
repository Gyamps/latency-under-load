use std::{collections::HashMap, str::FromStr};

use chrono::{TimeZone, Utc};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::Value;

use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    NormalizedOrderBookUpdate, PriceLevel,
    ws_connect::{Client, ServerEvent},
};

// Binance `depthUpdate` payload
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BinanceDepthUpdate {
    #[serde(rename = "e")]
    event_type: String,

    #[serde(rename = "E")]
    event_time: u64,

    #[serde(rename = "T", default)]
    transaction_time: Option<u64>,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "ps", default)]
    pair_symbol: Option<String>,

    #[serde(rename = "U")]
    first_update_id: u64,

    #[serde(rename = "u")]
    final_update_id: u64,

    #[serde(rename = "pu", default)]
    prev_final_update_id: Option<u64>,

    #[serde(rename = "b")]
    bids: Vec<(String, String)>,

    #[serde(rename = "a")]
    asks: Vec<(String, String)>,

    #[serde(rename = "st", default)]
    symbol_type: Option<u8>,
}

enum SymbolSyncState {
    Syncing {
        buffer: Vec<BinanceDepthUpdate>,
        snapshot_rx: oneshot::Receiver<anyhow::Result<BinanceRestSnapshot>>,
    },
    Synced {
        last_u: u64,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct BinanceRestSnapshot {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    #[serde(rename = "T", default)]
    pub transaction_time: Option<u64>,
    pub bids: Vec<(String, String)>,
    pub asks: Vec<(String, String)>,
}

pub async fn binance_parser_task(
    client: Client,
    mut raw_events: broadcast::Receiver<ServerEvent>,
    kafka_tx: mpsc::Sender<NormalizedOrderBookUpdate>,
    streams: Vec<String>,
) {
    let mut buffer: Vec<BinanceDepthUpdate> = Vec::new();
    let mut symbol_states: HashMap<String, SymbolSyncState> = HashMap::new();

    while let Ok(event) = raw_events.recv().await {
        match event {
            ServerEvent::Connected => {
                tracing::info!(
                    "Socket connected. Resetting per-symbol state and sending subscription payload..."
                );
                symbol_states.clear();
                buffer.clear();

                let sub_payload = serde_json::json!({
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": rand::random::<u32>()
                });
                let _ = client.send(sub_payload.to_string()).await;
            }

            ServerEvent::Message(text) => {
                if text.contains("\"id\":") && !text.contains("\"e\":") {
                    if let Ok(json) = serde_json::from_str::<Value>(&text) {
                        // Subscription acknowledgement?
                        if let Some(id) = json.get("id").and_then(|i| i.as_str()) {
                            tracing::info!("Binance subscription acknowledged: {}", id);
                        }
                        continue;
                    }
                }

                let update = match serde_json::from_str::<BinanceDepthUpdate>(&text) {
                    Ok(u) => u,
                    Err(e) => {
                        tracing::error!("Failed to parse WebSocket depth update: {}", e);
                        continue;
                    }
                };

                let raw_symbol = update.symbol.clone();
                let state = symbol_states.entry(raw_symbol.clone()).or_insert_with(|| {
                    tracing::info!("Fetching REST depth snapshot for symbol {}...", raw_symbol);
                    let (tx, rx) = oneshot::channel();
                    let sym = raw_symbol.clone();
                    tokio::spawn(async move {
                        let res = fetch_rest_snapshot(&sym).await;
                        let _ = tx.send(res);
                    });
                    SymbolSyncState::Syncing {
                        buffer: Vec::new(),
                        snapshot_rx: rx,
                    }
                });

                match state {
                    SymbolSyncState::Syncing {
                        buffer,
                        snapshot_rx,
                    } => {
                        buffer.push(update);

                        match snapshot_rx.try_recv() {
                            Ok(Ok(snapshot)) => {
                                let last_update_id = snapshot.last_update_id;
                                buffer.retain(|e| e.final_update_id > last_update_id);

                                let target_id = last_update_id + 1;
                                let start_idx = buffer.iter().position(|e| {
                                    e.first_update_id <= target_id && e.final_update_id >= target_id
                                });

                                if let Some(idx) = start_idx {
                                    let valid_events = buffer.drain(idx..).collect::<Vec<_>>();

                                    // Emit initial REST snapshot to kafka
                                    let (snap_bids, snap_asks) =
                                        match parse_bids_asks(&snapshot.bids, &snapshot.asks) {
                                            Ok(pairs) => pairs,
                                            Err(e) => {
                                                tracing::error!(
                                                    "Failed to parse REST snapshot bids/asks: {}",
                                                    e
                                                );
                                                continue;
                                            }
                                        };

                                    let snapshot_update = NormalizedOrderBookUpdate {
                                        exchange: "Binance".to_string(),
                                        symbol: raw_symbol.to_string(),
                                        exchange_event_timestamp: Utc::now(),
                                        received_timestamp: Utc::now(),
                                        bids: snap_bids,
                                        asks: snap_asks,
                                        is_snapshot: true,
                                    };
                                    if let Err(e) = kafka_tx.send(snapshot_update).await {
                                        tracing::error!(
                                            "Failed to send snapshot update to Kafka: {}",
                                            e
                                        );
                                        break;
                                    }

                                    // Replay buffered Websocket deltas downstream
                                    let mut last_u = last_update_id;
                                    for event in valid_events {
                                        let (bids, asks) =
                                            match parse_bids_asks(&event.bids, &event.asks) {
                                                Ok(pairs) => pairs,
                                                Err(e) => {
                                                    tracing::error!(
                                                        "Failed to parse buffered event for {}: {}",
                                                        raw_symbol,
                                                        e
                                                    );
                                                    continue;
                                                }
                                            };

                                        let exchange_event_timestamp = Utc
                                            .timestamp_millis_opt(event.event_time as i64)
                                            .single()
                                            .unwrap_or(Utc::now());

                                        let delta_update = NormalizedOrderBookUpdate {
                                            exchange: "Binance".to_string(),
                                            symbol: event.symbol,
                                            exchange_event_timestamp,
                                            received_timestamp: Utc::now(),
                                            bids,
                                            asks,
                                            is_snapshot: false,
                                        };

                                        if let Err(e) = kafka_tx.send(delta_update).await {
                                            tracing::error!(
                                                "Failed to send delta update to Kafka: {}",
                                                e
                                            );
                                            break;
                                        }
                                        last_u = event.final_update_id;
                                    }

                                    tracing::info!(
                                        "Successfully synced {}! Resume live processing from u={}",
                                        raw_symbol,
                                        last_u
                                    );
                                    *state = SymbolSyncState::Synced { last_u };
                                } else {
                                    tracing::warn!(
                                        "Snapshot gap too large for buffer on {}. Clearing buffer to retry...",
                                        raw_symbol
                                    );
                                    // buffer.clear();
                                    symbol_states.remove(&raw_symbol);
                                }
                            }
                            Ok(Err(e)) => {
                                tracing::error!(
                                    "REST snapshot fetch failed for {}: {}",
                                    raw_symbol,
                                    e
                                );
                                symbol_states.remove(&raw_symbol);
                            }
                            Err(oneshot::error::TryRecvError::Empty) => {
                                // Http request still in flight, keep buffering incoming WebSocket
                                // messages
                            }
                            Err(oneshot::error::TryRecvError::Closed) => {
                                tracing::error!(
                                    "REST snapshot task channel closed for {}",
                                    raw_symbol
                                );
                                symbol_states.remove(&raw_symbol);
                            }
                        }
                    }

                    SymbolSyncState::Synced { last_u } => {
                        if let Some(pu) = update.prev_final_update_id {
                            if pu != *last_u {
                                tracing::warn!(
                                    "GAP DETECTED on {}! Last u={}, got U={}. Restarting sync...",
                                    raw_symbol,
                                    last_u,
                                    pu
                                );
                                symbol_states.remove(&raw_symbol);
                                // buffer.clear();
                                continue;
                            }
                        }

                        // Update RAM state
                        let (bids, asks) = match parse_bids_asks(&update.bids, &update.asks) {
                            Ok(pair) => pair,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse Binance bids/asks for {}: {}",
                                    raw_symbol,
                                    e
                                );
                                continue;
                            }
                        };

                        *last_u = update.final_update_id;

                        let exchange_event_timestamp = Utc
                            .timestamp_millis_opt(update.event_time as i64)
                            .single()
                            .unwrap_or_else(Utc::now);

                        let received_timestamp = Utc::now();

                        let normalised = NormalizedOrderBookUpdate {
                            exchange: "Binance".to_string(),
                            symbol: update.symbol,
                            exchange_event_timestamp,
                            received_timestamp,
                            bids,
                            asks,
                            is_snapshot: false,
                        };

                        let _ = kafka_tx.send(normalised).await;
                    }
                }
            }

            ServerEvent::Disconnected { reason } => {
                tracing::error!("Disconnected: {}", reason);
            }
        }
    }
}

async fn fetch_rest_snapshot(symbol: &str) -> anyhow::Result<BinanceRestSnapshot> {
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
        symbol.to_uppercase()
    );

    let snapshot = reqwest::get(&url)
        .await?
        .json::<BinanceRestSnapshot>()
        .await?;

    Ok(snapshot)
}

fn parse_bids_asks(
    bids: &[(String, String)],
    asks: &[(String, String)],
) -> anyhow::Result<(Vec<PriceLevel>, Vec<PriceLevel>)> {
    let bids = bids
        .iter()
        .map(|(price_str, qty_str)| {
            let price = Decimal::from_str(price_str).map_err(|e| anyhow::anyhow!(e))?;
            let qty = Decimal::from_str(qty_str).map_err(|e| anyhow::anyhow!(e))?;
            Ok(PriceLevel { price, qty })
        })
        .collect::<anyhow::Result<Vec<PriceLevel>>>()?;
    let asks = asks
        .iter()
        .map(|(price_str, qty_str)| {
            let price = Decimal::from_str(price_str).map_err(|e| anyhow::anyhow!(e))?;
            let qty = Decimal::from_str(qty_str).map_err(|e| anyhow::anyhow!(e))?;
            Ok(PriceLevel { price, qty })
        })
        .collect::<anyhow::Result<Vec<PriceLevel>>>()?;
    Ok((bids, asks))
}

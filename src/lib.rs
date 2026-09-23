use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

pub mod binance_parser;
pub mod coinbase_parser;
pub mod cross_exchange_arbitrage;
pub mod db_writer;
pub mod order_book;
pub mod order_book_manager;
// pub mod resync;
pub mod ws_connect;

pub mod kafka_consumer;
pub mod kafka_producer;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PriceLevel {
    pub price: Decimal,
    pub qty: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedOrderBookUpdate {
    pub exchange: String, // "binance" or "coinbase"
    pub symbol: String,   // like "BTC-USD"

    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub exchange_event_timestamp: DateTime<Utc>,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub received_timestamp: DateTime<Utc>, // when the update was received in my program

    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
    pub is_snapshot: bool,
}

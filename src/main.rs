use std::{env, time::Duration};

use l2_aggregator::{
    NormalizedOrderBookUpdate,
    binance_parser::binance_parser_task,
    coinbase_parser::coinbase_parser_task,
    db_writer::db_writer_task,
    kafka_consumer::{self, RedisDeduplicator},
    kafka_producer::{create_production_producer, kafka_producer_task},
    order_book_manager::order_book_manager_task,
    ws_connect::Client,
};
use rdkafka::producer::Producer;
use sqlx::postgres::PgPoolOptions;
use tokio::{
    signal::unix::{SignalKind, signal},
    sync::mpsc,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // Extract ConfigMap/Secrent env variables
    let symbols_env = env::var("SYMBOLS").unwrap_or_else(|_| "BTC-USD".to_string());
    let kafka_brokers = env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let db_url =
        env::var("DATABASE_URL").expect("DATABASE_URL must be set via k8s Secret/ConfigMap");
    let redis_url = env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());

    // Parse and map symbols
    let raw_symbols: Vec<String> = symbols_env
        .split(',')
        .map(|s| s.trim().to_string())
        .collect();

    // Binance format: btcusdt@depth
    let binance_streams: Vec<String> = raw_symbols
        .iter()
        .map(|s| {
            let binance_pair = s.replace("-USD", "USDT").replace('-', "").to_lowercase();
            format!("{}@depth", binance_pair)
        })
        .collect();

    // Coinbase format: BTC-USD
    let coinbase_streams = raw_symbols.clone();

    // Infrastructure connections
    tracing::info!("Connecting to PostgreSQL...");
    let db_pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&db_url)
        .await?;

    tracing::info!("Running database schema initialisation...");
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS order_book_updates (
            exchange_event_timestamp TIMESTAMPTZ NOT NULL,
            received_timestamp TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(32) NOT NULL,
            price NUMERIC NOT NULL,
            qty NUMERIC NOT NULL,
            is_bid BOOLEAN NOT NULL,
            is_snapshot BOOLEAN NOT NULL
        );
        "#,
    )
    .execute(&db_pool)
    .await?;

    let _ = sqlx::query(
        "SELECT create_hypertable('order_book_updates', 'received_timestamp', if_not_exists => TRUE);"
    )
        .execute(&db_pool)
        .await;

    tracing::info!("Database schema verified successfully.");

    tracing::info!("Initializing Kafka Producer...");
    let producer = create_production_producer(&kafka_brokers);
    let deduplicator = RedisDeduplicator::new(&redis_url, Duration::from_secs(60))
        .await
        .expect("Failed to connect to Redis");

    // Initialise MPSC channels
    let (kafka_tx, kafka_rx) = mpsc::channel::<NormalizedOrderBookUpdate>(10_000);
    let (obm_tx, obm_rx) = mpsc::channel::<NormalizedOrderBookUpdate>(10_000);
    let (db_tx, db_rx) = mpsc::channel::<NormalizedOrderBookUpdate>(10_000);

    // Spawn internal consumers
    let db_handle = tokio::spawn(db_writer_task(db_rx, db_pool));

    let obm_handle = tokio::spawn(order_book_manager_task(obm_rx, db_tx));

    let consumer_handle = tokio::spawn(kafka_consumer::kafka_consumer(
        kafka_brokers.clone(),
        deduplicator,
        obm_tx,
    ));

    let prod_handle = tokio::spawn(kafka_producer_task(
        producer.clone(),
        "l2-market-data".to_string(),
        kafka_rx,
    ));

    // Spawn exchange websocket clients and parsers
    tracing::info!("Starting Binance stream...");
    let binance_client = Client::connect("wss://stream.binance.com:9443/ws").await?;
    let binance_events = binance_client.subscribe();
    let binance_handle = tokio::spawn(binance_parser_task(
        binance_client,
        binance_events,
        kafka_tx.clone(),
        binance_streams,
    ));

    tracing::info!("Starting Coinbase stream...");
    let coinbase_client = Client::connect("wss://ws-feed.exchange.coinbase.com").await?;
    let coinbase_events = coinbase_client.subscribe();
    let coinbase_handle = tokio::spawn(coinbase_parser_task(
        coinbase_client,
        coinbase_events,
        kafka_tx.clone(),
        coinbase_streams,
    ));

    // Graceful shutdown listener
    shutdown_signal().await;

    binance_handle.abort();
    coinbase_handle.abort();

    // Drop transmit channels so receivers gracefully drain and exit
    drop(kafka_tx);

    tracing::info!("[System] Waiting for background pipelines to drain...");

    // Wait for kafka producer and DB writer to finish writing their buffers
    let _ = tokio::join!(prod_handle, consumer_handle, obm_handle, db_handle,);

    // Final librdkafka buffer flush
    tracing::info!("[System] Flushing Kafka C-core queue...");
    if let Err(e) = producer.flush(Duration::from_secs(5)) {
        tracing::error!("[System] Kafka flush timed out or failed: {:?}", e);
    }

    tracing::info!("[System] Shutdown complete.");
    Ok(())
}

async fn shutdown_signal() {
    let mut sigterm = signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("[SYSTEM] SIGINT (Ctrl+C) received. Starting graceful shutdown...");
        }

        _ = sigterm.recv() => {
            tracing::info!("[System] SIGTERM (k8s) received. Starting graceful shutdown...");
        }
    }
}

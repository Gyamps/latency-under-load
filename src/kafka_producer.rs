use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;
use tokio::sync::mpsc;

use crate::NormalizedOrderBookUpdate;

pub fn create_production_producer(brokers: &str) -> FutureProducer {
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.max.bytes", "10485760") // 10MB max message size
        // Enable idempotent producer to prevent duplicates
        .set("enable.idempotence", "true")
        // Wait for all in-sync replicas to acknowledge
        // Options: 0 (no wait), 1 (leader only), all (all replicas)
        // Also required by idempotent producer
        .set("acks", "all")
        // Retry configuration for transient failures
        // Ultra-low latency: retry quickly with short backoff
        .set("retries", "5")
        .set("retry.backoff.ms", "10")
        .set("linger.ms", "0") // Send immediately
        .set("batch.size", "65536") // large batches if ticks spike simultaneously
        // Compression reduces network bandwidth
        // Options: none, gzip, snappy, lz4, zstd
        .set("compression.type", "lz4") // LZ4 significantly faster for x86/ARM CPUs
        // Timeout for message delivery
        .set("delivery.timeout.ms", "5000")
        .set("queue.buffering.max.messages", "100000")
        .create()
        .expect("Failed to create producer")
}

async fn send_update(
    producer: &FutureProducer,
    topic: String,
    payload: &NormalizedOrderBookUpdate,
) -> Result<(), Box<dyn std::error::Error>> {
    // Serialise payload to JSON
    let payload_json = serde_json::to_vec(payload)?;

    // Create a record with topic, key, and payload
    // The key determines which partition receives the message
    let record = FutureRecord::to(&topic)
        .key(payload.symbol.as_str())
        .payload(&payload_json);

    // Send and await delivery confirmation
    // The timeout specifies how long to wait for the queue
    producer
        .send(record, Duration::from_secs(5))
        .await
        .map_err(|(e, _)| e)?;

    Ok(())
}

pub async fn kafka_producer_task(
    producer: FutureProducer,
    topic: String,
    mut kafka_rx: mpsc::Receiver<NormalizedOrderBookUpdate>,
) {
    tracing::info!("Kafka producer task started.");

    while let Some(update) = kafka_rx.recv().await {
        if let Err(e) = send_update(&producer, topic.clone(), &update).await {
            tracing::error!("Failed to produce message to Kafka: {}", e);
        }
    }

    tracing::info!("Kafka producer channel closed. Shutting down task.");
}

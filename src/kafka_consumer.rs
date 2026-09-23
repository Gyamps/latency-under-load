use std::time::Duration;

use futures_util::StreamExt;
use rdkafka::{
    Message,
    config::ClientConfig,
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
};
use redis::{RedisError, aio::ConnectionManager};
use tokio::{sync::mpsc, time::sleep};

use crate::NormalizedOrderBookUpdate;

#[derive(Debug, thiserror::Error)]
enum ProcessingError {
    #[error("Transient error, can retry: {0}")]
    Transient(String),

    #[error("Permanent error, skipping message: {0}")]
    Permanent(String),

    #[error("Fatal error, shutting down consumer: {0}")]
    Fatal(String),
}

// Convert JSON deserialisation errors to Permanent errors
impl From<serde_json::Error> for ProcessingError {
    fn from(err: serde_json::Error) -> Self {
        ProcessingError::Permanent(format!("JSON parse error: {}", err))
    }
}

// Convert string literals into Permanent errors
impl From<&'static str> for ProcessingError {
    fn from(err: &'static str) -> Self {
        ProcessingError::Permanent(err.to_string())
    }
}

// Convert channel send errors into Fatal errors
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for ProcessingError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        ProcessingError::Fatal(format!("Channel receiver dropped: {}", err))
    }
}

impl From<RedisError> for ProcessingError {
    fn from(err: RedisError) -> Self {
        ProcessingError::Transient(format!("Redis error: {}", err))
    }
}

#[derive(Clone)]
pub struct RedisDeduplicator {
    redis: ConnectionManager,
    ttl_ms: u64,
}

impl RedisDeduplicator {
    pub async fn new(redis_url: &str, ttl: Duration) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(redis_url)?;
        let manager = ConnectionManager::new(client).await?;
        Ok(Self {
            redis: manager,
            ttl_ms: ttl.as_millis() as u64,
        })
    }

    // `Ok(true)` if the message is UNIQUE and was successfully stored in Redis, else `Ok(false)`
    pub async fn is_unique(&self, deduplication_key: &str) -> Result<bool, redis::RedisError> {
        let mut conn = self.redis.clone();
        let redis_key = format!("dedup:{}", deduplication_key);

        // SET key "1" NX (only if Not Exists) PX ttl_ms
        let set_result: Option<String> = redis::cmd("SET")
            .arg(&redis_key)
            .arg("1")
            .arg("NX")
            .arg("PX")
            .arg(self.ttl_ms)
            .query_async(&mut conn)
            .await?;

        // `Ok(true)`, Successfully inserted (unique)
        // `Ok(false)`, key already exists (duplicate)
        Ok(set_result.is_some())
    }
}

pub async fn kafka_consumer(
    brokers: String,
    deduplicator: RedisDeduplicator,
    obm_tx: mpsc::Sender<NormalizedOrderBookUpdate>,
) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "orderbook-engine-group")
        .set("auto.offset.reset", "latest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Failed to create Kafka consumer");

    consumer
        .subscribe(&["l2-market-data"])
        .expect("Failed to subscribe");

    let mut stream = consumer.stream();
    let mut consecutive_errors = 0;
    const MAX_CONSECUTIVE_ERRORS: u32 = 10;

    while let Some(result) = stream.next().await {
        match result {
            Ok(message) => {
                consecutive_errors = 0;

                match process_with_retry(&message, 3, &deduplicator, &obm_tx).await {
                    Ok(_) => {
                        // Commit after successful processing
                        let _ =
                            consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async);
                    }

                    Err(ProcessingError::Permanent(e)) => {
                        // Log and move on - don't block queue
                        tracing::error!("Permanent error, skipping message: {}", e);
                        let _ =
                            consumer.commit_message(&message, rdkafka::consumer::CommitMode::Async);
                    }
                    Err(ProcessingError::Fatal(e)) => {
                        tracing::error!("Fatal error, shutting down: {}", e);
                        break;
                    }
                    Err(ProcessingError::Transient(_)) => {
                        // Transient errors handled by retry logic
                    }
                }
            }
            Err(KafkaError::PartitionEOF(_)) => {
                // End of partition - not an error, just no more messages,
                // move on to next partition
                continue;
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::error!("Kafka error: ({}): {}", consecutive_errors, e);

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    tracing::error!("Too many consecutive errors, shutting down");
                    break;
                }

                // Backoff before retrying
                sleep(Duration::from_millis(100 * consecutive_errors as u64)).await;
            }
        }
    }
}

async fn process_with_retry<M: Message>(
    message: &M,
    max_retries: u32,
    deduplicator: &RedisDeduplicator,
    obm_tx: &mpsc::Sender<NormalizedOrderBookUpdate>,
) -> Result<(), ProcessingError> {
    let mut attempts = 0;

    loop {
        attempts += 1;

        match process_message(message, deduplicator, obm_tx).await {
            Ok(_) => return Ok(()),
            Err(ProcessingError::Transient(e)) if attempts < max_retries => {
                tracing::warn!("Transient error (attempt {}): {}", attempts, e);
                sleep(Duration::from_millis(100 * attempts as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn process_message<M: Message>(
    message: &M,
    deduplicator: &RedisDeduplicator,
    obm_tx: &mpsc::Sender<NormalizedOrderBookUpdate>,
) -> Result<(), ProcessingError> {
    let payload = message.payload().ok_or("Empty payload")?;
    let deserialised_event: NormalizedOrderBookUpdate = serde_json::from_slice(payload)?;

    let timestamp_ms = deserialised_event
        .exchange_event_timestamp
        .timestamp_millis();
    let top_bid = deserialised_event
        .bids
        .first()
        .map(|b| format!("{}-{}", b.price, b.qty))
        .unwrap_or_default();
    let top_ask = deserialised_event
        .asks
        .first()
        .map(|a| format!("{}-{}", a.price, a.qty))
        .unwrap_or_default();

    let dedup_key = format!(
        "{}:{}:{}:{}:{}",
        deserialised_event.exchange, deserialised_event.symbol, timestamp_ms, top_bid, top_ask
    );

    if !deduplicator.is_unique(&dedup_key).await? {
        tracing::info!("Duplicate market update skipped: {}", dedup_key);
        return Ok(());
    }

    obm_tx.send(deserialised_event).await?;

    Ok(())
}

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sqlx::{PgPool, Postgres, QueryBuilder};
use std::time::Duration;
use tokio::{sync::mpsc, time::interval};

use crate::NormalizedOrderBookUpdate;

#[derive(Debug, Clone)]
pub struct OrderBookRow {
    pub exchange_event_timestamp: DateTime<Utc>,
    pub received_timestamp: DateTime<Utc>,
    pub symbol: String,
    pub price: Decimal,
    pub qty: Decimal,
    pub is_bid: bool,
    pub is_snapshot: bool,
}

pub async fn db_writer_task(mut db_rx: mpsc::Receiver<NormalizedOrderBookUpdate>, pool: PgPool) {
    let mut buffer: Vec<OrderBookRow> = Vec::with_capacity(5000);
    let mut timer = interval(Duration::from_millis(500));
    const BATCH_THRESHOLD: usize = 2000;

    loop {
        tokio::select! {
            maybe_row = db_rx.recv() => {
                match maybe_row {
                    Some(row) => {
                        for level in row.bids {
                            buffer.push(OrderBookRow {
                                exchange_event_timestamp: row.exchange_event_timestamp,
                                received_timestamp: row.received_timestamp,
                                symbol: row.symbol.clone(),
                                price: level.price,
                                qty: level.qty,
                                is_bid: true,
                                is_snapshot: row.is_snapshot,
                            });
                        }
                        for level in row.asks {
                            buffer.push(OrderBookRow {
                                exchange_event_timestamp: row.exchange_event_timestamp,
                                received_timestamp: row.received_timestamp,
                                symbol: row.symbol.clone(),
                                price: level.price,
                                qty: level.qty,
                                is_bid: false,
                                is_snapshot: row.is_snapshot,
                            });
                        }

                        if buffer.len() >= BATCH_THRESHOLD {
                            flush_buffer(&mut buffer, &pool).await;
                        }
                    }
                    None => {
                        // Channel closed, flushing remaining rows and exiting
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &pool).await;
                        }
                        break;
                    }
                }
            }

            _ = timer.tick() => {
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &pool).await;
                }
            }
        }
    }
}

async fn flush_buffer(buffer: &mut Vec<OrderBookRow>, pool: &PgPool) {
    if buffer.is_empty() {
        return;
    }

    let rows_to_insert = buffer.drain(..).collect::<Vec<_>>();
    let count = rows_to_insert.len();

    // Chunk into batches of 1k rows to stay under Postgres'  65,535 limits
    for chunk in rows_to_insert.chunks(1000) {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO order_book_updates (exchange_event_timestamp, received_timestamp, symbol, price, qty, is_bid, is_snapshot) ",
        );

        query_builder.push_values(chunk, |mut b, row| {
            b.push_bind(row.exchange_event_timestamp)
                .push_bind(row.received_timestamp)
                .push_bind(row.symbol.clone())
                .push_bind(row.price)
                .push_bind(row.qty)
                .push_bind(row.is_bid)
                .push_bind(row.is_snapshot);
        });

        let query = query_builder.build();

        if let Err(e) = query.execute(pool).await {
            tracing::error!("Failed to write batch to TimeScaleDB: {}", e);
        }
    }

    tracing::debug!("Flushed {} rows into TimeScaleDB", count);
}

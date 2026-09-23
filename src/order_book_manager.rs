use std::{collections::HashMap, time::Duration};

use tokio::{sync::mpsc, time};

use crate::{
    NormalizedOrderBookUpdate, cross_exchange_arbitrage::calculate_cross_exchange_arbitrage,
    order_book::OrderBook,
};

pub async fn order_book_manager_task(
    mut obm_rx: mpsc::Receiver<NormalizedOrderBookUpdate>,
    db_tx: mpsc::Sender<NormalizedOrderBookUpdate>,
) {
    let mut books: HashMap<(String, String), OrderBook> = HashMap::new();
    let mut heartbeat = time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            Some(update) = obm_rx.recv() => {
                let key = (update.exchange.clone(), update.symbol.clone());
                let book = books.entry(key).or_insert_with(OrderBook::new);

                if update.is_snapshot {
                    book.set_snapshot(&update.bids, &update.asks);
                } else {
                    book.apply_deltas(&update.bids, &update.asks);
                }

                calculate_cross_exchange_arbitrage(&update.symbol, &books);

                let _ = db_tx.send(update).await;
            }

            _ = heartbeat.tick() => {
                for ((exchange, symbol), book) in &books {
                    if let (Some(bid), Some(ask)) = (book.best_bid(), book.best_ask()) {
                        tracing::info!(
                            "[HEARTBEAT] Venue: {:<8} | Pair: {:<7} | Best Bid: {:<10} | Best Ask: {:<10}",
                            exchange, symbol, bid.price, ask.price
                        )
                    }
                }
            }
        }
    }
}

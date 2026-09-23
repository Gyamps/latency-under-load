use std::collections::HashMap;

use crate::order_book::OrderBook;

pub fn calculate_cross_exchange_arbitrage(
    symbol: &str,
    books: &HashMap<(String, String), OrderBook>,
) {
    let binance_book = books.get(&("Binance".to_string(), symbol.to_string()));
    let coinbase_book = books.get(&("Coinbase".to_string(), symbol.to_string()));

    let (binance_b, binance_a) = match binance_book {
        Some(b) => (b.best_bid(), b.best_ask()),
        None => return,
    };

    let (coinbase_b, coinbase_a) = match coinbase_book {
        Some(c) => (c.best_bid(), c.best_ask()),
        None => return,
    };

    // Scenario 1: Binance bid > Coinbase ask (Buy Coinbase, Sell Binance)
    if let (Some(b_bid), Some(c_ask)) = (binance_b.as_ref(), coinbase_a.as_ref()) {
        if b_bid.price > c_ask.price {
            let spread = b_bid.price - c_ask.price;
            tracing::warn!(
                "ARBITRAGE DETECTED [{}]! Buy Coinbase @ {}, Sell Binance @ {} | Spread: {}",
                symbol,
                c_ask.price,
                b_bid.price,
                spread
            );
        }
    }

    // Scenario 2: Coinbase bid > Binance ask (Buy Binance, Sell Coinbase)
    if let (Some(c_bid), Some(b_ask)) = (coinbase_b.as_ref(), binance_a.as_ref()) {
        if c_bid.price > b_ask.price {
            let spread = c_bid.price - b_ask.price;
            tracing::warn!(
                "ARBITRAGE DETECTED [{}]! Buy Binance @ {}, Sell Coinbase @ {} | Spread: {}",
                symbol,
                b_ask.price,
                c_bid.price,
                spread
            );
        }
    }
}

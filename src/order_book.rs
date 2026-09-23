use rust_decimal::Decimal;
use std::cmp::Reverse;
use std::collections::BTreeMap;

use crate::PriceLevel;

// Bids sorted highest to lowest
// Asks sorted lowest to highest
#[derive(Debug, Default)]
pub struct OrderBook {
    // pub last_update_id: u64,
    pub bids: BTreeMap<Reverse<Decimal>, Decimal>,
    pub asks: BTreeMap<Decimal, Decimal>,
}

impl OrderBook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn set_snapshot(&mut self, bids: &Vec<PriceLevel>, asks: &Vec<PriceLevel>) {
        self.clear();
        self.apply_deltas(&bids, &asks);
    }

    pub fn apply_deltas(&mut self, bids: &[PriceLevel], asks: &[PriceLevel]) {
        for bid in bids {
            if bid.qty.is_zero() {
                self.bids.remove(&Reverse(bid.price));
            } else {
                self.bids.insert(Reverse(bid.price), bid.qty);
            }
        }
        for ask in asks {
            if ask.qty.is_zero() {
                self.asks.remove(&ask.price);
            } else {
                self.asks.insert(ask.price, ask.qty);
            }
        }
    }

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bids
            .iter()
            .next()
            .map(|(&Reverse(price), &qty)| PriceLevel { price, qty })
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.asks
            .iter()
            .next()
            .map(|(&price, &qty)| PriceLevel { price, qty })
    }
}

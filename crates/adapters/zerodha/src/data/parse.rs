// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Mapping from a decoded [`KiteTick`] to Nautilus domain types.
//!
//! # This is INTERPRETATION, and the captured corpus says nothing about it
//!
//! The corpus and the fixture tests establish that the decoder reads the venue's *bytes*
//! correctly. **They cannot establish that this module reads them with the right meaning.** A
//! decoder that agrees with the vendor client on every packet still tells you nothing about
//! whether `buy[0]` is the best bid, or whether `exchange_timestamp` is the right stamp for a
//! `QuoteTick`. Those are modelling decisions and they are argued here, not measured.
//!
//! # ⚠️ Only FULL mode can produce a `QuoteTick`
//!
//! `QuoteTick` requires top-of-book on both sides. Kite carries depth **only in the 184-byte
//! full-mode packet**:
//!
//! | packet | mode | has bid/ask? |
//! |--------|------|--------------|
//! | 8      | ltp   | no — last price only |
//! | 28/44  | quote | **no** — OHLC and volumes, but no book |
//! | 32/184 | full  | 184 only: five-deep ladder |
//!
//! The 44-byte "quote" packet is the trap: it is *named* quote and carries no quote. Anything
//! wanting `QuoteTick`s must subscribe in **full** mode, and [`quote_tick_from`] returns an error
//! rather than inventing a book from a last price.

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::QuoteTick,
    identifiers::InstrumentId,
    types::{Price, Quantity},
};

use crate::websocket::messages::{KiteDepth, KiteTick};

/// Nanoseconds in one second, for the venue's epoch-**seconds** timestamps.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// Converts the venue's epoch-seconds stamp to [`UnixNanos`], treating zero as absent.
///
/// **Zero is a sentinel, not a time.** Kite emits `0` for "no timestamp" on instruments that have
/// not traded, and a naive conversion turns that into 1970-01-01 — a value that sorts before every
/// other event and is not obviously wrong in a log line.
#[must_use]
pub fn venue_time_to_unix_nanos(secs: Option<u32>) -> Option<UnixNanos> {
    match secs {
        None | Some(0) => None,
        Some(s) => Some(UnixNanos::from(u64::from(s) * NANOS_PER_SEC)),
    }
}

/// The best bid and ask taken from a five-deep ladder.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TopOfBook {
    /// Highest bid price with a non-zero price.
    pub bid_price: f64,
    /// Lowest ask price with a non-zero price.
    pub ask_price: f64,
    /// Quantity resting at the best bid.
    pub bid_size: u32,
    /// Quantity resting at the best ask.
    pub ask_size: u32,
}

/// Extracts top-of-book by VALUE, not by position.
///
/// # Why this does not read `buy[0]`
///
/// The decoder preserves the wire order of the ladder exactly, and **whether Zerodha sends it
/// best-first has never been measured** — `messages::KiteDepth` says so explicitly, and the
/// constructed fixtures cannot settle it because their ordering was chosen by whoever wrote them.
///
/// Indexing position 0 would be correct *if* the venue sorts, and silently wrong if it does not —
/// producing a plausible book that is simply not the best price. Taking the max bid and min ask is
/// correct **either way**, so this removes the open question from the path rather than betting on
/// it. If the ordering is later measured and confirmed, this stays correct; it just does slightly
/// more work.
///
/// # Zero-priced levels are EXCLUDED, and that is load-bearing
///
/// An unfilled ladder slot arrives as a zero price. `min()` over the ask side including zeros
/// returns **0**, which is a valid-looking `Price` and would cross every book in the system. The
/// filter is not tidiness.
///
/// Returns `None` when either side has no non-zero level.
#[must_use]
pub fn top_of_book(depth: &KiteDepth) -> Option<TopOfBook> {
    let best_bid = depth
        .buy
        .iter()
        .filter(|level| level.price > 0.0)
        .max_by(|a, b| a.price.total_cmp(&b.price))?;

    let best_ask = depth
        .sell
        .iter()
        .filter(|level| level.price > 0.0)
        .min_by(|a, b| a.price.total_cmp(&b.price))?;

    Some(TopOfBook {
        bid_price: best_bid.price,
        ask_price: best_ask.price,
        bid_size: best_bid.quantity,
        ask_size: best_ask.quantity,
    })
}

/// Builds a [`QuoteTick`] from a full-mode tick.
///
/// `ts_init` is the local receive time; `ts_event` is the venue's exchange timestamp when present
/// and falls back to `ts_init` when it is absent or zero.
///
/// # Errors
///
/// Returns an error if the tick carries no depth (i.e. it is not a full-mode packet), or if either
/// side of the ladder has no non-zero level.
pub fn quote_tick_from(
    tick: &KiteTick,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    ts_init: UnixNanos,
) -> anyhow::Result<QuoteTick> {
    let depth = tick.depth.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "cannot build a QuoteTick from a {:?}-mode tick for instrument_token {}: only the \
             184-byte full-mode packet carries a book, and the 44-byte packet named `quote` does \
             not",
            tick.mode,
            tick.instrument_token,
        )
    })?;

    let top = top_of_book(depth).ok_or_else(|| {
        anyhow::anyhow!(
            "instrument_token {} has no non-zero level on one or both sides; an empty ladder slot \
             arrives as a zero price and must not be published as a quote",
            tick.instrument_token,
        )
    })?;

    let ts_event = venue_time_to_unix_nanos(tick.exchange_timestamp).unwrap_or(ts_init);

    Ok(QuoteTick::new(
        instrument_id,
        Price::new(top.bid_price, price_precision),
        Price::new(top.ask_price, price_precision),
        Quantity::new(f64::from(top.bid_size), size_precision),
        Quantity::new(f64::from(top.ask_size), size_precision),
        ts_event,
        ts_init,
    ))
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::InstrumentId;
    use rstest::rstest;

    use super::*;
    use crate::{
        common::enums::{ZerodhaSegment, ZerodhaTickMode},
        websocket::messages::KiteDepthEntry,
    };

    fn level(price: f64, quantity: u32) -> KiteDepthEntry {
        KiteDepthEntry {
            price,
            quantity,
            orders: 1,
        }
    }

    fn tick_with(depth: Option<KiteDepth>, exchange_timestamp: Option<u32>) -> KiteTick {
        KiteTick {
            instrument_token: 408_065,
            segment: ZerodhaSegment::Nse,
            tradable: true,
            mode: if depth.is_some() {
                ZerodhaTickMode::Full
            } else {
                ZerodhaTickMode::Quote
            },
            last_price: 100.0,
            exchange_timestamp,
            depth,
            ..Default::default()
        }
    }

    fn instrument_id() -> InstrumentId {
        InstrumentId::from("RELIANCE.NSE")
    }

    // THE DISCRIMINATING TEST. The ladder below is deliberately NOT best-first: the best bid sits
    // at index 2 and the best ask at index 1. A `buy[0]`/`sell[0]` implementation passes every
    // other test in this file and fails this one -- which is the whole reason the unknown about
    // wire ordering is handled by value rather than by position.
    #[rstest]
    fn test_top_of_book_ignores_wire_order() {
        let depth = KiteDepth {
            buy: vec![level(99.0, 10), level(98.5, 20), level(99.5, 30)],
            sell: vec![level(101.0, 40), level(100.5, 50), level(102.0, 60)],
        };

        let top = top_of_book(&depth).expect("both sides have levels");

        assert_eq!(top.bid_price, 99.5, "best bid is the HIGHEST bid, not buy[0]");
        assert_eq!(top.ask_price, 100.5, "best ask is the LOWEST ask, not sell[0]");
        assert_eq!(top.bid_size, 30, "size must come from the same level as the price");
        assert_eq!(top.ask_size, 50);
    }

    // The zero-price filter is the difference between a quote and a book-crossing artefact.
    #[rstest]
    fn test_zero_priced_levels_are_excluded() {
        let depth = KiteDepth {
            buy: vec![level(99.0, 10), level(0.0, 0), level(0.0, 0)],
            sell: vec![level(0.0, 0), level(101.0, 40), level(0.0, 0)],
        };

        let top = top_of_book(&depth).expect("one real level per side is enough");

        assert_eq!(top.bid_price, 99.0);
        assert_eq!(
            top.ask_price, 101.0,
            "min() over a side containing zeros would return 0.0 and cross every book downstream",
        );
    }

    #[rstest]
    #[case(vec![level(0.0, 0)], vec![level(101.0, 5)])]
    #[case(vec![level(99.0, 5)], vec![level(0.0, 0)])]
    #[case(vec![], vec![])]
    fn test_a_side_with_no_real_level_yields_none(
        #[case] buy: Vec<KiteDepthEntry>,
        #[case] sell: Vec<KiteDepthEntry>,
    ) {
        assert!(top_of_book(&KiteDepth { buy, sell }).is_none());
    }

    #[rstest]
    fn test_quote_tick_is_built_from_the_best_levels() {
        let depth = KiteDepth {
            buy: vec![level(99.0, 10), level(99.5, 30)],
            sell: vec![level(101.0, 40), level(100.5, 50)],
        };
        let tick = tick_with(Some(depth), Some(1_786_695_200));

        let quote = quote_tick_from(&tick, instrument_id(), 2, 0, UnixNanos::from(42))
            .expect("full-mode tick with a two-sided book");

        assert_eq!(quote.bid_price, Price::new(99.5, 2));
        assert_eq!(quote.ask_price, Price::new(100.5, 2));
        assert_eq!(quote.bid_size, Quantity::new(30.0, 0));
        assert_eq!(quote.ask_size, Quantity::new(50.0, 0));
        assert_eq!(quote.ts_event, UnixNanos::from(1_786_695_200 * NANOS_PER_SEC));
        assert_eq!(quote.ts_init, UnixNanos::from(42));
    }

    // A quote-mode tick has no book at all. Returning an error rather than synthesising one from
    // `last_price` is deliberate: a client that publishes a bid == ask == last is indistinguishable
    // from a real zero-spread market to everything downstream.
    #[rstest]
    fn test_a_tick_without_depth_is_rejected() {
        let err = match quote_tick_from(
            &tick_with(None, Some(1_786_695_200)),
            instrument_id(),
            2,
            0,
            UnixNanos::from(42),
        ) {
            Ok(_) => panic!("a tick with no book must not produce a QuoteTick"),
            Err(e) => e.to_string(),
        };

        assert!(
            err.contains("full-mode"),
            "the error should say which mode is required; was: {err}",
        );
    }

    #[rstest]
    #[case(None)]
    #[case(Some(0))]
    fn test_absent_or_zero_venue_time_falls_back_to_ts_init(#[case] stamp: Option<u32>) {
        let depth = KiteDepth {
            buy: vec![level(99.0, 10)],
            sell: vec![level(101.0, 40)],
        };

        let quote = quote_tick_from(
            &tick_with(Some(depth), stamp),
            instrument_id(),
            2,
            0,
            UnixNanos::from(4_242),
        )
        .expect("book is two-sided");

        assert_eq!(
            quote.ts_event,
            UnixNanos::from(4_242),
            "epoch zero is Kite's ABSENT sentinel; converting it would stamp the tick 1970-01-01",
        );
    }

    #[rstest]
    fn test_venue_time_conversion_is_seconds_to_nanos() {
        assert_eq!(
            venue_time_to_unix_nanos(Some(1_786_695_200)),
            Some(UnixNanos::from(1_786_695_200_000_000_000)),
        );
        assert_eq!(venue_time_to_unix_nanos(Some(0)), None);
        assert_eq!(venue_time_to_unix_nanos(None), None);
    }
}

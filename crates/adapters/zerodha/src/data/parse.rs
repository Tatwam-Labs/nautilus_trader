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
//!
//! # ⚠️ A tick is a SNAPSHOT, not a trade report — see [`TradeTracker`]
//!
//! Kite pushes a full-mode packet on a **timer**, roughly once per second, whether or not anything
//! traded. There is no "a trade happened" flag on the wire. A `TradeTick` therefore cannot be a
//! function of one tick; it is a function of the *difference* between two, which is why the trade
//! path carries state and the quote path does not.

use std::collections::HashMap;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{QuoteTick, TradeTick},
    enums::AggressorSide,
    identifiers::{InstrumentId, TradeId},
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
/// ⭐ **THIS GUARD IS BACKED BY AN OBSERVED FRAME, NOT ONLY BY REASONING — do not "simplify" it.**
/// It was originally written from an argument (a fixed five-slot ladder must be paddable, so some
/// slots must arrive empty). On 2026-08-14 a live MCX socket then delivered a **fully zero-filled
/// side, at one instrument in four**, in a single captured frame. The argument was right and the
/// case is common rather than pathological.
///
/// This note exists because a defence written from reasoning and a defence written from evidence
/// are **indistinguishable from the outside**: same code, same test, same green build. So the next
/// reader cannot tell whether the filter is load-bearing or belt-and-braces — and belt-and-braces
/// is precisely what gets deleted in a tidy-up. Recording the frame is cheap now and unrecoverable
/// after the deletion.
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

/// Builds a [`TradeTick`] for `traded_size` units done at the tick's last traded price.
///
/// `traded_size` is supplied by the caller because **the tick does not carry it** — see
/// [`TradeTracker`], which is the only thing that can know it. Calling this directly means
/// asserting that a trade of that size happened, which nothing in a single packet can establish.
///
/// # `ts_event` prefers `last_trade_time` over `exchange_timestamp`
///
/// `exchange_timestamp` is when the venue *published the snapshot*; `last_trade_time` is when the
/// trade being reported actually happened. They differ by up to the snapshot interval, and the
/// trade's own stamp is the one that belongs on a trade. Both are epoch **seconds** and both use
/// zero as the absent sentinel, so both go through [`venue_time_to_unix_nanos`]; `ts_init` is the
/// last resort.
///
/// # `AggressorSide::NoAggressor`, and it is not a placeholder
///
/// **Zerodha's binary feed carries no aggressor flag at all** — no side byte, no maker/taker
/// marker, nothing from which the initiating side can be read. It could be *guessed* by comparing
/// `last_price` against the mid, but that guess is wrong for every trade inside the spread and,
/// worse, this print may aggregate several trades that went both ways. A wrong `AggressorSide` is
/// a plausible value that no downstream check rejects, so the honest encoding is the one the enum
/// provides for exactly this case. Sibling adapters do the same where the venue is silent —
/// Betfair (`stream::parse::make_trade_tick`) and Interactive Brokers (`data::parse`) both pin
/// `NoAggressor` with the same reasoning.
///
/// # The `TradeId` is SYNTHETIC — `{instrument_token}-{cumulative_volume}`
///
/// The venue assigns no match ID on this feed, and `TradeId` is not optional. The cumulative
/// session volume is the one value on the packet that is **monotonic per instrument per session**,
/// so pairing it with the token yields an ID that is unique across the instruments on the socket
/// and, crucially, *deterministic*: a replay of the same packets mints the same IDs, whereas a
/// counter or a clock would mint new ones and defeat any downstream de-duplication. It is at most
/// 21 ASCII characters (10 + 1 + 10), inside `TradeId`'s 36-character limit.
///
/// # Errors
///
/// Returns an error if the tick carries no `volume_traded` (only quote and full packets do, and
/// the ID is derived from it), if `traded_size` is zero (`TradeTick` rejects a non-positive size),
/// or if `last_price` is not positive — a zero-priced print would flow straight into bar
/// aggregation and no downstream check would reject it.
pub fn trade_tick_from(
    tick: &KiteTick,
    instrument_id: InstrumentId,
    price_precision: u8,
    size_precision: u8,
    traded_size: u32,
    ts_init: UnixNanos,
) -> anyhow::Result<TradeTick> {
    let cumulative_volume = tick.volume_traded.ok_or_else(|| {
        anyhow::anyhow!(
            "cannot build a TradeTick from a {:?}-mode tick for instrument_token {}: it carries no \
             volume_traded, which is both the evidence that a trade occurred and the source of the \
             synthetic trade ID",
            tick.mode,
            tick.instrument_token,
        )
    })?;

    if traded_size == 0 {
        anyhow::bail!(
            "instrument_token {} traded zero units; TradeTick requires a positive size and a \
             zero-size print carries no information",
            tick.instrument_token,
        );
    }

    // `is_finite` is not belt-and-braces: a NaN would pass a bare `<= 0.0` test, and `Price::new`
    // would take it.
    if !tick.last_price.is_finite() || tick.last_price <= 0.0 {
        anyhow::bail!(
            "instrument_token {} reported traded volume with a last_price of {}; a non-positive \
             traded price cannot be published, it would reach bar aggregation unchallenged",
            tick.instrument_token,
            tick.last_price,
        );
    }

    let ts_event = venue_time_to_unix_nanos(tick.last_trade_time)
        .or_else(|| venue_time_to_unix_nanos(tick.exchange_timestamp))
        .unwrap_or(ts_init);

    // ⚠️ THE `{token}-{cumulative_volume}` SHAPE IS LOAD-BEARING. DO NOT "SIMPLIFY" IT.
    //
    // Determinism across a replay is the stated reason, but it is not the only one. Because the ID
    // carries the running total and `size` is the delta between consecutive prints, **the two
    // verify each other**: for successive trades on one instrument, the difference between the ID
    // suffixes must equal the later trade's size.
    //
    // That is a free consistency check on the delta arithmetic, and it fired on live MCX data on
    // 2026-08-14 — IDs `…33223`, `…33229`, `…33232` against sizes 1, 6, 3, where 33229−33223 = 6
    // and 33232−33229 = 3. Had the differencing been wrong, the sizes and the IDs would have
    // disagreed visibly.
    //
    // Replacing this with a counter, a UUID or a clock keeps every test in this file green and
    // **silently removes the only cross-check the trade path has**.
    let trade_id =
        TradeId::new_checked(format!("{}-{}", tick.instrument_token, cumulative_volume))?;

    Ok(TradeTick::new(
        instrument_id,
        Price::new(tick.last_price, price_precision),
        Quantity::new(f64::from(traded_size), size_precision),
        AggressorSide::NoAggressor,
        trade_id,
        ts_event,
        ts_init,
    ))
}

/// Turns a stream of Kite snapshots into trades, by remembering the previous snapshot per
/// instrument.
///
/// # ⚠️ Emitting one `TradeTick` per tick would FABRICATE a trade every second
///
/// Measured on a live MCX socket on 2026-08-14: **21 full-mode ticks arrived in 20 seconds**, the
/// exchange timestamps stepping by exactly one second, and across three consecutive ticks
/// `last_price` stayed at 7811 **while the best ask moved 7814 → 7815**. The book moved; nothing
/// traded. A mapper that emits on every tick would have printed three trades there — and would go
/// on printing one per second per instrument, forever, with no error anywhere. Those prints reach
/// bar aggregation and become volume and prices that never existed.
///
/// # Why `volume_traded` is the authoritative signal
///
/// Three fields could plausibly answer "did a trade happen":
///
/// - `last_traded_quantity` — **unusable.** It is the size of the most recent trade, so
///   consecutive same-sized trades (the common case: one lot at a time) leave it *unchanged*. An
///   unchanged value here is not evidence that nothing traded.
/// - `last_trade_time` — **not authoritative.** It is epoch **seconds**, so two trades in the same
///   second are one value; against a ~1s snapshot cadence that collision is the normal case, not
///   an edge case. It also cannot say *how much* traded, and its zero is Kite's absent sentinel,
///   which a naive comparison reads as a real time.
/// - `volume_traded` — **authoritative.** Cumulative for the session and monotonic while the
///   session lasts, so `now > previous` is exactly "at least one trade happened since the previous
///   snapshot", and the *difference* is exactly how much traded — including trades batched inside
///   one snapshot, which the other two fields silently lose.
///
/// Using the delta rather than `last_traded_quantity` means the emitted sizes **sum to the
/// venue's own session volume**, which is the property bar aggregation needs. The cost is honest
/// and stated here: when several trades fall inside one snapshot they are published as **one
/// aggregated print**, priced at `last_price` — the last of them. This client cannot do better;
/// the intermediate trades are not on the wire.
///
/// # The first tick for an instrument emits NOTHING
///
/// On the first snapshot there is no previous value, and `volume_traded` is the session total to
/// date — millions of units accumulated before this client connected. Emitting it would print the
/// whole day's volume as a single trade at the current price. The first tick therefore only
/// establishes the baseline. The cost is at most one missed aggregated print per instrument per
/// connection, against a fabricated print that would corrupt every bar it touched.
///
/// # A DECREASE is a session reset, and is treated as a new baseline
///
/// `volume_traded` restarts at the session boundary, and a reconnect can straddle one. A
/// subtraction there would underflow — panicking in a debug build, and in release wrapping to
/// roughly four billion units, which is the largest fabricated trade this code could possibly
/// emit. Any decrease is therefore read as "new session", rebaselined, and emits nothing. This is
/// the same rule as the first tick, for the same reason.
///
/// # Where this state lives, and why not in the client
///
/// One [`TradeTracker`] is owned by the feed task that [`crate::data::ZerodhaDataClient`] spawns
/// on connect, which is the single reader of the tick stream — so the map needs no lock, and is
/// not shared with the engine-side client the way the instrument registry is. It is also
/// per-connection:
/// a reconnect starts a fresh tracker, which rebaselines every instrument rather than carrying a
/// stale volume across a gap that may have crossed a session boundary.
#[derive(Clone, Debug, Default)]
pub struct TradeTracker {
    /// Instrument token to the last cumulative session volume seen for it.
    last_volume: HashMap<u32, u32>,
}

impl TradeTracker {
    /// Creates an empty [`TradeTracker`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `tick` and returns a [`TradeTick`] only if a trade actually occurred.
    ///
    /// `Ok(None)` is the *expected* outcome for most ticks and is not a problem: it means the
    /// snapshot reported no additional traded volume, this is the first snapshot for the
    /// instrument, or the packet carries no volume at all (LTP and index packets never do —
    /// an index does not trade).
    ///
    /// # Errors
    ///
    /// Returns an error only when a trade *was* detected and could not be represented — see
    /// [`trade_tick_from`]. A caller should log these rather than treat them as fatal, but they
    /// are genuinely lost trades, unlike `Ok(None)`.
    pub fn observe(
        &mut self,
        tick: &KiteTick,
        instrument_id: InstrumentId,
        price_precision: u8,
        size_precision: u8,
        ts_init: UnixNanos,
    ) -> anyhow::Result<Option<TradeTick>> {
        let Some(volume) = tick.volume_traded else {
            return Ok(None);
        };

        // Insert first and read the previous value back, so every path -- first tick, reset,
        // no-change, real trade -- leaves the baseline updated with a single lookup.
        let Some(previous) = self.last_volume.insert(tick.instrument_token, volume) else {
            return Ok(None);
        };

        // `checked_sub` returning None IS the session-reset case; `Some(0)` is the no-trade case
        // that a per-tick emitter gets wrong.
        let traded_size = match volume.checked_sub(previous) {
            None | Some(0) => return Ok(None),
            Some(delta) => delta,
        };

        trade_tick_from(
            tick,
            instrument_id,
            price_precision,
            size_precision,
            traded_size,
            ts_init,
        )
        .map(Some)
    }
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

    // A full-mode tick carrying session volume. It deliberately has NO depth: a trade must not
    // depend on a book, and building the fixture with one would hide it if it did.
    fn traded_tick(volume: Option<u32>, last_price: f64, last_trade_time: Option<u32>) -> KiteTick {
        KiteTick {
            instrument_token: 408_065,
            segment: ZerodhaSegment::Nse,
            tradable: true,
            mode: ZerodhaTickMode::Full,
            last_price,
            volume_traded: volume,
            last_trade_time,
            exchange_timestamp: Some(1_786_695_200),
            ..Default::default()
        }
    }

    // Precision 2/0 and a fixed `ts_init` throughout, so the only thing varying between calls is
    // the tick itself.
    fn observed(tracker: &mut TradeTracker, tick: &KiteTick) -> Option<TradeTick> {
        tracker
            .observe(tick, instrument_id(), 2, 0, UnixNanos::from(42))
            .expect("this fixture is representable as a trade")
    }

    // THE DISCRIMINATING TEST. Four snapshots arrive; the venue traded exactly once. Ticks 2 and 3
    // repeat the same cumulative volume while the last price and the book move around them --
    // which is what a real idle instrument looks like, measured on a live MCX socket on
    // 2026-08-14. A mapper that emits a TradeTick per tick passes every other test in this file
    // and produces THREE trades here, one per second, out of thin air.
    #[rstest]
    fn test_an_unchanged_volume_across_ticks_emits_no_trade() {
        let mut tracker = TradeTracker::new();

        let idle = traded_tick(Some(5_000), 7811.0, Some(1_786_695_100));
        let idle_reprice = traded_tick(Some(5_000), 7814.0, Some(1_786_695_100));
        let traded = traded_tick(Some(5_010), 7815.0, Some(1_786_695_204));

        let first = observed(&mut tracker, &idle);
        let second = observed(&mut tracker, &idle);
        let third = observed(&mut tracker, &idle_reprice);
        let fourth = observed(&mut tracker, &traded);

        assert!(
            first.is_none(),
            "the first tick has no predecessor to difference against",
        );
        assert!(
            second.is_none(),
            "cumulative volume did not move, so nothing traded -- emitting here fabricates a trade",
        );
        assert!(
            third.is_none(),
            "a moving last_price with a FROZEN volume is a re-quote, not a trade",
        );
        assert_eq!(
            fourth.map(|t| t.size),
            Some(Quantity::new(10.0, 0)),
            "the one real trade must still come through -- an always-None mapper is not the fix",
        );
    }

    // The first snapshot's `volume_traded` is the session total accumulated BEFORE this client
    // connected. Publishing it would print the entire day's volume as one trade at the current
    // price.
    #[rstest]
    fn test_the_first_tick_for_an_instrument_only_sets_the_baseline() {
        let mut tracker = TradeTracker::new();

        let baseline = traded_tick(Some(2_400_000), 100.0, Some(1_786_695_100));
        let advanced = traded_tick(Some(2_400_025), 100.5, Some(1_786_695_201));

        let first = observed(&mut tracker, &baseline);
        let second = observed(&mut tracker, &advanced);

        assert!(
            first.is_none(),
            "2.4M units did not trade in the instant we connected",
        );
        assert_eq!(
            second.expect("volume advanced").size,
            Quantity::new(25.0, 0),
            "only the volume that arrived while we were watching is ours to publish",
        );
    }

    // `last_traded_quantity` is the size of the LAST trade; the delta is the size of ALL of them.
    // Kite's snapshot batches whatever traded in the interval, so publishing the former loses
    // volume and the emitted sizes stop summing to the venue's session total.
    #[rstest]
    fn test_size_is_the_volume_delta_not_the_last_traded_quantity() {
        let mut tracker = TradeTracker::new();
        let baseline = KiteTick {
            last_traded_quantity: Some(3),
            ..traded_tick(Some(1_000), 100.0, Some(1_786_695_100))
        };
        let advanced = KiteTick {
            last_traded_quantity: Some(3),
            ..traded_tick(Some(1_025), 100.0, Some(1_786_695_201))
        };

        assert!(observed(&mut tracker, &baseline).is_none());
        let trade = observed(&mut tracker, &advanced).expect("volume advanced");

        assert_eq!(
            trade.size,
            Quantity::new(25.0, 0),
            "25 units traded across several prints; last_traded_quantity reports 3 and loses 22",
        );
    }

    // Session boundary. `900_000 - 5` is not a trade, and `5u32 - 900_000` is not a number: the
    // subtraction underflows, panicking in debug and wrapping to ~4.29 BILLION units in release.
    #[rstest]
    fn test_a_volume_decrease_is_read_as_a_session_reset() {
        let mut tracker = TradeTracker::new();

        let close_of_session = traded_tick(Some(900_000), 100.0, Some(1_786_695_100));
        let new_session = traded_tick(Some(5), 101.0, Some(1_786_781_500));
        let new_session_next = traded_tick(Some(9), 101.0, Some(1_786_781_505));

        assert!(observed(&mut tracker, &close_of_session).is_none());
        let after_reset = observed(&mut tracker, &new_session);
        let next = observed(&mut tracker, &new_session_next).expect("volume advanced");

        assert!(
            after_reset.is_none(),
            "a decrease means a new session; the only safe reading is to rebaseline",
        );
        assert_eq!(
            next.size,
            Quantity::new(4.0, 0),
            "the new session's baseline is 5, so 9 means 4 units traded -- not 9",
        );
    }

    // Two instruments share one socket and one tracker. A tracker keyed by nothing, or keyed by
    // instrument id when the feed speaks tokens, would difference one instrument's volume against
    // the other's.
    #[rstest]
    fn test_instruments_are_baselined_independently() {
        let mut tracker = TradeTracker::new();

        let reliance = traded_tick(Some(1_000), 100.0, Some(1_786_695_100));
        let other_first_tick = KiteTick {
            instrument_token: 884_737,
            ..traded_tick(Some(40), 250.0, Some(1_786_695_201))
        };
        let other_second_tick = KiteTick {
            instrument_token: 884_737,
            ..traded_tick(Some(46), 250.0, Some(1_786_695_202))
        };

        assert!(observed(&mut tracker, &reliance).is_none());
        let other_first = observed(&mut tracker, &other_first_tick);
        let other_second = observed(&mut tracker, &other_second_tick).expect("volume advanced");

        assert!(
            other_first.is_none(),
            "the second instrument's first tick is its own baseline, not a 960-unit sell-off",
        );
        assert_eq!(other_second.size, Quantity::new(6.0, 0));
    }

    // LTP and index packets carry no `volume_traded` at all -- and an index never trades. The
    // trade path must stay silent for them however many arrive.
    #[rstest]
    fn test_a_tick_without_volume_never_produces_a_trade() {
        let mut tracker = TradeTracker::new();
        let index = KiteTick {
            tradable: false,
            mode: ZerodhaTickMode::Ltp,
            ..traded_tick(None, 24_500.0, None)
        };

        assert!(observed(&mut tracker, &index).is_none());
        assert!(
            observed(&mut tracker, &index).is_none(),
            "with no volume field there is no evidence of a trade, on any tick",
        );
    }

    // `last_trade_time` is when the trade happened; `exchange_timestamp` is when the venue
    // published the snapshot that reports it. Zero is Kite's absent sentinel on both.
    #[rstest]
    #[case(Some(1_786_695_100), Some(1_786_695_200), 1_786_695_100 * NANOS_PER_SEC)]
    #[case(Some(0), Some(1_786_695_200), 1_786_695_200 * NANOS_PER_SEC)]
    #[case(None, Some(1_786_695_200), 1_786_695_200 * NANOS_PER_SEC)]
    #[case(None, None, 42)]
    #[case(Some(0), Some(0), 42)]
    fn test_ts_event_prefers_the_trade_time_over_the_snapshot_time(
        #[case] last_trade_time: Option<u32>,
        #[case] exchange_timestamp: Option<u32>,
        #[case] expected: u64,
    ) {
        let tick = KiteTick {
            exchange_timestamp,
            ..traded_tick(Some(1_025), 100.0, last_trade_time)
        };

        let trade = trade_tick_from(&tick, instrument_id(), 2, 0, 25, UnixNanos::from(42))
            .expect("a positively priced, positively sized trade");

        assert_eq!(
            trade.ts_event,
            UnixNanos::from(expected),
            "epoch zero is the ABSENT sentinel on both stamps; converting it would say 1970-01-01",
        );
    }

    // The venue supplies neither of these, so both are this crate's choice and both are asserted
    // against a stated format rather than against whatever the code happens to produce.
    #[rstest]
    fn test_aggressor_is_unknown_and_the_trade_id_is_token_and_cumulative_volume() {
        let tick = traded_tick(Some(1_025), 100.5, Some(1_786_695_201));

        let trade = trade_tick_from(&tick, instrument_id(), 2, 0, 25, UnixNanos::from(42))
            .expect("a positively priced, positively sized trade");

        assert_eq!(
            trade.aggressor_side,
            AggressorSide::NoAggressor,
            "the binary feed has no side flag; guessing from the mid is a plausible WRONG value",
        );
        assert_eq!(
            trade.trade_id,
            TradeId::new("408065-1025"),
            "token plus cumulative volume: unique on the socket and stable across a replay",
        );
        assert_eq!(trade.price, Price::new(100.5, 2));
        assert_eq!(trade.ts_init, UnixNanos::from(42));
        assert!(
            trade.trade_id.as_str().len() <= 36,
            "TradeId caps at 36 characters and two u32s plus a dash is at most 21",
        );
    }

    // A zero or negative traded price is not an error the venue reports -- it would simply flow
    // into bar aggregation and set a bar's low.
    #[rstest]
    #[case(0.0)]
    #[case(-5.0)]
    #[case(f64::NAN)]
    fn test_a_non_positive_last_price_is_rejected(#[case] last_price: f64) {
        let tick = traded_tick(Some(1_025), last_price, Some(1_786_695_201));

        let err = match trade_tick_from(&tick, instrument_id(), 2, 0, 25, UnixNanos::from(42)) {
            Ok(_) => panic!("a trade at {last_price} must not be published"),
            Err(e) => e.to_string(),
        };

        assert!(
            err.contains("last_price"),
            "the error should name the offending field; was: {err}",
        );
    }

    #[rstest]
    fn test_trade_tick_from_rejects_a_zero_size() {
        let tick = traded_tick(Some(1_025), 100.5, Some(1_786_695_201));

        assert!(
            trade_tick_from(&tick, instrument_id(), 2, 0, 0, UnixNanos::from(42)).is_err(),
            "TradeTick::new PANICS on a non-positive size; this must fail as an error instead",
        );
    }

    #[rstest]
    fn test_trade_tick_from_rejects_a_tick_with_no_volume() {
        let tick = traded_tick(None, 100.5, Some(1_786_695_201));

        let err = match trade_tick_from(&tick, instrument_id(), 2, 0, 25, UnixNanos::from(42)) {
            Ok(_) => panic!("no volume field means no trade evidence and no trade ID source"),
            Err(e) => e.to_string(),
        };

        assert!(
            err.contains("volume_traded"),
            "the error should name the missing field; was: {err}",
        );
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

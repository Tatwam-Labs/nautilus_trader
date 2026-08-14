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

//! Builds Nautilus instruments from Zerodha's instrument dump.
//!
//! [`KiteInstrument::to_details`] produces the token/id/precision triple the tick path needs; that
//! is deliberately the *minimum* to decode a tick and is not an instrument. A strategy that holds
//! only an [`InstrumentDetails`](crate::common::instruments::InstrumentDetails) can subscribe to an
//! option but cannot ask it for a strike, an expiry or a lot size, because nothing in the crate
//! ever put an [`InstrumentAny`] into the cache. This module is that conversion.
//!
//! # ⭐ `segment` is read BEFORE `instrument_type`, and that order is load-bearing
//!
//! The live NSE dump carries **136 rows whose `instrument_type` is `EQ` but which are indices**:
//!
//! ```text
//! tradingsymbol      instrument_type   segment    tick_size   lot_size
//! NIFTY 50           EQ                INDICES    0           0
//! NIFTY BANK         EQ                INDICES    0           0
//! NIFTY MIDCAP 100   EQ                INDICES    0           0
//! ```
//!
//! A mapping keyed on `instrument_type` alone would call all 136 of them equities. `segment` is
//! the only column that separates an index from a share, so it is checked first and it wins.
//!
//! # The mapping
//!
//! | condition | Nautilus |
//! |---|---|
//! | `segment == INDICES` | [`InstrumentAny::IndexInstrument`] |
//! | `instrument_type == EQ` | [`InstrumentAny::Equity`] |
//! | `instrument_type == FUT` | [`InstrumentAny::FuturesContract`] |
//! | `instrument_type == CE` | [`InstrumentAny::OptionContract`] with [`OptionKind::Call`] |
//! | `instrument_type == PE` | [`InstrumentAny::OptionContract`] with [`OptionKind::Put`] |
//!
//! Any other `instrument_type` is an **error naming the type**, not a default: silently mapping an
//! unrecognised type onto the nearest variant would put a tradable-looking instrument in the cache
//! for something that cannot be traded.
//!
//! # ⭐ Order quantity is in UNITS, so the multiplier is 1 and `lot_size` carries the lot
//!
//! Zerodha's order `quantity` field is a number of *underlying units*, constrained to a multiple
//! of the instrument's lot size — one lot of NIFTY options is sent as `quantity: 65`, not `1`.
//! Nautilus computes notional as `quantity * price * multiplier`, so the multiplier must be `1`
//! here. With `65` it would value that order at 65x its real notional, and every margin and risk
//! check downstream would be wrong by that factor.
//!
//! Zerodha's `lot_size` therefore lands on Nautilus's `lot_size`, which is exactly what that field
//! means: the round-lot unit an order quantity must be a multiple of.
//!
//! This is the opposite of a CME-style venue (see the Interactive Brokers adapter), where order
//! quantity counts *contracts* and the multiplier converts contracts to underlying units. Copying
//! that convention here is the silent mis-sizing this module exists to avoid.
//!
//! # Known limitations
//!
//! - `size_increment` is fixed at `1` by the Nautilus constructors and cannot be set from here, so
//!   it does not express "orders must move in whole lots". `lot_size` and `min_quantity` do.
//! - The expiry time of day below is the NSE/BSE equity-segment close. MCX and CDS close at other
//!   times, so their expiry instants are early by the difference (see `EXPIRY_TIME_OF_DAY_UTC`).
//! - Index rows publish `tick_size = 0`, which Nautilus rejects on every instrument variant. See
//!   `INDEX_PRICE_INCREMENT` for what is substituted and why that is not a fabrication.

use nautilus_core::{Params, UnixNanos, datetime::iso8601_to_unix_nanos};
use nautilus_model::{
    enums::{AssetClass, OptionKind},
    identifiers::{InstrumentId, Symbol},
    instruments::{Equity, FuturesContract, IndexInstrument, InstrumentAny, OptionContract},
    types::{Currency, Price, Quantity},
};
use serde_json::Value;

use crate::http::parse::KiteInstrument;

/// The UTC time of day at which an Indian exchange contract stops trading on its expiry date.
///
/// # Why this is not midnight
///
/// The dump gives `expiry` as a bare `YYYY-MM-DD`, but Nautilus's `expiration_ns` is an *instant*.
/// NSE and BSE derivatives cease trading at **15:30 IST** on the expiry date. IST is a fixed
/// UTC+05:30 offset with no daylight saving, so 15:30 IST is `10:00:00` UTC on every date of every
/// year — there is no transition to track and no per-date arithmetic to get wrong.
///
/// Taking the date at face value would place expiry at 00:00 UTC, i.e. **15 hours 30 minutes
/// early**. Every expiry-day strategy would then see its own instrument as already expired before
/// the market opened, which is a silent no-op rather than a failure.
///
/// # What this constant does not cover
///
/// MCX commodity contracts and CDS currency contracts close at other times of day. Their expiry
/// instants are therefore early by the difference. That is a knowingly-accepted approximation:
/// the dump publishes no closing time, and being early by hours on a non-equity segment is a much
/// smaller error than being early by the whole trading day on every segment.
const EXPIRY_TIME_OF_DAY_UTC: &str = "10:00:00";

/// The number of characters in the venue's `expiry` format, `YYYY-MM-DD`.
///
/// The vendor client gates its own expiry coercion on exactly this length
/// (`kiteconnect` 5.2.0 `connect.py`: `if len(row["expiry"]) == 10`), so anything else is a shape
/// this crate has not seen and must not guess at.
const EXPIRY_DATE_LEN: usize = 10;

/// The Nautilus contract multiplier for every Zerodha instrument.
///
/// See the module docs: Zerodha order quantity is denominated in underlying units, so the
/// contracts-to-units conversion a multiplier normally performs has already happened.
const MULTIPLIER_UNITS: u32 = 1;

/// The value of the `segment` column that marks a row as an index rather than a tradable
/// instrument.
///
/// 136 rows of the live NSE dump carry this segment with `instrument_type = EQ`. They are indices.
const INDICES_SEGMENT: &str = "INDICES";

/// The price increment given to an index, whose `tick_size` the venue publishes as `0`.
///
/// # Why a substitution is needed at all
///
/// An index has no order book, so it has no minimum price increment — the `0` is the venue saying
/// exactly that, not a missing value. Nautilus nonetheless requires a **positive**
/// `price_increment` on every instrument variant, [`IndexInstrument`] included
/// (`check_positive_price` in its `new_checked`), so a value has to be supplied.
///
/// # Why `0.01` rather than an arbitrary number
///
/// NSE publishes index levels to two decimals (`NIFTY 50 = 24712.05`), so this is the granularity
/// at which the index actually moves. It is read off the venue's own quote format rather than
/// invented.
///
/// It also repairs a precision the row cannot supply: `decimals_in("0")` is `0`, so an index built
/// from its own `tick_size` text would carry precision `0` and round every level to whole rupees.
/// Because nothing prices an order off an index, this increment has no sizing or execution
/// consequence — unlike a substituted lot size would, which is why no lot size is ever
/// substituted here.
///
/// If the venue ever publishes a positive `tick_size` on an index row, that value is used instead.
const INDEX_PRICE_INCREMENT: &str = "0.01";

/// Maps a Zerodha exchange code to a Nautilus [`AssetClass`].
///
/// The dump has no asset-class column; the exchange code is the only classification it publishes.
/// Note that NSE index options (NIFTY, BANKNIFTY) land on [`AssetClass::Equity`] alongside single
/// stock options: they share the NFO exchange, and the only column that separates them is `name`,
/// which is a ticker rather than a classification. Enumerating index tickers here would be a list
/// that rots every time the exchange lists a new index.
///
/// An exchange this function has not seen falls back to [`AssetClass::Equity`], which is the
/// dominant case by row count. Asset class does not participate in sizing or pricing, so unlike an
/// unmapped `instrument_type` this is a safe default rather than a silent mis-trade.
fn asset_class_for_exchange(exchange: &str) -> AssetClass {
    match exchange {
        // Currency derivatives: the underlying is an FX rate (USDINR, EURINR).
        "CDS" | "BCD" => AssetClass::FX,
        // Commodity derivatives.
        "MCX" | "NCDEX" => AssetClass::Commodity,
        // NSE/BSE cash plus their derivative segments NFO/BFO.
        _ => AssetClass::Equity,
    }
}

impl KiteInstrument {
    /// Converts this dump row into a Nautilus instrument.
    ///
    /// `ts_init` is the time the dump was received. The dump carries no per-row timestamp, so
    /// `ts_event` is the same value: the venue tells us *what* an instrument is but never *when*
    /// that definition changed, and inventing a distinct `ts_event` would imply information the
    /// response does not contain.
    ///
    /// # Errors
    ///
    /// Returns an error if `instrument_type` is not one of `EQ`, `FUT`, `CE` or `PE`, or if the
    /// row's own fields cannot make a valid instrument (an empty identity, a non-positive tick
    /// size or lot size, a derivative with no expiry, or an option with no strike).
    pub fn to_instrument_any(&self, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        // `segment` is read FIRST and it wins. 136 rows of the live NSE dump carry
        // `instrument_type = EQ` with `segment = INDICES`; falling through to the match below
        // would call `NIFTY 50` an equity. See the module docs.
        if self.segment.trim() == INDICES_SEGMENT {
            return self.to_index_instrument(ts_init);
        }

        match self.instrument_type.trim() {
            "EQ" => self.to_equity(ts_init),
            "FUT" => self.to_futures_contract(ts_init),
            "CE" => self.to_option_contract(OptionKind::Call, ts_init),
            "PE" => self.to_option_contract(OptionKind::Put, ts_init),
            other => anyhow::bail!(
                "unmapped Zerodha `instrument_type` `{other}` on `{}` at `{}`; this adapter maps \
                 EQ, FUT, CE and PE",
                self.tradingsymbol,
                self.exchange,
            ),
        }
    }

    /// Validates the identity fields and returns the pair every constructor needs.
    ///
    /// [`KiteInstrument::instrument_id`] builds an [`InstrumentId`] through a panicking
    /// constructor, so the emptiness check has to happen *before* it rather than being left to
    /// Nautilus — a malformed row must surface as an error naming the row, not as a panic that
    /// takes down the whole dump.
    fn checked_identity(&self) -> anyhow::Result<(InstrumentId, Symbol)> {
        let symbol = self.tradingsymbol.trim();
        let exchange = self.exchange.trim();

        if symbol.is_empty() || exchange.is_empty() || !symbol.is_ascii() {
            anyhow::bail!(
                "cannot build an instrument identity from tradingsymbol `{}` and exchange `{}`; \
                 both must be non-empty and the symbol must be ASCII",
                self.tradingsymbol,
                self.exchange,
            );
        }

        Ok((self.instrument_id(), Symbol::from_str_unchecked(symbol)))
    }

    /// The underlying, as the venue's `name` column.
    ///
    /// Returned as a [`Symbol`] rather than the interned string the Nautilus constructors take,
    /// because `ustr` is not a dependency of this crate and `nautilus_model` does not re-export
    /// it. [`Symbol`] wraps exactly that interned string, so `.inner()` at the call site yields
    /// it without naming the type. `from_str_unchecked` is safe here only because the non-empty
    /// and ASCII checks in this function are the same two things Nautilus asserts about
    /// `underlying` — which it asserts by panicking.
    fn underlying_symbol(&self) -> anyhow::Result<Symbol> {
        let name = self.name.trim();

        if name.is_empty() || !name.is_ascii() {
            anyhow::bail!(
                "derivative `{}` has no usable underlying; its `name` column was `{}`, which must \
                 be non-empty and ASCII",
                self.tradingsymbol,
                self.name,
            );
        }

        Ok(Symbol::from_str_unchecked(name))
    }

    /// The tick size as a [`Price`], at the precision already derived from its text form.
    ///
    /// The precision is [`KiteInstrument::price_precision`], counted from the CSV text when the
    /// row was parsed. It is not recomputed from the `f64`: `0.0025` has no exact binary form, so
    /// a round-trip through the float is a guess where the dump held the answer.
    fn price_increment(&self) -> anyhow::Result<Price> {
        if self.tick_size <= 0.0 || !self.tick_size.is_finite() {
            anyhow::bail!(
                "`tick_size` is {} for `{}`; a price increment must be positive and finite",
                self.tick_size,
                self.tradingsymbol,
            );
        }

        Price::new_checked(self.tick_size, self.price_precision).map_err(|e| {
            anyhow::anyhow!(
                "`tick_size` {} at precision {} is not a valid price for `{}`: {e}",
                self.tick_size,
                self.price_precision,
                self.tradingsymbol,
            )
        })
    }

    /// The venue's lot size as a [`Quantity`], at precision `0`.
    ///
    /// Indian equity and derivative quantities are whole units and the venue publishes `lot_size`
    /// as an integer, so there is no fractional case to carry.
    fn lot_quantity(&self) -> anyhow::Result<Quantity> {
        if self.lot_size == 0 {
            anyhow::bail!(
                "`lot_size` is zero for `{}`; Nautilus requires a positive lot size, and an order \
                 sized from a zero lot could never reach the venue",
                self.tradingsymbol,
            );
        }

        Ok(Quantity::from(self.lot_size))
    }

    /// The expiry date turned into the instant the contract stops trading.
    ///
    /// See [`EXPIRY_TIME_OF_DAY_UTC`] for why this is not midnight.
    fn expiration_ns(&self) -> anyhow::Result<UnixNanos> {
        let date = self.expiry.trim();

        if date.len() != EXPIRY_DATE_LEN {
            anyhow::bail!(
                "derivative `{}` has expiry `{}`, which is not the venue's `YYYY-MM-DD` shape; a \
                 contract with no expiry date cannot be given an expiration instant",
                self.tradingsymbol,
                self.expiry,
            );
        }

        iso8601_to_unix_nanos(&format!("{date}T{EXPIRY_TIME_OF_DAY_UTC}Z"))
    }

    /// Carries the Zerodha instrument token across into the Nautilus instrument.
    ///
    /// The token is the only handle the WebSocket feed accepts, and no Nautilus instrument field
    /// holds a venue-specific integer. Without this the token is lost the moment a row becomes an
    /// [`InstrumentAny`], and the token registry could not be rebuilt from the cache.
    fn venue_info(&self) -> Params {
        let mut info = Params::new();
        info.insert(
            "instrument_token".to_string(),
            Value::from(self.instrument_token),
        );
        info
    }

    /// Builds the [`InstrumentAny::IndexInstrument`] case, for `segment = INDICES` rows.
    ///
    /// # Why an index is built rather than skipped
    ///
    /// Index spot is the primary input to every option strategy on this venue, and the Zerodha
    /// WebSocket feed publishes index ticks. Skipping these rows would mean a strategy could never
    /// name `NIFTY 50` at all. [`IndexInstrument`] is also the variant that cannot be mis-traded:
    /// it carries no lot size, no multiplier and no expiry, so none of the sizing fields an index
    /// has no answer for get a fabricated value.
    ///
    /// The zero `lot_size` these rows carry is therefore never read — which is the point. Routed
    /// to [`Self::to_equity`] instead, the same row would have failed on `lot_quantity`, with an
    /// error blaming the lot size rather than naming the real problem.
    fn to_index_instrument(&self, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        let (instrument_id, raw_symbol) = self.checked_identity()?;

        // The venue writes `tick_size = 0` for an index. Prefer a real tick if one ever appears,
        // and fall back to the documented substitute otherwise. Taking the precision from the
        // increment itself keeps `check_equal_u8(price_precision, price_increment.precision)`
        // true by construction rather than by two constants agreeing.
        let price_increment = if self.tick_size > 0.0 {
            self.price_increment()?
        } else {
            Price::from(INDEX_PRICE_INCREMENT)
        };

        Ok(InstrumentAny::IndexInstrument(IndexInstrument::new(
            instrument_id,
            raw_symbol,
            Currency::INR(),
            price_increment.precision,
            0, // size_precision
            price_increment,
            // An index has no size at all, but `check_positive_quantity` rejects a zero size
            // increment, so this is a neutral placeholder rather than a claim about tradable size.
            Quantity::from(1u32),
            None, // tick_scheme
            Some(self.venue_info()),
            ts_init, // ts_event
            ts_init,
        )))
    }

    /// Builds the [`InstrumentAny::Equity`] case.
    fn to_equity(&self, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        let (instrument_id, raw_symbol) = self.checked_identity()?;
        let lot_size = self.lot_quantity()?;

        Ok(InstrumentAny::Equity(Equity::new(
            instrument_id,
            raw_symbol,
            None, // isin -- the dump carries no ISIN column
            Currency::INR(),
            self.price_precision,
            self.price_increment()?,
            Some(lot_size),
            None, // max_quantity -- the dump publishes no freeze quantity
            Some(lot_size),
            None, // max_price
            None, // min_price
            None, // margin_init -- SPAN margins come from a different endpoint
            None, // margin_maint
            None, // maker_fee -- Zerodha brokerage is per-order, not a rate on notional
            None, // taker_fee
            None, // tick_scheme -- NSE ticks are uniform, so the flat increment is exact
            Some(self.venue_info()),
            ts_init, // ts_event -- the dump has no per-row event time
            ts_init,
        )))
    }

    /// Builds the [`InstrumentAny::FuturesContract`] case.
    fn to_futures_contract(&self, ts_init: UnixNanos) -> anyhow::Result<InstrumentAny> {
        let (instrument_id, raw_symbol) = self.checked_identity()?;
        let lot_size = self.lot_quantity()?;

        // Note the argument order differs from `OptionContract::new`: futures take `currency`
        // AFTER the two timestamps, options take it BEFORE them. Both are positional, so a
        // copy-paste between the two does not fail to compile -- it silently swaps the fields.
        //
        // `exchange` is left `None`: Nautilus documents it as an ISO 10383 MIC, and `NFO` is a
        // Zerodha code rather than a MIC. The venue is already carried by the instrument ID.
        Ok(InstrumentAny::FuturesContract(FuturesContract::new(
            instrument_id,
            raw_symbol,
            asset_class_for_exchange(self.exchange.trim()),
            None, // exchange
            self.underlying_symbol()?.inner(),
            UnixNanos::default(),
            self.expiration_ns()?,
            Currency::INR(),
            self.price_precision,
            self.price_increment()?,
            Quantity::from(MULTIPLIER_UNITS),
            lot_size,
            None, // max_quantity
            Some(lot_size),
            None, // max_price
            None, // min_price
            None, // margin_init
            None, // margin_maint
            None, // maker_fee
            None, // taker_fee
            None, // tick_scheme
            Some(self.venue_info()),
            ts_init, // ts_event
            ts_init,
        )))
    }

    /// Builds the [`InstrumentAny::OptionContract`] case.
    fn to_option_contract(
        &self,
        option_kind: OptionKind,
        ts_init: UnixNanos,
    ) -> anyhow::Result<InstrumentAny> {
        let (instrument_id, raw_symbol) = self.checked_identity()?;
        let lot_size = self.lot_quantity()?;

        if self.strike <= 0.0 || !self.strike.is_finite() {
            anyhow::bail!(
                "option `{}` has strike {}; the dump writes `0` for non-options, so a zero strike \
                 on a CE or PE row means the strike column was not populated",
                self.tradingsymbol,
                self.strike,
            );
        }

        // The strike is carried at the instrument's own price precision, so it is comparable with
        // quotes without a rescale. A venue strike finer than its tick size would round here, but
        // NSE strikes are whole rupees and the tick is 0.05.
        let strike_price = Price::new_checked(self.strike, self.price_precision).map_err(|e| {
            anyhow::anyhow!(
                "strike {} at precision {} is not a valid price for `{}`: {e}",
                self.strike,
                self.price_precision,
                self.tradingsymbol,
            )
        })?;

        Ok(InstrumentAny::OptionContract(OptionContract::new(
            instrument_id,
            raw_symbol,
            asset_class_for_exchange(self.exchange.trim()),
            None, // exchange -- see the futures case
            self.underlying_symbol()?.inner(),
            option_kind,
            strike_price,
            Currency::INR(),
            // The dump publishes no activation or listing date, so this is the epoch. That is the
            // one value which can never mark a live contract as "not yet active". Back-dating
            // activation by a fixed window from expiry -- the convention some adapters use -- can:
            // NSE lists weeklies a few weeks out but monthlies and long-dated series up to three
            // years out, so any single window is wrong for most of the chain. The same reasoning
            // applies to the futures case above.
            UnixNanos::default(),
            self.expiration_ns()?,
            self.price_precision,
            self.price_increment()?,
            Quantity::from(MULTIPLIER_UNITS),
            lot_size,
            None, // max_quantity
            // The smallest order the venue accepts is one lot, expressed in units.
            Some(lot_size),
            None, // max_price
            None, // min_price
            None, // margin_init
            None, // margin_maint
            None, // maker_fee
            None, // taker_fee
            None, // tick_scheme
            Some(self.venue_info()),
            ts_init, // ts_event
            ts_init,
        )))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::{identifiers::InstrumentId, instruments::Instrument, types::Money};
    use rstest::rstest;

    use super::*;
    use crate::http::parse::parse_instruments;

    const HEADER: &str = "instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,\
                          strike,tick_size,lot_size,instrument_type,segment,exchange";

    /// A NIFTY weekly call expiring 2026-08-28, lot 65, tick 0.05.
    const NIFTY_CE: &str =
        "12345,999,NIFTY26AUG24000CE,NIFTY,0,2026-08-28,24000,0.05,65,CE,NFO-OPT,NFO";
    /// The matching put.
    const NIFTY_PE: &str =
        "12346,999,NIFTY26AUG24000PE,NIFTY,0,2026-08-28,24000,0.05,65,PE,NFO-OPT,NFO";
    /// The NIFTY future on the same expiry.
    const NIFTY_FUT: &str = "13000,999,NIFTY26AUGFUT,NIFTY,0,2026-08-28,0,0.05,65,FUT,NFO-FUT,NFO";
    /// An NSE cash equity: no expiry, no strike, lot 1.
    const RELIANCE_EQ: &str = "408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE";
    /// A currency future: 4dp tick, lot 1000, on the CDS exchange.
    const USDINR_FUT: &str =
        "12345,999,USDINR26AUGFUT,USDINR,0,2026-08-27,0,0.0025,1000,FUT,CDS,CDS";
    /// A commodity future on MCX.
    const GOLD_FUT: &str = "20000,999,GOLD26AUGFUT,GOLD,0,2026-08-28,0,1,100,FUT,MCX-FUT,MCX";
    /// A live-dump index row, verbatim in shape: `instrument_type` is `EQ`, `segment` is
    /// `INDICES`, and BOTH the tick size and the lot size are zero.
    const NIFTY_50_INDEX: &str = "256265,1001,NIFTY 50,NIFTY 50,0,,0,0,0,EQ,INDICES,NSE";
    /// A second index row, to show the first is not a special case.
    const NIFTY_BANK_INDEX: &str = "260105,1002,NIFTY BANK,NIFTY BANK,0,,0,0,0,EQ,INDICES,NSE";

    /// 2026-08-28 15:30 IST as UNIX nanoseconds.
    ///
    /// Computed outside this crate (`TZ=UTC date -j -f '%Y-%m-%d %H:%M:%S' '2026-08-28 10:00:00'
    /// +%s` -> 1787911200, then x10^9), so nothing under test supplied it.
    const EXPIRY_2026_08_28_NS: u64 = 1_787_911_200_000_000_000;
    /// The same date at 00:00 UTC — what a midnight implementation would produce.
    const MIDNIGHT_2026_08_28_NS: u64 = 1_787_875_200_000_000_000;
    /// 2026-08-27 15:30 IST, derived the same independent way (1787824800 x10^9).
    const EXPIRY_2026_08_27_NS: u64 = 1_787_824_800_000_000_000;

    /// An arbitrary, fixed receipt time so no test depends on the wall clock.
    const TS_INIT: UnixNanos = UnixNanos::new(1_700_000_000_000_000_000);

    fn dump(rows: &[&str]) -> String {
        let mut out = String::from(HEADER);

        for line in rows {
            out.push('\n');
            out.push_str(line);
        }

        out
    }

    /// Parses a single fixture row into the venue-shaped struct.
    fn parse_row(csv_row: &str) -> KiteInstrument {
        let (instruments, skipped) = parse_instruments(&dump(&[csv_row])).expect("valid dump");

        assert_eq!(skipped, 0, "the fixture row must parse: {csv_row}");
        instruments.into_iter().next().expect("one instrument")
    }

    /// Converts a fixture row, expecting success.
    fn convert(csv_row: &str) -> InstrumentAny {
        parse_row(csv_row)
            .to_instrument_any(TS_INIT)
            .expect("the fixture row must convert")
    }

    fn as_option(csv_row: &str) -> OptionContract {
        let InstrumentAny::OptionContract(option) = convert(csv_row) else {
            panic!("a CE/PE row must map to an option contract: {csv_row}");
        };

        option
    }

    fn as_future(csv_row: &str) -> FuturesContract {
        let InstrumentAny::FuturesContract(future) = convert(csv_row) else {
            panic!("a FUT row must map to a futures contract: {csv_row}");
        };

        future
    }

    // THE DISCRIMINATING TEST FOR THE INDICES SEGMENT. These rows carry `instrument_type = EQ`, so
    // a mapping keyed on that column alone calls all 136 of them equities and passes every other
    // test in this file. `segment` is the only column that separates an index from a share.
    #[rstest]
    #[case(NIFTY_50_INDEX)]
    #[case(NIFTY_BANK_INDEX)]
    fn test_an_indices_row_is_not_an_equity_despite_instrument_type_eq(#[case] csv_row: &str) {
        let instrument = convert(csv_row);

        assert!(
            !matches!(instrument, InstrumentAny::Equity(_)),
            "`instrument_type` is EQ on this row, but `segment` is INDICES and an index is not a \
             share: {csv_row}",
        );
        assert!(
            matches!(instrument, InstrumentAny::IndexInstrument(_)),
            "an INDICES row must map to an index instrument, which carries no lot size, no \
             multiplier and no expiry to fabricate: {csv_row}",
        );
    }

    // The negative control for the check above: the segment test must not swallow real equities.
    #[rstest]
    fn test_a_cash_segment_row_is_still_an_equity() {
        assert!(
            matches!(convert(RELIANCE_EQ), InstrumentAny::Equity(_)),
            "RELIANCE is `instrument_type = EQ` on `segment = NSE`; only INDICES diverts",
        );
    }

    // Those same 136 rows carry `tick_size = 0`, which every Nautilus constructor rejects. An
    // index built from its own tick text would also take precision 0 and round NIFTY 50 from
    // 24712.05 to 24712.
    #[rstest]
    fn test_an_index_gets_a_positive_two_decimal_increment_despite_a_zero_tick_size() {
        let InstrumentAny::IndexInstrument(index) = convert(NIFTY_50_INDEX) else {
            panic!("an INDICES row must map to an index instrument");
        };

        assert_eq!(index.price_increment, Price::from("0.01"));
        assert_eq!(
            index.price_precision, 2,
            "NSE publishes index levels to two decimals; the row's own zero tick would give 0",
        );
        assert_eq!(index.currency, Currency::INR());
        assert_eq!(index.id, InstrumentId::from("NIFTY 50.NSE"));
    }

    // The zero lot size on an index row is never read, because an index has no lot. Routed to the
    // equity path the same row fails on `lot_quantity` -- with an error blaming the lot size
    // rather than naming the real problem.
    #[rstest]
    fn test_a_zero_lot_size_on_an_index_is_not_an_error() {
        assert!(
            parse_row(NIFTY_50_INDEX)
                .to_instrument_any(TS_INIT)
                .is_ok(),
            "an index legitimately has no lot size, so the zero must not be treated as malformed",
        );
    }

    #[rstest]
    fn test_an_equity_row_maps_to_the_equity_variant() {
        let InstrumentAny::Equity(equity) = convert(RELIANCE_EQ) else {
            panic!("an EQ row must map to an equity");
        };

        assert_eq!(equity.id, InstrumentId::from("RELIANCE.NSE"));
        assert_eq!(equity.currency, Currency::INR());
        assert_eq!(
            equity.lot_size,
            Some(Quantity::from(1u32)),
            "NSE cash trades in single shares, and that comes from the column, not a default",
        );
    }

    #[rstest]
    #[case(NIFTY_CE, OptionKind::Call)]
    #[case(NIFTY_PE, OptionKind::Put)]
    fn test_ce_and_pe_map_to_the_matching_option_kind(
        #[case] csv_row: &str,
        #[case] expected: OptionKind,
    ) {
        assert_eq!(
            as_option(csv_row).option_kind,
            expected,
            "the C/P in `instrument_type` is the only thing that decides call versus put",
        );
    }

    #[rstest]
    fn test_a_futures_row_maps_to_the_futures_contract_variant() {
        let future = as_future(NIFTY_FUT);

        assert_eq!(future.id, InstrumentId::from("NIFTY26AUGFUT.NFO"));
        assert_eq!(future.currency, Currency::INR());
        assert_eq!(future.underlying.as_str(), "NIFTY");
    }

    // THE DISCRIMINATING TEST FOR EXPIRY. A conversion that took the `YYYY-MM-DD` date at face
    // value would produce the midnight value, which every other test here would still pass. NSE
    // derivatives stop trading at 15:30 IST (= 10:00 UTC), so midnight is 15h30m early and would
    // make an expiry-day contract look dead before the open.
    #[rstest]
    #[case(NIFTY_CE, EXPIRY_2026_08_28_NS, MIDNIGHT_2026_08_28_NS)]
    #[case(NIFTY_FUT, EXPIRY_2026_08_28_NS, MIDNIGHT_2026_08_28_NS)]
    fn test_expiry_is_1530_ist_not_midnight(
        #[case] csv_row: &str,
        #[case] expected_ns: u64,
        #[case] midnight_ns: u64,
    ) {
        let expiration_ns = match convert(csv_row) {
            InstrumentAny::OptionContract(option) => option.expiration_ns,
            InstrumentAny::FuturesContract(future) => future.expiration_ns,
            other => panic!("expected an expiring instrument, was {other:?}"),
        };

        assert_eq!(
            expiration_ns,
            UnixNanos::from(expected_ns),
            "15:30 IST is 10:00 UTC on every date; IST has no daylight saving",
        );
        assert_ne!(
            expiration_ns,
            UnixNanos::from(midnight_ns),
            "midnight would expire the contract 15h30m early, on its most-traded day",
        );
        assert_eq!(
            expiration_ns.as_u64() - midnight_ns,
            10 * 3_600 * 1_000_000_000,
            "the gap from midnight must be exactly the 10-hour UTC offset of the 15:30 IST close",
        );
    }

    #[rstest]
    fn test_a_second_expiry_date_shifts_by_a_whole_day() {
        assert_eq!(
            as_future(USDINR_FUT).expiration_ns,
            UnixNanos::from(EXPIRY_2026_08_27_NS),
            "the date comes from the column; only the time of day is a constant",
        );
    }

    // THE DISCRIMINATING TEST FOR THE lot_size/multiplier DECISION. Nautilus values a position as
    // `quantity * price * multiplier`. Zerodha's order quantity is already in underlying UNITS
    // (one NIFTY lot is sent as 65), so the multiplier must be 1. Putting the venue's lot size in
    // `multiplier` -- the CME/IB convention -- passes every structural assertion in this file and
    // silently values this order at 65x.
    #[rstest]
    fn test_notional_is_units_times_price_not_multiplied_by_the_lot() {
        let instrument = convert(NIFTY_CE);
        let notional =
            instrument.calculate_notional_value(Quantity::from(65u32), Price::new(120.0, 2), None);

        assert_eq!(
            notional,
            Money::new(7_800.0, Currency::INR()),
            "one NIFTY lot at a premium of 120 is 65 x 120 = 7,800 INR; a multiplier of 65 would \
             report 507,000",
        );
    }

    #[rstest]
    #[case(NIFTY_CE, 65)]
    #[case(NIFTY_FUT, 65)]
    #[case(USDINR_FUT, 1_000)]
    #[case(RELIANCE_EQ, 1)]
    fn test_lot_size_is_read_from_the_column_never_assumed(
        #[case] csv_row: &str,
        #[case] expected: u32,
    ) {
        let instrument = convert(csv_row);

        assert_eq!(
            instrument.lot_size(),
            Some(Quantity::from(expected)),
            "NIFTY's lot is 65, not 75, and no lot may be hardcoded: {csv_row}",
        );
        assert_eq!(
            instrument.multiplier(),
            Quantity::from(1u32),
            "the multiplier is 1 for every Zerodha instrument; the lot lives in `lot_size`",
        );
    }

    #[rstest]
    #[case(NIFTY_CE)]
    #[case(NIFTY_FUT)]
    #[case(RELIANCE_EQ)]
    #[case(USDINR_FUT)]
    fn test_currency_is_inr_for_every_mapped_type(#[case] csv_row: &str) {
        assert_eq!(
            convert(csv_row).quote_currency(),
            Currency::INR(),
            "every instrument on an Indian exchange is quoted in INR: {csv_row}",
        );
    }

    // The precision is the one `KiteInstrument` already counted from the tick's text form, so a
    // 4dp currency tick survives and is not flattened to the 2dp that suits NSE equities.
    #[rstest]
    #[case(NIFTY_CE, 2, "0.05")]
    #[case(RELIANCE_EQ, 2, "0.05")]
    #[case(USDINR_FUT, 4, "0.0025")]
    #[case(GOLD_FUT, 0, "1")]
    fn test_price_increment_comes_from_tick_size(
        #[case] csv_row: &str,
        #[case] expected_precision: u8,
        #[case] expected_increment: &str,
    ) {
        let instrument = convert(csv_row);

        assert_eq!(instrument.price_precision(), expected_precision);
        assert_eq!(
            instrument.price_increment(),
            Price::from(expected_increment),
            "the increment is the venue's tick size at the venue's own precision: {csv_row}",
        );
    }

    #[rstest]
    fn test_the_strike_is_carried_at_the_instrument_precision() {
        let option = as_option(NIFTY_CE);

        assert_eq!(option.strike_price, Price::new(24_000.0, 2));
        assert_eq!(
            option.strike_price.precision, 2,
            "the strike must share the quote precision so it compares with quotes without a \
             rescale",
        );
    }

    #[rstest]
    #[case(NIFTY_CE, "NIFTY")]
    #[case(USDINR_FUT, "USDINR")]
    fn test_the_underlying_comes_from_the_name_column(
        #[case] csv_row: &str,
        #[case] expected: &str,
    ) {
        let underlying = match convert(csv_row) {
            InstrumentAny::OptionContract(option) => option.underlying,
            InstrumentAny::FuturesContract(future) => future.underlying,
            other => panic!("expected a derivative, was {other:?}"),
        };

        assert_eq!(
            underlying.as_str(),
            expected,
            "the trading symbol concatenates underlying, expiry and strike; `name` is the only \
             column that holds the underlying on its own",
        );
    }

    // The dump has no listing date. The epoch is chosen so that `activation_ns <= now` holds for
    // every row the venue published as tradable; a fixed window back from expiry would mark a
    // three-year-dated series as not yet active.
    #[rstest]
    fn test_activation_is_the_epoch_not_a_window_back_from_expiry() {
        let option = as_option(NIFTY_CE);

        assert_eq!(option.activation_ns, UnixNanos::from(0u64));
        assert!(
            option.activation_ns < option.expiration_ns,
            "activation must precede expiration for the contract to be tradable at all",
        );
    }

    #[rstest]
    #[case("NFO", AssetClass::Equity)]
    #[case("CDS", AssetClass::FX)]
    #[case("MCX", AssetClass::Commodity)]
    #[case("XYZ", AssetClass::Equity)]
    fn test_asset_class_follows_the_exchange_code(
        #[case] exchange: &str,
        #[case] expected: AssetClass,
    ) {
        assert_eq!(asset_class_for_exchange(exchange), expected);
    }

    #[rstest]
    #[case(NIFTY_FUT, AssetClass::Equity)]
    #[case(USDINR_FUT, AssetClass::FX)]
    #[case(GOLD_FUT, AssetClass::Commodity)]
    fn test_the_converted_instrument_carries_that_asset_class(
        #[case] csv_row: &str,
        #[case] expected: AssetClass,
    ) {
        assert_eq!(as_future(csv_row).asset_class, expected);
    }

    #[rstest]
    fn test_the_instrument_token_survives_conversion_in_info() {
        let option = as_option(NIFTY_CE);
        let info = option.info.as_ref().expect("info must be populated");

        assert_eq!(
            info.get_u64("instrument_token"),
            Some(12_345),
            "the token is the only handle the WebSocket feed accepts, and no Nautilus field holds \
             it -- losing it here would make the cache unusable for subscribing",
        );
    }

    #[rstest]
    fn test_both_timestamps_are_the_supplied_receipt_time() {
        let option = as_option(NIFTY_CE);

        assert_eq!(option.ts_init, TS_INIT);
        assert_eq!(
            option.ts_event, TS_INIT,
            "the dump carries no per-row event time, so inventing a distinct one would imply \
             information the response does not contain",
        );
    }

    #[rstest]
    fn test_min_quantity_is_one_whole_lot() {
        let option = as_option(NIFTY_CE);

        assert_eq!(
            option.min_quantity,
            Some(Quantity::from(65u32)),
            "the smallest order the venue accepts is one lot, expressed in units; the Nautilus \
             default of 1 would let a 1-unit order through to a venue that rejects it",
        );
    }

    #[rstest]
    #[case("INDICES")]
    #[case("COM")]
    #[case("ce")]
    fn test_an_unmapped_instrument_type_is_an_error_naming_it(#[case] instrument_type: &str) {
        let csv_row =
            format!("99,999,SOMETHING,SOMETHING,0,2026-08-28,0,0.05,1,{instrument_type},SEG,NSE");
        let err = match parse_row(&csv_row).to_instrument_any(TS_INIT) {
            Ok(built) => panic!("`{instrument_type}` must not map silently, was {built:?}"),
            Err(e) => e.to_string(),
        };

        assert!(
            err.contains(instrument_type),
            "the error must name the unmapped type so the schema change is identifiable; was: \
             {err}",
        );
    }

    #[rstest]
    #[case("13000,999,NIFTY26AUGFUT,NIFTY,0,,0,0.05,65,FUT,NFO-FUT,NFO")]
    #[case("12345,999,NIFTY26AUG24000CE,NIFTY,0,,24000,0.05,65,CE,NFO-OPT,NFO")]
    #[case("12345,999,NIFTY26AUG24000CE,NIFTY,0,2026-8-28,24000,0.05,65,CE,NFO-OPT,NFO")]
    fn test_a_derivative_without_a_usable_expiry_is_an_error(#[case] csv_row: &str) {
        assert!(
            parse_row(csv_row).to_instrument_any(TS_INIT).is_err(),
            "a derivative with no `YYYY-MM-DD` expiry cannot be given an expiration instant, and \
             defaulting one would invent a contract life: {csv_row}",
        );
    }

    #[rstest]
    fn test_an_option_with_a_zero_strike_is_an_error() {
        let csv_row = "12345,999,NIFTY26AUG24000CE,NIFTY,0,2026-08-28,0,0.05,65,CE,NFO-OPT,NFO";

        assert!(
            parse_row(csv_row).to_instrument_any(TS_INIT).is_err(),
            "the dump writes 0 for non-options, so a 0 on a CE row means the column was empty; \
             building the option anyway would put a strike-zero contract in the cache",
        );
    }

    #[rstest]
    #[case("408065,1594,RELIANCE,RELIANCE,0,,0,0.05,0,EQ,NSE,NSE")]
    #[case("12345,999,NIFTY26AUG24000CE,NIFTY,0,2026-08-28,24000,0.05,0,CE,NFO-OPT,NFO")]
    fn test_a_zero_lot_size_is_an_error_not_a_panic(#[case] csv_row: &str) {
        assert!(
            parse_row(csv_row).to_instrument_any(TS_INIT).is_err(),
            "Nautilus panics on a non-positive lot size, so this must be caught here and returned \
             as an error naming the row: {csv_row}",
        );
    }

    #[rstest]
    fn test_a_non_positive_tick_size_is_an_error_not_a_panic() {
        let csv_row = "408065,1594,RELIANCE,RELIANCE,0,,0,0,1,EQ,NSE,NSE";

        assert!(
            parse_row(csv_row).to_instrument_any(TS_INIT).is_err(),
            "a zero price increment fails Nautilus's own check by panic; it must surface as an \
             error naming the row instead",
        );
    }
}

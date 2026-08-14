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

//! Parsing for Zerodha's instrument dump.
//!
//! # This endpoint is the ONLY source of instrument tokens
//!
//! Not the WebSocket feed, not the historical catalog — both carry tokens but neither *publishes*
//! them. Everything in this crate that subscribes needs a token, so nothing streams until this
//! parses.
//!
//! # The column set, taken from the vendor client
//!
//! `kiteconnect` 5.2.0 `connect.py:855` reads the response with `csv.DictReader` and coerces five
//! fields, which is the closest thing to a schema the vendor publishes:
//!
//! ```text
//! instrument_token  -> int      exchange_token   tradingsymbol   name
//! last_price        -> float    expiry (date, ONLY when len == 10)
//! strike            -> float    tick_size -> float    lot_size -> int
//! instrument_type   segment     exchange
//! ```
//!
//! **The `expiry` coercion is conditional in the vendor client** — `if len(row["expiry"]) == 10`.
//! Equities carry an empty expiry, so a parser that treats the column as mandatory rejects the
//! majority of the dump.
//!
//! # ⭐ Price precision is DERIVED from `tick_size`, not configured
//!
//! `Price` is fixed-point and needs a precision. Taking it from `tick_size` means it is a property
//! of the instrument as the venue states it, rather than a constant that is right for NSE equities
//! (`0.05` → 2dp) and silently wrong for currency derivatives (`0.0025` → 4dp).
//!
//! **The precision is counted from the STRING, never from the parsed `f64`.** `0.0025` has no
//! exact binary representation, so recovering "4" from the float means formatting it back to
//! decimal and hoping the round-trip is faithful. The CSV already contains the decimal text; using
//! it is both simpler and exact.

use nautilus_model::identifiers::InstrumentId;

use crate::common::instruments::InstrumentDetails;

/// One row of the instrument dump, in the venue's own terms.
#[derive(Clone, Debug, PartialEq)]
pub struct KiteInstrument {
    /// The streaming instrument token.
    pub instrument_token: u32,
    /// The venue's trading symbol, e.g. `NIFTY24AUG24000CE`.
    pub tradingsymbol: String,
    /// The exchange the instrument trades on, e.g. `NSE`, `NFO`, `MCX`.
    pub exchange: String,
    /// `EQ`, `FUT`, `CE`, `PE`, …
    pub instrument_type: String,
    /// The minimum price increment, as the venue wrote it.
    pub tick_size: f64,
    /// Decimal places implied by `tick_size`, counted from its text form.
    pub price_precision: u8,
    /// The contract multiplier.
    pub lot_size: u32,
    /// The expiry as `YYYY-MM-DD`, empty for instruments that do not expire.
    pub expiry: String,
}

impl KiteInstrument {
    /// Builds the [`InstrumentId`] as `TRADINGSYMBOL.EXCHANGE`.
    ///
    /// The exchange is the venue, not the segment: an NFO option and an NSE equity can share a
    /// trading symbol prefix, and only the exchange separates them.
    #[must_use]
    pub fn instrument_id(&self) -> InstrumentId {
        InstrumentId::from(format!("{}.{}", self.tradingsymbol, self.exchange).as_str())
    }

    /// Converts to the registry's view.
    ///
    /// Size precision is `0`: Indian equity and derivative quantities are whole lots, and the
    /// venue expresses `lot_size` as an integer.
    #[must_use]
    pub fn to_details(&self) -> InstrumentDetails {
        InstrumentDetails {
            token: self.instrument_token,
            instrument_id: self.instrument_id(),
            price_precision: self.price_precision,
            size_precision: 0,
        }
    }
}

/// Counts the decimal places in a number's **text** form.
///
/// Trailing zeros are ignored, so `0.0500` yields 2 rather than 4 — the venue's formatting should
/// not change an instrument's precision.
///
/// Returns `0` for integers and for anything without a decimal point.
#[must_use]
pub fn decimals_in(text: &str) -> u8 {
    let Some((_, fraction)) = text.trim().split_once('.') else {
        return 0;
    };

    let significant = fraction.trim_end_matches('0');
    u8::try_from(significant.len()).unwrap_or(u8::MAX)
}

/// Parses the instrument dump CSV.
///
/// Rows that cannot be parsed are **skipped with a warning** rather than failing the batch. The
/// dump is ~100k rows covering every segment; one malformed row must not deny the system every
/// other instrument. The count of skipped rows is returned so a caller can tell "a few odd rows"
/// from "the schema changed".
///
/// # Errors
///
/// Returns an error if the header row is absent or does not carry the columns this parser needs.
pub fn parse_instruments(csv: &str) -> anyhow::Result<(Vec<KiteInstrument>, usize)> {
    let mut lines = csv.lines();
    let header = lines
        .next()
        .ok_or_else(|| anyhow::anyhow!("instrument dump is empty; expected a CSV header row"))?;

    let columns: Vec<&str> = header.split(',').map(str::trim).collect();
    let index_of = |name: &str| -> anyhow::Result<usize> {
        columns
            .iter()
            .position(|c| *c == name)
            .ok_or_else(|| anyhow::anyhow!("instrument dump has no `{name}` column; header was: {header}"))
    };

    // Resolved by NAME rather than by position. The vendor client uses `DictReader`, so column
    // order is not part of the contract and a positional parser would silently misread if the
    // venue reordered them -- reading a strike as a tick size, not failing.
    let (i_token, i_symbol, i_exchange, i_type, i_tick, i_lot, i_expiry) = (
        index_of("instrument_token")?,
        index_of("tradingsymbol")?,
        index_of("exchange")?,
        index_of("instrument_type")?,
        index_of("tick_size")?,
        index_of("lot_size")?,
        index_of("expiry")?,
    );
    let widest = [i_token, i_symbol, i_exchange, i_type, i_tick, i_lot, i_expiry]
        .into_iter()
        .max()
        .unwrap_or(0);

    let mut instruments = Vec::new();
    let mut skipped = 0usize;

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();
        if fields.len() <= widest {
            skipped += 1;
            continue;
        }

        let tick_text = fields[i_tick].trim();
        let parsed = (|| -> Option<KiteInstrument> {
            Some(KiteInstrument {
                instrument_token: fields[i_token].trim().parse().ok()?,
                tradingsymbol: fields[i_symbol].trim().to_string(),
                exchange: fields[i_exchange].trim().to_string(),
                instrument_type: fields[i_type].trim().to_string(),
                tick_size: tick_text.parse().ok()?,
                price_precision: decimals_in(tick_text),
                lot_size: fields[i_lot].trim().parse().ok()?,
                expiry: fields[i_expiry].trim().to_string(),
            })
        })();

        match parsed {
            Some(instrument) => instruments.push(instrument),
            None => skipped += 1,
        }
    }

    Ok((instruments, skipped))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const HEADER: &str = "instrument_token,exchange_token,tradingsymbol,name,last_price,expiry,\
                          strike,tick_size,lot_size,instrument_type,segment,exchange";

    fn dump(rows: &[&str]) -> String {
        let mut out = String::from(HEADER);
        for row in rows {
            out.push('\n');
            out.push_str(row);
        }
        out
    }

    #[rstest]
    #[case("0.05", 2)]
    #[case("0.0025", 4)]
    #[case("1", 0)]
    #[case("5", 0)]
    #[case("0.0500", 2)] // trailing zeros must not inflate precision
    #[case("0.10", 1)]
    #[case("0.000001", 6)]
    fn test_precision_is_counted_from_the_text(#[case] tick: &str, #[case] expected: u8) {
        assert_eq!(decimals_in(tick), expected);
    }

    // The reason precision is derived rather than configured: one constant cannot serve both.
    #[rstest]
    fn test_nse_equity_and_currency_derivative_get_different_precisions() {
        let csv = dump(&[
            "408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE",
            "12345,999,USDINR24AUGFUT,USDINR,0,2026-08-27,0,0.0025,1000,FUT,CDS,CDS",
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(skipped, 0);
        assert_eq!(instruments[0].price_precision, 2, "NSE equity ticks at 0.05");
        assert_eq!(
            instruments[1].price_precision, 4,
            "CDS ticks at 0.0025; a hardcoded 2dp would round every currency price",
        );
    }

    #[rstest]
    fn test_instrument_id_is_symbol_dot_exchange() {
        let csv = dump(&["12345,999,NIFTY24AUG24000CE,NIFTY,0,2026-08-28,24000,0.05,65,CE,NFO-OPT,NFO"]);
        let (instruments, _) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(
            instruments[0].instrument_id(),
            InstrumentId::from("NIFTY24AUG24000CE.NFO"),
            "the EXCHANGE disambiguates; the segment column does not",
        );
    }

    // Equities carry an empty expiry. The vendor client only parses the column when it is exactly
    // 10 characters, so a parser treating expiry as mandatory would reject most of the dump.
    #[rstest]
    fn test_an_empty_expiry_is_not_a_parse_failure() {
        let csv = dump(&["408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE"]);
        let (instruments, skipped) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(skipped, 0, "an equity with no expiry is normal, not malformed");
        assert!(instruments[0].expiry.is_empty());
    }

    // THE DISCRIMINATING TEST FOR COLUMN HANDLING. Columns are resolved by NAME, so a reordered
    // header still parses correctly. A positional parser passes every other test here and reads
    // the strike as a tick size on this one -- producing precision 0 rather than failing.
    #[rstest]
    fn test_columns_are_resolved_by_name_not_position() {
        let reordered = "tick_size,instrument_token,lot_size,expiry,exchange,tradingsymbol,\
                         instrument_type,strike,name,last_price,segment,exchange_token";
        let csv = format!("{reordered}\n0.0025,12345,1000,2026-08-27,CDS,USDINR24AUGFUT,FUT,0,USDINR,0,CDS,999");

        let (instruments, skipped) = parse_instruments(&csv).expect("reordered header is valid");

        assert_eq!(skipped, 0);
        assert_eq!(instruments[0].instrument_token, 12_345);
        assert_eq!(instruments[0].price_precision, 4);
        assert_eq!(instruments[0].lot_size, 1_000);
    }

    #[rstest]
    fn test_a_malformed_row_is_skipped_not_fatal() {
        let csv = dump(&[
            "408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE",
            "not-a-number,1594,BROKEN,BROKEN,0,,0,0.05,1,EQ,NSE,NSE",
            "short,row",
            "12345,999,TCS,TCS,0,,0,0.05,1,EQ,NSE,NSE",
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid header");

        assert_eq!(instruments.len(), 2, "good rows survive a bad neighbour");
        assert_eq!(skipped, 2, "the count is what distinguishes odd rows from a schema change");
    }

    #[rstest]
    fn test_a_missing_required_column_is_fatal() {
        let csv = "instrument_token,tradingsymbol,exchange\n1,X,NSE";
        let err = match parse_instruments(csv) {
            Ok(_) => panic!("a dump without tick_size cannot yield a usable precision"),
            Err(e) => e.to_string(),
        };

        assert!(err.contains("tick_size"), "the error should name the missing column; was: {err}");
    }

    #[rstest]
    fn test_an_empty_dump_is_an_error_not_an_empty_list() {
        assert!(
            parse_instruments("").is_err(),
            "an empty response is a failed fetch, and returning an empty list would look like a \
             venue with no instruments",
        );
    }

    #[rstest]
    fn test_to_details_carries_token_id_and_precision() {
        let csv = dump(&["12345,999,USDINR24AUGFUT,USDINR,0,2026-08-27,0,0.0025,1000,FUT,CDS,CDS"]);
        let (instruments, _) = parse_instruments(&csv).expect("valid dump");

        let details = instruments[0].to_details();
        assert_eq!(details.token, 12_345);
        assert_eq!(details.instrument_id, InstrumentId::from("USDINR24AUGFUT.CDS"));
        assert_eq!(details.price_precision, 4);
        assert_eq!(details.size_precision, 0);
    }
}

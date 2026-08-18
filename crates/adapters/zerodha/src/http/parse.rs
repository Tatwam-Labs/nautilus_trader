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
    /// The venue's `name` column, e.g. `NIFTY` for `NIFTY24AUG24000CE`.
    ///
    /// For a derivative this is the underlying's ticker, which is the only place in the dump the
    /// underlying appears — the trading symbol concatenates it with an expiry and a strike, and
    /// splitting that back apart needs a format the venue does not publish. For an equity it is
    /// the company name, and nothing treats it as an identifier.
    pub name: String,
    /// The exchange the instrument trades on, e.g. `NSE`, `NFO`, `MCX`.
    pub exchange: String,
    /// `EQ`, `FUT`, `CE`, `PE`, …
    pub instrument_type: String,
    /// The venue's segment, e.g. `NSE`, `NFO-OPT`, `NFO-FUT`, `INDICES`, `MCX-FUT`.
    ///
    /// ⚠️ **This is not redundant with `instrument_type`.** The live NSE dump carries 136 index
    /// rows — `NIFTY 50`, `NIFTY BANK` — whose `instrument_type` is `EQ` and whose `segment` is
    /// `INDICES`. `instrument_type` alone cannot tell an index from a share, so any consumer
    /// deciding what an instrument *is* must read this column too.
    pub segment: String,
    /// The option strike, `0` for every instrument that is not an option.
    pub strike: f64,
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
/// Strips RFC 4180 quoting from one CSV field, after trimming whitespace.
///
/// Kite quotes the `name` column on 106,683 of 114,851 rows. `.trim()` removes whitespace and
/// leaves the quotes, so an unquoted read yields `"NIFTY"` — **six characters, two of them
/// quotes** — which is non-empty and ASCII and therefore passes every downstream guard before
/// surfacing as `OptionContract.underlying`. A consumer filtering `underlying == "NIFTY"` then
/// rejects the entire chain and logs `registered 0 contracts`, which reads as *nothing to trade*.
///
/// Applied to every text field, not only `name`. Only `name` is quoted in today's dump, so the
/// rest are behaviour-neutral — and that is the point: the narrow fix leaves the identical trap
/// armed for the day Kite starts quoting `tradingsymbol`.
///
/// A doubled `""` inside a quoted field is the RFC 4180 escape for one literal quote.
fn unquote(field: &str) -> String {
    let trimmed = field.trim();
    match trimmed.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => trimmed.to_string(),
    }
}

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
    let (i_token, i_symbol, i_name, i_exchange, i_type, i_strike, i_tick, i_lot, i_expiry) = (
        index_of("instrument_token")?,
        index_of("tradingsymbol")?,
        index_of("name")?,
        index_of("exchange")?,
        index_of("instrument_type")?,
        index_of("strike")?,
        index_of("tick_size")?,
        index_of("lot_size")?,
        index_of("expiry")?,
    );
    let i_seg = index_of("segment")?;

    let mut instruments = Vec::new();
    let mut skipped = 0usize;

    for line in lines {
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.split(',').collect();

        // An EXACT match against the header, not merely "enough fields".
        //
        // # This is the guard against an embedded comma, and `<= widest` was not one
        //
        // The split is naive: it does not honour CSV quoting. **106,683 of the 114,851 rows in the
        // live dump are quoted** (measured 2026-08-14 across all nine exchanges), every one of them
        // in `name` — and **zero rows currently contain a comma inside those quotes**. So the
        // naive split is correct today, by luck rather than by design.
        //
        // 🔴 THIS COMMENT ONCE READ "`name` — a column this parser does not read". THAT WAS FALSE
        // IN THE COMMIT THAT WROTE IT: `name:` was already read twenty lines below, and it is the
        // sole source of `OptionContract.underlying`. The MEASUREMENT was right and the PREMISE
        // drawn from it was wrong, so the quoting question was waved off — and the quotes reached
        // a running paper node as `underlying == "\"NIFTY\""`, where an `== "NIFTY"` filter
        // discarded all 1,726 contracts and logged `registered 0 NIFTY option contracts`, which
        // reads as "nothing to trade today". Found by AT-V0.4-Code 2026-08-18.
        //
        // ⚠️ A FALSE REASSURANCE IS WORSE THAN NO COMMENT. No comment leaves a reader curious;
        // "a column this parser does not read" retired the question for four days. Before writing
        // that something is unused, grep for it.
        //
        // The day a `name` does contain a comma, that row gains a field and **everything after it
        // shifts**: `tick_size` would be read from `lot_size`, `instrument_type` from `segment`.
        // A length check of `<= widest` passes such a row — it has *more* than enough fields — and
        // the instrument parses with a plausible wrong precision and a wrong type. No error.
        //
        // Requiring the exact count converts that silent misread into a counted skip, which
        // `skipped` already surfaces to the caller. It is strictly better than a full CSV reader
        // for the risk that actually exists: no dependency, and it fails loudly on **any** shape
        // change rather than only on quoting.
        if fields.len() != columns.len() {
            skipped += 1;
            continue;
        }

        let tick_text = fields[i_tick].trim();
        let parsed = (|| -> Option<KiteInstrument> {
            Some(KiteInstrument {
                instrument_token: fields[i_token].trim().parse().ok()?,
                tradingsymbol: unquote(fields[i_symbol]),
                name: unquote(fields[i_name]),
                exchange: unquote(fields[i_exchange]),
                instrument_type: unquote(fields[i_type]),
                segment: unquote(fields[i_seg]),
                strike: {
                    // The venue writes `0` for everything that is not an option, but an empty
                    // cell says the same thing -- and equities are the majority of the dump, so
                    // treating a blank as unparseable would skip most of it.
                    let strike_text = fields[i_strike].trim();
                    if strike_text.is_empty() {
                        0.0
                    } else {
                        strike_text.parse().ok()?
                    }
                },
                tick_size: tick_text.parse().ok()?,
                price_precision: decimals_in(tick_text),
                lot_size: fields[i_lot].trim().parse().ok()?,
                expiry: unquote(fields[i_expiry]),
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
    // ⛔ THE REGRESSION TEST THAT DID NOT EXIST, AND WHOSE ABSENCE IS THE WHOLE STORY.
    //
    // Every fixture in this file and in `instruments.rs` writes `name` BARE, so
    // `test_the_underlying_comes_from_the_name_column` asserted "NIFTY" and passed against a
    // parser that returned `"\"NIFTY\""` from the real dump. The fixtures were written from a
    // reading of the format, so they CONFIRMED that reading rather than testing it — a fixture
    // built from your own understanding of a spec agrees with your misreading by construction.
    //
    // 106,683 of 114,851 live rows are quoted. Not one test row was.
    #[rstest]
    fn test_a_quoted_name_column_yields_an_unquoted_underlying() {
        let csv = dump(&[
            r#"12345,999,NIFTY2690125200PE,"NIFTY",0,2026-09-01,25200,0.05,65,PE,NFO-OPT,NFO"#,
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(skipped, 0);
        assert_eq!(instruments[0].name, "NIFTY", "quotes must not survive into `name`");
    }

    // Hardening: only `name` is quoted in today's dump, so these are behaviour-neutral NOW. That
    // is exactly why they are asserted — the narrow fix would leave the trap armed for the day
    // Kite starts quoting `tradingsymbol`, and nothing would fail until a filter silently emptied.
    #[rstest]
    fn test_quoting_is_stripped_from_every_text_column() {
        let csv = dump(&[
            r#"12345,999,"NIFTY2690125200PE","NIFTY",0,"2026-09-01",25200,0.05,65,"PE","NFO-OPT","NFO""#,
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(skipped, 0);
        let i = &instruments[0];
        assert_eq!(i.tradingsymbol, "NIFTY2690125200PE");
        assert_eq!(i.name, "NIFTY");
        assert_eq!(i.expiry, "2026-09-01");
        assert_eq!(i.instrument_type, "PE");
        assert_eq!(i.segment, "NFO-OPT");
        assert_eq!(i.exchange, "NFO");
    }

    // RFC 4180: a doubled `""` inside a quoted field is one literal quote. Unmeasured in the live
    // dump -- no row currently needs it -- so this pins the helper's behaviour rather than a
    // venue observation.
    #[rstest]
    fn test_an_escaped_quote_inside_a_quoted_field_survives_as_one_quote() {
        assert_eq!(unquote(r#""BHARTI ""AIRTEL""""#), r#"BHARTI "AIRTEL""#);
        assert_eq!(unquote("  NIFTY  "), "NIFTY");
        assert_eq!(unquote(r#""""#), "");
    }

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

    // The live NSE dump carries 136 rows whose `instrument_type` is EQ but whose `segment` is
    // INDICES -- `NIFTY 50`, `NIFTY BANK`. Both columns have to survive parsing, because
    // `instrument_type` alone cannot tell an index from a share.
    #[rstest]
    fn test_segment_and_instrument_type_are_both_carried() {
        let csv = dump(&[
            "256265,1001,NIFTY 50,NIFTY 50,0,,0,0,0,EQ,INDICES,NSE",
            "408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE",
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid dump");

        assert_eq!(skipped, 0, "an index row with a zero tick and zero lot still parses");
        assert_eq!(instruments[0].instrument_type, "EQ");
        assert_eq!(
            instruments[0].segment, "INDICES",
            "the segment is the only column that separates NIFTY 50 from a share",
        );
        assert_eq!(instruments[1].instrument_type, "EQ");
        assert_eq!(instruments[1].segment, "NSE");
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

    // THE DISCRIMINATING TEST FOR AN EMBEDDED COMMA. This is what a quoted `name` containing a
    // comma looks like once the naive split has run: one extra field, and everything after it
    // shifted. A `fields.len() <= widest` guard PASSES this row -- it has more than enough fields
    // -- and parses `tick_size` from the `lot_size` column, yielding precision 0 and instrument
    // type `NSE`. A plausible wrong value, with nothing raised.
    #[rstest]
    fn test_a_row_with_an_extra_field_is_skipped_not_misread() {
        let csv = dump(&[
            "408065,1594,RELIANCE,RELIANCE,0,,0,0.05,1,EQ,NSE,NSE",
            // `name` split by an embedded comma: 13 fields where the header declares 12.
            "12345,999,ACME,\"ACME LTD, INC\",0,,0,0.05,1,EQ,NSE,NSE",
        ]);

        let (instruments, skipped) = parse_instruments(&csv).expect("valid header");

        assert_eq!(skipped, 1, "the shifted row must be rejected, not parsed from the wrong columns");
        assert_eq!(instruments.len(), 1);
        assert_eq!(
            instruments[0].tradingsymbol, "RELIANCE",
            "the surviving row must be the intact one",
        );
    }

    #[rstest]
    fn test_a_missing_required_column_is_fatal() {
        // Every required column EXCEPT tick_size, so the error names the one under test
        // rather than whichever happens to be looked up first.
        let csv = "instrument_token,lot_size,expiry,exchange,tradingsymbol,instrument_type,\
strike,name,last_price,segment,exchange_token\n1,1,,NSE,X,EQ,0,X,0,NSE,1";
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

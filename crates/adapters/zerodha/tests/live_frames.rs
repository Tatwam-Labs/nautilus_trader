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

//! Decoder tests against **real bytes captured from the Zerodha WebSocket**.
//!
//! # How this differs from `decoder.rs`, and why it matters
//!
//! `decoder.rs` uses frames **constructed** from the published wire layout. That set has a
//! structural weakness which is worth stating exactly, because it is not merely "unverified":
//!
//! > **When you build the frame from your own reading of the spec, the frame agrees with the
//! > misreading by construction.** A transposed field order is encoded into both the fixture and
//! > the decoder, and every assertion passes.
//!
//! Those fixtures are therefore *self-confirming* about layout. This file removes that: the frames
//! are bytes Zerodha actually sent, captured before anything decoded them
//! (`test_data/captured-*.json`), and the expected values were derived from those bytes by
//! **`kiteconnect`, the vendor's own client, run by someone who did not write this decoder**
//! (`test_data/derived-fixtures-*.json`, which names the oracle and its version).
//!
//! # What a pass here does and does not mean
//!
//! **Does:** the decoder agrees with the vendor's own client on real packets spanning all five
//! layouts, including the 184-byte full-depth layout that constructed fixtures could never
//! validate.
//!
//! **Does not:** prove the oracle is right. If `kiteconnect` misreads a field, this file encodes
//! the same misreading, and the decoder would be "wrong" for agreeing with reality. That residue
//! is irreducible without vendor documentation or a third independent implementation.
//!
//! So the honest sentence is **"agrees with the vendor's client on N real packets"**, never
//! "venue fidelity verified".
//!
//! # What this corpus CANNOT discriminate, stated so a green run is not over-read
//!
//! **The 184-byte timestamp field mapping.** In every captured 184-byte packet,
//! `exchange_timestamp` and `last_trade_time` hold the **same value** — the trade happened in the
//! same second the frame was stamped. Both candidate offsets (44 and 60) therefore match both
//! fields, so the corpus confirms the *values* and **not** the field-to-offset mapping.
//!
//! **If this decoder had those two transposed, nothing here would fail.** The decoder reads
//! `last_trade_time` at 44 and `exchange_timestamp` at 60, which agrees with the reference client —
//! but those offsets were transcribed *from* that client, so the agreement is not independent
//! evidence. It is one source, read twice.
//!
//! The discriminating capture is a packet from a **quiet trade minute**, where the last trade is
//! older than the frame and the two values separate. Until such a frame is in the corpus, treat
//! 184-byte timestamp *mapping* as unverified even when this file is green.
//!
//! # A disagreement here is a RESULT, not a failure
//!
//! If one of these fails, do not adjust the decoder to match. Report the packet hex, both
//! readings, and the byte offsets in question. `kiteconnect` is one implementation reading real
//! bytes; it is not an authority.

use nautilus_zerodha::{
    common::enums::ZerodhaSegment,
    websocket::parse::parse_packet,
};
use serde_json::Value;

/// Tolerance for price comparison. Both sides divide an integer by a power of ten in `f64`, so
/// this guards the JSON formatting round-trip rather than the arithmetic.
const EPSILON: f64 = 1e-9;

fn fixtures() -> Value {
    let raw = include_str!("../test_data/derived-fixtures-2026-08-14.json");
    serde_json::from_str(raw).expect("derived fixtures are not valid JSON")
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "odd-length hex");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("invalid hex"))
        .collect()
}

/// Formats a disagreement so it can be adjudicated without re-running anything.
fn disagreement(field: &str, hex: &str, ours: String, theirs: String) -> String {
    format!(
        "\n  FIELD    {field}\n  DECODER  {ours}\n  ORACLE   {theirs}\n  PACKET   {hex}\n\
         \n  This is a RESULT, not necessarily a decoder bug. Report the packet, both readings and\
         \n  the offsets before changing either side.\n"
    )
}

fn close_enough(a: f64, b: f64) -> bool {
    (a - b).abs() < EPSILON
}

/// Reads an optional `u32` from the oracle's expected map.
///
/// Absence is asserted rather than skipped: an LTP packet that grew an `oi` field would otherwise
/// pass silently.
fn expect_opt_u32(actual: Option<u32>, expected: Option<&Value>, field: &str, hex: &str) {
    match expected {
        Some(v) => {
            let want = v
                .as_u64()
                .unwrap_or_else(|| panic!(
                    "{field} is not an integer in the fixture: {v}. Timestamps must be emitted as \
                     epoch SECONDS, not a rendered datetime string -- a string is host-timezone \
                     dependent and silently shifts by the deriving machine's UTC offset."
                ));
            assert_eq!(
                actual,
                Some(u32::try_from(want).expect("exceeds u32")),
                "{}",
                disagreement(field, hex, format!("{actual:?}"), format!("{want}")),
            );
        }
        None => assert_eq!(
            actual, None,
            "{}",
            disagreement(field, hex, format!("{actual:?}"), "absent".to_string()),
        ),
    }
}

#[test]
fn decoder_agrees_with_the_vendor_client_on_every_captured_packet() {
    let doc = fixtures();
    let records = doc["records"].as_array().expect("records must be an array");
    assert!(!records.is_empty(), "the fixture set is empty");

    let mut checked = 0usize;
    let mut lengths: Vec<usize> = Vec::new();

    for record in records {
        for pair in record["pairs"].as_array().expect("pairs must be an array") {
            let hex = pair["packet_hex"].as_str().expect("packet_hex");
            let packet = decode_hex(hex);
            let want = &pair["expected"];

            let tick = parse_packet(&packet)
                .unwrap_or_else(|e| panic!("{}", disagreement("decode", hex, e.to_string(), "decoded ok".into())));

            lengths.push(packet.len());
            checked += 1;

            assert_eq!(
                u64::from(tick.instrument_token),
                want["instrument_token"].as_u64().expect("instrument_token"),
                "{}", disagreement("instrument_token", hex, tick.instrument_token.to_string(), want["instrument_token"].to_string()),
            );
            assert_eq!(
                tick.tradable,
                want["tradable"].as_bool().expect("tradable"),
                "{}", disagreement("tradable", hex, tick.tradable.to_string(), want["tradable"].to_string()),
            );
            assert_eq!(
                tick.mode.to_string(),
                want["mode"].as_str().expect("mode"),
                "{}", disagreement("mode", hex, tick.mode.to_string(), want["mode"].to_string()),
            );

            let lp = want["last_price"].as_f64().expect("last_price");
            assert!(
                close_enough(tick.last_price, lp),
                "{}", disagreement("last_price", hex, tick.last_price.to_string(), lp.to_string()),
            );

            // OHLC. The FIELD ORDER differs between index and tradable layouts, and a
            // transposition is exactly what constructed fixtures cannot catch -- so this
            // comparison against the vendor's reading is the point of the whole file.
            match (tick.ohlc, want.get("ohlc")) {
                (Some(ohlc), Some(w)) => {
                    for (ours, key) in [
                        (ohlc.open, "open"),
                        (ohlc.high, "high"),
                        (ohlc.low, "low"),
                        (ohlc.close, "close"),
                    ] {
                        let theirs = w[key].as_f64().unwrap_or_else(|| panic!("ohlc.{key}"));
                        assert!(
                            close_enough(ours, theirs),
                            "{}", disagreement(&format!("ohlc.{key}"), hex, ours.to_string(), theirs.to_string()),
                        );
                    }
                    let ch = want["change"].as_f64().expect("change");
                    assert!(
                        close_enough(tick.change, ch),
                        "{}", disagreement("change", hex, tick.change.to_string(), ch.to_string()),
                    );
                }
                (None, None) => {}
                (a, w) => panic!(
                    "{}", disagreement("ohlc presence", hex, a.is_some().to_string(), w.is_some().to_string()),
                ),
            }

            expect_opt_u32(tick.last_traded_quantity, want.get("last_traded_quantity"), "last_traded_quantity", hex);
            expect_opt_u32(tick.volume_traded, want.get("volume_traded"), "volume_traded", hex);
            expect_opt_u32(tick.total_buy_quantity, want.get("total_buy_quantity"), "total_buy_quantity", hex);
            expect_opt_u32(tick.total_sell_quantity, want.get("total_sell_quantity"), "total_sell_quantity", hex);
            expect_opt_u32(tick.oi, want.get("oi"), "oi", hex);
            expect_opt_u32(tick.oi_day_high, want.get("oi_day_high"), "oi_day_high", hex);
            expect_opt_u32(tick.oi_day_low, want.get("oi_day_low"), "oi_day_low", hex);
            expect_opt_u32(tick.exchange_timestamp, want.get("exchange_timestamp"), "exchange_timestamp", hex);
            expect_opt_u32(tick.last_trade_time, want.get("last_trade_time"), "last_trade_time", hex);

            if let Some(avg) = want.get("average_traded_price").and_then(Value::as_f64) {
                let ours = tick.average_traded_price.expect("decoder produced no average_traded_price");
                assert!(
                    close_enough(ours, avg),
                    "{}", disagreement("average_traded_price", hex, ours.to_string(), avg.to_string()),
                );
            }

            // Five-deep depth: the densest part of the 184-byte layout and the only nested
            // structure in the protocol.
            match (tick.depth.as_ref(), want.get("depth")) {
                (Some(depth), Some(w)) => {
                    for (side, ours) in [("buy", &depth.buy), ("sell", &depth.sell)] {
                        let theirs = w[side].as_array().unwrap_or_else(|| panic!("depth.{side}"));
                        assert_eq!(
                            ours.len(), theirs.len(),
                            "{}", disagreement(&format!("depth.{side}.len"), hex, ours.len().to_string(), theirs.len().to_string()),
                        );
                        for (i, (a, b)) in ours.iter().zip(theirs).enumerate() {
                            assert_eq!(
                                u64::from(a.quantity), b["quantity"].as_u64().expect("quantity"),
                                "{}", disagreement(&format!("depth.{side}[{i}].quantity"), hex, a.quantity.to_string(), b["quantity"].to_string()),
                            );
                            assert_eq!(
                                u64::from(a.orders), b["orders"].as_u64().expect("orders"),
                                "{}", disagreement(&format!("depth.{side}[{i}].orders"), hex, a.orders.to_string(), b["orders"].to_string()),
                            );
                            let bp = b["price"].as_f64().expect("price");
                            assert!(
                                close_enough(a.price, bp),
                                "{}", disagreement(&format!("depth.{side}[{i}].price"), hex, a.price.to_string(), bp.to_string()),
                            );
                        }
                    }
                }
                (None, None) => {}
                (a, w) => panic!(
                    "{}", disagreement("depth presence", hex, a.is_some().to_string(), w.is_some().to_string()),
                ),
            }
        }
    }

    // Guard the corpus itself, not just the decoder: a fixture file that silently lost its
    // hardest layouts would otherwise pass this test with fewer packets and no complaint.
    lengths.sort_unstable();
    lengths.dedup();
    assert_eq!(
        lengths,
        vec![8, 28, 32, 44, 184],
        "the captured corpus no longer spans all five layouts (saw {lengths:?}); a subset passing \
         is NOT the same evidence, and the 184-byte depth layout is the one that matters most",
    );
    assert_eq!(checked, 20, "expected 20 captured packets, checked {checked}");
}

#[test]
fn the_184_byte_timestamp_mapping_is_known_to_be_uncovered() {
    // This test asserts a LIMITATION, so that the limitation cannot quietly disappear.
    //
    // If a future capture contains a 184-byte packet where exchange_timestamp and last_trade_time
    // DIFFER, this fails -- and that is good news: it means the corpus has finally gained the
    // power to discriminate a transposition of those two fields, and the module-level caveat
    // should be deleted rather than carried forward.
    let doc = fixtures();
    let separated: Vec<(u64, u64)> = doc["records"]
        .as_array()
        .expect("records")
        .iter()
        .flat_map(|r| r["pairs"].as_array().expect("pairs"))
        .filter(|p| p["packet_len"].as_u64() == Some(184))
        .filter_map(|p| {
            let e = &p["expected"];
            Some((e["exchange_timestamp"].as_u64()?, e["last_trade_time"].as_u64()?))
        })
        .filter(|(ts, ltt)| ts != ltt)
        .collect();

    assert!(
        separated.is_empty(),
        "A captured 184-byte packet now has exchange_timestamp != last_trade_time {separated:?}.\n\
         The corpus can finally discriminate the field-to-offset mapping for those two fields.\n\
         DELETE this test and the 'cannot discriminate' caveat in the module docs -- the gap is closed.",
    );
}

#[test]
fn captured_corpus_carries_no_decoded_values() {
    // The corpus must hold BYTES ONLY. If a decode ever gets stored beside the frames, every
    // fixture derived from it inherits that one reading and the circularity this whole exercise
    // removed quietly returns. Cheap to check, and the failure would otherwise be invisible.
    let raw = include_str!("../test_data/captured-2026-08-14-sensex-live.json");
    let doc: Value = serde_json::from_str(raw).expect("corpus is not valid JSON");

    for record in doc["records"].as_array().expect("records") {
        let keys: Vec<&str> = record.as_object().expect("record").keys().map(String::as_str).collect();
        for forbidden in ["reference_decoded", "expected", "last_price", "ohlc", "depth", "oi"] {
            assert!(
                !keys.contains(&forbidden),
                "the raw corpus contains a decoded field {forbidden:?}. It must hold bytes only -- \
                 see test_data/capture_live_frames.py, section 'Why no decoded values are recorded'",
            );
        }
    }
}

#[test]
fn sensex_index_token_decodes_as_a_real_captured_index_packet() {
    // 265 is fetched from the BSE instrument dump but STREAMS under the INDICES segment. The two
    // meanings of "segment" disagree here, and this is the one instrument where that is visible.
    // Asserted against a captured packet rather than a constructed one.
    let doc = fixtures();
    let found = doc["records"]
        .as_array()
        .expect("records")
        .iter()
        .flat_map(|r| r["pairs"].as_array().expect("pairs"))
        .find(|p| p["expected"]["instrument_token"].as_u64() == Some(265))
        .expect("no captured packet for SENSEX (token 265)");

    let tick = parse_packet(&decode_hex(found["packet_hex"].as_str().expect("packet_hex")))
        .expect("SENSEX packet failed to decode");

    assert_eq!(tick.segment, ZerodhaSegment::Indices);
    assert!(!tick.tradable, "an index must decode as non-tradable");
    assert!(tick.depth.is_none(), "an index packet carries no depth");
}

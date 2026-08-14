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

//! Fixture tests for the Zerodha binary tick decoder.
//!
//! The expected values in `test_data/fixtures.json` are the output of the **`kiteconnect` Python
//! reference client**, not of the decoder under test — see `test_data/generate_fixtures.py`. This
//! file walks those fixtures and asserts the Rust decoder reaches the same decoding.
//!
//! **What this can and cannot catch.** It catches a disagreement between the two implementations:
//! a transposed OHLC field, a wrong divisor, a bad offset. It cannot catch a misreading of the
//! wire format that both implementations share, because the frames are constructed from the
//! published layout rather than captured from a live socket. Only captured frames settle that.

use nautilus_zerodha::{
    common::enums::{ZerodhaSegment, ZerodhaTickMode},
    websocket::{parse::parse_binary, ZerodhaWsError},
};
use rstest::rstest;
use serde_json::Value;

/// Tolerance for price comparison.
///
/// Both sides divide an integer by a power of ten in `f64`, so the same operation should give
/// bit-identical results; the epsilon guards the formatting round-trip through JSON, not the
/// arithmetic.
const EPSILON: f64 = 1e-9;

fn load_fixtures() -> Value {
    let raw = include_str!("../test_data/fixtures.json");
    serde_json::from_str(raw).expect("fixtures.json is not valid JSON")
}

fn decode_hex(hex: &str) -> Vec<u8> {
    assert!(hex.len().is_multiple_of(2), "hex string has an odd length");
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("invalid hex"))
        .collect()
}

fn assert_close(actual: f64, expected: f64, case: &str, field: &str) {
    assert!(
        (actual - expected).abs() < EPSILON,
        "{case}: {field} — decoder gave {actual}, reference gave {expected}",
    );
}

/// Asserts an optional integer field matches the reference, including when it should be absent.
///
/// Absence is asserted, not skipped: an LTP packet that grew an `oi` field would otherwise pass.
fn assert_opt_u32(actual: Option<u32>, expected: Option<&Value>, case: &str, field: &str) {
    match expected {
        Some(v) => {
            let want = u32::try_from(v.as_u64().expect("expected an integer"))
                .expect("value exceeds u32");
            assert_eq!(actual, Some(want), "{case}: {field}");
        }
        None => assert_eq!(actual, None, "{case}: {field} should be absent"),
    }
}

#[test]
fn every_fixture_case_matches_the_reference_client() {
    let doc = load_fixtures();
    let cases = doc["cases"].as_array().expect("cases must be an array");
    assert!(!cases.is_empty(), "fixtures.json contains no cases");

    for case in cases {
        let name = case["name"].as_str().expect("case needs a name");
        let frame = decode_hex(case["frame_hex"].as_str().expect("case needs frame_hex"));
        let expected = case["expected"].as_array().expect("expected must be an array");

        let ticks = parse_binary(&frame).unwrap_or_else(|e| panic!("{name}: decode failed: {e}"));

        assert_eq!(
            ticks.len(),
            expected.len(),
            "{name}: decoded {} ticks, reference decoded {}",
            ticks.len(),
            expected.len(),
        );

        for (tick, want) in ticks.iter().zip(expected) {
            assert_eq!(
                u64::from(tick.instrument_token),
                want["instrument_token"].as_u64().expect("token"),
                "{name}: instrument_token",
            );
            assert_eq!(
                tick.tradable,
                want["tradable"].as_bool().expect("tradable"),
                "{name}: tradable",
            );
            assert_eq!(
                tick.mode.to_string(),
                want["mode"].as_str().expect("mode"),
                "{name}: mode",
            );
            assert_close(
                tick.last_price,
                want["last_price"].as_f64().expect("last_price"),
                name,
                "last_price",
            );

            match (tick.ohlc, want.get("ohlc")) {
                (Some(ohlc), Some(w)) => {
                    for (actual, key) in [
                        (ohlc.open, "open"),
                        (ohlc.high, "high"),
                        (ohlc.low, "low"),
                        (ohlc.close, "close"),
                    ] {
                        assert_close(actual, w[key].as_f64().expect(key), name, key);
                    }
                    assert_close(
                        tick.change,
                        want["change"].as_f64().expect("change"),
                        name,
                        "change",
                    );
                }
                (None, None) => {}
                (a, w) => panic!(
                    "{name}: ohlc presence disagrees — decoder {}, reference {}",
                    a.is_some(),
                    w.is_some(),
                ),
            }

            assert_opt_u32(
                tick.last_traded_quantity,
                want.get("last_traded_quantity"),
                name,
                "last_traded_quantity",
            );
            assert_opt_u32(tick.volume_traded, want.get("volume_traded"), name, "volume_traded");
            assert_opt_u32(
                tick.total_buy_quantity,
                want.get("total_buy_quantity"),
                name,
                "total_buy_quantity",
            );
            assert_opt_u32(
                tick.total_sell_quantity,
                want.get("total_sell_quantity"),
                name,
                "total_sell_quantity",
            );
            assert_opt_u32(tick.oi, want.get("oi"), name, "oi");
            assert_opt_u32(tick.oi_day_high, want.get("oi_day_high"), name, "oi_day_high");
            assert_opt_u32(tick.oi_day_low, want.get("oi_day_low"), name, "oi_day_low");
            assert_opt_u32(
                tick.exchange_timestamp,
                want.get("exchange_timestamp"),
                name,
                "exchange_timestamp",
            );
            assert_opt_u32(
                tick.last_trade_time,
                want.get("last_trade_time"),
                name,
                "last_trade_time",
            );

            if let Some(w) = want.get("depth") {
                let depth = tick.depth.as_ref().unwrap_or_else(|| {
                    panic!("{name}: reference decoded depth, decoder did not")
                });
                for (side, entries) in [("buy", &depth.buy), ("sell", &depth.sell)] {
                    let w_side = w[side].as_array().expect("depth side");
                    assert_eq!(entries.len(), w_side.len(), "{name}: {side} depth levels");
                    for (i, (entry, w_entry)) in entries.iter().zip(w_side).enumerate() {
                        assert_eq!(
                            u64::from(entry.quantity),
                            w_entry["quantity"].as_u64().expect("quantity"),
                            "{name}: {side}[{i}].quantity",
                        );
                        assert_eq!(
                            u64::from(entry.orders),
                            w_entry["orders"].as_u64().expect("orders"),
                            "{name}: {side}[{i}].orders",
                        );
                        assert_close(
                            entry.price,
                            w_entry["price"].as_f64().expect("price"),
                            name,
                            &format!("{side}[{i}].price"),
                        );
                    }
                }
            } else {
                assert!(tick.depth.is_none(), "{name}: depth should be absent");
            }
        }
    }
}

#[test]
fn heartbeat_frame_decodes_to_no_ticks() {
    assert!(parse_binary(&[0x00]).expect("heartbeat must not error").is_empty());
    assert!(parse_binary(&[]).expect("empty frame must not error").is_empty());
}

#[rstest]
#[case::one_byte_short(7)]
#[case::between_layouts(30)]
#[case::past_the_largest(200)]
fn a_packet_length_with_no_layout_is_rejected(#[case] len: usize) {
    // A single packet of `len` bytes, correctly framed. The FRAME is well-formed; only the packet
    // length is unrecognised, so this isolates the layout check from the truncation check.
    let mut frame = vec![0x00, 0x01];
    frame.extend_from_slice(&u16::try_from(len).expect("len fits u16").to_be_bytes());
    frame.extend(std::iter::repeat_n(0u8, len));

    match parse_binary(&frame) {
        Err(ZerodhaWsError::UnknownPacketLength(got)) => assert_eq!(got, len),
        other => panic!("expected UnknownPacketLength({len}), got {other:?}"),
    }
}

#[test]
fn a_packet_length_running_past_the_frame_is_rejected() {
    // Declares one packet of 184 bytes but supplies 10. The reference client slices past the end
    // and yields a SHORT packet, which then decodes as a different mode than the venue sent.
    let mut frame = vec![0x00, 0x01, 0x00, 0xB8];
    frame.extend(std::iter::repeat_n(0u8, 10));

    assert!(
        matches!(parse_binary(&frame), Err(ZerodhaWsError::Truncated { .. })),
        "a declared length past the end of the frame must be an error, not a short packet",
    );
}

#[rstest]
#[case::nse(1, ZerodhaSegment::Nse, 100.0, true)]
#[case::nfo(2, ZerodhaSegment::Nfo, 100.0, true)]
#[case::cds(3, ZerodhaSegment::Cds, 10_000_000.0, true)]
#[case::bse(4, ZerodhaSegment::Bse, 100.0, true)]
#[case::bfo(5, ZerodhaSegment::Bfo, 100.0, true)]
#[case::bcd(6, ZerodhaSegment::Bcd, 10_000.0, true)]
#[case::mcx(7, ZerodhaSegment::Mcx, 100.0, true)]
#[case::indices(9, ZerodhaSegment::Indices, 100.0, false)]
#[case::undocumented(200, ZerodhaSegment::Unknown, 100.0, true)]
fn segment_is_decoded_from_the_low_byte_of_the_token(
    #[case] code: u32,
    #[case] expected: ZerodhaSegment,
    #[case] divisor: f64,
    #[case] tradable: bool,
) {
    let token = (4451 << 8) | code;
    let segment = ZerodhaSegment::from_instrument_token(token);
    assert_eq!(segment, expected);
    assert!((segment.price_divisor() - divisor).abs() < EPSILON);
    assert_eq!(segment.is_tradable(), tradable);
}

#[test]
fn sensex_index_token_is_in_the_indices_segment_not_bse() {
    // SENSEX is fetched from the `BSE` instrument dump over REST, but its STREAMING segment — the
    // low byte of the token — is INDICES, so it decodes as non-tradable. The two "segments" are
    // different things and 265 is where they visibly disagree.
    let segment = ZerodhaSegment::from_instrument_token(265);
    assert_eq!(segment, ZerodhaSegment::Indices);
    assert!(!segment.is_tradable());
}

#[test]
fn ltp_and_quote_modes_are_distinguished_by_packet_length_alone() {
    let doc = load_fixtures();
    let cases = doc["cases"].as_array().expect("cases");

    let modes: Vec<ZerodhaTickMode> = cases
        .iter()
        .filter_map(|c| {
            let frame = decode_hex(c["frame_hex"].as_str().expect("frame_hex"));
            parse_binary(&frame).ok()?.first().map(|t| t.mode)
        })
        .collect();

    // The fixture set must exercise every mode, or a mode-selection bug hides in an untested arm.
    for mode in [ZerodhaTickMode::Ltp, ZerodhaTickMode::Quote, ZerodhaTickMode::Full] {
        assert!(modes.contains(&mode), "no fixture exercises {mode:?}");
    }
}

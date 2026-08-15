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

//! Differential tests: the Rust order encoder against `kiteconnect`'s own output.
//!
//! # What makes this different from the unit tests in `src/http/orders.rs`
//!
//! Those tests assert that the encoder produces what **the author believed** the vendor client
//! produces, from reading `connect.py`. If the author misread it, the fixture agrees with the
//! misreading by construction and every test stays green. The suite is self-confirming.
//!
//! This file asserts against a corpus produced by **running** `kiteconnect` 5.2.0 with its
//! transport removed (`test_data/capture_vendor_requests.py`). No human transcribed it. That
//! removes the author from the chain, which is the whole point — it is the same argument
//! `capture_live_frames.py` makes one layer down for the tick decoder.
//!
//! # ⚠️ What this CANNOT show
//!
//! `kiteconnect` is the reference client, **not the exchange.** Agreement here means the Rust and
//! the vendor build the same request; it does not mean the venue accepts it. Both could share a
//! misreading of the API. Only a live call settles that, and for the order routes a live call
//! places a real order on a real account.
//!
//! It also cannot check the **URL**, and the reason is worth stating rather than papering over:
//! `place_order` builds its URL internally and is not callable without a client and a socket.
//! Asserting `format!("{base}/orders/{variety}")` here would re-state the expression under test
//! and could not fail. URL shape is the mock-server test's job, not this file's.
//!
//! # The numeric fields are compared BY VALUE, and that is deliberate
//!
//! `kiteconnect` hands Python floats to `requests`, which encodes them with `str()`. So the vendor
//! puts `price=1450.0` on the wire where this crate puts `price=1450.00`, taken from [`Price`]'s
//! own fixed-point rendering at the instrument's precision. Both parse to the same number.
//!
//! **Do not "fix" the Rust to match the vendor's spelling.** `str()` on a float is the lossy side:
//! `str(0.1 + 0.2)` is `'0.30000000000000004'`, and a 4dp CDS tick of `0.0025` has no exact binary
//! form. Rendering from `Price` is the same argument `http::parse` makes for counting precision
//! from the `tick_size` *text* rather than from a parsed `f64`.
//!
//! [`Price`]: nautilus_model::types::Price

use nautilus_zerodha::{
    common::enums::{
        ZerodhaExchange, ZerodhaOrderType, ZerodhaProduct, ZerodhaTransactionType, ZerodhaValidity,
        ZerodhaVariety,
    },
    http::orders::{ModifyOrderRequest, PlaceOrderRequest},
};
use rstest::rstest;
use serde_json::Value;

/// Prices agree to within this. They are decimal strings on both sides, so any real difference is
/// far larger than a float epsilon; this only absorbs the `1450.0` vs `1450.00` re-parse.
const PRICE_TOLERANCE: f64 = 1e-9;

fn corpus() -> Value {
    let raw = include_str!("../test_data/vendor-requests-2026-08-14.json");
    serde_json::from_str(raw).expect("the vendor request corpus is not valid JSON")
}

/// Returns one recorded request by case name.
fn case(name: &str) -> Value {
    let doc = corpus();
    let requests = doc["requests"]
        .as_array()
        .expect("corpus has no `requests` array")
        .clone();

    requests
        .into_iter()
        .find(|record| record["case"] == name)
        .unwrap_or_else(|| panic!("no recorded case named `{name}` in the corpus"))
}

/// The `data_key_order` list, which is the vendor's own body field order.
///
/// Read from the explicit list rather than from the `data` object's key order: a JSON object is
/// formally unordered and `serde_json::Map` is a `BTreeMap` unless `preserve_order` is enabled, so
/// reading the object would silently yield alphabetical order and assert nothing useful.
fn vendor_key_order(record: &Value) -> Vec<String> {
    record["data_key_order"]
        .as_array()
        .expect("recorded case has no `data_key_order`")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("a key name is not a string")
                .to_string()
        })
        .collect()
}

fn rust_key_order(pairs: &[(&'static str, String)]) -> Vec<String> {
    pairs.iter().map(|(key, _)| (*key).to_string()).collect()
}

/// Asserts every emitted value equals the vendor's, numerics by value and the rest by text.
fn assert_values_match(record: &Value, pairs: &[(&'static str, String)]) {
    let data = &record["data"];

    for (key, rust_value) in pairs {
        let vendor_value = &data[*key];
        assert!(
            !vendor_value.is_null(),
            "the Rust encoder emitted `{key}`, which the vendor's request does not carry",
        );

        if let Some(number) = vendor_value.as_f64() {
            let parsed: f64 = rust_value.parse().unwrap_or_else(|_| {
                panic!("`{key}` is numeric for the vendor but the Rust value `{rust_value}` is not")
            });
            assert!(
                (parsed - number).abs() < PRICE_TOLERANCE,
                "`{key}`: vendor sends {number}, Rust sends `{rust_value}`",
            );
            continue;
        }

        let vendor_text = vendor_value
            .as_str()
            .unwrap_or_else(|| panic!("`{key}` is neither a number nor a string in the corpus"));
        assert_eq!(
            rust_value, vendor_text,
            "`{key}`: vendor sends `{vendor_text}`, Rust sends `{rust_value}`",
        );
    }
}

/// The maximal place request, mirroring the `place_with_disclosed_quantity` case.
///
/// Note this deliberately pairs a `LIMIT` with a `trigger_price`, which the vendor client accepts
/// and [`PlaceOrderRequest::validate`] **refuses**. That is not a contradiction to fix: the venue
/// would take such an order and quietly ignore one of the two fields, which is exactly the class of
/// plausible-wrong value this crate errors on. The differential is about ENCODING, so it exercises
/// `to_form_pairs` directly and leaves `validate` to the unit tests.
fn maximal_place_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        variety: ZerodhaVariety::Regular,
        exchange: ZerodhaExchange::Nse,
        tradingsymbol: "RELIANCE".to_string(),
        transaction_type: ZerodhaTransactionType::Buy,
        quantity: 100,
        product: ZerodhaProduct::Cnc,
        order_type: ZerodhaOrderType::Limit,
        price: Some("1400.50".to_string()),
        trigger_price: Some("1390.00".to_string()),
        validity: Some(ZerodhaValidity::Day),
        disclosed_quantity: Some(10),
        tag: Some("O-002".to_string()),
    }
}

/// The minimal place request, mirroring `place_market_no_validity`.
fn minimal_place_request() -> PlaceOrderRequest {
    PlaceOrderRequest {
        variety: ZerodhaVariety::Regular,
        exchange: ZerodhaExchange::Nse,
        tradingsymbol: "RELIANCE".to_string(),
        transaction_type: ZerodhaTransactionType::Buy,
        quantity: 1,
        product: ZerodhaProduct::Cnc,
        order_type: ZerodhaOrderType::Market,
        price: None,
        trigger_price: None,
        validity: None,
        disclosed_quantity: None,
        tag: None,
    }
}

#[rstest]
fn test_the_corpus_names_its_oracle() {
    // A corpus that does not say what produced it cannot be re-derived when the vendor moves. The
    // crate's doc comments cite `kiteconnect` 5.2.0 by line number; if this ever fails, those
    // citations refer to a file that no longer says what they claim.
    let doc = corpus();

    assert_eq!(
        doc["oracle"].as_str(),
        Some("kiteconnect 5.2.0"),
        "the corpus was captured against a different vendor version than the crate documents",
    );
}

// ⭐ THE FIELD-ORDER TEST. This is the one the unit tests structurally cannot perform: they assert
// the order the author chose, against a fixture the author wrote. Here the order comes from running
// the vendor client.
#[rstest]
fn test_place_field_order_matches_the_vendor() {
    let record = case("place_with_disclosed_quantity");
    let pairs = maximal_place_request().to_form_pairs();

    assert_eq!(
        rust_key_order(&pairs),
        vendor_key_order(&record),
        "the encoder's field order diverged from kiteconnect's",
    );
}

#[rstest]
fn test_place_field_values_match_the_vendor() {
    let record = case("place_with_disclosed_quantity");
    let pairs = maximal_place_request().to_form_pairs();

    assert_values_match(&record, &pairs);
}

// ⭐ THE OMISSION TEST. `connect.py:364-366` deletes `None` keys before sending; it does not send
// them empty. An encoder that emits every field unconditionally produces a body that looks right
// and is not the vendor's shape, and no unit test written by the same author would notice.
#[rstest]
fn test_absent_optionals_produce_the_vendor_key_set() {
    let record = case("place_market_no_validity");
    let pairs = minimal_place_request().to_form_pairs();

    let rust_keys = rust_key_order(&pairs);
    let vendor_keys = vendor_key_order(&record);
    assert_eq!(
        rust_keys, vendor_keys,
        "a market order with no validity must carry exactly the vendor's keys, no more and no less",
    );
    assert!(
        !rust_keys.iter().any(|key| key == "price"),
        "an absent price must not appear at all: {rust_keys:?}",
    );
}

// ⭐ THE MODIFY-ORDER TEST. `quantity` precedes `price` in the vendor's dict because that is the
// order of its function signature. One field alone cannot show that, so the case that proves it
// sets both -- the encoder had only INFERRED the relative order from reading the signature.
#[rstest]
fn test_modify_field_order_matches_the_vendor() {
    let record = case("modify_quantity_and_price");
    let request = ModifyOrderRequest {
        variety: ZerodhaVariety::Regular,
        order_id: "240814000123456".to_string(),
        quantity: Some(130),
        price: Some("101.55".to_string()),
        order_type: None,
        trigger_price: None,
        validity: None,
        disclosed_quantity: None,
    };

    let pairs = request.to_form_pairs();
    assert_eq!(rust_key_order(&pairs), vendor_key_order(&record));
    assert_values_match(&record, &pairs);
}

#[rstest]
fn test_modify_carries_only_the_changed_field() {
    let record = case("modify_price_only");
    let request = ModifyOrderRequest {
        variety: ZerodhaVariety::Regular,
        order_id: "240814000123456".to_string(),
        quantity: None,
        price: Some("101.55".to_string()),
        order_type: None,
        trigger_price: None,
        validity: None,
        disclosed_quantity: None,
    };

    let pairs = request.to_form_pairs();
    assert_eq!(rust_key_order(&pairs), vendor_key_order(&record));
}

// ⭐ THE VOCABULARY TEST, and the reason it reads the constants from the corpus rather than listing
// them here: an enumeration is only as broad as its net. A hand-written list would cover the values
// the author thought of, which is the same population the unit tests already cover. The corpus
// takes them from the `KiteConnect` class, so it includes `SL-M`, `TTL`, `CO` and `auction` --
// values no case in the matrix exercises and the unit tests never round-trip.
#[rstest]
#[case("exchange")]
#[case("product")]
#[case("variety")]
#[case("transaction_type")]
#[case("order_type")]
#[case("validity")]
fn test_every_vendor_constant_round_trips(#[case] group: &str) {
    let doc = corpus();
    let constants = doc["vendor_constants"][group]
        .as_array()
        .unwrap_or_else(|| panic!("corpus has no vendor constants for `{group}`"))
        .clone();

    assert!(
        !constants.is_empty(),
        "`{group}` has no constants; an empty vocabulary would pass this test vacuously",
    );

    for constant in constants {
        let value = constant.as_str().expect("a constant is not a string");
        let round_tripped = match group {
            "exchange" => ZerodhaExchange::from_venue_str(value).map(ZerodhaExchange::as_str),
            "product" => ZerodhaProduct::from_venue_str(value).map(ZerodhaProduct::as_str),
            "variety" => ZerodhaVariety::from_venue_str(value).map(ZerodhaVariety::as_str),
            "transaction_type" => {
                ZerodhaTransactionType::from_venue_str(value).map(ZerodhaTransactionType::as_str)
            }
            "order_type" => ZerodhaOrderType::from_venue_str(value).map(ZerodhaOrderType::as_str),
            "validity" => ZerodhaValidity::from_venue_str(value).map(ZerodhaValidity::as_str),
            other => panic!("unhandled constant group `{other}`"),
        };

        let round_tripped = round_tripped.unwrap_or_else(|e| {
            panic!("the venue names `{value}` as a {group} and this crate cannot parse it: {e}")
        });
        assert_eq!(
            round_tripped, value,
            "`{value}` did not survive a round trip through the {group} enum",
        );
    }
}

// The status vocabulary is the ONE place the vendor SDK is a weaker oracle than the documentation,
// and this test exists to keep that visible rather than let it pass unremarked. The SDK names three
// statuses; `ZerodhaOrderStatus` models twelve. The other nine rest on the Kite Connect v3 docs and
// only a live order-book capture can confirm them.
#[rstest]
fn test_the_status_vocabulary_is_known_to_be_incomplete() {
    let doc = corpus();
    let statuses = doc["vendor_constants"]["status"]
        .as_array()
        .expect("corpus has no status constants");

    assert_eq!(
        statuses.len(),
        3,
        "the SDK gained or lost status constants; `ZerodhaOrderStatus`'s provenance note and the \
         nine documentation-only variants should be revisited",
    );
}

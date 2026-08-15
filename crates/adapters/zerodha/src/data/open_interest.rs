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

//! Open interest as Nautilus custom data.
//!
//! # Why this type exists at all
//!
//! [`QuoteTick`] has **no open-interest field**. Zerodha's 184-byte full packet carries OI at
//! offsets 48/52/56 and the decoder has read it since before this file existed — but a decoded
//! value with nowhere to go is not delivered. Carrying it to a strategy needs a type of its own.
//!
//! [`QuoteTick`]: nautilus_model::data::QuoteTick
//!
//! # This follows a pattern already shipping in-tree
//!
//! `BinanceFuturesOpenInterest` (`crates/adapters/binance/src/data_types.rs`) is the same data
//! concept, from the same producer type — an in-tree Rust `DataClient` — published on the same
//! `data_sender` rail as `Data::Custom`. This file is deliberately modelled on it rather than
//! invented, so a failure here means the rail is broken rather than that the type was built wrong.
//!
//! # ⚠️ WHAT THIS TYPE CANNOT DO, AND IT IS STRUCTURAL
//!
//! **It will never reach the `Cache`.** `DataEngine::handle_custom_data` publishes to a msgbus
//! topic and does not cache, and the `Cache` exposes roughly twenty *typed* `add_*` methods with no
//! generic slot — there is nowhere to put a custom type and no API by which to ask for one later.
//! That is a different and stronger statement than "it did not arrive".
//!
//! The consequence for callers: a strategy that **subscribes** receives OI and must hold its own
//! last value. A component that did not subscribe cannot ask for it, and nothing survives a
//! restart.

use std::sync::Arc;

use nautilus_core::UnixNanos;
use nautilus_model::{
    data::{HasTsInit, custom::CustomDataTrait},
    identifiers::InstrumentId,
};
use serde::{Deserialize, Serialize};

/// An open-interest snapshot from a Zerodha full-mode packet.
///
/// `u32` rather than a decimal: the venue sends these as big-endian unsigned 32-bit integers at
/// offsets 48/52/56, and open interest is a whole number of contracts. Widening it here would
/// invent precision the wire does not carry.
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.zerodha", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.zerodha")
)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ZerodhaOpenInterest {
    /// The instrument this snapshot belongs to.
    pub instrument_id: InstrumentId,
    /// Open interest, in contracts.
    pub open_interest: u32,
    /// The session's highest open interest so far.
    pub open_interest_day_high: u32,
    /// The session's lowest open interest so far.
    pub open_interest_day_low: u32,
    /// The venue's exchange timestamp, or the receipt time when the packet carried none.
    pub ts_event: UnixNanos,
    /// When this instance was constructed.
    pub ts_init: UnixNanos,
}

impl ZerodhaOpenInterest {
    /// Creates a new [`ZerodhaOpenInterest`] instance.
    #[must_use]
    pub const fn new(
        instrument_id: InstrumentId,
        open_interest: u32,
        open_interest_day_high: u32,
        open_interest_day_low: u32,
        ts_event: UnixNanos,
        ts_init: UnixNanos,
    ) -> Self {
        Self {
            instrument_id,
            open_interest,
            open_interest_day_high,
            open_interest_day_low,
            ts_event,
            ts_init,
        }
    }
}

impl HasTsInit for ZerodhaOpenInterest {
    fn ts_init(&self) -> UnixNanos {
        self.ts_init
    }
}

impl CustomDataTrait for ZerodhaOpenInterest {
    fn type_name(&self) -> &'static str {
        "ZerodhaOpenInterest"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn ts_event(&self) -> UnixNanos {
        self.ts_event
    }

    fn to_json(&self) -> anyhow::Result<String> {
        Ok(serde_json::to_string(self)?)
    }

    fn clone_arc(&self) -> Arc<dyn CustomDataTrait> {
        Arc::new(self.clone())
    }

    fn eq_arc(&self, other: &dyn CustomDataTrait) -> bool {
        if let Some(other) = other.as_any().downcast_ref::<Self>() {
            self == other
        } else {
            false
        }
    }

    #[cfg(feature = "python")]
    fn to_pyobject(&self, py: pyo3::Python<'_>) -> pyo3::PyResult<pyo3::Py<pyo3::PyAny>> {
        nautilus_model::data::custom::clone_pyclass_to_pyobject(self, py)
    }

    fn type_name_static() -> &'static str {
        "ZerodhaOpenInterest"
    }

    fn from_json(value: serde_json::Value) -> anyhow::Result<Arc<dyn CustomDataTrait>> {
        let json_str = serde_json::to_string(&value)?;
        let parsed: Self = serde_json::from_str(&json_str)?;
        Ok(Arc::new(parsed))
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::signal::Signal;
    use rstest::rstest;

    use super::*;

    fn oi() -> ZerodhaOpenInterest {
        ZerodhaOpenInterest::new(
            InstrumentId::from("CRUDEOIL26AUGFUT.MCX"),
            12_345,
            12_900,
            11_800,
            UnixNanos::from(1_786_728_495_000_000_000_u64),
            UnixNanos::from(1_786_728_495_100_000_000_u64),
        )
    }

    #[rstest]
    fn test_json_round_trips() {
        let original = oi();
        let json = original.to_json().expect("serialises");
        let value: serde_json::Value = serde_json::from_str(&json).expect("valid json");
        let restored = ZerodhaOpenInterest::from_json(value).expect("deserialises");

        assert!(
            original.eq_arc(restored.as_ref()),
            "a round trip must preserve the value; the msgbus serialiser depends on it",
        );
    }

    // `eq_arc` downcasts. A type that answered `true` for a *different* custom type would make two
    // unrelated data streams compare equal, which is worse than answering false.
    #[rstest]
    fn test_eq_arc_is_false_against_a_different_type() {
        let mine = oi();
        let other = Signal::new(
            "unrelated".into(),
            "1".into(),
            UnixNanos::default(),
            UnixNanos::default(),
        );

        assert!(!mine.eq_arc(&other), "distinct custom types must not compare equal");
    }

    #[rstest]
    fn test_ts_event_and_ts_init_are_not_conflated() {
        let oi = oi();

        assert_ne!(
            CustomDataTrait::ts_event(&oi),
            HasTsInit::ts_init(&oi),
            "the venue's event time and our receipt time are different clocks",
        );
    }
}

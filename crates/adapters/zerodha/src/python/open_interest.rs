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

//! Python getters for [`ZerodhaOpenInterest`].
//!
//! # ⚠️ WHY THIS FILE EXISTS — "delivered" and "usable" are different states
//!
//! `#[pyclass]` alone makes a type CROSS the boundary. It does **not** make any field readable.
//! Measured on 2026-08-15, before this file existed: a Python strategy received 98 open-interest
//! items, `type(payload).__name__` was `ZerodhaOpenInterest` — the right type, arriving reliably —
//! and **every single field read as `None`**:
//!
//! ```text
//! {'wrapper': 'CustomData', 'payload': 'ZerodhaOpenInterest',
//!  'instrument_id': '?', 'open_interest': None, 'day_high': None, 'day_low': None}
//! ```
//!
//! That is a third state between "arrived" and "did not arrive", and it is the dangerous one: a
//! carriage measurement that stops at "did the object arrive" reports SUCCESS for a stream that
//! carries no accessible data. The item is delivered and useless.
//!
//! Modelled on `BinanceFuturesOpenInterest`'s block in `binance/src/python/types.rs:491`, which
//! solves it the same way — a separate `#[pymethods]` impl with explicit `#[getter]`s. The
//! `#[pyclass]` attribute on the struct and the getters here are two different jobs, and copying
//! only the first is what produced the failure above.

use nautilus_model::identifiers::InstrumentId;
use pyo3::prelude::*;

use crate::data::open_interest::ZerodhaOpenInterest;

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaOpenInterest {
    #[getter]
    #[pyo3(name = "instrument_id")]
    const fn py_instrument_id(&self) -> InstrumentId {
        self.instrument_id
    }

    #[getter]
    #[pyo3(name = "open_interest")]
    const fn py_open_interest(&self) -> u32 {
        self.open_interest
    }

    #[getter]
    #[pyo3(name = "open_interest_day_high")]
    const fn py_open_interest_day_high(&self) -> u32 {
        self.open_interest_day_high
    }

    #[getter]
    #[pyo3(name = "open_interest_day_low")]
    const fn py_open_interest_day_low(&self) -> u32 {
        self.open_interest_day_low
    }

    /// The venue's exchange timestamp, as nanoseconds.
    ///
    /// `u64` rather than `UnixNanos` to match every other timestamp getter in the tree — Binance,
    /// Databento and the model types all hand Python a plain integer.
    #[getter]
    #[pyo3(name = "ts_event")]
    const fn py_ts_event(&self) -> u64 {
        self.ts_event.as_u64()
    }

    #[getter]
    #[pyo3(name = "ts_init")]
    const fn py_ts_init(&self) -> u64 {
        self.ts_init.as_u64()
    }

    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

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

//! Python bindings for Zerodha factory types.

use nautilus_model::{
    enums::AccountType,
    identifiers::{AccountId, TraderId},
};
use pyo3::prelude::*;

use crate::{
    common::consts::ZERODHA,
    factories::{ZerodhaDataClientFactory, ZerodhaExecutionClientFactory},
};

/// `name` is a METHOD, not a getter — that is a requirement, not a style choice.
///
/// `PyO3ClientRegistry::extract_factory` does `factory.getattr("name")?.call0(py)?`
/// (`crates/system/src/python/registry.rs:176-179`), so a `#[getter]` returning a `str` would fail
/// on the `call0`. Without this block the pyclass has no `name` at all and the registry cannot
/// resolve an extractor, which is the failure `tests/python.rs` surfaced.
#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaDataClientFactory {
    /// Factory for creating Zerodha data clients.
    #[new]
    fn py_new() -> Self {
        Self::new()
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ZERODHA
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaExecutionClientFactory {
    /// Factory for creating Zerodha execution clients.
    ///
    /// Unlike the data factory this is not a unit type. `ExecutionClientFactory::create` receives
    /// only `(name, config, cache)` — no trader or account identity — so those arrive here and are
    /// held until `create` is called. `bybit` carries the same shape for the same reason.
    ///
    /// `account_type` sits on the factory rather than the config because it is a Nautilus concept
    /// with no Zerodha field behind it: an Indian broker account holds cash and margin at once, and
    /// which one a node models is the operator's choice, not something derived from the venue.
    #[new]
    fn py_new(trader_id: TraderId, account_id: AccountId, account_type: AccountType) -> Self {
        Self::new(trader_id, account_id, account_type)
    }

    #[pyo3(name = "name")]
    fn py_name(&self) -> &'static str {
        ZERODHA
    }
}

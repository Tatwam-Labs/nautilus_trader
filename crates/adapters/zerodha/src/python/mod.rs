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

//! Python bindings from `pyo3`.

pub mod config;
pub mod factories;

use nautilus_common::factories::{ClientConfig, DataClientFactory};
use nautilus_core::python::{to_pyruntime_err, to_pyvalue_err};
use nautilus_system::get_global_pyo3_registry;
use pyo3::prelude::*;

use crate::{
    common::consts::ZERODHA, config::ZerodhaDataClientConfig, factories::ZerodhaDataClientFactory,
};

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_data_factory(
    py: Python<'_>,
    factory: Py<PyAny>,
) -> PyResult<Box<dyn DataClientFactory>> {
    match factory.extract::<ZerodhaDataClientFactory>(py) {
        Ok(f) => Ok(Box::new(f)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaDataClientFactory: {e}"
        ))),
    }
}

#[expect(clippy::needless_pass_by_value)]
fn extract_zerodha_data_config(
    py: Python<'_>,
    config: Py<PyAny>,
) -> PyResult<Box<dyn ClientConfig>> {
    match config.extract::<ZerodhaDataClientConfig>(py) {
        Ok(c) => Ok(Box::new(c)),
        Err(e) => Err(to_pyvalue_err(format!(
            "Failed to extract ZerodhaDataClientConfig: {e}"
        ))),
    }
}

/// Exposed through `nautilus_trader.adapters.zerodha`.
///
/// # No venue constants
///
/// Single-venue adapters export `<VENUE>`, `<VENUE>_CLIENT_ID` and `<VENUE>_VENUE`. Zerodha brokers
/// **both NSE and BSE over one connection**, so there is no single venue to name, and this follows
/// `interactive_brokers` — the other multi-venue broker — in exporting none of them. Per-instrument
/// venues come from the instrument definitions.
///
/// This is also enforced: `test_public_exports.py` fails any adapter outside its `VENUE_ADAPTERS`
/// map that exports a name ending in `_VENUE`.
///
/// # Errors
///
/// Returns an error if any bindings fail to register with the Python module.
#[pymodule]
pub fn zerodha(_: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<crate::common::enums::ZerodhaSegment>()?;
    m.add_class::<crate::common::enums::ZerodhaTickMode>()?;
    m.add_class::<ZerodhaDataClientConfig>()?;
    m.add_class::<ZerodhaDataClientFactory>()?;

    let registry = get_global_pyo3_registry();

    if let Err(e) =
        registry.register_factory_extractor(ZERODHA.to_string(), extract_zerodha_data_factory)
    {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha data factory extractor: {e}"
        )));
    }

    if let Err(e) = registry.register_config_extractor(
        "ZerodhaDataClientConfig".to_string(),
        extract_zerodha_data_config,
    ) {
        return Err(to_pyruntime_err(format!(
            "Failed to register Zerodha data config extractor: {e}"
        )));
    }

    Ok(())
}

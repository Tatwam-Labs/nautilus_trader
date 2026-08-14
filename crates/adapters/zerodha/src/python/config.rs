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

//! Python bindings for the Zerodha client configuration.
//!
//! The field getters are generated separately by `impl_pyo3_config_getters!` in
//! [`crate::config`], which deliberately exposes only the non-credential fields. Two
//! `#[pymethods]` blocks on one type are legal here because the workspace enables pyo3's
//! `multiple-pymethods` feature (`Cargo.toml:211`); `coinbase` is arranged the same way.

use pyo3::pymethods;

use crate::config::ZerodhaDataClientConfig;

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaDataClientConfig {
    /// Configuration for the Zerodha live data client.
    #[new]
    #[pyo3(signature = (
        api_key = None,
        access_token = None,
        base_url_http = None,
        base_url_ws = None,
        http_timeout_secs = None,
        ws_timeout_secs = None,
        update_instruments_interval_mins = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        access_token: Option<String>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_timeout_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            access_token,
            base_url_http,
            base_url_ws,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_timeout_secs: ws_timeout_secs.unwrap_or(defaults.ws_timeout_secs),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
        }
    }

    /// Routed through the hand-written `Debug`, which redacts both credential fields.
    ///
    /// `coinbase` returns a bare type name here. Going through `Debug` instead keeps the
    /// non-secret fields visible AND puts the Python repr behind the same redaction the
    /// `config::tests` regression tests already assert on — one guarded path rather than a second
    /// formatting surface that nothing checks.
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

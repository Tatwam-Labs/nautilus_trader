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

//! Configuration structures for the Zerodha adapter.

use serde::{Deserialize, Serialize};

use crate::common::consts::{ZERODHA_HTTP_URL, ZERODHA_WS_URL};

/// Configuration for the Zerodha data client.
#[derive(Debug, Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.zerodha", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.zerodha")
)]
pub struct ZerodhaDataClientConfig {
    /// The Kite Connect API key (falls back to the `ZERODHA_API_KEY` env var).
    pub api_key: Option<String>,
    /// The session access token from the daily login flow (falls back to `ZERODHA_ACCESS_TOKEN`).
    ///
    /// This expires each morning; it is not a long-lived secret.
    pub access_token: Option<String>,
    /// Override for the REST API base URL.
    pub base_url_http: Option<String>,
    /// Override for the WebSocket streaming URL.
    pub base_url_ws: Option<String>,
    /// HTTP timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// WebSocket timeout in seconds.
    #[builder(default = 30)]
    pub ws_timeout_secs: u64,
    /// Interval for refreshing instrument definitions, in minutes.
    ///
    /// Instrument tokens come only from the REST instrument dump — the historical data catalog
    /// carries no tokens — so this is the sole source and not a cache warmer.
    #[builder(default = 60)]
    pub update_instruments_interval_mins: u64,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ZerodhaDataClientConfig {
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_timeout_secs: u64,
    update_instruments_interval_mins: u64,
});

impl Default for ZerodhaDataClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ZerodhaDataClientConfig {
    /// Creates a new [`ZerodhaDataClientConfig`] with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns whether both credentials are populated and non-empty.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.api_key
            .as_deref()
            .is_some_and(|s| !s.trim().is_empty())
            && self
                .access_token
                .as_deref()
                .is_some_and(|s| !s.trim().is_empty())
    }

    /// Returns the REST API base URL, respecting any override.
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| ZERODHA_HTTP_URL.to_string())
    }

    /// Returns the WebSocket streaming URL, respecting any override.
    #[must_use]
    pub fn ws_url(&self) -> String {
        self.base_url_ws
            .clone()
            .unwrap_or_else(|| ZERODHA_WS_URL.to_string())
    }
}

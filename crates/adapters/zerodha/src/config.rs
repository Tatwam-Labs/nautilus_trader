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

use crate::common::{
    consts::{ZERODHA_HTTP_URL, ZERODHA_WS_URL},
    credential::ZerodhaCredential,
};

/// Configuration for the Zerodha data client.
///
/// [`Debug`] is implemented BY HAND to redact both credential fields. Deriving it would print a
/// live session token into any log line or error that formats a config — and one already does:
/// [`crate::factories::ZerodhaDataClientFactory::create`] formats `{config:?}` into its
/// wrong-config error. Deriving `Debug` here also leaked through
/// [`crate::data::ZerodhaDataClient`], which derives `Debug` over a `config` field.
///
/// `Serialize` is still derived, because round-tripping the config must preserve the values. The
/// distinction is deliberate: serialisation is asked for, formatting happens by accident.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
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

impl std::fmt::Debug for ZerodhaDataClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ZerodhaDataClientConfig))
            .field("api_key", &self.api_key.as_ref().map(|_| "***redacted***"))
            .field("access_token", &self.access_token.as_ref().map(|_| "***redacted***"))
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_timeout_secs", &self.ws_timeout_secs)
            .field(
                "update_instruments_interval_mins",
                &self.update_instruments_interval_mins,
            )
            .finish()
    }
}

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

    /// Resolves the credential pair from this config, falling back to the environment.
    ///
    /// Returns `None` unless **both** halves resolve — a key without a token cannot authenticate,
    /// so a partial pair is treated as absent rather than passed on to fail at the venue.
    #[must_use]
    pub fn credential(&self) -> Option<ZerodhaCredential> {
        ZerodhaCredential::resolve(self.api_key.as_deref(), self.access_token.as_deref())
    }

    /// Returns whether a complete credential pair is available.
    ///
    /// This consults the **process environment** as well as the config, because the fields above
    /// document an environment fallback. Checking only the struct fields would report "no
    /// credentials" for a correctly-configured environment-only deployment.
    ///
    /// **Therefore this is not a pure function of `self`.** A test asserting that a default config
    /// has no credentials will pass on a clean machine and fail on a developer's machine with
    /// `ZERODHA_API_KEY` exported. Tests that care must clear the variables explicitly.
    #[must_use]
    pub fn has_credentials(&self) -> bool {
        self.credential().is_some()
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const TOKEN: &str = "ldyr-live-session-token-value";
    const KEY: &str = "c5gabz-api-key-value";

    fn populated() -> ZerodhaDataClientConfig {
        ZerodhaDataClientConfig {
            api_key: Some(KEY.to_string()),
            access_token: Some(TOKEN.to_string()),
            ..ZerodhaDataClientConfig::default()
        }
    }

    #[rstest]
    fn test_debug_does_not_print_credentials() {
        // REGRESSION. This config derived `Debug` until 2026-08-14, and the crate formats a config
        // into an error string in `factories.rs` -- so a wrong-config error printed a live session
        // token. Found by review, not by any test, which is why this one exists.
        let rendered = format!("{:?}", populated());
        assert!(!rendered.contains(TOKEN), "access token leaked into Debug: {rendered}");
        assert!(!rendered.contains(KEY), "api key leaked into Debug: {rendered}");
        assert!(rendered.contains("redacted"), "redaction marker missing: {rendered}");
    }

    #[rstest]
    fn test_debug_of_the_client_does_not_print_credentials() {
        // The leak reached `Debug` on the CLIENT through its `config` field, which is the path the
        // client's own doc comment wrongly asserted was safe. Asserted at that level too, because
        // fixing the config alone would not have been visible here.
        let client = crate::data::ZerodhaDataClient::new(
            nautilus_model::identifiers::ClientId::from("ZERODHA-DEBUG-TEST"),
            populated(),
        )
        .expect("a fully populated config should construct");

        let rendered = format!("{client:?}");
        assert!(!rendered.contains(TOKEN), "access token leaked via the client: {rendered}");
        assert!(!rendered.contains(KEY), "api key leaked via the client: {rendered}");
    }

    #[rstest]
    fn test_debug_still_shows_the_non_secret_fields() {
        // Redaction that hides everything is unusable and invites someone to remove it. The
        // operational fields must survive.
        let rendered = format!("{:?}", populated());
        for field in ["http_timeout_secs", "ws_timeout_secs", "update_instruments_interval_mins"] {
            assert!(rendered.contains(field), "{field} missing from Debug: {rendered}");
        }
    }

    #[rstest]
    fn test_serialize_still_round_trips_the_credentials() {
        // Serialization must NOT be redacted -- the distinction is deliberate: serialising is asked
        // for, formatting happens by accident. If someone "fixes" serde the same way as Debug, a
        // persisted config would silently lose its credentials.
        let json = serde_json::to_string(&populated()).expect("config should serialize");
        let back: ZerodhaDataClientConfig =
            serde_json::from_str(&json).expect("config should deserialize");
        assert_eq!(back.access_token.as_deref(), Some(TOKEN));
        assert_eq!(back.api_key.as_deref(), Some(KEY));
    }
}

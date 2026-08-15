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

use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::common::{
    consts::{ZERODHA_HTTP_URL, ZERODHA_WS_URL},
    credential::ZerodhaCredential,
    enums::{ZerodhaProduct, ZerodhaVariety},
};

/// Configuration for the Zerodha data client.
///
/// [`Debug`] is implemented BY HAND to redact both credential fields. Deriving it would print a
/// live session token into any log line or error that formats a config — and one already does:
/// `ZerodhaDataClientFactory::create` formats `{config:?}` into its
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
    /// A captured frame corpus to replay INSTEAD of opening a socket.
    ///
    /// ⚠️ **When set, no socket is opened and no tick comes from the venue.** Ticks are decoded
    /// from the file by the same decoder the live feed uses, so everything from decode onward —
    /// token resolution, quote/trade/open-interest mapping, publication to the engine — is the
    /// identical code path.
    ///
    /// ⚠️ **THIS IS NOT AN OFFLINE MODE. It is an offline TICK SOURCE.** `connect()` still calls
    /// `load_instruments`, which is a REST fetch over the network, because token resolution needs
    /// the instrument dump and a frame corpus carries no instrument definitions. A run in replay
    /// mode measurably fetched 114,870 instruments.
    ///
    /// This doc previously said "the venue is never contacted", which was false. It is recorded
    /// rather than silently corrected because that sentence asserted a NETWORK-ISOLATION property
    /// the code does not have — a reader would reasonably have concluded the process touched
    /// nothing, and this is the API doc `cargo doc` renders and an IDE shows on hover.
    ///
    /// What it therefore CANNOT tell you: anything about auth, subscribe, mode, reconnect or the
    /// transport. A green replay is not a green session, and reporting one as the other would be
    /// the same error as reporting a passing test suite as a working feature.
    ///
    /// Exists because MCX is shut for most of the week and a carriage question should not have to
    /// wait for a market.
    pub replay_frames_path: Option<String>,
}

#[cfg(feature = "python")]
nautilus_core::impl_pyo3_config_getters!(ZerodhaDataClientConfig {
    base_url_http: Option<String>,
    base_url_ws: Option<String>,
    http_timeout_secs: u64,
    ws_timeout_secs: u64,
    update_instruments_interval_mins: u64,
    // Readable from Python on purpose: a caller inspecting a client needs to be able to SEE that
    // it is replaying rather than live. A field that changes whether the venue is contacted at all
    // should never be write-only.
    replay_frames_path: Option<String>,
});

impl Debug for ZerodhaDataClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ZerodhaDataClientConfig))
            .field("api_key", &self.api_key.as_ref().map(|_| "***redacted***"))
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "***redacted***"),
            )
            .field("base_url_http", &self.base_url_http)
            .field("base_url_ws", &self.base_url_ws)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("ws_timeout_secs", &self.ws_timeout_secs)
            .field(
                "update_instruments_interval_mins",
                &self.update_instruments_interval_mins,
            )
            // Shown in full and NOT redacted: it is a local file path, carries no secret, and is
            // the single field that changes whether this client contacts the venue at all. A
            // diagnostic that omitted it would let someone read a Debug dump of a replaying client
            // and conclude it was live.
            .field("replay_frames_path", &self.replay_frames_path)
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

/// Configuration for the Zerodha execution client.
///
/// [`Debug`] is implemented BY HAND for the same reason as [`ZerodhaDataClientConfig`]: the factory
/// formats `{config:?}` into its wrong-config error, and a derived `Debug` on a config holding an
/// access token puts a live session credential into that error string.
///
/// # ⭐ `default_product` has NO default, and that is the point
///
/// Zerodha's `product` decides margin and intraday square-off (see [`ZerodhaProduct`]). Nothing in
/// a Nautilus order carries it, so it has to come from configuration — and every candidate default
/// is wrong for somebody:
///
/// - `MIS` silently arms the broker to force-close every position around 15:20 IST.
/// - `CNC` silently demands full delivery margin and rejects a leveraged strategy for funds.
/// - `NRML` is meaningless on an equity segment.
///
/// So there is no default. A client configured without one fails to **construct**, which the live
/// node builder surfaces at build time — not at 09:15 on the first order.
///
/// A per-order override is available through `SubmitOrder.params["product"]`, so a strategy that
/// legitimately mixes products can say so per order without changing the account-wide setting.
///
/// # There is no Python binding on this type, deliberately
///
/// The data config carries `pyclass` attributes; this one does not. Exposing it would require
/// [`ZerodhaProduct`] and [`ZerodhaVariety`] to be `pyclass` enums as well, and adding a Python
/// surface for an execution path that has never placed an order is a decision to take separately
/// from writing the path.
#[derive(Clone, Serialize, Deserialize, bon::Builder)]
#[serde(default, deny_unknown_fields)]
pub struct ZerodhaExecClientConfig {
    /// The Kite Connect API key (falls back to the `ZERODHA_API_KEY` env var).
    pub api_key: Option<String>,
    /// The session access token from the daily login flow (falls back to `ZERODHA_ACCESS_TOKEN`).
    pub access_token: Option<String>,
    /// Override for the REST API base URL.
    pub base_url_http: Option<String>,
    /// HTTP timeout in seconds.
    #[builder(default = 10)]
    pub http_timeout_secs: u64,
    /// The margin and square-off regime every order is placed under unless a per-order
    /// `params["product"]` overrides it.
    ///
    /// **Required.** See the type docs for why this has no default.
    pub default_product: Option<ZerodhaProduct>,
    /// The order variety used for placement unless a per-order `params["variety"]` overrides it.
    ///
    /// Unlike `default_product` this *does* default, and the difference is not inconsistency:
    /// `regular` is the only variety whose placement, modification and cancellation parameter sets
    /// this adapter builds. `amo`, `co`, `iceberg` and `auction` each need fields the request types
    /// do not carry, so a client cannot silently end up using one.
    #[builder(default)]
    pub default_variety: ZerodhaVariety,
}

impl Debug for ZerodhaExecClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ZerodhaExecClientConfig))
            .field("api_key", &self.api_key.as_ref().map(|_| "***redacted***"))
            .field(
                "access_token",
                &self.access_token.as_ref().map(|_| "***redacted***"),
            )
            .field("base_url_http", &self.base_url_http)
            .field("http_timeout_secs", &self.http_timeout_secs)
            .field("default_product", &self.default_product)
            .field("default_variety", &self.default_variety)
            .finish()
    }
}

impl Default for ZerodhaExecClientConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ZerodhaExecClientConfig {
    /// Creates a new [`ZerodhaExecClientConfig`] with default settings.
    ///
    /// The result is **not usable as-is**: `default_product` is `None` and a client built from it
    /// returns an error. That is deliberate; see the type docs.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Resolves the credential pair from this config, falling back to the environment.
    ///
    /// Returns `None` unless **both** halves resolve.
    #[must_use]
    pub fn credential(&self) -> Option<ZerodhaCredential> {
        ZerodhaCredential::resolve(self.api_key.as_deref(), self.access_token.as_deref())
    }

    /// Returns the REST API base URL, respecting any override.
    #[must_use]
    pub fn http_url(&self) -> String {
        self.base_url_http
            .clone()
            .unwrap_or_else(|| ZERODHA_HTTP_URL.to_string())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::identifiers::ClientId;
    use rstest::rstest;

    use super::*;
    use crate::data::ZerodhaDataClient;

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
        assert!(
            !rendered.contains(TOKEN),
            "access token leaked into Debug: {rendered}"
        );
        assert!(
            !rendered.contains(KEY),
            "api key leaked into Debug: {rendered}"
        );
        assert!(
            rendered.contains("redacted"),
            "redaction marker missing: {rendered}"
        );
    }

    #[rstest]
    fn test_debug_of_the_client_does_not_print_credentials() {
        // The leak reached `Debug` on the CLIENT through its `config` field, which is the path the
        // client's own doc comment wrongly asserted was safe. Asserted at that level too, because
        // fixing the config alone would not have been visible here.
        let client = ZerodhaDataClient::new(ClientId::from("ZERODHA-DEBUG-TEST"), populated())
            .expect("a fully populated config should construct");

        let rendered = format!("{client:?}");
        assert!(
            !rendered.contains(TOKEN),
            "access token leaked via the client: {rendered}"
        );
        assert!(
            !rendered.contains(KEY),
            "api key leaked via the client: {rendered}"
        );
    }

    #[rstest]
    fn test_debug_still_shows_the_non_secret_fields() {
        // Redaction that hides everything is unusable and invites someone to remove it. The
        // operational fields must survive.
        let rendered = format!("{:?}", populated());

        for field in [
            "http_timeout_secs",
            "ws_timeout_secs",
            "update_instruments_interval_mins",
        ] {
            assert!(
                rendered.contains(field),
                "{field} missing from Debug: {rendered}"
            );
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

    fn populated_exec() -> ZerodhaExecClientConfig {
        ZerodhaExecClientConfig {
            api_key: Some(KEY.to_string()),
            access_token: Some(TOKEN.to_string()),
            default_product: Some(ZerodhaProduct::Nrml),
            ..ZerodhaExecClientConfig::default()
        }
    }

    // The exec config reaches the SAME wrong-config error path in `factories.rs` that leaked a
    // token from the data config, so it needs the same hand-written redaction rather than
    // inheriting the habit by assumption.
    #[rstest]
    fn test_exec_debug_does_not_print_credentials() {
        let rendered = format!("{:?}", populated_exec());

        assert!(
            !rendered.contains(TOKEN),
            "access token leaked into Debug: {rendered}"
        );
        assert!(
            !rendered.contains(KEY),
            "api key leaked into Debug: {rendered}"
        );
        assert!(
            rendered.contains("redacted"),
            "redaction marker missing: {rendered}"
        );
    }

    // The product is visible on purpose. It is not a secret, and it is the single field most worth
    // seeing in a log line, because it decides margin and intraday square-off.
    #[rstest]
    fn test_exec_debug_shows_the_product_and_variety() {
        let rendered = format!("{:?}", populated_exec());

        assert!(rendered.contains("Nrml"), "{rendered}");
        assert!(rendered.contains("Regular"), "{rendered}");
    }

    // THE DISCRIMINATING TEST FOR THE PRODUCT DEFAULT. A `#[builder(default)]` on this field would
    // pass every other test in this file and quietly pick a margin regime -- MIS would have the
    // broker force-close every position intraday, and nothing in the order response says so.
    #[rstest]
    fn test_the_default_config_has_no_product() {
        assert_eq!(
            ZerodhaExecClientConfig::default().default_product,
            None,
            "no product may be inferred; it decides leverage and intraday square-off",
        );
    }

    #[rstest]
    fn test_the_default_variety_is_regular() {
        assert_eq!(
            ZerodhaExecClientConfig::default().default_variety,
            ZerodhaVariety::Regular,
            "regular is the only variety whose parameter set this adapter builds",
        );
    }

    #[rstest]
    fn test_exec_config_round_trips_through_serde() {
        let json = serde_json::to_string(&populated_exec()).expect("config should serialize");
        let back: ZerodhaExecClientConfig =
            serde_json::from_str(&json).expect("config should deserialize");

        assert_eq!(back.access_token.as_deref(), Some(TOKEN));
        assert_eq!(back.default_product, Some(ZerodhaProduct::Nrml));
        assert_eq!(back.default_variety, ZerodhaVariety::Regular);
    }
}

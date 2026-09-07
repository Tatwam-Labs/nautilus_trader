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

//! The Zerodha Kite REST client.
//!
//! ⚠️ **This has never made a request.** The endpoint, the auth scheme and the response shape are
//! read from `kiteconnect` 5.2.0, not observed.
//!
//! # ⭐ REST authenticates by HEADER; the WebSocket authenticates by QUERY PARAMETER
//!
//! The two are different and conflating them is easy, because the same two secrets appear in both:
//!
//! | transport | how the credential travels | source |
//! |---|---|---|
//! | REST | `Authorization: token {api_key}:{access_token}` | `connect.py:944` |
//! | WebSocket | `?api_key=…&access_token=…` in the URL | `ticker.py:443` |
//!
//! A header on the socket or a query string on REST authenticates nothing, and the failure is a
//! 403 rather than anything that names the cause.
//!
//! # The dump is large and unpaginated
//!
//! `/instruments` returns **every instrument on every exchange** as a single CSV body — on the
//! order of 100k rows and several megabytes. There is no pagination and no filter beyond the
//! per-exchange variant, so requesting a single instrument is not possible: the only choices are
//! one exchange or all of them.

use std::collections::HashMap;

// `Method` comes from `nautilus_network`'s re-export rather than a direct `reqwest` dependency —
// no sibling adapter declares reqwest, and taking it from the shared crate keeps the version tied
// to whatever the transport itself uses.
use nautilus_network::http::{HttpClient, Method};

use crate::{
    common::credential::ZerodhaCredential,
    http::parse::{KiteInstrument, parse_instruments},
};

/// The Kite REST root, matching `kiteconnect` 5.2.0 `connect.py:36`.
pub const ZERODHA_HTTP_URL: &str = "https://api.kite.trade";

/// The API version header the vendor client sends (`connect.py:41`).
const KITE_VERSION: &str = "3";

/// Default request timeout. The instrument dump is several megabytes, so this is deliberately
/// longer than a normal REST timeout would be.
const DEFAULT_TIMEOUT_SECS: u64 = 60;

/// A minimal Kite REST client, covering what the data path needs.
#[derive(Debug)]
pub struct ZerodhaHttpClient {
    base_url: String,
    credential: ZerodhaCredential,
    client: HttpClient,
}

impl ZerodhaHttpClient {
    /// Creates a new client against `base_url` or the public endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be built.
    pub fn new(credential: ZerodhaCredential, base_url: Option<String>) -> anyhow::Result<Self> {
        // `X-Kite-Version` is a default header; `Authorization` is NOT, deliberately. It is built
        // per request in `auth_headers` so the token is not retained in a struct that the shared
        // client may format or log.
        let mut headers = HashMap::new();
        headers.insert("X-Kite-Version".to_string(), KITE_VERSION.to_string());

        // v2.0.0rc4: `HttpClient::new` became a `bon` builder. `header_keys`, `keyed_quotas`,
        // `default_quota`, `proxy_url` and `rate_limiters` were all empty/None before and are
        // omitted here -- the builder defaults match what we passed.
        let client = HttpClient::builder()
            .headers(headers)
            .timeout_secs(DEFAULT_TIMEOUT_SECS)
            .build()
            .map_err(|e| anyhow::anyhow!("failed to build Zerodha HTTP client: {e}"))?;

        Ok(Self {
            base_url: base_url.unwrap_or_else(|| ZERODHA_HTTP_URL.to_string()),
            credential,
            client,
        })
    }

    /// Builds the per-request authorisation header.
    ///
    /// **The returned map contains a live session token.** It is constructed at the call site and
    /// dropped with the request rather than stored.
    pub(crate) fn auth_headers(&self) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        headers.insert(
            "Authorization".to_string(),
            format!(
                "token {}:{}",
                self.credential.api_key(),
                self.credential.access_token(),
            ),
        );
        headers
    }

    /// Returns the REST root this client was built against.
    ///
    /// `pub(crate)` so [`crate::http::orders`] can extend this client rather than construct a
    /// second one. Privacy in Rust is per-module and `http::orders` is a *sibling* of this module,
    /// not a child, so a private field is not reachable from there — and the alternative, a second
    /// `HttpClient` with its own connection pool and its own copy of the credential, is exactly
    /// what the module docs of the order surface argue against.
    pub(crate) fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Returns the shared transport.
    ///
    /// See [`Self::base_url`] for why this is `pub(crate)` rather than private.
    pub(crate) fn transport(&self) -> &HttpClient {
        &self.client
    }

    /// Fetches the instrument dump, for one exchange or for all of them.
    ///
    /// Passing `None` requests every exchange, which is the several-megabyte response described in
    /// the module docs. Prefer naming an exchange when you know it.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, if the venue answers with a non-success status, if
    /// the body is not UTF-8, or if the CSV has no usable header.
    pub async fn instruments(&self, exchange: Option<&str>) -> anyhow::Result<Vec<KiteInstrument>> {
        let url = match exchange {
            Some(exchange) => format!("{}/instruments/{exchange}", self.base_url),
            None => format!("{}/instruments", self.base_url),
        };

        let response = self
            .client
            .request(
                Method::GET,
                url,
                None,
                Some(self.auth_headers()),
                None,
                Some(DEFAULT_TIMEOUT_SECS),
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Zerodha instruments request failed: {e}"))?;

        // The body is checked BEFORE the status is trusted to imply usable content: Kite answers
        // an expired token with a JSON error document and a 403, and a parser handed that would
        // report "no tradingsymbol column" rather than "your token is dead".
        // `as_u16` rather than formatting the status directly: `HttpStatus` derives only `Clone`
        // and `Debug`, so `{}` on it does not compile and `{:?}` would print the wrapper.
        anyhow::ensure!(
            response.status.is_success(),
            "Zerodha instruments request returned HTTP {}; an expired access_token answers 403 \
             here, and the token is flushed daily around 06:00-07:30 IST",
            response.status.as_u16(),
        );

        let body = std::str::from_utf8(&response.body)
            .map_err(|e| anyhow::anyhow!("instrument dump is not valid UTF-8: {e}"))?;

        let (instruments, skipped) = parse_instruments(body)?;

        if skipped > 0 {
            // Deliberately a warning rather than silence: a handful of odd rows is normal, and a
            // sudden jump means the schema moved. The number is the only thing that separates
            // those two, so it has to be visible.
            log::warn!(
                "Parsed {} Zerodha instruments, skipped {skipped} unparseable row(s)",
                instruments.len(),
            );
        } else {
            log::info!("Parsed {} Zerodha instruments", instruments.len());
        }

        Ok(instruments)
    }
}

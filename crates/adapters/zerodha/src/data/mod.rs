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

//! The Zerodha data client.
//!
//! # Status
//!
//! The binary tick decoder ([`crate::websocket::parse`]) is complete and covered by fixture tests.
//! **The WebSocket transport is not yet wired.**
//!
//! # Where the guard actually is — an earlier version of this comment was wrong
//!
//! [`DataClient::connect`] returns an error, and **that does not stop anything**.
//! `DataEngine::connect` collects client errors with `filter_map(Result::err)` into `log::error!`
//! and carries on — its own doc says *"Connection failures are logged but do not prevent the node
//! from running."* So the observable difference between `bail!` and `Ok(())` here is **one ERROR
//! log line**; the node starts either way.
//!
//! The real guard is at **construction**. [`ZerodhaDataClient::new`] returns `Result`, and
//! `DataClientFactory::create` is called with `?` in `LiveNodeBuilder` — so a client that cannot be
//! built aborts the build. That is why the credential check lives in `new` and not in `connect`.
//!
//! The `connect` error is kept because it is **true and loud**, not because it is protective. This
//! crate spent a day insisting that a client which reports health while streaming nothing is
//! indistinguishable from a quiet market; the correction is that refusing in `connect` is not what
//! prevents it.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use nautilus_common::clients::DataClient;
use nautilus_model::identifiers::{ClientId, Venue};

use crate::{
    common::{
        consts::NSE_VENUE,
        credential::{ZerodhaCredential, credential_env_vars},
    },
    config::ZerodhaDataClientConfig,
};

/// A Nautilus data client for the Zerodha Kite Connect streaming API.
///
/// `Debug` is derived, and both credential-bearing fields redact themselves:
/// [`ZerodhaCredential`] by a hand-written impl, and [`ZerodhaDataClientConfig`] likewise.
///
/// The config's redaction is load-bearing rather than belt-and-braces. It derived `Debug` until
/// 2026-08-14, which meant formatting this client printed the access token in full whenever it was
/// supplied on the config rather than through the environment — through a field the doc comment
/// asserted was safe.
#[derive(Debug)]
pub struct ZerodhaDataClient {
    client_id: ClientId,
    config: ZerodhaDataClientConfig,
    /// Resolved at construction from the config or the environment.
    #[expect(dead_code, reason = "consumed once the WebSocket transport is wired")]
    credential: ZerodhaCredential,
    is_connected: AtomicBool,
}

impl ZerodhaDataClient {
    /// Creates a new [`ZerodhaDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is missing credentials.
    pub fn new(client_id: ClientId, config: ZerodhaDataClientConfig) -> anyhow::Result<Self> {
        // Resolved once here rather than re-read later, so the client cannot pick up a different
        // credential mid-session if the environment changes under it.
        let (key_var, token_var) = credential_env_vars();
        let credential = config.credential().ok_or_else(|| {
            anyhow::anyhow!(
                "Zerodha data client requires both an API key and an access token \
                 (set them on the config, or via {key_var} / {token_var})"
            )
        })?;

        Ok(Self {
            credential,
            client_id,
            config,
            is_connected: AtomicBool::new(false),
        })
    }

    /// Returns the primary venue for this client.
    ///
    /// Zerodha brokers both NSE and BSE through one connection, so the venue on the client is the
    /// primary one and per-instrument venues come from the instrument definitions.
    #[must_use]
    pub fn venue(&self) -> Venue {
        *NSE_VENUE
    }
}

#[async_trait(?Send)]
impl DataClient for ZerodhaDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(Self::venue(self))
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting Zerodha data client: client_id={}, ws_url={}",
            self.client_id,
            self.config.ws_url(),
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Zerodha data client {}", self.client_id);
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting Zerodha data client {}", self.client_id);
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing Zerodha data client {}", self.client_id);
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        // Deliberately an error, not a no-op: the default trait body returns `Ok(())`, which would
        // report a healthy client that never streams. See the module docs.
        anyhow::bail!(
            "Zerodha WebSocket transport is not implemented yet — the tick decoder is complete but \
             nothing is connected to it"
        )
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }
}

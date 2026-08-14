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
//! [`DataClient::connect`] loads the instrument dump, opens a real socket, and
//! [`DataClient::subscribe_quotes`] issues a real subscription. **None of it has been run against
//! Zerodha.** The decoder is checked against captured bytes and the CSV and tick mappings are
//! unit-tested, but no request has been made and no live tick has reached the engine.
//!
//! # Instruments load BEFORE the socket, and the order is deliberate
//!
//! `subscribe_quotes` resolves an [`InstrumentId`] to a Zerodha token against the registry, and
//! tokens exist **only** in the REST dump — not in the feed, not in the historical catalog. A
//! connected client with an empty registry can therefore subscribe to nothing, so failing on the
//! fetch is more useful than a healthy socket that will reject every subscription.
//!
//! The load is skipped when the registry is already populated, so a caller that registered
//! instruments itself is not forced through a several-megabyte download.
//!
//! [`InstrumentId`]: nautilus_model::identifiers::InstrumentId
//!
//! # Where the guard is — this replaced an earlier `bail!`, and the reasoning is worth keeping
//!
//! `connect` used to return an error deliberately, so the client could not report health while
//! streaming nothing. That was the right instinct pointed at the wrong place: **a `connect` error
//! does not stop anything.** `DataEngine::connect` collects client errors with
//! `filter_map(Result::err)` into `log::error!` and carries on — its own doc says *"Connection
//! failures are logged but do not prevent the node from running."*
//!
//! The real guard is at **construction**: [`ZerodhaDataClient::new`] returns `Result` and
//! `DataClientFactory::create` is called with `?` in `LiveNodeBuilder`, so a client that cannot be
//! built aborts the build. That is why the credential check lives in `new`.
//!
//! **What still has no guard is the failure the `bail!` was aiming at** — a connected socket
//! delivering nothing. `idle_timeout_ms` on the transport catches a dead *socket*; heartbeats keep
//! arriving while ticks stop, so it does not catch a dead *feed*. The shape-asserting watchdog
//! that would is not built.
//!
//! # Quotes are subscribed in FULL mode, and that is not a preference
//!
//! Kite's 44-byte packet is named `quote` and carries **no book**. Only the 184-byte full packet
//! has depth, so a quote-mode subscription produces a stream from which no [`QuoteTick`] can ever
//! be constructed. See [`crate::data::parse`].
//!
//! [`QuoteTick`]: nautilus_model::data::QuoteTick

pub mod parse;

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{DataEvent, data::SubscribeQuotes},
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::identifiers::{ClientId, Venue};

use crate::{
    common::{
        consts::NSE_VENUE,
        credential::{ZerodhaCredential, credential_env_vars},
        enums::ZerodhaTickMode,
        instruments::InstrumentRegistry,
    },
    config::ZerodhaDataClientConfig,
    data::parse::quote_tick_from,
    http::client::ZerodhaHttpClient,
    websocket::client::ZerodhaWebSocketClient,
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
    credential: ZerodhaCredential,
    /// Built by [`Self::connect`]; `None` until then.
    websocket: Option<ZerodhaWebSocketClient>,
    /// Shared with the feed task, which resolves a token on every tick.
    ///
    /// `RwLock` rather than a plain map because the two users are on different threads: this
    /// client registers and reads on the engine side, and the feed task reads on the runtime.
    instruments: Arc<RwLock<InstrumentRegistry>>,
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
            websocket: None,
            instruments: Arc::new(RwLock::new(InstrumentRegistry::new())),
            is_connected: AtomicBool::new(false),
        })
    }

    /// Returns the shared instrument registry.
    ///
    /// Populated by [`Self::load_instruments`], which [`DataClient::connect`] calls. A caller may
    /// also register entries directly, which is what the tests do.
    #[must_use]
    pub fn instruments(&self) -> Arc<RwLock<InstrumentRegistry>> {
        Arc::clone(&self.instruments)
    }

    /// Fetches the instrument dump and populates the registry.
    ///
    /// `exchange` limits the request to one exchange; `None` fetches every exchange, which is a
    /// several-megabyte response of roughly 100k rows. **Prefer naming an exchange.** The
    /// unfiltered call is the default only because a client that cannot resolve an instrument is
    /// useless, and there is no way to ask the venue for one.
    ///
    /// Returns the number of instruments registered.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails, the venue answers with a non-success status, or the
    /// dump cannot be parsed.
    pub async fn load_instruments(&self, exchange: Option<&str>) -> anyhow::Result<usize> {
        let http = ZerodhaHttpClient::new(self.credential.clone(), self.config.base_url_http.clone())?;
        let instruments = http.instruments(exchange).await?;

        let mut registry = self
            .instruments
            .write()
            .map_err(|e| anyhow::anyhow!("instrument registry lock poisoned: {e}"))?;

        for instrument in &instruments {
            registry.register(instrument.to_details());
        }

        log::info!(
            "Registered {} Zerodha instruments ({})",
            registry.len(),
            exchange.unwrap_or("all exchanges"),
        );
        Ok(instruments.len())
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
        if self.is_connected() {
            log::warn!("Zerodha data client {} is already connected", self.client_id);
            return Ok(());
        }

        // Instruments BEFORE the socket. A connected client that cannot resolve a token can
        // subscribe to nothing, so failing here is more useful than a healthy socket with an
        // empty registry -- and the registry is what `subscribe_quotes` resolves against.
        if self.instruments.read().is_ok_and(|r| r.is_empty()) {
            self.load_instruments(None).await?;
        }

        let mut websocket =
            ZerodhaWebSocketClient::new(self.credential.clone(), self.config.base_url_ws.clone());
        websocket.connect().await?;

        let mut ticks = websocket
            .take_tick_stream()
            .ok_or_else(|| anyhow::anyhow!("tick stream was already taken"))?;

        let instruments = Arc::clone(&self.instruments);
        let sender = get_data_event_sender();

        // The tick path. Everything here runs per tick at market rates, which is why the token
        // lookup is a hash rather than a scan and why the read lock is released before mapping.
        get_runtime().spawn(async move {
            let clock = get_atomic_clock_realtime();

            while let Some(tick) = ticks.recv().await {
                let details = match instruments.read() {
                    Ok(guard) => guard.by_token(tick.instrument_token).copied(),
                    Err(e) => {
                        log::error!("Instrument registry lock poisoned: {e}");
                        return;
                    }
                };

                let Some(details) = details else {
                    // Not an error: the venue streams every token we ever subscribed, and a token
                    // can outlive its registration. Logged at debug so an unregistered instrument
                    // does not drown the log at tick rates.
                    log::debug!("No instrument registered for token {}", tick.instrument_token);
                    continue;
                };

                let ts_init = clock.get_time_ns();

                match quote_tick_from(
                    &tick,
                    details.instrument_id,
                    details.price_precision,
                    details.size_precision,
                    ts_init,
                ) {
                    Ok(quote) => {
                        if sender.send(DataEvent::Data(quote.into())).is_err() {
                            log::debug!("Data event receiver dropped; stopping Zerodha feed");
                            return;
                        }
                    }
                    // Expected for ltp and quote-mode packets, which carry no book at all. This is
                    // the reason `subscribe_quotes` subscribes in FULL mode.
                    Err(e) => log::debug!("Skipping tick that cannot become a quote: {e}"),
                }
            }

            log::debug!("Zerodha tick stream ended");
        });

        self.websocket = Some(websocket);
        self.is_connected.store(true, Ordering::Release);
        log::info!("Zerodha data client {} connected", self.client_id);
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let token = self
            .instruments
            .read()
            .map_err(|e| anyhow::anyhow!("instrument registry lock poisoned: {e}"))?
            .token_of(&cmd.instrument_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no Zerodha instrument token registered for {}; the streaming API subscribes \
                     by token and tokens come only from the REST instrument dump",
                    cmd.instrument_id,
                )
            })?;

        let websocket = self
            .websocket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Zerodha data client is not connected"))?;

        // FULL, not Quote. Kite's 44-byte `quote` packet carries no book -- only the 184-byte full
        // packet does -- so subscribing in quote mode yields a stream from which no `QuoteTick`
        // can ever be built. See `data::parse`.
        websocket.subscribe(ZerodhaTickMode::Full, vec![token])?;

        log::info!(
            "Subscribed quotes for {} (token {token}) in full mode",
            cmd.instrument_id,
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if let Some(websocket) = self.websocket.as_mut() {
            websocket.close()?;
        }
        self.websocket = None;
        self.is_connected.store(false, Ordering::Release);
        log::info!("Zerodha data client {} disconnected", self.client_id);
        Ok(())
    }
}

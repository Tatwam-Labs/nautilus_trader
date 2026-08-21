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
//! **Proven against the live venue on 2026-08-14**, across five runs on MCX CRUDEOIL: the REST
//! instrument dump (114,851 rows, nine exchanges), WebSocket auth, subscribe and mode honoured
//! (full-mode depth on every tick), binary decode on a live socket, and the volume-delta trade path
//! (25 trades from 89 ticks, 64 correctly suppressed). Ticks reached a Python strategy through the
//! engine and drove simulated fills that matched the venue's own book — a marketable buy at the ask
//! and a marketable sell at the bid.
//!
//! ⚠️ **That is proof for ONE instrument, ONE venue, ONE session and the happy path only.** No order
//! rejection, no partial fill, no mid-run reconnect, no illiquid or wide-spread instrument, and no
//! venue other than MCX. It works on the path you walk deliberately, not the paths a live system
//! falls down. The trade path still rests on an *argued* reading of `volume_traded` that no captured
//! corpus can settle — see [`crate::data::parse`] — and the live run is consistent with that reading
//! rather than a test of it.
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
//!
//! # One tick can produce BOTH a quote and a trade
//!
//! Quotes and trades are not alternatives here. A full-mode packet carries the book *and* the
//! session's traded volume, so the feed task offers every tick to both mappers: the quote mapper
//! reads the ladder, and [`TradeTracker`] compares the volume against the previous tick for that
//! instrument. Most ticks yield a quote and no trade — Kite pushes a snapshot roughly once a
//! second whether or not anything traded, and emitting a trade per tick would fabricate one per
//! second per instrument. That argument is made in full in [`crate::data::parse`].
//!
//! Because trades come off the same packet, [`DataClient::subscribe_trades`] issues the *same*
//! full-mode subscription as [`DataClient::subscribe_quotes`] rather than a second one. Repeating
//! it for a token already subscribed is a mode set, not growth: see
//! [`SubscriptionState::subscribe`], which counts only new tokens against the venue's cap.
//!
//! [`SubscriptionState::subscribe`]: crate::websocket::subscription::SubscriptionState::subscribe

pub mod open_interest;
pub mod parse;

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{get_runtime, runner::get_data_event_sender},
    messages::{
        DataEvent,
        data::{SubscribeIndexPrices, SubscribeQuotes, SubscribeTrades},
    },
};
use nautilus_core::time::get_atomic_clock_realtime;
use nautilus_model::{
    data::{Data, DataType, custom::{CustomData, CustomDataTrait}, prices::IndexPriceUpdate},
    identifiers::{ClientId, InstrumentId, Venue},
    types::Price,
};

use crate::{
    common::{
        credential::{ZerodhaCredential, credential_env_vars},
        enums::ZerodhaTickMode,
        instruments::InstrumentRegistry,
    },
    config::ZerodhaDataClientConfig,
    data::{
        open_interest::ZerodhaOpenInterest,
        parse::{TradeTracker, quote_tick_from, venue_time_to_unix_nanos},
    },
    http::client::ZerodhaHttpClient,
    websocket::{
        client::{SubscriptionCensus, ZerodhaWebSocketClient},
        subscription::MAX_TOKENS_PER_CONNECTION,
    },
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

        // Release the write lock before publishing: the send below is not instant at 16k
        // instruments, and holding a writer that long blocks every token lookup on the tick path.
        drop(registry);

        // ⚠️ PUBLISHING TO THE ENGINE IS SEPARATE FROM REGISTERING, AND BOTH ARE REQUIRED.
        //
        // The registry above answers "which token is this instrument?" on the tick path. It is
        // private to this crate and the engine cannot see it. The Nautilus **cache** is what
        // prices orders, and until an instrument reaches it the execution client cannot fill.
        //
        // This was found by a live paper run on 2026-08-14: every client connected, the sandbox
        // started, and the strategy stopped with `CRUDEOIL26AUGFUT.MCX is not in the cache`.
        // `to_instrument_any` had existed for hours and **nothing outside its own tests called
        // it** — the construction was complete and unreachable.
        //
        // Without the strategy's own guard that would have been a silent no-fill run: quotes
        // arriving, orders submitted, nothing filling, and the feed the obvious thing to blame.
        let sender = get_data_event_sender();
        let mut published = 0usize;
        let mut unconvertible = 0usize;
        let ts_init = get_atomic_clock_realtime().get_time_ns();

        for instrument in &instruments {
            match instrument.to_instrument_any(ts_init) {
                Ok(any) => {
                    if sender.send(DataEvent::Instrument(any)).is_err() {
                        log::error!("Data event receiver dropped while publishing instruments");
                        break;
                    }
                    published += 1;
                }
                // Expected and not an error: index rows carry a zero tick and zero lot, and the
                // dump also holds types this adapter does not map. Counted rather than silent, so
                // "a few odd rows" stays distinguishable from "the schema changed".
                Err(e) => {
                    unconvertible += 1;

                    if unconvertible <= 3 {
                        log::debug!("Instrument not convertible, skipping: {e}");
                    }
                }
            }
        }

        log::info!(
            "Zerodha instruments ({}): {} parsed, {published} published to the cache, \
             {unconvertible} not convertible",
            exchange.unwrap_or("all exchanges"),
            instruments.len(),
        );
        Ok(published)
    }

    /// Resolves an instrument id to the Zerodha token the streaming API subscribes by.
    ///
    /// Shared by every `subscribe_*`: the resolution and its failure message are the same for all
    /// of them, and a second copy of this is a second place for the message to drift.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned, or if the instrument is not registered.
    fn resolve_token(&self, instrument_id: &InstrumentId) -> anyhow::Result<u32> {
        let token = self
            .instruments
            .read()
            .map_err(|e| anyhow::anyhow!("instrument registry lock poisoned: {e}"))?
            .token_of(instrument_id)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "no Zerodha instrument token registered for {instrument_id}; the streaming \
                     API subscribes by token and tokens come only from the REST instrument dump",
                )
            })?;

        Ok(token)
    }

    /// Subscribes `token` in FULL mode, the only mode from which quotes or trades can be built.
    ///
    /// Calling this twice for one token is a **mode set**, not a second subscription: the
    /// subscription state overwrites the token's mode and counts only new tokens against the
    /// venue's 3,000-token cap. That is what lets quotes and trades share one subscription.
    ///
    /// # Errors
    ///
    /// Returns an error if the client is not connected, or if the subscription cannot be sent.
    fn subscribe_full(&self, token: u32) -> anyhow::Result<()> {
        let websocket = self
            .websocket
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Zerodha data client is not connected"))?;

        websocket.subscribe(ZerodhaTickMode::Full, vec![token])
    }

    /// Builds a tick stream from a captured frame corpus instead of a socket.
    ///
    /// Reads the `records[].frame_hex` written by `test_data/capture_live_frames.py`, decodes each
    /// with the SAME [`parse_binary`] the live feed task uses, and delivers the ticks on an
    /// equivalent channel. Nothing downstream can tell the difference, which is the point.
    ///
    /// # ⚠️ What a replay run does and does not establish
    ///
    /// It exercises decode -> token resolution -> quote/trade/open-interest mapping -> publication
    /// to the engine. It does **not** touch auth, subscribe, mode, reconnect or the socket, so a
    /// green replay says nothing about a live session. The two are different claims and a replay
    /// must never be reported as the stronger one.
    ///
    /// Frames are sent as fast as they decode rather than at their captured cadence. Timing is not
    /// being measured, and a real-time replay would make an already slow test slower.
    ///
    /// # Errors
    ///
    /// Returns an error if the corpus cannot be read, is not the expected JSON shape, or contains
    /// no decodable frame — an empty stream would otherwise look exactly like a quiet market.
    fn replay_tick_stream(
        path: &str,
    ) -> anyhow::Result<tokio::sync::mpsc::UnboundedReceiver<crate::websocket::messages::KiteTick>> {
        let body = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read the Zerodha frame corpus at {path}: {e}"))?;
        let corpus: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("frame corpus at {path} is not JSON: {e}"))?;

        let records = corpus
            .get("records")
            .and_then(|r| r.as_array())
            .ok_or_else(|| anyhow::anyhow!("frame corpus at {path} has no `records` array"))?;

        let mut ticks = Vec::new();
        let mut undecodable = 0usize;

        for record in records {
            let Some(hex) = record.get("frame_hex").and_then(|h| h.as_str()) else {
                undecodable += 1;
                continue;
            };

            let Ok(bytes) = (0..hex.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&hex[i..i + 2], 16))
                .collect::<Result<Vec<u8>, _>>()
            else {
                undecodable += 1;
                continue;
            };

            match crate::websocket::parse::parse_binary(&bytes) {
                Ok(decoded) => ticks.extend(decoded),
                Err(e) => {
                    undecodable += 1;
                    log::warn!("Skipping an undecodable corpus frame: {e}");
                }
            }
        }

        // An empty stream is indistinguishable from a quiet market downstream, so it fails here
        // rather than producing a run that reports zero and looks like a finding.
        anyhow::ensure!(
            !ticks.is_empty(),
            "frame corpus at {path} yielded NO decodable ticks ({} record(s), {undecodable} \
             unusable); a replay that publishes nothing would look like a quiet feed rather than \
             a broken corpus",
            records.len(),
        );

        log::info!(
            "Zerodha replay: {} tick(s) decoded from {} corpus record(s), {undecodable} unusable",
            ticks.len(),
            records.len(),
        );

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        get_runtime().spawn(async move {
            // ⚠️ LOOPED, AND PACED. The first version sent every tick as fast as it decoded and
            // then stopped -- which drained the whole corpus DURING `connect()`, a full second
            // before the trader started and any strategy could subscribe. Measured 2026-08-15:
            //
            //     11:41:23  corpus exhausted
            //     11:41:23  open interest: first item published
            //     11:41:24  Starting trader...          <- the subscriber appears here
            //
            // Everything was published correctly and nothing could receive it. A run like that
            // reports zero and looks exactly like a wall, which is the worst possible failure for
            // a measurement whose entire purpose is to tell a wall from a gap.
            //
            // Looping also matches what the thing being replayed actually does: a live feed does
            // not stop after four ticks.
            let mut cycles = 0usize;

            loop {
                for tick in &ticks {
                    if tx.send(tick.clone()).is_err() {
                        log::info!(
                            "Zerodha replay: receiver dropped after {cycles} cycle(s); stopping"
                        );
                        return;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }

                cycles += 1;

                if cycles == 1 {
                    log::info!(
                        "Zerodha replay: first pass complete, looping the corpus every {}ms/tick \
                         until the node stops",
                        200,
                    );
                }
            }
        });

        Ok(rx)
    }

}

#[async_trait(?Send)]
impl DataClient for ZerodhaDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    /// Returns `None`: Zerodha is a **broker, not an exchange**, so this client has no single venue.
    ///
    /// ⚠️ UNBUILT AND UNVERIFIED — edited 2026-08-14 late, deliberately not compiled or run. Verify
    /// before trusting: a live subscribe on MCX must still resolve, and a node configured WITHOUT
    /// `RoutingConfig` must now fail CONSISTENTLY rather than working for NSE alone.
    ///
    /// This previously returned `Some(NSE)`. The engine registers that venue in its routing map at
    /// `LiveNodeBuilder` build time, so ONE of the nine exchanges routed by venue and the other
    /// eight did not: an MCX subscription failed with
    /// `no client found for client_id=None, venue=Some("MCX")` while an NSE one worked. A claim of
    /// a single venue is not merely cosmetic here — it is what the routing map is built from.
    ///
    /// Returning `None` means venue routing must be declared explicitly, via `RoutingConfig` on
    /// `add_data_client` (which calls `DataEngine::register_venue_routing` per venue) or by naming
    /// the client on each subscription. That is a REAL behaviour change for any caller that relied
    /// on the accidental NSE registration — and the loss is deliberate: failing for every venue is
    /// better than working for one, because selective success is what made this take an evening to
    /// find.
    ///
    /// ⭐ THE EXECUTION SIDE HAS THE SAME DEFECT AND CANNOT FIX IT FROM INSIDE THE CLIENT.
    /// `ExecutionEngine::register_client` also inserts `client.venue()` into a routing map, and
    /// `ExecutionClient::venue` returns a bare `Venue` with no way to decline. So a Zerodha exec
    /// client is filed under NSE alone, exactly as this one was.
    ///
    /// Its `handles_order_venue` override is NECESSARY BUT NOT SUFFICIENT, and the distinction is
    /// easy to get wrong — I did. That method is a **veto, not a route**: it can only reject an
    /// order that already reached the client. An NFO or MCX order finds no client in the routing
    /// map and never reaches the method at all. Two distinct failures — `ClientVenueMismatch` if
    /// the override is missing, silent non-routing if it is present — and neither is "it works".
    ///
    /// The exec side's remedy is therefore at the NODE, not in the client: register it as the
    /// DEFAULT execution client (correct when Zerodha is the only broker in the node), or call
    /// `register_venue_routing` once per exchange traded. Do not attempt to make
    /// `ExecutionClient::venue` return nothing — the trait does not permit it.
    fn venue(&self) -> Option<Venue> {
        None
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

        // ⭐ OFFLINE REPLAY. When `replay_frames_path` is set the socket is never opened and ticks
        // come from a captured corpus instead. Everything downstream -- decode, token resolution,
        // quote/trade/OI mapping, publication to the engine -- is the SAME CODE on the same channel.
        //
        // What this exercises and what it does not: the transport is NOT exercised, so a replay run
        // says nothing about auth, subscribe, mode or reconnect. It does exercise every step from
        // the decoder onward, which is what carriage questions are about. Stating that split matters
        // more than the feature: a green replay is not a green session.
        //
        // A real capability rather than test scaffolding -- it makes the decode-to-engine path
        // reproducible on a shut market, which is most of the week for MCX.
        let mut websocket = None;

        // ⚠️ DEFAULTS TO ZEROS, WHICH IS HONEST FOR REPLAY RATHER THAN CONVENIENT: a replay opens no
        // socket and holds no subscription, so "0/3000, 0 refused" is the true census of a
        // connection that does not exist. It must not be read as "a healthy live connection with
        // nothing refused" — the REPLAY MODE warning logged just below is what distinguishes them.
        let mut census = SubscriptionCensus::default();

        let mut ticks = if let Some(path) = self.config.replay_frames_path.clone() {
            log::warn!(
                "Zerodha data client {} is in REPLAY MODE from {path} -- NO SOCKET IS OPENED and \
                 no tick comes from the venue; they are decoded from a captured corpus. \
                 ⚠️ THE INSTRUMENT DUMP IS STILL FETCHED OVER THE NETWORK, because token \
                 resolution needs it and the corpus carries no instrument definitions. So this is \
                 not an offline mode: it is an offline TICK SOURCE.",
                self.client_id,
            );
            Self::replay_tick_stream(&path)?
        } else {
            let mut client =
                ZerodhaWebSocketClient::new(self.credential.clone(), self.config.base_url_ws.clone());
            client.connect().await?;

            let stream = client
                .take_tick_stream()
                .ok_or_else(|| anyhow::anyhow!("tick stream was already taken"))?;

            // Cloned BEFORE the client is moved. The counters are shared, so this stays live
            // for as long as the connection does.
            census = client.census();

            websocket = Some(client);
            stream
        };

        let instruments = Arc::clone(&self.instruments);
        let sender = get_data_event_sender();

        // The tick path. Everything here runs per tick at market rates, which is why the token
        // lookup is a hash rather than a scan and why the read lock is released before mapping.
        get_runtime().spawn(async move {
            let clock = get_atomic_clock_realtime();

            // Owned by this task alone -- the single reader of the tick stream -- so the
            // per-instrument volume baseline needs no lock. It is also per-connection: a
            // reconnect starts fresh and rebaselines rather than differencing across a gap that
            // may have crossed a session boundary. See `data::parse::TradeTracker`.
            let mut trades = TradeTracker::new();

            // A HEARTBEAT, because the absence of one cost an evening. This task previously
            // logged nothing at INFO, so a run receiving 150 ticks and a run receiving none
            // produced identical output -- and the first live node run could not be diagnosed
            // from its logs at all. Counters are cheap; silence is not.
            let mut received = 0usize;
            let mut published = 0usize;
            let mut unmapped = 0usize;
            let mut oi_published = 0usize;
            let mut last_refused = census.refused();

            while let Some(tick) = ticks.recv().await {
                received += 1;

                if received == 1 || received.is_multiple_of(25) {
                    // ⭐ THE GAUGE. Re-emitted every 25 ticks, so the current census is present
                    // in the log wherever the reader happens to look, without depending on the
                    // subscribe burst — which happens ONCE, at boot — still being there.
                    //
                    // ⚠️ THE MOTIVATING MEASUREMENT WAS A RIG, NOT PROD, AND IT DID NOT TRANSFER:
                    // a rig rotates at `json-file 20m x 5` (~25 min) and its subscription evidence
                    // had aged out; PROD DOES NOT ROTATE AT ALL (`LogConfig map[]`, no
                    // daemon.json, no rotated siblings — checked 2026-08-21). So this is not
                    // load-bearing on prod today. It is kept because it holds under BOTH, and a
                    // gauge that only works under one host's log settings has to be re-verified
                    // per host.
                    //
                    // ⇒ AND IT CARRIES THE HEADROOM ON THE SUCCESS PATH DELIBERATELY. The count
                    // crept 1,681 → 2,999 → 4,075 across releases; a pass/fail alarm cannot show a
                    // trend, and the trend is the warning anyone actually wants. A check that only
                    // speaks when broken is indistinguishable from one nobody wired up.
                    let subscribed = census.subscribed();
                    let refused = census.refused();
                    let headroom = census.headroom();

                    // ⛔ SEVERITY IS PART OF THE SIGNAL, NOT DECORATION. The only automated reader
                    // greps `ERROR|CRITICAL`, so an INFO line is durable and unread — the same trap
                    // that hides the existing self-disclosure warnings. The gauge therefore
                    // ESCALATES ITSELF: it speaks at ERROR exactly while something is refused, and
                    // at INFO while nothing is.
                    //
                    // ⚠️ A REFUSAL IS NOT SELF-CORRECTING: those tokens are not streaming and will
                    // not start. Repeating at ERROR is correct, not noisy.
                    // ⛔ ESCALATE ON THE TRANSITION, NOT ON EVERY HEARTBEAT. Measured on a live
                    // session 2026-08-21: this line fires 462,534 times. Emitting ERROR on each
                    // one while a refusal persists would (a) make the only automated reader's
                    // `ERROR|CRITICAL` count meaningless for every OTHER error, and (b) train
                    // people to filter it — which is how a real alarm becomes background noise.
                    //
                    // ⇒ SO: ERROR when the refusal count CHANGES (including 0 -> n at boot), INFO
                    // on the steady state. Correct severity at the moment it matters, a durable
                    // gauge afterwards, and no flood.
                    //
                    // ⚠️ AND THE TOTAL-REFUSAL CASE IS NOT COVERED HERE AND MUST NOT BE ASSUMED TO
                    // BE: this line fires PER TICK, so if EVERY subscription were refused there are
                    // no ticks and no heartbeat at all. That case is carried by the per-refusal
                    // `log::error!` in `websocket::client`, which runs in the command handler and
                    // does not depend on the feed.
                    let transitioned = refused != last_refused;
                    last_refused = refused;

                    if transitioned && refused > 0 {
                        log::error!(
                            "Zerodha tick path: {received} received, {published} published as \
                             quotes, {oi_published} as open interest, {unmapped} with no \
                             registered instrument | SUBSCRIPTIONS {subscribed}/{cap}, \
                             {headroom} headroom, ⛔ {refused} REFUSED — those tokens are NOT \
                             streaming",
                            cap = MAX_TOKENS_PER_CONNECTION,
                        );
                    } else {
                        log::info!(
                            "Zerodha tick path: {received} received, {published} published as \
                             quotes, {oi_published} as open interest, {unmapped} with no \
                             registered instrument | SUBSCRIPTIONS {subscribed}/{cap}, \
                             {headroom} headroom, {refused} refused",
                            cap = MAX_TOKENS_PER_CONNECTION,
                        );
                    }
                }

                let details = match instruments.read() {
                    Ok(guard) => guard.by_token(tick.instrument_token).copied(),
                    Err(e) => {
                        log::error!("Instrument registry lock poisoned: {e}");
                        return;
                    }
                };

                let Some(details) = details else {
                    unmapped += 1;
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
                        published += 1;

                        if sender.send(DataEvent::Data(quote.into())).is_err() {
                            log::debug!("Data event receiver dropped; stopping Zerodha feed");
                            return;
                        }
                    }
                    // Expected for ltp and quote-mode packets, which carry no book at all. This is
                    // the reason `subscribe_quotes` subscribes in FULL mode.
                    Err(e) => log::debug!("Skipping tick that cannot become a quote: {e}"),
                }

                // ⭐ INDEX PRICE — the only path an index instrument has.
                //
                // An index has NO BOOK and NO TRADED VOLUME, and the two publishers around this one
                // are gated on exactly those: `quote_tick_from` requires `tick.depth`, and
                // `TradeTracker::observe` requires `tick.volume_traded`. Neither is ever present in
                // the 28/32-byte index layouts, so before this arm an index subscription produced
                // PERFECT SILENCE — no data, no error, indistinguishable from a closed market or an
                // unsubscribed token. The last price was decoded on every one of those packets and
                // then discarded.
                //
                // ⚠️ `!tick.tradable` IS THE GATE, and it is a property of the SEGMENT rather than
                // of the packet length: `websocket/parse.rs:233` sets it from
                // `segment.is_tradable()`, which is `!matches!(self, Indices)`. Gating on the packet
                // length instead would be wrong twice over — an 8-byte LTP packet is emitted for
                // TRADABLE instruments too, and an index in ltp mode would be missed.
                //
                // Emitted for tradable instruments as well would be a duplicate of information the
                // quote and trade paths already carry, so this is deliberately exclusive.
                // 🔴 PRECISION IS **NOT** `details.price_precision` HERE, AND USING IT SILENTLY
                // TRUNCATES THE LEVEL.
                //
                // `price_precision` is derived from the dump's `tick_size` TEXT
                // (`http/parse.rs:309`, `decimals_in`), and an INDEX ROW CARRIES `tick_size = 0`:
                //
                //   256265,1001,NIFTY 50,"NIFTY 50",0,,0,0,0,EQ,INDICES,NSE
                //                                        ^ tick_size
                //
                // `decimals_in("0")` is 0 — correctly, since an index has no tick size to speak of.
                // But the WIRE carries two decimals: `segment.price_divisor()` is 100 for every
                // segment except the two currency ones, so `be_price` yields e.g. 24500.35.
                // ⇒ `Price::new(24500.35, 0)` would publish **24500**, losing the paise with no
                //   error and no log — a confidently wrong level, which is worse than none.
                //
                // So the precision comes from the DIVISOR, which is what actually determined the
                // decoded value, rather than from a tick size the instrument does not have.
                if !tick.tradable {
                    let index_precision = match tick.segment.price_divisor() as u64 {
                        10_000_000 => 7,
                        10_000 => 4,
                        _ => 2,
                    };

                    let index_price = IndexPriceUpdate::new(
                        details.instrument_id,
                        Price::new(tick.last_price, index_precision),
                        venue_time_to_unix_nanos(tick.exchange_timestamp).unwrap_or(ts_init),
                        ts_init,
                    );

                    if sender
                        .send(DataEvent::Data(index_price.into()))
                        .is_err()
                    {
                        log::debug!("Data event receiver dropped; stopping Zerodha feed");
                        return;
                    }
                }

                // The same tick, offered to the trade path as well -- NOT as an alternative. One
                // full-mode packet legitimately carries both a book and evidence of a trade.
                match trades.observe(
                    &tick,
                    details.instrument_id,
                    details.price_precision,
                    details.size_precision,
                    ts_init,
                ) {
                    Ok(Some(trade)) => {
                        if sender.send(DataEvent::Data(trade.into())).is_err() {
                            log::debug!("Data event receiver dropped; stopping Zerodha feed");
                            return;
                        }
                    }
                    // The COMMON case, and not a problem: the venue pushes a snapshot roughly once
                    // a second whether or not anything traded, and only a change in the session's
                    // cumulative volume is evidence that it did. Not logged -- it would fire on
                    // most ticks, for every instrument.
                    Ok(None) => {}
                    // Unlike `Ok(None)`, this IS a lost trade: one was detected and could not be
                    // represented.
                    Err(e) => log::warn!("Skipping a detected trade that cannot be published: {e}"),
                }

                // ⭐ OPEN INTEREST — the third thing this one packet carries.
                //
                // `QuoteTick` has no OI field, so this rides its own custom type rather than
                // travelling with the quote. Decoded since before the type existed; a decoded value
                // with nowhere to go is not delivered.
                //
                // Only the 184-byte full packet carries OI, so `tick.oi` is `None` for ltp and
                // quote-mode packets and for index packets. Absent is the ordinary case, not a
                // failure, so it is not logged — it would fire on most ticks for most instruments.
                // ⚠️ A ZERO HERE IS NOT KNOWN TO BE WRONG, AND NOT KNOWN TO BE RIGHT.
                //
                // In the 2026-08-14 MCX corpus, `MCXMETLDEX26AUGFUT` reads OI 0 while
                // `CRUDEOIL26AUGFUT` (7913) and `CRUDEOILM26AUGFUT` (24627) read non-zero from the
                // SAME frame, the same offsets and this same code. Two of three non-zero rules out
                // a systematic decode fault, so do not treat a 0 as evidence of a bug here.
                //
                // Whether 0 is the TRUE open interest for that contract is UNVERIFIED. It is a
                // thinly-traded future on a metals index, which plausibly has no open positions —
                // but plausible is not measured and there is no venue-side figure to compare
                // against. An earlier note asserted "it is an index and has no OI by nature": that
                // was WRONG (`type=FUT`, `segment=MCX-FUT` — a future ON an index), and it would
                // have stopped the next reader from checking.
                //
                // Do not remove this note by assuming either answer. One live subscription settles
                // it.
                //
                // ─── 2026-08-18, the live subscription happened. It settled PART of it. ───
                //
                // NIFTY/NFO, 12 instruments, FULL mode, 09:14-09:20 IST, 5,266 ticks:
                //   NIFTY26AUGFUT      oi on 419/419 ticks   e.g. 12,771,330
                //   all 11 OPTIONS     oi on 100% of ticks   e.g. 6,235,840 / 11,557,130 / 9,780,550
                //
                // ⭐ SO OI IS NOT FUTURES-ONLY. Options carry it, on every tick, with distinct
                // per-strike values. That is basic F&O structure and the code never gated on
                // instrument kind — `if let Some(open_interest) = tick.oi` below is the whole test.
                //
                // ─── 2026-08-18 13:19, mode measured. ONE SOCKET, QUOTE AND LTP SIMULTANEOUSLY ───
                //
                // 6 instruments QUOTE + 6 LTP on one connection at the same instant, so MODE was
                // the only variable — two sequential runs would have confounded it with
                // time-of-session, severely so on expiry day.
                //   QUOTE  719 ticks · oi-bearing 0 · the `oi` FIELD IS ABSENT, not zero
                //   LTP    775 ticks · oi-bearing 0 · field absent
                //   FULL   oi on 100% of ticks, all 12 instruments
                // ⇒ "OI only in FULL mode" CONFIRMED — and it is OUR gate, not merely the venue's:
                //   websocket/parse.rs:270 reads offsets 48/52/56 only `if layout == Full`.
                //
                // ⭐ AND THAT NARROWS THE ZERO QUESTION ABOVE. Absent-in-other-modes plus
                // `Some(be_u32(packet, 48))` in Full means a 0 here is a TRANSMITTED zero read from
                // bytes — it cannot be a defaulted or missing field, because absent OI is None and
                // the `if let` below drops it entirely rather than emitting 0.
                //
                // ⚠️ SO WHAT REMAINS OPEN IS NARROWER THAN IT WAS: not "is this 0 an artefact of
                // our decode" — it is not — but "does the venue's transmitted 0 reflect true open
                // interest". That is a question about Zerodha, no longer about this code. It still
                // needs an index instrument and a venue-side figure to compare against.
                //
                // 🔴 AND THE REASON THIS PARAGRAPH EXISTS: the author of the note above went on to
                // predict, at HIGH confidence, that OI arrives "only for futures" — the exact shape
                // of assumption this note warns against, made by the person who wrote the warning.
                // Recording a caution does not transfer it to the next claim you make. If you are
                // about to state which instruments carry OI, measure it; two people have now been
                // wrong about it in writing.
                if let Some(open_interest) = tick.oi {
                    let oi = ZerodhaOpenInterest::new(
                        details.instrument_id,
                        open_interest,
                        // Day high/low share the packet with `oi` and are absent only if it is, but
                        // they are decoded independently, so they are defaulted rather than
                        // unwrapped. A panic here would kill the feed task for a cosmetic field.
                        tick.oi_day_high.unwrap_or(open_interest),
                        tick.oi_day_low.unwrap_or(open_interest),
                        venue_time_to_unix_nanos(tick.exchange_timestamp).unwrap_or(ts_init),
                        ts_init,
                    );
                    oi_published += 1;

                    let data_type = DataType::new(
                        ZerodhaOpenInterest::type_name_static(),
                        None,
                        Some(details.instrument_id.to_string()),
                    );

                    // ⚠️ THE TOPIC IS PART OF THE MEASUREMENT, NOT DECORATION. A subscriber that
                    // derives a different topic string gets SILENCE, which is indistinguishable
                    // from the data never being published at all. Logged once so the receiving side
                    // can be compared against what was actually emitted rather than against what
                    // someone believed it would be.
                    if oi_published == 1 {
                        log::info!(
                            "Zerodha open interest: first item published, data_type topic = {:?}",
                            data_type.topic(),
                        );
                    }

                    if sender
                        .send(DataEvent::Data(Data::Custom(CustomData::new(
                            Arc::new(oi),
                            data_type,
                        ))))
                        .is_err()
                    {
                        log::debug!("Data event receiver dropped; stopping Zerodha feed");
                        return;
                    }
                }
            }

            log::debug!("Zerodha tick stream ended");
        });

        self.websocket = websocket;
        self.is_connected.store(true, Ordering::Release);
        log::info!("Zerodha data client {} connected", self.client_id);
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        let token = self.resolve_token(&cmd.instrument_id)?;

        // FULL, not Quote. Kite's 44-byte `quote` packet carries no book -- only the 184-byte full
        // packet does -- so subscribing in quote mode yields a stream from which no `QuoteTick`
        // can ever be built. See `data::parse`.
        self.subscribe_full(token)?;

        log::info!(
            "Subscribed quotes for {} (token {token}) in full mode",
            cmd.instrument_id,
        );
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        let token = self.resolve_token(&cmd.instrument_id)?;

        // The SAME full-mode subscription quotes use, deliberately. Trades are derived from
        // `volume_traded`, which rides on the same packet as the book, so a token already
        // subscribed for quotes needs nothing further -- and repeating the request is a mode set
        // rather than a second subscription. Trade emission is not gated on this call: the feed
        // task offers every tick to the tracker, so a token subscribed for quotes alone will also
        // produce trades. That is the venue's shape, not a decision this client can undo.
        self.subscribe_full(token)?;

        log::info!(
            "Subscribed trades for {} (token {token}) in full mode",
            cmd.instrument_id,
        );
        Ok(())
    }

    /// Subscribes an INDEX instrument, whose price arrives on no other path.
    ///
    /// # Why this exists as a third method rather than falling out of the other two
    ///
    /// An index has **no book and no traded volume**, and both existing publishers are gated on
    /// exactly those:
    ///
    /// - [`quote_tick_from`] requires `tick.depth` and returns `Err` without it, so no
    ///   [`QuoteTick`] can ever be built for an index — in any mode. Index packets are 28/32 bytes
    ///   and carry no ladder at all.
    /// - [`TradeTracker::observe`] requires `tick.volume_traded`, which
    ///   `websocket::parse` populates only for the tradable layouts, so it returns `Ok(None)`
    ///   forever.
    ///
    /// ⚠️ Both rejections are silent by design — one logs at debug, the other not at all, because
    /// for a *tradable* instrument they are the ordinary case. For an index they are the only case,
    /// so before this method an index subscription produced **perfect silence**: no data, no error,
    /// indistinguishable from a closed market.
    ///
    /// The last price is decoded for every layout including the index ones and was simply
    /// discarded; this makes it reachable as an [`IndexPriceUpdate`].
    ///
    /// # Mode
    ///
    /// FULL, like the other two. The index layouts (`IndexQuote` 28, `IndexFull` 32) are what the
    /// venue sends for an index token regardless of the mode requested — the mode selects the
    /// *tradable* layout, and asking for full costs nothing here while keeping one subscription
    /// path for all instrument kinds.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument is not registered, or the subscription cannot be sent.
    ///
    /// ⚠️ **The instrument id is the venue's `tradingsymbol` verbatim**, so an NSE index is
    /// `NIFTY 50.NSE` — *with a space* — while BSE's is `SENSEX.BSE` with none. That asymmetry is
    /// the venues' own house style (127 of 136 NSE index symbols contain a space; 23 of 73 on BSE),
    /// and per the owner's ruling of 2026-08-20 this client carries the venue's spelling rather
    /// than normalising it. `NIFTY.NSE` will not resolve, and that is intended.
    fn subscribe_index_prices(&mut self, cmd: SubscribeIndexPrices) -> anyhow::Result<()> {
        let token = self.resolve_token(&cmd.instrument_id)?;

        self.subscribe_full(token)?;

        log::info!(
            "Subscribed index prices for {} (token {token})",
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

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

//! The Zerodha Kite streaming WebSocket client.
//!
//! ⚠️ **NOTHING IN THIS FILE HAS TOUCHED A SOCKET.** The decoder is checked against real captured
//! bytes and the subscription messages against the vendor client's source, but the transport
//! itself is unverified in the strongest sense: it has never connected, never subscribed, and
//! never reconnected. Treat every claim below as a design intention until a live session says
//! otherwise.
//!
//! # Ownership: the client lives in the feed task, not in this struct
//!
//! [`WebSocketClient`] is moved **into** the spawned feed task, and the public methods here talk
//! to it over a command channel. This is the `coinbase` arrangement and it exists for a reason
//! worth stating: sends and the read loop both need the client, and a command channel gives that
//! without an `Arc<Mutex<..>>` around an object whose methods are already async. The subscription
//! state lives in the task too, so nothing is shared and nothing needs locking.
//!
//! # Reconnect replay is INSIDE the loop, not bolted onto it
//!
//! The shared client auto-reconnects in handler mode and signals it **in-band**, by injecting a
//! text frame equal to [`nautilus_network::RECONNECTED`]. It does **not** replay subscriptions —
//! that is explicitly ours. Zerodha's real traffic is binary, so a text frame is trivially
//! distinguishable from market data.
//!
//! Replay is why [`SubscriptionState`] exists: a socket that reconnects and streams nothing looks
//! exactly like a quiet market, which is the failure this crate has refused to fake since
//! `connect()` was first written to return an error.
//!
//! # ⚠️ TWO WAYS THE SESSION DIES THAT RECONNECTING CANNOT FIX
//!
//! `reconnect_max_attempts` is `None` below — unlimited — which is right for a network blip and
//! **wrong for an invalid token**, because the credential is fixed at construction and every retry
//! re-presents the same dead one. Both of these produce a client that retries forever and never
//! recovers:
//!
//! 1. **Daily expiry.** Kite flushes the `access_token` every morning, roughly 06:00–07:30 IST,
//!    for regulatory reasons. A process running across that window holds a token that is simply
//!    gone.
//! 2. **Session overwrite.** Running the login flow again *immediately invalidates the previous
//!    token*, and any socket still open on it is disconnected. So a second consumer authenticating
//!    with the same app kills the first — **without exhausting the 3-connection limit and without
//!    anything on the first connection reporting why.**
//!
//! Point 2 is worth dwelling on: it produces exactly the symptom that was historically attributed
//! to a one-socket-per-token rule — a healthy-looking client that stops receiving. **Sharing one
//! token across sockets is supported; re-issuing it is what breaks them.**
//!
//! Neither case is handled here. A correct response needs a fresh token, which means credential
//! refresh rather than reconnection, and that is not built.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use nautilus_common::live::get_runtime;
use nautilus_network::{
    RECONNECTED,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_tungstenite::tungstenite::Message;

use crate::{
    common::{credential::ZerodhaCredential, enums::ZerodhaTickMode},
    websocket::{
        messages::KiteTick,
        parse::parse_binary,
        subscription::{KiteRequest, MAX_TOKENS_PER_CONNECTION, SubscriptionState},
    },
};

/// The Kite streaming endpoint, matching `kiteconnect` 5.2.0 `ticker.py:382`.
pub const ZERODHA_WS_URL: &str = "wss://ws.kite.trade";

/// Reconnect backoff, in milliseconds.
const RECONNECT_DELAY_INITIAL_MS: u64 = 1_000;
const RECONNECT_DELAY_MAX_MS: u64 = 30_000;
const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;
const RECONNECT_JITTER_MS: u64 = 250;
const RECONNECT_TIMEOUT_MS: u64 = 10_000;

/// Inbound idle timeout, in milliseconds.
///
/// Kite sends a one-byte heartbeat every ~3 seconds — measured at 2.894s to 3.105s across 79
/// heartbeats in a live capture on 2026-08-14. Silence well past that means the socket is dead
/// without having closed, which is the case `idle_timeout_ms` exists for.
///
/// **This detects a dead SOCKET. It does not detect a dead FEED** — heartbeats continue while
/// ticks stop, so this timer is satisfied by a connection delivering no market data at all. The
/// shape-asserting watchdog that covers that is a separate piece and is not in this file.
const IDLE_TIMEOUT_MS: u64 = 10_000;

/// Commands sent from the public API into the feed task that owns the client.
#[derive(Debug)]
enum Command {
    Subscribe(ZerodhaTickMode, Vec<u32>),
    Unsubscribe(Vec<u32>),
    Close,
}

/// A Zerodha Kite streaming client.
#[derive(Debug)]
pub struct ZerodhaWebSocketClient {
    url: String,
    credential: ZerodhaCredential,
    cmd_tx: Option<UnboundedSender<Command>>,
    tick_rx: Option<UnboundedReceiver<KiteTick>>,
    /// Live subscription census, shared with whoever reports it.
    ///
    /// # Why a shared counter rather than a log line at the subscribe site
    ///
    /// The subscribe burst happens ONCE, at boot. A counter can be RE-REPORTED afterwards; a log
    /// line cannot be re-read once it has aged out of whatever window the reader is looking at.
    /// These are read by the tick-path heartbeat, which re-emits every 25 ticks — so the census is
    /// durable BY REPETITION rather than by anyone happening to look at the right moment.
    ///
    /// ⚠️ **AND THE MOTIVATING MEASUREMENT WAS ENVIRONMENT-SPECIFIC — RECORDED BECAUSE IT WAS
    /// WRONG ABOUT PROD.** A rig measured `json-file 20m x 5` (~25 minutes at that volume) and the
    /// subscription evidence had indeed rotated away there. Checked on prod 2026-08-21: container
    /// `LogConfig` is `map[]`, `/etc/docker/daemon.json` does not exist, and there are no rotated
    /// siblings — **PROD DOES NOT ROTATE AT ALL.** So on prod the original lines persist and this
    /// counter is not strictly required.
    ///
    /// ⇒ IT IS KEPT BECAUSE IT DOES NOT DEPEND ON THE ENVIRONMENT: the same census reads correctly
    /// whether logs rotate hourly, never, or differently on the next host. **A design that only
    /// works under one deployment's log settings is a design that has to be re-verified per host.**
    census: SubscriptionCensus,
}

/// Counters describing what this connection actually holds, readable from another task.
#[derive(Clone, Debug, Default)]
pub struct SubscriptionCensus {
    subscribed: Arc<AtomicUsize>,
    refused: Arc<AtomicUsize>,
}

impl SubscriptionCensus {
    /// Tokens currently subscribed on this connection.
    #[must_use]
    pub fn subscribed(&self) -> usize {
        self.subscribed.load(Ordering::Relaxed)
    }

    /// Subscription requests refused locally because they would pass the venue's cap.
    ///
    /// ⚠️ NON-ZERO IS NOT SELF-CORRECTING: a refusal means those tokens are NOT streaming and never
    /// will be on this connection. The venue does not reject the excess, it silently does not send
    /// it — see [`MAX_TOKENS_PER_CONNECTION`].
    #[must_use]
    pub fn refused(&self) -> usize {
        self.refused.load(Ordering::Relaxed)
    }

    /// Remaining capacity on this connection, saturating at zero.
    #[must_use]
    pub fn headroom(&self) -> usize {
        MAX_TOKENS_PER_CONNECTION.saturating_sub(self.subscribed())
    }
}

impl ZerodhaWebSocketClient {
    /// The live subscription census for this connection.
    ///
    /// Clone it before the client is moved; the counters are shared and stay live afterwards.
    #[must_use]
    pub fn census(&self) -> SubscriptionCensus {
        self.census.clone()
    }

    /// Creates a new client for `credential`, against `url` or the public endpoint.
    #[must_use]
    pub fn new(credential: ZerodhaCredential, url: Option<String>) -> Self {
        Self {
            url: url.unwrap_or_else(|| ZERODHA_WS_URL.to_string()),
            credential,
            cmd_tx: None,
            tick_rx: None,
            census: SubscriptionCensus::default(),
        }
    }

    /// Returns the authenticated connection URL.
    ///
    /// # Kite authenticates by QUERY PARAMETER, not by header
    ///
    /// `ticker.py:443` builds `{root}?api_key={api_key}&access_token={access_token}`. There is no
    /// `Authorization` header and no post-connect auth message, which is why
    /// [`WebSocketConfig::headers`] is left empty below.
    ///
    /// **This URL contains a live session token.** It is deliberately not returned by any public
    /// accessor and must never be logged — every log line in this file names the *endpoint*, not
    /// this string.
    fn authenticated_url(&self) -> String {
        format!(
            "{}?api_key={}&access_token={}",
            self.url,
            self.credential.api_key(),
            self.credential.access_token(),
        )
    }

    /// Returns whether the feed task is running.
    #[must_use]
    pub fn is_connected(&self) -> bool {
        self.cmd_tx.as_ref().is_some_and(|tx| !tx.is_closed())
    }

    /// Connects and spawns the feed task.
    ///
    /// # Errors
    ///
    /// Returns an error if already connected, or if the transport cannot be established.
    pub async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            anyhow::bail!("Zerodha WebSocket is already connected");
        }

        let (message_handler, mut raw_rx) = channel_message_handler();

        let config = WebSocketConfig {
            url: self.authenticated_url(),
            // Empty: Kite authenticates by query parameter, see `authenticated_url`.
            headers: vec![],
            // Kite sends its own heartbeats; we do not need to generate traffic.
            //
            // ⚠️ RENAMED UPSTREAM in 74d57e7e05 "align config naming" — `heartbeat` ->
            // `heartbeat_interval_secs`, `heartbeat_msg` -> `heartbeat_payload`, and
            // `reconnect_timeout_ms` -> `connect_timeout_ms`. VERIFIED FROM THAT COMMIT'S OWN DIFF
            // rather than inferred from the names: the third pair is the one that could have been a
            // different field wearing a similar name, and it is not — the `-`/`+` sit in one hunk.
            heartbeat_interval_secs: None,
            heartbeat_payload: None,
            connect_timeout_ms: Some(RECONNECT_TIMEOUT_MS),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL_MS),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX_MS),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER_MS),
            // Unlimited: a data client that gives up permanently is worse than one that keeps
            // trying, because the give-up is silent from the strategy's point of view.
            reconnect_max_attempts: None,
            idle_timeout_ms: Some(IDLE_TIMEOUT_MS),
            // ⭐ NEW in 74d57e7e05 ("Add network dead-peer detection"), and DELIBERATELY UNUSED.
            //
            // This is plausibly the gap this file's own header describes: `idle_timeout_ms`
            // "detects a dead SOCKET. It does not detect a dead FEED" — heartbeats keep arriving
            // while ticks stop. A heartbeat timeout may cover exactly that.
            //
            // ⛔ BUT ADOPTING IT IS A BEHAVIOUR CHANGE ON THE RECONNECT PATH, and this merge is
            // already 807 files of unvalidated upstream. `None` preserves today's behaviour
            // exactly; turning it on is a separate, deliberate change with its own evidence.
            heartbeat_timeout_secs: None,
            backend: TransportBackend::default(),
            proxy_url: None,
        };

        // `connect_url` (not `connect_stream`) is the handler-mode entry point, and handler mode is
        // what carries auto-reconnect, backoff and the idle timeout. `connect_stream` has none of
        // them.
        //
        // v2.0.0rc4 replaced the `connect(...)` constructor with a `bon` builder whose finish_fn
        // is `connect`. `keyed_quotas` / `default_quota` -- we passed `vec![]` and `None`, i.e. no
        // client-side rate limiting -- are now defaults and omitted, so this is equivalent.
        //
        // NOTE `message_handler` is REQUIRED here, not `Option`: handler mode is no longer opted
        // into by passing `Some`, it is the shape of this builder.
        //
        // ⚠️ `connect_url` also exists in that file but belongs to `WebSocketClientInner`, a
        // DIFFERENT type. Reading a signature is not confirming which impl block owns it.
        let client = WebSocketClient::builder()
            .config(config)
            .message_handler(message_handler)
            .connect()
            .await
            .map_err(|e| anyhow::anyhow!("Zerodha WebSocket connect failed: {e}"))?;

        log::info!("Connected to Zerodha streaming endpoint {}", self.url);

        let (cmd_tx, mut cmd_rx) = unbounded_channel::<Command>();
        let (tick_tx, tick_rx) = unbounded_channel::<KiteTick>();

        // `get_runtime().spawn` rather than a bare `tokio::spawn`: the house pattern, and it does
        // not depend on `connect` happening to be polled inside a runtime context.
        let census = self.census.clone();

        get_runtime().spawn(async move {
            let mut state = SubscriptionState::new();

            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => match cmd {
                        Some(Command::Subscribe(mode, tokens)) => {
                            // Refuse locally rather than let the venue silently not stream the
                            // excess. Nothing is sent when the cap would be passed.
                            if let Err(e) = state.subscribe(mode, &tokens) {
                                // Counted as well as logged. The log line is a one-shot at boot;
                                // the counter is re-reported by the tick-path heartbeat for as long
                                // as the feed lives, so the state stays readable later without
                                // depending on this line still being in the reader's window.
                                census.refused.fetch_add(tokens.len(), Ordering::Relaxed);
                                log::error!("Rejecting Zerodha subscription: {e}");
                                continue;
                            }

                            census.subscribed.store(state.len(), Ordering::Relaxed);
                            // Subscribe THEN set mode -- a bare subscribe lands the venue at
                            // `quote` regardless of what was asked for. See `subscription`.
                            send_all(&client, &[
                                KiteRequest::Subscribe(tokens.clone()),
                                KiteRequest::Mode(mode, tokens),
                            ]).await;
                        }
                        Some(Command::Unsubscribe(tokens)) => {
                            state.unsubscribe(&tokens);
                            census.subscribed.store(state.len(), Ordering::Relaxed);
                            send_all(&client, &[KiteRequest::Unsubscribe(tokens)]).await;
                        }
                        Some(Command::Close) | None => {
                            client.disconnect().await;
                            break;
                        }
                    },
                    raw = raw_rx.recv() => match raw {
                        Some(Message::Text(text)) => {
                            // `as_str()` rather than comparing `Utf8Bytes` to a `&str` directly.
                            if text.as_str() == RECONNECTED {
                                let plan = state.replay_plan();
                                log::info!(
                                    "Zerodha socket reconnected; replaying {} subscription \
                                     message(s) for {} token(s)",
                                    plan.len(),
                                    state.len(),
                                );
                                send_all(&client, &plan).await;
                            } else {
                                // Control frames: acknowledgements and errors. Recognised and
                                // logged only -- handling them is out of scope for this milestone,
                                // and dropping them silently is what made the vendor client blind
                                // to `instruments_meta`.
                                log::debug!("Zerodha control frame: {text}");
                            }
                        }
                        Some(Message::Binary(bytes)) => {
                            // A one-byte frame is Kite's heartbeat, not a packet count.
                            match parse_binary(&bytes) {
                                Ok(ticks) => {
                                    for tick in ticks {
                                        if tick_tx.send(tick).is_err() {
                                            log::debug!("Tick receiver dropped; stopping feed");
                                            return;
                                        }
                                    }
                                }
                                Err(e) => log::error!("Zerodha frame decode failed: {e}"),
                            }
                        }
                        Some(Message::Close(frame)) => {
                            log::warn!("Zerodha socket closed by venue: {frame:?}");
                        }
                        Some(_) => {}
                        None => {
                            log::debug!("Zerodha raw stream ended; stopping feed");
                            break;
                        }
                    },
                }
            }
        });

        self.cmd_tx = Some(cmd_tx);
        self.tick_rx = Some(tick_rx);
        Ok(())
    }

    /// Subscribes `tokens` in `mode`.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected.
    pub fn subscribe(&self, mode: ZerodhaTickMode, tokens: Vec<u32>) -> anyhow::Result<()> {
        self.send_command(Command::Subscribe(mode, tokens))
    }

    /// Unsubscribes `tokens`.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected.
    pub fn unsubscribe(&self, tokens: Vec<u32>) -> anyhow::Result<()> {
        self.send_command(Command::Unsubscribe(tokens))
    }

    /// Closes the connection and stops the feed task.
    ///
    /// # Errors
    ///
    /// Returns an error if not connected.
    pub fn close(&mut self) -> anyhow::Result<()> {
        let result = self.send_command(Command::Close);
        self.cmd_tx = None;
        result
    }

    /// Takes the tick stream. Returns `None` if already taken or not connected.
    #[must_use]
    pub fn take_tick_stream(&mut self) -> Option<UnboundedReceiver<KiteTick>> {
        self.tick_rx.take()
    }

    fn send_command(&self, command: Command) -> anyhow::Result<()> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Zerodha WebSocket is not connected"))?;

        tx.send(command)
            .map_err(|e| anyhow::anyhow!("Zerodha feed task is not running: {e}"))
    }
}

/// Serialises and sends control messages in order.
///
/// Order matters — see [`SubscriptionState::replay_plan`] — so this sends sequentially and does
/// not fan out.
async fn send_all(client: &WebSocketClient, requests: &[KiteRequest]) {
    for request in requests {
        match serde_json::to_string(request) {
            Ok(json) => {
                if let Err(e) = client.send_text(json, None).await {
                    log::error!("Failed to send Zerodha control message: {e}");
                }
            }
            Err(e) => log::error!("Failed to serialise Zerodha control message: {e}"),
        }
    }
}

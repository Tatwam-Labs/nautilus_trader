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
        subscription::{KiteRequest, SubscriptionState},
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
}

impl ZerodhaWebSocketClient {
    /// Creates a new client for `credential`, against `url` or the public endpoint.
    #[must_use]
    pub fn new(credential: ZerodhaCredential, url: Option<String>) -> Self {
        Self {
            url: url.unwrap_or_else(|| ZERODHA_WS_URL.to_string()),
            credential,
            cmd_tx: None,
            tick_rx: None,
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
            heartbeat: None,
            heartbeat_msg: None,
            reconnect_timeout_ms: Some(RECONNECT_TIMEOUT_MS),
            reconnect_delay_initial_ms: Some(RECONNECT_DELAY_INITIAL_MS),
            reconnect_delay_max_ms: Some(RECONNECT_DELAY_MAX_MS),
            reconnect_backoff_factor: Some(RECONNECT_BACKOFF_FACTOR),
            reconnect_jitter_ms: Some(RECONNECT_JITTER_MS),
            // Unlimited: a data client that gives up permanently is worse than one that keeps
            // trying, because the give-up is silent from the strategy's point of view.
            reconnect_max_attempts: None,
            idle_timeout_ms: Some(IDLE_TIMEOUT_MS),
            backend: TransportBackend::default(),
            proxy_url: None,
        };

        // `connect` (not `connect_stream`) is the handler-mode entry point, and handler mode is
        // what carries auto-reconnect, backoff and the idle timeout. `connect_stream` has none of
        // them.
        let client = WebSocketClient::connect(config, Some(message_handler), None, vec![], None)
            .await
            .map_err(|e| anyhow::anyhow!("Zerodha WebSocket connect failed: {e}"))?;

        log::info!("Connected to Zerodha streaming endpoint {}", self.url);

        let (cmd_tx, mut cmd_rx) = unbounded_channel::<Command>();
        let (tick_tx, tick_rx) = unbounded_channel::<KiteTick>();

        // `get_runtime().spawn` rather than a bare `tokio::spawn`: the house pattern, and it does
        // not depend on `connect` happening to be polled inside a runtime context.
        get_runtime().spawn(async move {
            let mut state = SubscriptionState::new();

            loop {
                tokio::select! {
                    cmd = cmd_rx.recv() => match cmd {
                        Some(Command::Subscribe(mode, tokens)) => {
                            // Refuse locally rather than let the venue silently not stream the
                            // excess. Nothing is sent when the cap would be passed.
                            if let Err(e) = state.subscribe(mode, &tokens) {
                                log::error!("Rejecting Zerodha subscription: {e}");
                                continue;
                            }
                            // Subscribe THEN set mode -- a bare subscribe lands the venue at
                            // `quote` regardless of what was asked for. See `subscription`.
                            send_all(&client, &[
                                KiteRequest::Subscribe(tokens.clone()),
                                KiteRequest::Mode(mode, tokens),
                            ]).await;
                        }
                        Some(Command::Unsubscribe(tokens)) => {
                            state.unsubscribe(&tokens);
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

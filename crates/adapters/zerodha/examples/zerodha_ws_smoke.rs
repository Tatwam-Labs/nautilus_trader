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

//! A time-boxed live smoke test for the Zerodha WebSocket path.
//!
//! # What this is for
//!
//! Everything in the transport — the auth scheme, the subscribe and mode message shapes, the
//! reconnect replay, the depth ordering — is written from the vendor client's source and has never
//! been sent to Zerodha. **This is the first thing that puts any of it in front of the venue.**
//!
//! # BUILD ON ONE MACHINE, RUN ON ANOTHER
//!
//! The build host has the toolchain; the credential lives only on the machine with the database.
//! So this is built there and run here. **The binary crosses the mailbox; the token never does.**
//! Both hosts are arm64 Darwin, so the artefact is portable between them.
//!
//! ```bash
//! cargo build -p nautilus-zerodha --example zerodha_ws_smoke
//! # ship target/debug/examples/zerodha_ws_smoke, then on the machine with credentials:
//! ZERODHA_API_KEY=… ZERODHA_ACCESS_TOKEN=… ./zerodha_ws_smoke CRUDEOIL 20
//! ```
//!
//! # It is deliberately SHORT
//!
//! Zerodha allows three concurrent sockets per access token, and this takes one. The per-*account*
//! cap — LocalDocker and prod share an account on different keys — is **unknown**, so the run is
//! time-boxed rather than left open. Default 20 seconds.
//!
//! # It prints no secrets
//!
//! Prefixes and lengths only. The authenticated URL is never logged by the client and is never
//! constructed here.

use std::{env, time::Duration};

use nautilus_zerodha::{
    common::{credential::ZerodhaCredential, enums::ZerodhaTickMode},
    http::client::ZerodhaHttpClient,
    websocket::client::ZerodhaWebSocketClient,
};

// No logger is initialised, deliberately: the crate's own `log::info!`/`warn!` lines will not
// appear. Adding `tracing-subscriber` would mean a dependency no sibling adapter declares, and this
// example has to build on a machine where I cannot check that. Everything this test needs to report
// is printed directly. The cost is that a reconnect or a send failure logs nowhere — so treat a
// silent run with zero ticks as UNEXPLAINED rather than as evidence of anything.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let symbol_prefix = args.next().unwrap_or_else(|| "CRUDEOIL".to_string());
    let seconds: u64 = args
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20)
        .min(120);

    let credential = ZerodhaCredential::resolve(None, None).ok_or_else(|| {
        anyhow::anyhow!("set ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN in the environment")
    })?;
    println!(
        "credential: api_key {} / access_token present",
        credential.api_key_masked(),
    );

    // ---- REST: resolve a token. Proven working against the venue on 2026-08-14. ----
    let http = ZerodhaHttpClient::new(credential.clone(), None)?;
    let instruments = http.instruments(Some("MCX")).await?;
    println!("MCX instruments returned: {}", instruments.len());

    let target = instruments
        .iter()
        .filter(|i| i.instrument_type == "FUT" && i.tradingsymbol.starts_with(&symbol_prefix))
        .min_by_key(|i| i.expiry.clone())
        .ok_or_else(|| anyhow::anyhow!("no MCX future matching {symbol_prefix}"))?;

    println!(
        "target: {} token={} tick_size={} precision={} lot={} expiry={}",
        target.tradingsymbol,
        target.instrument_token,
        target.tick_size,
        target.price_precision,
        target.lot_size,
        target.expiry,
    );

    // ---- WebSocket: the part that has never run. ----
    let mut ws = ZerodhaWebSocketClient::new(credential, None);
    ws.connect().await?;
    println!("connected");

    let mut ticks = ws
        .take_tick_stream()
        .ok_or_else(|| anyhow::anyhow!("tick stream unavailable"))?;

    ws.subscribe(ZerodhaTickMode::Full, vec![target.instrument_token])?;
    println!("subscribed {} in FULL mode; listening {seconds}s", target.tradingsymbol);

    let mut count = 0usize;
    let mut with_depth = 0usize;
    let deadline = tokio::time::sleep(Duration::from_secs(seconds));
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            () = &mut deadline => break,
            tick = ticks.recv() => match tick {
                Some(tick) => {
                    count += 1;

                    if tick.depth.is_some() {
                        with_depth += 1;
                    }

                    // First few only: enough to see the shape, not a firehose.
                    if count <= 3 {
                        let best = tick.depth.as_ref().and_then(|d| {
                            Some((d.buy.first()?.price, d.sell.first()?.price))
                        });
                        println!(
                            "  tick {count}: token={} mode={:?} last={} ts={:?} depth[0]={:?}",
                            tick.instrument_token, tick.mode, tick.last_price,
                            tick.exchange_timestamp, best,
                        );
                    }
                }
                None => {
                    println!("stream ended early");
                    break;
                }
            },
        }
    }

    ws.close()?;

    println!("\n=== RESULT ===");
    println!("  ticks received      : {count}");
    println!("  ticks carrying depth: {with_depth}");
    println!(
        "  verdict             : {}",
        if count == 0 {
            "NO TICKS -- socket may have connected without streaming, or the market is closed"
        } else if with_depth == 0 {
            "TICKS BUT NO DEPTH -- full mode was requested; the venue may have downgraded us"
        } else {
            "FULL-MODE TICKS WITH DEPTH"
        },
    );

    Ok(())
}

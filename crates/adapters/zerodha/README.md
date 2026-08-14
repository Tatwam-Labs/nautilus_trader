# nautilus-zerodha

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-zerodha)](https://docs.rs/nautilus-zerodha/latest/nautilus-zerodha/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-zerodha.svg)](https://crates.io/crates/nautilus-zerodha)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

[NautilusTrader](https://nautilustrader.io) adapter for the [Zerodha Kite Connect](https://kite.trade/docs/connect/v3/) API.

The `nautilus-zerodha` crate provides integration with Zerodha's Kite Connect API for trading
Indian equities, futures and options on the **NSE** and **BSE**.

## Status

**Incomplete.** The binary streaming tick decoder is implemented and covered by fixture tests. The
WebSocket and REST transports are not yet wired, so `ZerodhaDataClient::connect` returns an error
rather than reporting a connected client that never streams.

| area | state |
|---|---|
| Binary tick decoder (LTP / quote / full, all segments, depth) | implemented — **17 constructed-fixture tests + 5 against real captured bytes** |
| Config, factory, credential resolution | implemented, **13 unit tests** |
| PyO3 registration | implemented, builds into the extension module |
| WebSocket transport | not started |
| REST instrument provider | not started |
| Historical requests, execution client | not started |

**35 tests total**, last verified on a build host at `92cdcaa146`. The two decoder suites are not
equal evidence — see *Fixtures* below.

## Venue notes

Several Zerodha behaviours are unusual enough to be worth stating, because each one fails quietly
rather than loudly. **Each is tagged with how it is known**, because they are not equally
established and a reader should be able to tell which they can lean on.

Covered by this crate's tests — a regression would fail CI:

- **Packet layout is selected by length, not by a type tag.** There is no discriminator field, so an
  unrecognised length cannot be partially decoded.
- **Currency segments use different price divisors from each other**: `CDS` scales by 10^7 and
  `BCD` by 10^4, against 10^2 everywhere else.
- **The streaming segment is the low byte of the instrument token**, and it is not the same thing as
  the exchange you fetched the instrument from. SENSEX (token `265`) is retrieved from the `BSE`
  dump but streams under the `INDICES` segment — `265 & 0xff == 9` — so it decodes as non-tradable.

Observed on a live session, **not** exercised by any test here:

- **Instrument tokens come only from the REST instrument dump.** They are not present in historical
  data, so the dump is the sole source rather than a cache warmer. *(2026-08-13)*
- **`INDICES` is not a valid argument to the instruments endpoint** — it returned `AccessDenied`.
  Index definitions come from the parent exchange's dump. *(2026-08-13)*
- **Heartbeats arrive every ~3s.** 139 intervals measured: median **3.000s**, range 2.844–3.156.
  This is the figure a liveness timeout should be built on. *(2026-08-14, one 7-minute window)*
- **No subscription acknowledgement was seen.** 1,810 binary frames arrived in the same window and
  no ack text frame did. **An observation, not a conclusion** — an ack sent before the handler
  attached would look identical. *(2026-08-14, one window)*
- **The venue sends `instruments_meta`**, a text type the vendor's client does not handle. See
  *Fixtures* below. *(2026-08-14, observed once)*

From the venue's documentation, not independently confirmed:

- **The access token is a session token** issued by the daily login flow, and expires each morning.

## Fixtures — two sets, and they are not equal evidence

In both, the expected values come from the **`kiteconnect` Python client**, never from this crate,
so the decoder is always asserted against a decoding it had no part in producing.

**`fixtures.json` — 9 CONSTRUCTED frames.** Built from our own reading of the published wire
layout. That is weaker than it looks, and worth stating precisely:

> When you build the frame from your own reading of the spec, the frame agrees with the misreading
> by construction.

So this set is **self-confirming about layout**. It catches an arithmetic slip; it structurally
cannot catch a misread format.

**`captured-*.json` + `derived-fixtures-*.json` — 24 REAL packets.** Bytes Zerodha actually sent,
recorded before anything decoded them (`capture_live_frames.py` hooks the WebSocket `on_message`
callback, the last point at which the raw frame exists). All five layouts, including the 184-byte
full-depth one. The corpus stores **bytes only** — expected values are derived downstream, so no
single reading is baked into the evidence.

**What even the real set does not establish:** that the oracle is right. If `kiteconnect` misreads
a field, the derived fixtures encode the same misreading. Irreducible without vendor documentation
or a third implementation.

**That bound is not hypothetical — it has been hit.** A third capture recorded a text frame the
vendor's own client has no branch for:

```json
{"type": "instruments_meta", "data": {"count": 114823, "etag": "W/…"}}
```

`kiteconnect.ticker._parse_text_message` branches on `order` and `error` only; `instruments_meta`
appears nowhere in that module, so the reference **silently drops it**. This is information that
could not exist in any artefact derived from the reference, because the reference does not know it.

> **If the oracle can be blind to an entire message type, it can be wrong about a field.** That is
> now an existence proof rather than an argument, and it is why "agrees with the vendor's client"
> is the honest claim and "venue fidelity verified" is not.

*(`instruments_meta` looks like an instrument-cache invalidation signal — count plus etag — and is
a **pointer for whoever builds the instrument provider, observed once**. One message in one window
is not a contract.)*

**One claim here does not depend on the oracle at all.** In packets where they separate,
`bytes[60:64]` is always later than `bytes[44:48]` — and a venue cannot stamp a frame before the
trade it reports, so the timestamp mapping is fixed by the data rather than by agreement. See
`tests/live_frames.rs`.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## License

The source code for NautilusTrader is available on GitHub under the
[GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).

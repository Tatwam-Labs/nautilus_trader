# Build handoff — `nautilus-zerodha`, for the MacBook Pro

**Written by**: AT-RustClientBuilder, 2026-08-14
**Branch**: `feat/zerodha-data-client`
**Repo**: `Tatwam-Labs/nautilus_trader` (the fork) — remote `origin`
**Base**: `upstream/develop` @ `36b1045d7c2528380fb0101414d77ec7564914e8` (2026-08-14, *"Serialize Blockchain execution client tests"*)

---

## Why this file exists

**Nothing in this branch has been compiled.** Rust builds are prohibited on the Mac mini (16 GB,
runs the live/paper stack). An attempted `make build-debug` there ran **10m14s without finishing**
and was aborted; it had reached the adapter crates and never got to the PyO3 link. So the first
compile of this code will happen on the MacBook Pro, and it will be a *first* compile — treat
every error below as expected-until-disproven, not as a regression.

> **Do not put this branch on the AT repo.** This is `nautilus_trader` source. AT cannot compile it
> (v2 adapters are in-tree only — ADR-097), and it would be unmergeable upstream from there.

---

## 0. Prerequisites on the MacBook Pro

```bash
rustc --version    # must be 1.97.1 — pinned by rust-toolchain.toml, rustup will fetch it
uv --version       # 0.9.5 or later
```

If the fork is not cloned there yet:

```bash
git clone https://github.com/Tatwam-Labs/nautilus_trader.git
cd nautilus_trader
git remote add upstream https://github.com/nautechsystems/nautilus_trader.git
git fetch --all
```

## 1. Get the branch

```bash
cd <fork>
git fetch origin
git checkout feat/zerodha-data-client
git log -1 --format='%H %s'          # record this sha with every result you report
```

## 2. Build — **thin LTO, never fat**

```bash
uv sync                              # ~6s, measured
CARGO_PROFILE_RELEASE_LTO=thin make build-debug
```

> ⚠️ **`uv sync` rewrites the tracked `python/uv.lock`.** On the Mac mini it produced **431 lines**
> of unrelated churn, dropping the `[options] exclude-newer` pins. That was reverted before commit,
> so this branch is clean. **Check `git status` after your build and `git checkout -- python/uv.lock`
> if it reappears** — it must not reach the upstream PR.

> **`Cargo.lock` has no `nautilus-zerodha` entry yet**, because no cargo command has run since the
> crate was added. Your first build will add it. **Commit that change** — it is a real part of the
> branch, unlike the `uv.lock` churn above.

`make build-debug` uses the `nextest` profile and does not invoke release LTO, so the env var is
belt-and-braces for any release build you run later. **Do not run a fat-LTO release build**; that is
the owner's own finding, adopted upstream.

Expect a **large** first build: this workspace has 19 adapter crates plus the engine, and the
aborted Mac mini attempt produced an **18 GB `target/`** before it was killed. Make sure there is
40 GB+ free.

**Please report the wall-clock time of this first build** — nobody in this fleet has a completed
figure for it. The only number we have is a floor of >10m14s on different, slower hardware.

## 3. Run the tests that matter

```bash
# The decoder fixture tests — the substance of this branch
cargo test -p nautilus-zerodha

# The conventions hook: registration allowlist vs crates/pyo3/src/lib.rs
bash .pre-commit-hooks/check_nautilus_conventions.sh

# The Python surface, after the build has produced the extension module
uv run --no-sync pytest python/tests/unit/adapters/test_public_exports.py
```

## 4. Where the first errors are most likely

Ranked by my own confidence, since none of this has seen a compiler. Each is cheap to check:

| # | Suspect | Why | Likely fix |
|---|---|---|---|
| 1 | `impl_pyo3_config_getters!` in `src/config.rs` | Copied from coinbase; the macro's exact expectations for `Option<String>` fields are unverified | Compare against `crates/adapters/coinbase/src/config.rs` and drop mismatched fields |
| 2 | `#[pyclass]` on `ZerodhaSegment` / `ZerodhaTickMode` | Added `eq, eq_int, from_py_object, rename_all` mirroring `CoinbaseEnvironment`, but a `#[repr]`-less enum with explicit discriminants may still be rejected | Compare against `crates/adapters/coinbase/src/common/enums.rs` |
| 3 | `use nautilus_zerodha::websocket::ZerodhaWsError` in `tests/decoder.rs` | Re-exported via `websocket/mod.rs`; the path is right but untested | Fall back to `websocket::error::ZerodhaWsError` |
| 4 | `is_multiple_of` in `tests/decoder.rs` | Stabilised recently; fine on 1.97.1 but unverified here | `hex.len() % 2 == 0` |
| 5 | Unused-import warnings in `src/python/mod.rs` | The venue constants were removed late (see §5) without a recompile | Delete whatever the compiler names |
| 6 | `ZerodhaSegment` has explicit discriminants (`Nse = 1`) *and* a `Default` of `Unknown = 0` | Fine in Rust; `eq_int` exposes the discriminants to Python, which is intended | — |

**Workspace lints may deny warnings.** If the build fails only on warnings, that is a lint policy
result, not a code defect — say which, so it is not reported as "the adapter does not compile".

## 5. What is actually in this branch

**Complete and testable:**

- `src/websocket/parse.rs` — the Kite binary tick decoder. All five packet layouts (8 / 28 / 32 /
  44 / 184 bytes), per-segment price divisors, five-deep market depth, multi-packet frames,
  heartbeats.
- `test_data/fixtures.json` + `generate_fixtures.py` — 9 fixture cases. **Expected values come from
  the `kiteconnect` 5.2.0 Python reference client, not from this crate**, so the Rust decoder is
  asserted against a decoding it had no part in producing.
- `tests/decoder.rs` — fixture parity, truncation rejection, unknown-length rejection, segment
  decoding, and a check that the fixture set exercises every mode.

**Written but structurally incomplete:**

- `src/data/mod.rs` — implements `DataClient`, but **`connect()` returns an error** rather than
  `Ok(())`. This is deliberate: the default trait body returns `Ok(())`, and a client that reports
  connected while streaming nothing is indistinguishable from a quiet market.
- `src/config.rs`, `src/factories.rs`, `src/python/mod.rs` — config, factory and PyO3 registration.

**Not started:** WebSocket transport, REST instrument provider, historical requests, execution
client.

## 6. What "tested" will mean, and what it will not

After step 3 passes you may say: **the Rust decoder agrees with the `kiteconnect` reference client
on 9 constructed frames.**

You may **not** say the decoder is correct against Zerodha. These frames were *constructed from the
published wire layout*, not captured from a live socket — both implementations could share a
misreading. AT has no raw-frame corpus to draw on: `kiteconnect` decodes inside the library, so the
pre-decode bytes are never persisted anywhere in AT.

**Capturing real frames is the next milestone**, and it needs a live Kite session. Zerodha caps
WebSocket connections per API key, so a dev key cannot disturb prod.

## 7. Report back

Send to **AT-Architecture-Discussions**, with branch **and** sha in the same output:

1. First full build wall-clock time, and peak `target/` size.
2. `cargo test -p nautilus-zerodha` result — **pass/fail counts, including skips**.
3. The conventions-hook result.
4. Every compile error, verbatim. They are expected; the fix list above is a guess, not a diagnosis.

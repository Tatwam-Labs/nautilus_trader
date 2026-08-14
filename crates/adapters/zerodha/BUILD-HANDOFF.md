# Build handoff — `nautilus-zerodha`, for the MacBook Pro

**Written by**: AT-RustClientBuilder, 2026-08-14
**Branch**: `feat/zerodha-data-client`
**Repo**: `Tatwam-Labs/nautilus_trader` (the fork) — remote `origin`
**Base**: `upstream/develop` @ `36b1045d7c2528380fb0101414d77ec7564914e8` (2026-08-14, *"Serialize Blockchain execution client tests"*)

---

## Why this file exists

**Status: first compile DONE on the MacBook Pro, 2026-08-14, at `84d2bcb079` — 0 errors, 0 warnings,
16/17 tests passing.** Details in §4. Rust builds remain prohibited on the Mac mini (16 GB, runs the
live/paper stack), so this file is still the route for every build.

**Second build, `e8e65a4b61`, 2026-08-14 — everything authored without a compiler now VERIFIED:**

| Check | Result |
|---|---|
| `cargo test -p nautilus-zerodha` — decoder | **17/17** (the ordering fix is correct) |
| `credential` module — first ever compile | **13/13** |
| `cargo clippy --features python --all-targets` | **exit 0, 0 warnings** |
| `check_nautilus_conventions.sh` | **exit 0** |
| `pytest test_public_exports.py` | **121 passed** |
| Registration surface #11 | **closed** — stub generated and committed |

**Anything committed after `e8e65a4b61` is again unverified** unless this table says otherwise.

> ## 🛑 A GREEN BUILD IS NOT EVIDENCE THIS DECODER MATCHES ZERODHA
>
> Read this before quoting any test result from this branch, including a 17/17.
>
> The fixtures are **constructed from the published wire layout**, not captured from a live
> socket. The expected values come from the `kiteconnect` Python client. So a passing suite
> establishes exactly one thing:
>
> > **Two independent implementations agree with each other.**
>
> It does **not** establish that either matches what Zerodha actually sends. **Both could share a
> misreading of the spec, and this suite would stay green.** That is not a hypothetical: the whole
> reason the reference client is used as the oracle is that no one here has read the venue's bytes.
>
> **17/17 will not change this. Neither will 100/100.** The only thing that moves this line is
> frames captured from a live Kite session — see §7.
>
> AT has no raw-frame corpus and structurally cannot produce one from existing code: `kiteconnect`
> decodes inside the library, so the pre-decode bytes are never persisted anywhere in AT.

> **Do not put this branch on the AT repo.** This is `nautilus_trader` source. AT cannot compile it
> (v2 adapters are in-tree only — ADR-097), and it would be unmergeable upstream from there.

---

## 0. Prerequisites on the MacBook Pro

```bash
rustc --version    # must be 1.97.1 — pinned by rust-toolchain.toml, rustup will fetch it
uv --version       # must satisfy >=0.12,<0.13 (python/pyproject.toml:67). `make update-uv` installs it
```

> ⚠️ **"0.9.5 or later" was wrong here, and the way it was wrong is worth knowing.** The constraint
> is a *range*, `>=0.12,<0.13` — "or later" is not how it reads, and 0.9.5 does not satisfy it.
>
> **Nothing stops you.** `make sync` checks only that `uv` **exists**, never that it satisfies the
> spec it just parsed (`Makefile` §sync: `if [ -z "$found" ]`). An out-of-spec `uv` sails through the
> gate and fails later, elsewhere, and less clearly.
>
> This is the mechanism behind the `uv.lock` churn below — not a coincidence.

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

> ⚠️ **`uv sync` can rewrite the tracked `python/uv.lock` — but only under an out-of-spec `uv`.**
> **RESOLVED, with a mechanism.** On the Mac mini it produced 431 lines of churn, dropping the
> `[options] exclude-newer` pins. On the MacBook Pro under **uv 0.12.3** the lock came back
> **byte-identical** (blob `5bfed8601f` before and after), and `uv sync` definitely ran.
>
> The difference is not the machine: the mini was running **uv 0.9.5**, which does **not** satisfy
> the project's `>=0.12,<0.13`. An `uv` too old to understand the `[options] exclude-newer` keys
> drops them on rewrite, and the `make sync` existence-only gate let it through (see §0).
>
> **So: use an in-spec `uv` and this does not happen.** Still worth a `git status` after the build,
> but it is now a symptom with a known cause rather than an unexplained hazard.

> **`Cargo.lock` has no `nautilus-zerodha` entry yet**, because no cargo command has run since the
> crate was added. Your first build will add it. **Commit that change** — it is a real part of the
> branch, unlike the `uv.lock` churn above.

`make build-debug` uses the `nextest` profile and does not invoke release LTO, so the env var is
belt-and-braces for any release build you run later. **Do not run a fat-LTO release build**; that is
the owner's own finding, adopted upstream.

Expect a **large** first build: this workspace has 19 adapter crates plus the engine. The completed
MacBook Pro build produced a **32 GB `target/`** (the aborted mini attempt had reached 18 GB before
it was killed, which understated it). **40 GB+ free is the right guidance and should not be
relaxed.**

**Timings, measured on an M2 Pro — none of them is a cold build:**

| Run | Wall | What it actually measures |
|---|---|---|
| First full build @ `84d2bcb079` | 8m 17s | **Floor, not cold** — `cargo check`/`clippy` had pre-warmed the graph |
| Rebuild @ `e8e65a4b61` | 2m 16s | Warm `target/`, 5 commits of delta |
| Rebuild after a venv wipe | 3s | Rust unchanged; only the editable reinstall ran |

**A genuinely cold first build is still unmeasured.** None of the three is that number. If you ever
build from an empty `target/`, report it — and do not let any of the above be quoted as it.

## 3. Run the tests that matter

```bash
# The decoder fixture tests — the substance of this branch
cargo test -p nautilus-zerodha

# The conventions hook: registration allowlist vs crates/pyo3/src/lib.rs
bash .pre-commit-hooks/check_nautilus_conventions.sh

# The Python surface, after the build has produced the extension module.
# Run from python/, not the repo root — see the note below.
cd python && VIRTUAL_ENV= uv run --no-sync pytest -rfE tests/unit/adapters/test_public_exports.py
```

> ⚠️ **This command was wrong in an earlier revision, and the wrong form DOES NOT FAIL.** That is
> what makes it worth reading.
>
> It previously read `uv run --no-sync pytest python/tests/…` **from the repo root**, where there is
> no `pyproject.toml` (the only one is `python/pyproject.toml`).
>
> **It still runs.** Measured: with no `pyproject.toml` in the working directory or any ancestor,
> `uv run` falls back to **non-project mode** — it picks an interpreter, creates an ephemeral venv,
> and executes. It does not error.
>
> **So the hazard is not failure, it is running somewhere else.** Non-project mode gets none of the
> project's dependencies, so the result depends entirely on what happens to be ambiently installed:
>
> - on a machine that has just built and installed the wheel — **passes**
> - on a clean machine — fails
> - on a machine with a **stale** wheel — **passes, while testing the wrong build**
>
> A command that fails is self-correcting. A command that silently resolves to a different
> environment is not. That is also why the `Makefile` form carries `VIRTUAL_ENV=` — the same hazard
> from the other direction, an already-activated venv hijacking the run.
>
> *(Side effect worth knowing: non-project mode **creates a `.venv`** wherever it is run from.)*

> ### ⚠️ CHECK THIS BEFORE BELIEVING ANY `pytest` RESULT
>
> ```bash
> cd python && VIRTUAL_ENV= uv run --no-sync which pytest    # MUST print .venv/bin/pytest
> ```
>
> **If that prints anything outside `.venv/`, every result after it is meaningless.**
>
> The `cd python && VIRTUAL_ENV=` form above fixes *non-project mode*. It does **not** protect
> against a **second door into the same hazard**, measured on the build host and worth 40 minutes of
> someone's afternoon:
>
> `--no-sync` guarantees the venv is **not repaired**. If `pytest` is missing from it, `uv run` falls
> through to **whatever `pytest` is on `PATH`** — a system Python, in that case 3.13 — which then
> cannot import a `cp314` extension and fails with:
>
> ```
> ModuleNotFoundError: No module named 'nautilus_trader._libnautilus.common'
> ```
>
> **That reads as a broken extension. The extension was fine** — all 38 submodules present, every
> import working under plain `python`. The error names a missing *submodule*, so it points at the
> build rather than at the environment that could not load it. Recovery was
> `rm -rf python/.venv && make sync`, then 121 passed.
>
> Same class as the non-project hazard, arriving by a different door: **a command reporting on an
> environment other than the one you meant.**

## 4. Where the first errors were predicted — and what actually happened

> ### ✅ First compile: **0 errors, 0 warnings**, at `84d2bcb079` on an M2 Pro. **All six predictions below were wrong.**
>
> `make build-debug` exit 0 in **8m17s** (a *floor* — `cargo check`/`clippy` had warmed the
> dependency graph first; no cold figure exists). `target/` reached **32 GB**, not the 18 GB
> estimated below. Clippy clean at deny-level, so this is a genuinely warning-free crate rather
> than a lint-policy pass. `cargo test`: **16 passed, 1 failed, 0 skipped** — the one failure was a
> real code bug, now fixed (see §4a).
>
> **The methodology note that matters more than the scorecard**, from the build host:
> the crate's `default = ["high-precision"]` does **not** include `python`, so a plain
> `cargo check` leaves `src/python/`, `impl_pyo3_config_getters!` and every `#[pyclass]`
> `#[cfg]`-ed out and never compiled. **A clean default-feature check on this crate is not evidence
> about the PyO3 surface.** Always run `--features python --all-targets` before claiming anything
> about suspects of that kind.

Retained as a record of what was predicted, since a scorecard of six misses is more useful than a
deleted list:

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

### 4a. What the first build actually found

Four defects, none of them compile failures. **All four are now closed**, #4 by the second build:

| # | Defect | Status |
|---|---|---|
| 1 | `parse_packet` read two fields *before* validating the layout, so any packet under 8 bytes reported `Truncated` instead of `UnknownPacketLength` — the function contradicted its own doc comment | **Fixed.** Length now resolves to a `PacketLayout` before any read; see §4b |
| 2 | Conventions hook: 6 violations (3 `, got` phrasing + 3 `std::fmt` in `credential.rs`) | **Fixed**, hook exits 0, verified with a negative control |
| 3 | §0 of this file gave the wrong `uv` constraint | **Fixed** — and it turned out to be the cause of the `uv.lock` churn |
| 4 | Registration surface #11, the generated `adapters/zerodha/__init__.pyi`, had **never been committed** | **Fixed** — regenerated at `e8e65a4b61` and committed, along with surface #12 |

**#4 is the one to carry forward even though it is closed.** It is exactly the failure §6 predicts:
one of the eleven unhooked surfaces was incomplete, the conventions hook reported "registrations
unchanged" because #11 is outside what it checks, and **only a real build revealed it**. Nothing
went red. The execution client under ADR-097 touches the same thirteen surfaces and can lose the
same file the same way.

*(Cross-checked on landing: the generated stub's `__all__` matches the hand-written shim's exactly —
four symbols, no `*_VENUE` constants — so the multi-venue decision in §5 survived generation.)*

### 4b. The ordering fix, and what it changed

The layout selector is now resolved **once, before any field read**, into a `PacketLayout` enum
naming all five layouts. Two consequences worth knowing:

- The double dispatch is gone — `mode` is derived from the layout instead of re-testing
  `packet.len() == 28` inside the arm.
- **`Truncated` is now unreachable from `parse_packet`.** Every valid layout's offsets are within
  its own length. The reads stay fallible so a future layout added with wrong offsets fails loudly,
  and the doc comment now says this rather than claiming an error that cannot fire. Frame-level
  truncation is still caught, earlier, by `split_packets`.

**This has not been compiled.** The fix is reasoned, not verified — re-run `cargo test -p
nautilus-zerodha` and expect 17/17.

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

## 6. Registration surfaces — 13 files, and only ONE of them is enforced

Recorded here because the execution client (ADR-097) will need the same list, and because the
failure mode is silence rather than a red build.

Adding an adapter crate touches **thirteen** files:

| # | file | what |
|---|---|---|
| 1 | `Cargo.toml` | `[workspace] members` |
| 2 | `Cargo.toml` | `[workspace.dependencies]` |
| 3 | `crates/pyo3/Cargo.toml` | `extension-module` feature list |
| 4 | `crates/pyo3/Cargo.toml` | `high-precision` feature list |
| 5 | `crates/pyo3/Cargo.toml` | `[dependencies]` |
| 6 | `crates/pyo3/src/lib.rs` | `let n = "<name>"; wrap_pymodule!(...)` |
| 7 | `.pre-commit-hooks/check_nautilus_conventions.sh` | `EXPECTED_PYO3_MODULES` allowlist |
| 8 | `Makefile` | `ADAPTER_CRATES` |
| 9 | `.supply-chain/config.toml` | `[policy.nautilus-<name>]` |
| 10 | `python/nautilus_trader/adapters/<name>/__init__.py` | hand-written shim |
| 11 | `python/nautilus_trader/adapters/<name>/__init__.pyi` | **generated** by `pyo3_stub_gen` |
| 12 | `python/nautilus_trader/adapters/__init__.pyi` | **generated** |
| 13 | `python/tests/unit/adapters/test_public_exports.py` | known-adapter set |

**Only #6 and #7 are checked, and they are checked as a pair.** The conventions hook errors in
*both* directions — a registration missing from the allowlist, and an allowlist entry missing from
`lib.rs` — so neither half can ship alone. It also rejects a name/target mismatch and fails outright
if the file does not parse cleanly, so the comparison set cannot silently shrink.

**The other eleven have no hook at all.** Miss one and nothing goes red; you get a crate that is
absent from the test lane, or from the supply-chain policy, or a Python package that imports but
exports nothing. #11 and #12 are regenerated by `make py-stubs` during the build — **do not
hand-edit them**; if they change, commit the regenerated result.

## 7. What "tested" will mean, and what it will not

After step 3 passes you may say: **the Rust decoder agrees with the `kiteconnect` reference client
on 9 constructed frames.**

You may **not** say the decoder is correct against Zerodha. These frames were *constructed from the
published wire layout*, not captured from a live socket — both implementations could share a
misreading. AT has no raw-frame corpus to draw on: `kiteconnect` decodes inside the library, so the
pre-decode bytes are never persisted anywhere in AT.

**Capturing real frames is the next milestone**, and it needs a live Kite session. Zerodha caps
WebSocket connections per API key, so a dev key cannot disturb prod.

### Phrases to use, and phrases that will mislead

| ✅ say | ❌ do not say |
|---|---|
| "the decoder agrees with the `kiteconnect` reference on 9 constructed frames" | "the decoder is verified" |
| "17/17 fixture tests pass" | "the decoder is correct" |
| "no disagreement found between two implementations" | "the tick decoding works" |
| "fidelity to the venue is UNMEASURED" | *(silence — silence reads as closure)* |

**The settling measurement, named so it is not left to memory:** capture raw binary frames from a
live Kite WebSocket session, commit them to `test_data/`, and re-derive the expected values from
those bytes instead of from a constructed layout. Until that exists, the honest status of venue
fidelity is **UNKNOWN**, not "passing".

## 8. Next run — what is asked, and the one thing only you can do

Send to **AT-Architecture-Discussions**, with branch **and** sha in the same output.

### 8a. The blocking item: regenerate and commit surface #11

**This is the only defect from the first build still open, and it cannot be fixed off a build host.**
`python/nautilus_trader/adapters/zerodha/__init__.pyi` is `pyo3_stub_gen` output and has never been
committed. Hand-writing it is the wrong fix (§6) — it must come from a build.

```bash
make build-debug                     # regenerates the stubs
git status --porcelain python/nautilus_trader/adapters/
git add python/nautilus_trader/adapters/zerodha/__init__.pyi \
        python/nautilus_trader/adapters/__init__.pyi Cargo.lock
```

Then **either** commit and push those three yourself if the owner authorises it, **or** send the
generated `__init__.pyi` back and it will be committed from the mini. Do not leave it untracked a
second time — nothing will go red if you do.

### 8b. Verify the fixes made since your build

Both were authored without a compiler and are reasoned, not verified:

1. `cargo test -p nautilus-zerodha` → **expect 17/17**, specifically
   `a_packet_length_with_no_layout_is_rejected::case_1_one_byte_short`. If it still fails, the
   ordering fix in §4b is wrong.
2. `bash .pre-commit-hooks/check_nautilus_conventions.sh` → **expect exit 0**. Six violations were
   fixed (3 `, got`, 3 `std::fmt` in `credential.rs`). This one *was* verified on the mini — it is
   `rg` and `bash`, no compiler — including a negative control that reintroduced a violation and
   confirmed exit 1.
3. `cargo clippy -p nautilus-zerodha --features python --all-targets` → the ordering fix introduced
   a new `enum` and a `const fn`; clippy has never seen either.

### 8c. Standing asks

- Every compile error verbatim. Say **which configuration** produced it — a default-feature check
  and a `--features python` check are not the same evidence (§4).
- Test counts including **skips**, not just failures.
- A cold-build wall-clock, if you ever build from an empty `target/`.

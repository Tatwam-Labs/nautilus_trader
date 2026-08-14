#!/usr/bin/env python3
"""Capture RAW Zerodha WebSocket frames, before anything decodes them.

WHY THIS EXISTS
---------------
`fixtures.json` is built from frames CONSTRUCTED from the published wire layout. Its
expected values come from the `kiteconnect` reference client, so a passing suite proves
that two implementations agree with each other -- **not** that either matches what Zerodha
actually sends. Both could share a misreading and every test would stay green.

Only frames captured from a live socket settle that. This script captures them.

WHY IT CANNOT REUSE ANY EXISTING AT CODE
----------------------------------------
`kiteconnect` decodes INSIDE the library: by the time `on_ticks` fires, the raw bytes are
gone. AT consumes `on_ticks`, so nothing in AT has ever seen a pre-decode frame and no
amount of searching AT will find one.

The interception point is the `on_message` callback, which `KiteTicker._on_message` invokes
with the untouched `payload` BEFORE `_parse_binary` runs. That is the only place the raw
bytes exist, so that is where this hooks. No subclassing and no patching -- it is a
supported public callback.

WHAT THIS DOES AND DOES NOT DO
------------------------------
DOES:      open a read-only market-data WebSocket, subscribe to the tokens you name, and
           write RAW FRAMES to a JSON file, with the subscription that produced them.
DOES NOT:  place, modify or cancel any order. It never imports `KiteConnect`, only
           `KiteTicker`, so there is no order API in the process at all.

WHY NO DECODED VALUES ARE RECORDED
----------------------------------
The recorder stores **bytes and nothing else**. It would be easy to also store the
`kiteconnect` decoding of each frame, and it would be a mistake.

The point of a live corpus is that expected values get **re-derived from the bytes later**.
If the recorder bakes in a decode, every fixture built from the corpus inherits that one
reading, and the result is two implementations agreeing with each other again -- the exact
weakness live capture exists to remove. Worse, it freezes the oracle: if the reference
client is later found wrong about a field, a corpus holding only bytes can be re-derived,
while one holding answers has to be recaptured from a market that has moved on.

So: **bytes are ground truth and live here. Interpretation happens downstream**, in
`generate_fixtures.py`, where the oracle is named and its version recorded.

*(Credit: this was a review catch. The first draft of this script stored a
`reference_decoded` field alongside each frame.)*

SAFETY
------
* **Read-only.** Market data only. No order path is imported.
* **Time-bounded.** `--duration` is required and capped; it cannot run unattended forever.
* **Shape-sampled, so the disk cost is bounded and small.** It keeps at most
  `MAX_PER_SHAPE` (3) examples of each distinct `(packet_length, segment)` pair. Five
  layouts across nine segments is 45 shapes, so **135 frames maximum, well under 100 KB --
  regardless of how long it runs.** Fixtures need coverage, not volume; recording a whole
  `full`-mode session across a wide strike band would be gigabytes and would add nothing.
  This matters because the machine that may run it is disk-constrained.
* **Secrets are never printed.** The access token is read from the environment and never
  logged, echoed, or written to the output file.
* **Connection limits.** Zerodha caps WebSocket connections PER API KEY. Use a dev key --
  a separate key cannot disturb a production connection.

USAGE
-----
    export ZERODHA_API_KEY=...        # never passed on the command line: argv is visible
    export ZERODHA_ACCESS_TOKEN=...   # in `ps` to every user on the machine

    # One --subscribe per MODE. The layout is a function of mode x tradability, so a
    # single-mode capture cannot produce all five layouts however many tokens it carries.
    python3 capture_live_frames.py --duration 120 \
        --subscribe ltp:<option_token> \
        --subscribe quote:265,<option_token> \
        --subscribe full:256265,<option_token>

Then check the capture is usable BEFORE building anything on it:

    python3 capture_live_frames.py --verify captured-<ts>.json

MARKET HOURS
------------
Outside them you will connect successfully and receive nothing but heartbeats. That is not
a failure and the script says so explicitly rather than leaving you to wonder -- an empty
capture and a broken capture look identical otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from kiteconnect import KiteTicker
    from kiteconnect.__version__ import __version__ as KITECONNECT_VERSION
except ImportError:
    sys.exit("kiteconnect is required: pip install kiteconnect")

# A capture is for shape coverage, not volume. Keeping a handful of each distinct shape
# gives every layout and segment combination without writing a gigabyte of near-duplicates.
MAX_PER_SHAPE = 3
# Hard ceiling on --duration. A capture script that can run indefinitely will eventually be
# left running by accident against a connection-capped API key.
MAX_DURATION_SECS = 900


def _packet_shapes(payload: bytes) -> list[tuple[int, int]]:
    """Return the (packet_length, segment) pairs in a frame, without fully decoding it.

    Only the framing header and each packet's first four bytes are read, so this stays
    independent of the decoding under test -- it is used to decide what to KEEP, and must
    not be able to bias what gets captured.
    """
    if len(payload) < 2:
        return []  # Heartbeat
    shapes: list[tuple[int, int]] = []
    count = int.from_bytes(payload[0:2], "big")
    cursor = 2
    for _ in range(count):
        if cursor + 2 > len(payload):
            break
        length = int.from_bytes(payload[cursor : cursor + 2], "big")
        cursor += 2
        packet = payload[cursor : cursor + length]
        if len(packet) < 4:
            break
        token = int.from_bytes(packet[0:4], "big")
        shapes.append((len(packet), token & 0xFF))
        cursor += length
    return shapes


def capture(groups: dict[str, list[int]], duration: int, out_path: Path) -> int:
    """Capture frames for a set of (mode -> tokens) subscription groups.

    # The layout is a function of MODE x TRADABILITY, not of the instrument

    This is the single most important thing about a useful capture, and it is easy to get
    wrong in a way that looks successful:

    | mode  | instrument | packet |
    |-------|------------|--------|
    | ltp   | any        | 8      |
    | quote | index      | 28     |
    | full  | index      | 32     |
    | quote | tradable   | 44     |
    | full  | tradable   | 184    |

    **A capture that subscribes everything in one mode cannot produce all five layouts, no
    matter how many tokens it carries** -- and it will still report frames captured and look
    like a complete run. One token can hold only one mode per connection, so covering both
    index rows needs TWO index tokens.
    """
    api_key = os.environ.get("ZERODHA_API_KEY", "").strip()
    access_token = os.environ.get("ZERODHA_ACCESS_TOKEN", "").strip()
    if not api_key or not access_token:
        sys.exit(
            "ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN must both be set in the environment.\n"
            "Do NOT pass them as arguments -- argv is visible in `ps` to every user."
        )

    # Prefix only. The full key is a secret and the token doubly so.
    print(f"api_key={api_key[:4]}... (token present, {len(access_token)} chars, not shown)")
    print(f"kiteconnect={KITECONNECT_VERSION}  duration={duration}s")
    for m, toks in groups.items():
        print(f"  {m:<5} x{len(toks):<3} {toks[:6]}{' ...' if len(toks) > 6 else ''}")

    all_tokens = [t for toks in groups.values() for t in toks]
    # A token in two modes at once is a silent misconfiguration: the last set_mode wins, so
    # one of the layouts you think you are capturing simply never arrives.
    dupes = {t for t in all_tokens if all_tokens.count(t) > 1}
    if dupes:
        sys.exit(f"token(s) {sorted(dupes)} appear in more than one mode; each can hold only one")

    kws = KiteTicker(api_key, access_token)
    seen: Counter[tuple[int, int]] = Counter()
    records: list[dict] = []
    stats = {"frames": 0, "heartbeats": 0, "kept": 0}
    started = time.monotonic()

    def on_message(ws, payload, is_binary):
        """Fires BEFORE _parse_binary. `payload` is the untouched frame."""
        stats["frames"] += 1
        if not is_binary:
            return  # Text control frames carry no ticks
        if len(payload) < 2:
            stats["heartbeats"] += 1
            return

        shapes = _packet_shapes(payload)
        # Keep the frame if it contains any shape we are still short of.
        if not any(seen[s] < MAX_PER_SHAPE for s in shapes):
            return
        for s in shapes:
            seen[s] += 1

        # BYTES ONLY. No decoded form is stored here, deliberately -- see the module docstring
        # section "Why no decoded values are recorded".
        records.append(
            {
                "captured_at": datetime.now(timezone.utc).isoformat(),
                "frame_hex": payload.hex(),
                "frame_len": len(payload),
                "shapes": [{"packet_len": p, "segment": s} for p, s in shapes],
            }
        )
        stats["kept"] += 1

    def on_connect(ws, response):
        ws.subscribe(all_tokens)
        # set_mode PER GROUP, not once for everything -- the mode is half of what selects
        # the packet layout, so a single blanket set_mode collapses the matrix.
        for m, toks in groups.items():
            ws.set_mode(m, toks)
        print(f"connected; subscribed {len(all_tokens)} tokens across {len(groups)} modes")

    def on_error(ws, code, reason):
        print(f"ERROR code={code} reason={reason}", file=sys.stderr)

    def stop(*_):
        try:
            kws.close()
        finally:
            _write(out_path, records, groups, stats, seen)
            _report(stats, seen, out_path)
            sys.stdout.flush()
            # os._exit, NOT sys.exit. MEASURED on the first live run: KiteTicker.connect()
            # runs a twisted reactor, and sys.exit() from inside a signal handler raises
            # SystemExit on the reactor thread where it is swallowed -- the capture file was
            # written correctly and the process then hung indefinitely, holding a WebSocket
            # connection against a per-key-capped budget until it was killed by hand.
            #
            # Everything is already flushed to disk above, so there is nothing for a clean
            # shutdown to do that has not been done.
            os._exit(0)

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    kws.on_message = on_message
    kws.on_connect = on_connect
    kws.on_error = on_error

    # A watchdog rather than a blocking sleep, so the bound holds even if the socket stalls.
    def on_ticks(ws, ticks):
        if time.monotonic() - started > duration:
            stop()

    kws.on_ticks = on_ticks
    signal.signal(signal.SIGALRM, stop)
    signal.alarm(duration + 5)

    kws.connect(threaded=False)
    return 0


def _jsonable(value):
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    return value


def _write(path: Path, records, groups, stats, seen) -> None:
    header = {
        "_comment": (
            "RAW frames captured from a live Zerodha WebSocket, recorded BEFORE any decode. "
            "Unlike fixtures.json these are ground truth from the venue, not constructed from "
            "the published layout. "
            "NO DECODED VALUES ARE STORED HERE, DELIBERATELY: a corpus that carries one "
            "implementation's reading hands that reading to every fixture derived from it, "
            "which re-closes the circle live capture exists to break. Expected values must be "
            "derived downstream by an oracle named with its version. "
            "STRUCTURE: a record is one WebSocket MESSAGE, not one packet. A message carries "
            "several packets and 'shapes' lists them, so the record count is NOT the packet "
            "count -- a deriver written against records will get the framing wrong."
        ),
        # The library used for TRANSPORT only. It decoded nothing that reached this file.
        "captured_with": f"kiteconnect {KITECONNECT_VERSION} (transport only, not an oracle)",
        # WHICH TOKEN WAS IN WHICH MODE. Without this the bytes cannot be interpreted
        # later: the mode is half of what determines the layout under test.
        "subscriptions": {m: toks for m, toks in groups.items()},
        "captured_tokens": sorted(t for toks in groups.values() for t in toks),
        "mode": "+".join(groups),
        "stats": dict(stats),
        "shape_coverage": [
            {"packet_len": p, "segment": s, "count": c} for (p, s), c in sorted(seen.items())
        ],
    }
    # Written whole then renamed, so a reader never sees a half-written capture.
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_text(json.dumps({"header": header, "records": records}, indent=2) + "\n")
    tmp.replace(path)


def _report(stats, seen, out_path) -> None:
    print(f"\nframes={stats['frames']}  heartbeats={stats['heartbeats']}  kept={stats['kept']}")
    if not seen:
        print(
            "\nNOTHING CAPTURED. Connecting successfully and receiving only heartbeats is what\n"
            "an out-of-hours run looks like -- it is NOT a broken capture. Check the market is\n"
            "open and the tokens are correct before suspecting the script."
        )
        return
    print("\nshape coverage (packet_len, segment) -> count:")
    for (plen, seg), n in sorted(seen.items()):
        print(f"  {plen:>4} bytes  segment {seg:<3}  x{n}")
    missing = {8, 28, 32, 44, 184} - {p for p, _ in seen}
    if missing:
        print(f"\nNOT SEEN: packet lengths {sorted(missing)}.")
        print("A layout absent here stays UNVERIFIED against the venue -- say so rather than")
        print("letting the captured ones imply whole-protocol coverage.")
    print(f"\nwrote {out_path}")


def verify(path: Path) -> int:
    """Negative control for the RECORDER: assert a capture could have caught a bad one.

    A recorder that has only ever produced files that "look fine" proves nothing. These
    checks are chosen so that a plausible failure trips at least one of them:

      * an empty or heartbeat-only capture (out of hours, wrong tokens, no subscription)
      * a capture whose frames decode to no known layout (framing misread, wrong offsets)
      * a capture holding decoded values (a future edit re-introducing the circularity)
    """
    doc = json.loads(path.read_text())
    header, records = doc.get("header", {}), doc.get("records", [])
    failures: list[str] = []

    if not records:
        failures.append(
            "NO RECORDS. Out of hours this is what a successful connection looks like — "
            "the socket opens and sends only heartbeats. Distinguish before re-running."
        )

    lengths = {s["packet_len"] for r in records for s in r.get("shapes", [])}
    known = {8, 28, 32, 44, 184}
    if records and not (lengths & known):
        failures.append(
            f"NO PACKET MATCHES A KNOWN LAYOUT (saw {sorted(lengths)}). Either the framing "
            "is being misread or the venue changed the protocol. Do not build fixtures."
        )

    if any("reference_decoded" in r or "expected" in r for r in records):
        failures.append(
            "A RECORD CONTAINS DECODED VALUES. The corpus must hold bytes only, or fixtures "
            "derived from it inherit one implementation's reading. See the module docstring."
        )

    if not header.get("captured_tokens") or not header.get("mode"):
        failures.append(
            "MISSING SUBSCRIPTION CONTEXT. The mode determines the layout, so a corpus "
            "without it cannot be interpreted later."
        )

    # Round-trip the hex: a frame that will not decode to bytes is unusable downstream.
    for i, r in enumerate(records):
        try:
            raw = bytes.fromhex(r["frame_hex"])
        except (ValueError, KeyError):
            failures.append(f"record {i}: frame_hex is not valid hex")
            break
        if len(raw) != r.get("frame_len"):
            failures.append(f"record {i}: frame_len disagrees with the payload length")
            break

    print(f"{path}: {len(records)} records, packet lengths {sorted(lengths) or '(none)'}")
    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1

    missing = known - lengths
    print("PASSED — the corpus is usable.")
    if missing:
        print(
            f"\nBut layouts {sorted(missing)} were NOT captured. Those stay UNVERIFIED "
            "against the venue; do not let the captured ones imply full coverage."
        )
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--subscribe",
        action="append",
        metavar="MODE:TOKENS",
        help="repeatable, e.g. --subscribe full:265,256265 (modes: ltp, quote, full)",
    )
    p.add_argument("--duration", type=int, default=120, help=f"seconds (max {MAX_DURATION_SECS})")
    p.add_argument("--out", type=Path, help="output path (default: captured-<ts>.json)")
    p.add_argument("--verify", type=Path, help="check a capture is usable, then exit")
    args = p.parse_args()

    if args.verify:
        return verify(args.verify)

    if not args.subscribe:
        p.error("at least one --subscribe MODE:TOKENS is required")
    if not 0 < args.duration <= MAX_DURATION_SECS:
        p.error(f"--duration must be 1..{MAX_DURATION_SECS}")

    groups: dict[str, list[int]] = {}
    for spec in args.subscribe:
        mode, _, toks = spec.partition(":")
        if mode not in ("ltp", "quote", "full") or not toks.strip():
            p.error(f"bad --subscribe {spec!r}; expected MODE:TOKENS with MODE in ltp|quote|full")
        groups.setdefault(mode, []).extend(int(t) for t in toks.split(",") if t.strip())

    out = args.out or Path(f"captured-{datetime.now().strftime('%Y-%m-%d-%H%M')}.json")
    return capture(groups, args.duration, out)


if __name__ == "__main__":
    raise SystemExit(main())

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
           Binary frames (ticks) and text frames (acks, errors) are both recorded, in
           separate arrays.
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
⚠️ **THE GATES BELOW PROTECT THE NEXT `git push`, NOT A FUTURE RELEASE.**
`Tatwam-Labs/nautilus_trader` is a fork of a public repository and is itself **PUBLIC** —
verified not from the repo setting but from the fact that an **unauthenticated** fetch of a
committed corpus returns **HTTP 200**. So anything committed and pushed to this branch is
published at that moment. There is no pre-release window in which to clean it up, and reading
the redaction as tidiness before a PR is the mistake this paragraph exists to prevent.

* **Read-only.** Market data only. No order path is imported.
* **Time-bounded.** `--duration` is required and capped; it cannot run unattended forever.
* **Sampled on SHAPE and spaced in TIME, so the disk cost is bounded and small.** The time
  spacing matters as much as the cap: shape sampling alone answers "which layouts exist" and
  cannot answer anything about how a field CHANGES, because a single message can fill a
  shape's quota with four copies of one instant.
* **Shape-sampled, so the disk cost is bounded and small.** It keeps at most
  `MAX_PER_SHAPE` (3) examples of each distinct `(packet_length, segment)` pair. Five
  layouts across nine segments is 45 shapes, so **135 frames maximum, well under 100 KB --
  regardless of how long it runs.** Fixtures need coverage, not volume; recording a whole
  `full`-mode session across a wide strike band would be gigabytes and would add nothing.
  This matters because the machine that may run it is disk-constrained.
* **Order data is never stored.** Text frames with `type == "order"` carry the account's own
  order details; they are counted and discarded. We record that one arrived and nothing about
  what it said. This matters if a corpus is ever published -- "no orders were placed that day"
  is an assumption, not a guarantee, and the exclusion does not depend on it.
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
# Minimum seconds between two KEPT samples of the same shape.
#
# Shape sampling alone answers "which layouts exist" and CANNOT answer anything about how a
# field changes over time. Measured 2026-08-14: one WebSocket message carried four packets of
# the same shape and took it straight to MAX_PER_SHAPE, so a 300-second run kept ONE message
# out of 952 frames. The question that run existed to answer -- whether two timestamp fields
# ever separate -- then rested entirely on that first message happening to contain the answer.
# Spacing repeat samples in time makes the corpus span the session rather than an instant.
MIN_SHAPE_INTERVAL_SECS = 20.0
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


# Keys that mark a payload as carrying the ACCOUNT's own order flow. Matched on KEYS, at ANY
# depth, regardless of the frame's declared `type`.
ORDER_IDENTIFYING_KEYS = {
    "order_id", "exchange_order_id", "parent_order_id", "tradingsymbol",
    "user_id", "account_id", "placed_by", "average_price", "filled_quantity",
}


def _has_order_keys(node) -> bool:
    """Recursively test whether any order-identifying KEY appears anywhere in a decoded payload.

    Keys, not values, deliberately: an error string that happens to mention a tradingsymbol is
    venue behaviour worth keeping, while `{"data": {"order_id": ...}}` with no `type` field at all
    is order flow wearing an unfamiliar shape.
    """
    if isinstance(node, dict):
        if ORDER_IDENTIFYING_KEYS & node.keys():
            return True
        return any(_has_order_keys(v) for v in node.values())
    if isinstance(node, list):
        return any(_has_order_keys(v) for v in node)
    return False


def _keep_text_frame(payload, text_records: list[dict], stats: dict) -> None:
    """Record a text control frame, subject to TWO INDEPENDENT GATES.

    Text frames are what the binary corpus is missing: acknowledgements and errors. Without them,
    handling written for them is written against the reference client's source rather than against
    observed bytes -- the self-confirming position captured frames exist to escape.

    # Why two gates rather than a list of types

    A DENY-LIST on `type` fails OPEN: a frame whose type is absent, misspelled, renamed by the
    venue or nested differently walks straight in -- and we have never observed a single Zerodha
    text frame, so the space of shapes is exactly what we do not know.

    An ALLOW-LIST on `type` fails CLOSED but throws away the unparseable and unrecognised frames,
    which are the most valuable thing here: a corpus built from documentation cannot contain them
    by construction.

    So `type` is not the gate at all. What is kept and what is redacted are separate questions:

      KEEP    everything -- unknown types, untyped frames, non-JSON
      REDACT  any payload carrying order-identifying KEYS at any depth, whatever its type

    A frame has to defeat both to leak.

    # Every push is a publication

    This repository is public. The gates are not pre-release hygiene; they run before data reaches
    a commit, because a commit here is the publication event.

    # Non-JSON cannot be scanned

    The key scan needs a decoded structure. Non-JSON text is kept and marked
    `review_before_publication`, which `--verify` treats as a hard gate rather than a note. It
    converts an unknown into a FLAGGED unknown, which is the most that can honestly be done.
    """
    stats["text_frames"] = stats.get("text_frames", 0) + 1
    raw = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else str(payload)

    def _discard(reason: str) -> None:
        stats.setdefault("text_discarded_by_reason", {})
        stats["text_discarded_by_reason"][reason] = (
            stats["text_discarded_by_reason"].get(reason, 0) + 1
        )
        stats["text_frames_discarded"] = stats.get("text_frames_discarded", 0) + 1

    try:
        parsed = json.loads(raw)
    except ValueError:
        # GATE 2 cannot run. Keep it -- undocumented venue behaviour is the point -- but flag it.
        if sum(1 for r in text_records if not r.get("parses_as_json")) >= MAX_PER_SHAPE:
            return
        text_records.append({
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "type": None,
            "raw": raw,
            "parses_as_json": False,
            "review_before_publication": True,
        })
        return

    if _has_order_keys(parsed):
        kind = parsed.get("type") if isinstance(parsed, dict) else None
        _discard(f"order-identifying keys (type={kind!r})")
        return

    kind = parsed.get("type") if isinstance(parsed, dict) else None
    if [r.get("type") for r in text_records].count(kind) >= MAX_PER_SHAPE:
        return

    text_records.append({
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "type": kind,
        "raw": raw,
        "parses_as_json": True,
        "review_before_publication": False,
    })


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
    last_kept: dict[tuple[int, int], float] = {}
    heartbeat_times: list[float] = []
    records: list[dict] = []
    text_records: list[dict] = []
    stats = {"frames": 0, "heartbeats": 0, "kept": 0, "text_frames": 0, "text_frames_discarded": 0}
    started = time.monotonic()

    def on_message(ws, payload, is_binary):
        """Fires BEFORE _parse_binary. `payload` is the untouched frame."""
        stats["frames"] += 1
        if not is_binary:
            _keep_text_frame(payload, text_records, stats)
            return
        if len(payload) < 2:
            # Heartbeats carry no content, but their TIMING is the thing a transport uses to
            # decide a connection is dead -- and two earlier captures counted 130 of them and
            # discarded every one, leaving no sample of the interval. Record arrival times only.
            stats["heartbeats"] += 1
            heartbeat_times.append(time.monotonic())
            return

        shapes = _packet_shapes(payload)
        now = time.monotonic()
        # Keep the frame if it carries any shape that is BOTH under its cap AND not sampled too
        # recently. The time gate is what makes repeat samples informative rather than four
        # copies of the same instant -- see MIN_SHAPE_INTERVAL_SECS.
        wanted = [
            s for s in shapes
            if seen[s] < MAX_PER_SHAPE
            and (s not in last_kept or now - last_kept[s] >= MIN_SHAPE_INTERVAL_SECS)
        ]
        if not wanted:
            return
        for s in wanted:
            seen[s] += 1
            last_kept[s] = now

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
            _write(out_path, records, text_records, groups, stats, seen, heartbeat_times)
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


def _write(path: Path, records, text_records, groups, stats, seen, heartbeat_times=()) -> None:
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
        # Intervals only, never absolute times -- the interval is the transport-relevant fact
        # and it carries nothing about when we were connected.
        "heartbeat_intervals_secs": (
            [round(b - a, 3) for a, b in zip(heartbeat_times, heartbeat_times[1:])]
            if len(heartbeat_times) > 1 else []
        ),
        "shape_coverage": [
            {"packet_len": p, "segment": s, "count": c} for (p, s), c in sorted(seen.items())
        ],
    }
    # LAST GATE, AT THE POINT OF WRITING. The per-frame scan should already have caught this;
    # this catches the case where it did not.
    #
    # It is here because a miss is UNRECOVERABLE. This repository is public, so the commit that
    # adds a corpus publishes it, and editing the file afterwards leaves the data in git history.
    # There is no "clean it up before the PR" step to fall back on.
    #
    # Refusing to write is the only remedy that works, so the check runs where the data would
    # otherwise reach disk rather than where someone has to remember to look.
    rendered = json.dumps({"header": header, "records": records, "text_records": text_records},
                          indent=2)
    # BARE substring, not the quoted key. json.dumps ESCAPES the inner quotes of a stored raw
    # string, so `"order_id"` inside a captured payload renders as `\"order_id\"` and a quoted
    # pattern never matches it. That exact mistake was in the first version of this gate and it
    # wrote the file its negative control was built to stop.
    #
    # A bare scan over-matches -- a legitimate error string containing "user_id" would block the
    # write. That is the correct direction for a last-resort gate whose miss is unrecoverable:
    # a false positive is visible and fixable in seconds, a false negative is public forever.
    offenders = sorted(k for k in ORDER_IDENTIFYING_KEYS if k in rendered)

    # SECOND, INDEPENDENT REPRESENTATION. The scan above reads the serialised TEXT; this one
    # re-parses each stored payload and walks the STRUCTURE. Deliberate duplication: the bug this
    # gate shipped with was a scan that read the wrong representation, so neither representation
    # is now the only thing checked.
    for record in text_records:
        try:
            if _has_order_keys(json.loads(record.get("raw", ""))):
                offenders.append(f"structural:{record.get('type')!r}")
        except (ValueError, TypeError):
            pass  # Non-JSON is flagged for human review elsewhere; it cannot be walked.

    if offenders:
        raise SystemExit(
            "REFUSING TO WRITE: order-identifying keys reached the corpus: "
            f"{offenders}\n"
            "The per-frame redaction did not hold. Nothing has been written. Do not work around "
            "this by editing the output -- fix the gate in _keep_text_frame and recapture."
        )

    # Written whole then renamed, so a reader never sees a half-written capture.
    tmp = path.with_suffix(path.suffix + ".partial")
    tmp.write_text(rendered + "\n")
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

    # Text frames: the redaction gates must be checkable after the fact, not trusted.
    text = doc.get("text_records", [])
    leaked = [
        i for i, r in enumerate(text)
        if any(k in r.get("raw", "") for k in ORDER_IDENTIFYING_KEYS)
    ]
    if leaked:
        failures.append(
            f"ORDER-IDENTIFYING KEYS IN STORED TEXT at record(s) {leaked}. Both redaction gates "
            "were defeated, or this corpus predates them. Do NOT publish or derive from it."
        )

    flagged = [i for i, r in enumerate(text) if r.get("review_before_publication")]

    print(f"{path}: {len(records)} records, packet lengths {sorted(lengths) or '(none)'}")
    if text:
        print(f"  text frames stored: {len(text)}  flagged for review: {len(flagged)}")
    if failures:
        print("\nFAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1

    missing = known - lengths
    print("PASSED — the corpus is usable.")
    if flagged:
        print(
            f"\n⚠️  {len(flagged)} text record(s) are marked review_before_publication — non-JSON "
            "frames the key scan could not read.\n"
            "    These must be read by a human before this corpus is published or sent anywhere."
        )
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

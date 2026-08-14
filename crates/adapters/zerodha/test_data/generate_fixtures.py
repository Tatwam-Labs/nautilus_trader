#!/usr/bin/env python3
"""Generate Zerodha Kite streaming fixtures for the Rust decoder tests.

WHY THIS SCRIPT EXISTS, AND WHY THE EXPECTED VALUES DO NOT COME FROM THE RUST CODE
---------------------------------------------------------------------------------
A fixture whose expected values were produced by the code under test cannot fail: it
compares a mapping against itself. So this script builds the frame bytes from the
published Kite Connect wire layout, then decodes them with the **reference**
implementation -- `kiteconnect.KiteTicker._parse_binary`, an independent codebase -- and
writes what the *reference* produced as the expected values.

The Rust decoder is then asserted against a decoding it had no part in producing.

WHAT THIS DOES AND DOES NOT ESTABLISH
------------------------------------
ESTABLISHES: the Rust decoder agrees with the reference client on these byte layouts.
DOES NOT ESTABLISH: that either implementation matches what Zerodha actually sends.
Both could share a misreading of the spec. Only frames captured from a live socket
settle that, and this fleet has no raw-frame corpus -- AT consumes kiteconnect's decoded
dicts via `on_ticks`, so the pre-decode bytes are never persisted anywhere.
Replacing these with captured frames is the next step and is tracked as such.

USAGE
-----
    python3 generate_fixtures.py            # writes fixtures.json next to this file
    python3 generate_fixtures.py --verify   # re-derive and diff, do not write

Requires `kiteconnect` (pinned below by the version this was generated against).
"""

from __future__ import annotations

import argparse
import json
import struct
import sys
from datetime import datetime
from pathlib import Path

try:
    from kiteconnect import KiteTicker
    # `kiteconnect.__version__` is a SUBMODULE, not the version string -- the string is one
    # level further in. Reading the attribute directly yields a repr of the module object.
    from kiteconnect.__version__ import __version__ as KITECONNECT_VERSION
except ImportError:  # pragma: no cover - the script is unusable without the reference
    sys.exit("kiteconnect is required to derive expected values: pip install kiteconnect")

HERE = Path(__file__).parent
OUT = HERE / "fixtures.json"

# Segment codes are the low byte of the instrument token (Kite Connect streaming docs).
SEG_NSE = 1
SEG_NFO = 2
SEG_CDS = 3
SEG_BSE = 4
SEG_BFO = 5
SEG_BCD = 6
SEG_INDICES = 9


def token(base: int, segment: int) -> int:
    """Build an instrument token carrying `segment` in its low byte."""
    return (base << 8) | segment


def frame(*packets: bytes) -> bytes:
    """Wrap packets in the count-prefixed binary frame envelope."""
    out = struct.pack(">H", len(packets))
    for p in packets:
        out += struct.pack(">H", len(p)) + p
    return out


def ltp_packet(tok: int, last_price: int) -> bytes:
    """8-byte LTP packet."""
    return struct.pack(">II", tok, last_price)


def index_quote_packet(tok: int, last, high, low, open_, close) -> bytes:
    """28-byte index quote packet. NOTE the field order: high, low, open, close."""
    return struct.pack(">IIIIII", tok, last, high, low, open_, close) + b"\x00" * 4


def index_full_packet(tok: int, last, high, low, open_, close, ts) -> bytes:
    """32-byte index full packet -- the 28-byte body plus an exchange timestamp."""
    # The 28-byte layout is 24 bytes of fields plus 4 bytes the venue does not use.
    return struct.pack(">IIIIIII", tok, last, high, low, open_, close, 0) + struct.pack(
        ">I", ts
    )


def quote_packet(tok, last, ltq, atp, vol, tbq, tsq, open_, high, low, close) -> bytes:
    """44-byte tradable quote packet. NOTE: open, high, low, close -- NOT the index order."""
    return struct.pack(
        ">IIIIIIIIIII", tok, last, ltq, atp, vol, tbq, tsq, open_, high, low, close
    )


def full_packet(
    tok, last, ltq, atp, vol, tbq, tsq, open_, high, low, close,
    ltt, oi, oi_hi, oi_lo, ts, depth,
) -> bytes:
    """184-byte full packet: the 44-byte quote body, timestamps, OI, then 10 depth levels."""
    body = quote_packet(tok, last, ltq, atp, vol, tbq, tsq, open_, high, low, close)
    body += struct.pack(">IIIII", ltt, oi, oi_hi, oi_lo, ts)
    assert len(body) == 64, f"header should be 64 bytes, was {len(body)}"
    # 10 levels x 12 bytes: u32 quantity, u32 price, u16 orders, 2 bytes padding.
    for qty, price, orders in depth:
        body += struct.pack(">IIHH", qty, price, orders, 0)
    assert len(body) == 184, f"full packet should be 184 bytes, was {len(body)}"
    return body


def build_cases() -> list[dict]:
    """Construct the fixture frames, each exercising one decision in the decoder."""
    depth = [
        # 5 bid levels then 5 ask levels, each (quantity, price_int, orders).
        (100, 2_450_000, 3), (250, 2_449_500, 5), (75, 2_449_000, 2),
        (300, 2_448_500, 7), (125, 2_448_000, 4),
        (150, 2_450_500, 2), (400, 2_451_000, 6), (90, 2_451_500, 1),
        (220, 2_452_000, 8), (60, 2_452_500, 3),
    ]
    ts = int(datetime(2026, 8, 13, 10, 15, 30).timestamp())

    return [
        {
            "name": "ltp_nse",
            "why": "8-byte LTP: the shortest layout, price divisor 100",
            "frame": frame(ltp_packet(token(2885, SEG_NSE), 145_035)),
        },
        {
            "name": "index_quote_sensex",
            "why": (
                "28-byte index quote for SENSEX (token 265, segment BSE). Indices are not "
                "tradable and carry no traded quantities"
            ),
            "frame": frame(index_quote_packet(265, 8_123_450, 8_150_000, 8_090_000,
                                              8_100_000, 8_095_000)),
        },
        {
            "name": "index_full_nifty",
            "why": "32-byte index full: adds the exchange timestamp to the 28-byte layout",
            "frame": frame(index_full_packet(256_265, 2_456_780, 2_460_000, 2_448_000,
                                             2_450_000, 2_452_000, ts)),
        },
        {
            "name": "quote_nfo",
            "why": (
                "44-byte tradable quote. Its OHLC field order (open,high,low,close) differs "
                "from the index layout (high,low,open,close) -- a transposition here is silent"
            ),
            "frame": frame(quote_packet(token(4451, SEG_NFO), 24_550, 50, 24_480,
                                        1_250_000, 3_400, 2_900,
                                        24_100, 24_800, 23_950, 24_200)),
        },
        {
            "name": "full_nfo_with_depth",
            "why": "184-byte full: timestamps, open interest, and the 5x2 depth ladder",
            "frame": frame(full_packet(token(4451, SEG_NFO), 2_449_000, 50, 2_448_000,
                                       1_250_000, 3_400, 2_900,
                                       2_410_000, 2_480_000, 2_395_000, 2_420_000,
                                       ts - 5, 12_500, 15_000, 9_800, ts, depth)),
        },
        {
            "name": "cds_divisor",
            "why": (
                "CDS scales prices by 10^7, not 100. A shared default divisor would make this "
                "case wrong by five orders of magnitude while every other case passed"
            ),
            "frame": frame(ltp_packet(token(1234, SEG_CDS), 874_500_000)),
        },
        {
            "name": "bcd_divisor",
            "why": "BCD scales by 10^4 -- and disagrees with CDS, so one currency rule is wrong",
            "frame": frame(ltp_packet(token(5678, SEG_BCD), 874_500)),
        },
        {
            "name": "multi_packet_frame",
            "why": "Three packets of three different lengths in one frame, decoded in order",
            "frame": frame(
                ltp_packet(token(2885, SEG_NSE), 145_035),
                index_quote_packet(265, 8_123_450, 8_150_000, 8_090_000, 8_100_000, 8_095_000),
                quote_packet(token(4451, SEG_NFO), 24_550, 50, 24_480, 1_250_000, 3_400,
                             2_900, 24_100, 24_800, 23_950, 24_200),
            ),
        },
        {
            "name": "heartbeat",
            "why": "A frame under 2 bytes is a heartbeat and must decode to zero ticks",
            "frame": b"\x00",
        },
    ]


def to_jsonable(value):
    """Render the reference client's output as JSON, keeping datetimes as epoch seconds."""
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, dict):
        return {k: to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_jsonable(v) for v in value]
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", action="store_true",
                        help="re-derive and diff against the committed file")
    args = parser.parse_args()

    # `_parse_binary` needs no connection; instantiate without contacting the venue.
    ticker = KiteTicker.__new__(KiteTicker)

    cases = []
    for case in build_cases():
        raw = case["frame"]
        cases.append({
            "name": case["name"],
            "why": case["why"],
            "frame_hex": raw.hex(),
            # Expected values come from the REFERENCE implementation, not from ours.
            "expected": to_jsonable(ticker._parse_binary(raw)),
        })

    doc = {
        "_comment": (
            "Generated by generate_fixtures.py. Expected values are the output of the "
            "kiteconnect reference client, NOT of the Rust decoder under test. These frames "
            "are CONSTRUCTED from the published layout, not captured from a live socket -- "
            "they prove agreement between two implementations, not fidelity to the venue."
        ),
        "reference_implementation": f"kiteconnect {KITECONNECT_VERSION}",
        "cases": cases,
    }
    rendered = json.dumps(doc, indent=2, sort_keys=False) + "\n"

    if args.verify:
        if not OUT.exists():
            print(f"MISSING: {OUT}")
            return 1
        if OUT.read_text() != rendered:
            print(f"DRIFT: {OUT} does not match freshly derived output")
            return 1
        print(f"OK: {OUT} matches ({len(cases)} cases)")
        return 0

    OUT.write_text(rendered)
    print(f"Wrote {OUT} ({len(cases)} cases, reference kiteconnect {KITECONNECT_VERSION})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

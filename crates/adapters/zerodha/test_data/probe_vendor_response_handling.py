#!/usr/bin/env python3
"""Observe how `kiteconnect` PARSES order responses, by feeding it synthetic ones.

WHY THIS EXISTS
---------------
`capture_vendor_requests.py` checks the REQUEST side of `src/http/orders.rs` against the vendor
client. This is its mirror for the RESPONSE side, and it answers questions the Rust DTOs currently
answer from the author's reading alone:

  * Which timestamp fields does the vendor convert, and under what condition? `orders.rs` keeps
    them as raw strings on the strength of `connect.py:459` only parsing when `len(...) == 19`.
  * Is the parsed timestamp TIMEZONE-AWARE? The Rust module docs assert the payload is IST with no
    offset in the text, which is the whole reason report generation is not implemented. If the
    vendor ends up with a naive datetime too, that assertion is corroborated by something other
    than the author staring at the string.
  * Does the vendor impose ANY schema on an order row -- i.e. is there a vendor oracle for which
    fields are required and which are optional?
  * What does an error envelope raise, and does the HTTP status matter?

NO CREDENTIAL, NO NETWORK
-------------------------
Same structure as the request harness: `reqsession` is replaced by a scripted responder, and
`socket.connect` is patched to raise. Nothing is sent and nothing is authenticated. The response
bodies below are SYNTHETIC -- hand-built to the documented shape, with fabricated ids. They are not
captured from any account.

⚠️ THAT IS THIS SCRIPT'S CEILING, AND IT IS A REAL ONE. Synthetic input cannot discover a field the
author did not know to include, and cannot settle whether a field the venue really sends is ever
absent. It tests the vendor's PARSING BEHAVIOUR, which is genuine vendor code, against inputs that
are still the author's invention. Only a live order-book capture closes that gap. Where a finding
depends on the synthetic input rather than on vendor logic, it is marked INPUT-DEPENDENT below.
"""

from __future__ import annotations

import json
import socket
import sys
from copy import deepcopy


class NoNetwork(RuntimeError):
    """Raised if anything attempts to open a connection."""


def _assert_no_network() -> None:
    """Block outbound connections. Must run AFTER importing the vendor SDK.

    Patches `connect`, not the `socket.socket` class: `ssl.py` declares `class SSLSocket(socket)`,
    so replacing the class breaks `import requests` from inside CPython's own stdlib.
    """

    def _blocked(*_args, **_kwargs):
        raise NoNetwork("this script must never open a network connection")

    socket.socket.connect = _blocked  # type: ignore[assignment]
    socket.socket.connect_ex = _blocked  # type: ignore[assignment]
    socket.create_connection = _blocked  # type: ignore[assignment]


class ScriptedResponse:
    """A canned HTTP response with a body chosen per probe."""

    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.headers = {"content-type": "application/json"}
        self.content = json.dumps(payload).encode()

    def json(self) -> dict:
        return self._payload


class ScriptedSession:
    """Stands in for `requests.Session`, returning whatever the current probe scripted."""

    def __init__(self) -> None:
        self.next_response: ScriptedResponse | None = None

    def request(self, *_args, **_kwargs):
        assert self.next_response is not None, "no response scripted for this call"
        return self.next_response


# A full order row, shaped as Kite Connect v3 documents it. FABRICATED -- see the ceiling note.
FULL_ORDER = {
    "order_id": "240814000123456",
    "parent_order_id": None,
    "status": "COMPLETE",
    "status_message": None,
    "variety": "regular",
    "exchange": "NFO",
    "tradingsymbol": "NIFTY24AUG24000CE",
    "instrument_token": 12345,
    "order_type": "LIMIT",
    "transaction_type": "BUY",
    "validity": "DAY",
    "product": "NRML",
    "quantity": 65,
    "disclosed_quantity": 0,
    "price": 123.45,
    "trigger_price": 0,
    "average_price": 121.5,
    "filled_quantity": 65,
    "pending_quantity": 0,
    "cancelled_quantity": 0,
    "order_timestamp": "2026-08-14 09:15:04",
    "exchange_timestamp": "2026-08-14 09:15:05",
    "tag": "O-001",
}


def probe(kite, session, label, payload, thunk):
    # DEEP COPY, and this is a finding rather than defensive habit: `_format_response`
    # (connect.py:456-461) assigns `item[field] = dateutil.parser.parse(...)` -- it MUTATES the
    # response structure IN PLACE rather than returning a transformed copy. Reusing a module-level
    # row across probes therefore fails on the second call with
    #     TypeError: Object of type datetime is not JSON serializable
    # because the first call already rewrote it. Harmless for the Rust, which shares nothing with
    # the vendor, but it means any Python caller holding a reference to the payload sees it change
    # under them.
    session.next_response = ScriptedResponse(deepcopy(payload))
    try:
        return label, thunk(), None
    except Exception as exc:  # noqa: BLE001 - the exception IS the observation here
        return label, None, exc


def main() -> int:
    try:
        from kiteconnect import KiteConnect
        from kiteconnect.__version__ import __version__ as kc_version
    except ImportError as exc:
        print(f"kiteconnect is required: {exc}", file=sys.stderr)
        return 1

    _assert_no_network()

    kite = KiteConnect(api_key="fake_api_key", access_token="fake_access_token")
    session = ScriptedSession()
    kite.reqsession = session

    print(f"oracle: kiteconnect {kc_version}\n")

    # ------------------------------------------------------------------ timestamps
    print("TIMESTAMP HANDLING (vendor logic -- connect.py:456-461)")
    _, orders, err = probe(
        kite, session, "full row", {"status": "success", "data": [FULL_ORDER]}, kite.orders
    )
    if err:
        print(f"  unexpectedly raised: {err!r}")
    else:
        row = orders[0]
        for field in ("order_timestamp", "exchange_timestamp"):
            value = row[field]
            kind = type(value).__name__
            tzinfo = getattr(value, "tzinfo", "n/a")
            print(f"  {field:20s} -> {kind:9s} tzinfo={tzinfo}  value={value}")
        # Fields the vendor did NOT convert, to show the conversion list is closed.
        print(f"  {'status':20s} -> {type(row['status']).__name__} (untouched)")

    # The len == 19 conditional, exercised on both sides of the boundary.
    print("\n  the `len(...) == 19` gate:")
    for label, stamp in [
        ("exactly 19 chars", "2026-08-14 09:15:04"),
        ("with millis (23)", "2026-08-14 09:15:04.123"),
        ("date only (10)", "2026-08-14"),
        ("empty string (0)", ""),
    ]:
        row = dict(FULL_ORDER, order_timestamp=stamp)
        _, out, err = probe(
            kite, session, label, {"status": "success", "data": [row]}, kite.orders
        )
        got = out[0]["order_timestamp"] if out else f"raised {err!r}"
        print(f"    {label:18s} {stamp!r:26s} -> {type(got).__name__:9s} {got!r}")

    # ------------------------------------------------------------------ schema
    print("\nDOES THE VENDOR IMPOSE A SCHEMA ON AN ORDER ROW?")
    sparse = {"order_id": "240814000123457", "status": "REJECTED"}
    _, out, err = probe(
        kite, session, "sparse", {"status": "success", "data": [sparse]}, kite.orders
    )
    if err:
        print(f"  a 2-field row RAISED: {err!r}")
    else:
        print(f"  a 2-field row parsed fine, returning exactly: {out}")
        print("  -> the vendor returns RAW DICTS and validates nothing.")
        print("  -> THERE IS NO VENDOR ORACLE for required-vs-optional fields. The Rust DTO's")
        print("     required/#[serde(default)] split is the author's judgement alone and only a")
        print("     live capture can confirm it. INPUT-DEPENDENT findings stop here.")

    # ------------------------------------------------------------------ empty book
    print("\nEMPTY ORDER BOOK")
    _, out, err = probe(kite, session, "empty", {"status": "success", "data": []}, kite.orders)
    print(f"  data=[] -> {out!r} (raised: {err!r})" if err else f"  data=[] -> {out!r}")

    # ------------------------------------------------------------------ errors
    print("\nERROR ENVELOPES (does the HTTP STATUS matter, or only the body?)")
    for label, payload, status in [
        (
            "status=error + TokenException, HTTP 403",
            {
                "status": "error",
                "message": "Incorrect `api_key` or `access_token`.",
                "error_type": "TokenException",
            },
            403,
        ),
        (
            "status=error + TokenException, HTTP 200",
            {
                "status": "error",
                "message": "Incorrect `api_key` or `access_token`.",
                "error_type": "TokenException",
            },
            200,
        ),
        (
            "error_type only, no status field, HTTP 200",
            {"message": "Something broke", "error_type": "GeneralException"},
            200,
        ),
        (
            "InputException, HTTP 400",
            {"status": "error", "message": "Invalid variety", "error_type": "InputException"},
            400,
        ),
    ]:
        session.next_response = ScriptedResponse(deepcopy(payload), status_code=status)
        try:
            kite.orders()
            print(f"  {label:44s} -> NO EXCEPTION (would be read as success!)")
        except Exception as exc:  # noqa: BLE001
            print(f"  {label:44s} -> {type(exc).__name__}: {exc}")

    print(
        "\n  -> a 200 carrying an error envelope RAISES for the vendor too, so checking the body"
        "\n     before the status is the vendor's own behaviour, not a Rust embellishment."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

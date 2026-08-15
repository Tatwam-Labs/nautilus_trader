#!/usr/bin/env python3
"""Record the HTTP requests `kiteconnect` WOULD send for the order routes, without sending any.

WHY THIS EXISTS
---------------
`crates/adapters/zerodha/src/http/orders.rs` was written by READING `kiteconnect/connect.py`.
Its unit tests assert that the Rust encoder produces what the author believed the vendor client
produces. If the author misread `connect.py`, the fixture agrees with the misreading BY
CONSTRUCTION and the whole suite stays green. That is the same weakness `capture_live_frames.py`
exists to remove for the tick decoder, one layer up.

This script removes the author from the chain. It RUNS the vendor client and records the exact
request object it hands to `requests`. The Rust test then asserts against a corpus that no human
transcribed.

It does NOT prove the venue accepts the request -- `kiteconnect` is the reference client, not the
exchange. It proves the Rust and the vendor agree, which is strictly more than the Rust agreeing
with its own author.

WHY NOTHING CAN BE SENT
-----------------------
⚠️ These are the ORDER routes. A `place_order` that reached the venue would be a REAL order on a
REAL account, and there is no test mode for them.

The protection is STRUCTURAL, not procedural. `KiteConnect` performs every request through
`self.reqsession`, which `__init__` sets to a `requests.Session`. This script REPLACES that
attribute with a recorder that has no socket, no connection pool and no `requests` import path
reachable from it. There is no code path from this process to api.kite.trade: not a flag that
could be left off, not a URL that could be edited back, not a dry-run branch someone could
invert. The transport is simply absent.

That distinction matters. A filter fails open -- forget one case and the request goes out. Scoping
fails closed: if the recorder is missing or broken, the call raises and nothing is sent.

As a second, independent belt: `_assert_no_network` monkeypatches `socket.socket` to raise, so even
an unexpected code path that built its own client would fail rather than connect.

WHAT IS RECORDED
----------------
For each case: the HTTP method, the full URL, the FORM BODY (`data`), the QUERY PARAMS (`params`),
and whether a JSON body was used. Those five are exactly the things the Rust implementation decides
and the four subtleties documented in `orders.rs` live entirely inside them:

  1. `variety` is a URL PATH segment, not only a body field  -> visible in `url`
  2. the body is form-encoded, not JSON                      -> visible in `data` vs `json`
  3. absent optionals are OMITTED, not sent empty            -> visible in `data`'s key SET
  4. DELETE moves params into the QUERY STRING               -> visible in `params`

WHAT IS DELIBERATELY NOT RECORDED
---------------------------------
The `Authorization` header VALUE. The credentials below are fabricated, so it would be harmless
today -- but a corpus that carries a header value invites someone to regenerate it with a real key
and publish the result. Only the header's SHAPE is recorded (`"token <api_key>:<access_token>"`),
which is the part the Rust implementation has to get right.

The recorder also stores no decoded interpretation of anything. Same reasoning as
`capture_live_frames.py`: the corpus is the vendor's OUTPUT, and expectations are derived from it
downstream in the Rust test, where the oracle is named and its version recorded.

PUBLISHABILITY
--------------
This fork is PUBLIC. This output is safe to commit: the api key and access token are fabricated
constants defined in this file, the tradingsymbols are well-known public instruments, and no
account, order, position or balance is touched -- no request is made at all.

USAGE
-----
    python3 capture_vendor_requests.py --out vendor-requests-<date>.json
    python3 capture_vendor_requests.py --print        # human-readable, no file written
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from datetime import datetime, timezone
from pathlib import Path

# Fabricated. These never authenticate anything because no request is ever sent; they exist only
# so the recorded Authorization header has the right SHAPE. Deliberately not read from the
# environment: a script that picks up a real token is one edit away from sending it somewhere.
FAKE_API_KEY = "fake_api_key"
FAKE_ACCESS_TOKEN = "fake_access_token"


class NoNetwork(RuntimeError):
    """Raised if anything in this process attempts to open a socket."""


def _assert_no_network() -> None:
    """Make outbound connections raise, so an unexpected transport cannot reach the venue.

    This is the SECOND barrier, not the first. The first is that `reqsession` is replaced. This one
    catches the case the first cannot: code that constructs its own client instead of using the
    session attribute. Neither barrier is trusted alone.

    ⚠️ **Patch `connect`, NOT the `socket.socket` class, and call this AFTER importing the vendor
    SDK.** The obvious version of this barrier -- `socket.socket = _blocked` -- does not work and
    fails in a way that looks unrelated: `ssl.py` declares `class SSLSocket(socket)`, so replacing
    the class with a function makes importing `ssl` die with

        TypeError: function() argument 'code' must be code, not str

    from inside CPython's own stdlib, several frames below `import requests`. The barrier has to
    leave the type intact and block the operation instead.
    """

    def _blocked(*_args, **_kwargs):
        raise NoNetwork(
            "This script must never open a network connection: it records the order requests "
            "kiteconnect WOULD send. A connection attempt means the recorder was bypassed."
        )

    socket.socket.connect = _blocked  # type: ignore[assignment]
    socket.socket.connect_ex = _blocked  # type: ignore[assignment]
    socket.create_connection = _blocked  # type: ignore[assignment]


class RecordedResponse:
    """The minimum `_request` needs to reach its `return data["data"]`.

    `connect.py:973-990` reads `headers["content-type"]`, calls `.json()`, checks `status` and
    `error_type`, then returns `data["data"]`. Anything less and the vendor client raises before
    the caller returns, which would work for recording but would make the script look broken.
    """

    status_code = 200
    headers = {"content-type": "application/json"}
    content = b'{"status":"success","data":{"order_id":"000000000000000"}}'

    @staticmethod
    def json() -> dict:
        return {"status": "success", "data": {"order_id": "000000000000000"}}


class RequestRecorder:
    """Stands in for `requests.Session`. Records; never sends.

    Note the signature: it accepts exactly what `connect.py:955-964` passes, positionally for
    `method` and `url` and by keyword for the rest. If the vendor client changes how it calls its
    session, this raises a TypeError rather than silently recording a different shape.
    """

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def request(
        self,
        method,
        url,
        json=None,
        data=None,
        params=None,
        headers=None,
        verify=None,
        allow_redirects=None,
        timeout=None,
        proxies=None,
    ):
        auth = (headers or {}).get("Authorization")
        self.calls.append(
            {
                "method": method,
                "url": url,
                # ⭐ THE ORDER IS CARRIED AS AN EXPLICIT LIST, and that is not redundant with the
                # key order of `data` below.
                #
                # A JSON object is formally UNORDERED, and both ends of this pipeline will happily
                # reorder one: `json.dumps(sort_keys=True)` alphabetises on the way out (the first
                # version of this script did exactly that, silently destroying what it was trying
                # to record), and `serde_json::Map` is a `BTreeMap` unless the `preserve_order`
                # feature is on, so a Rust consumer re-alphabetises on the way in.
                #
                # Field order is the thing the Rust encoder most plausibly gets wrong and no
                # compiler can catch, so it travels as a list, which nothing can reorder.
                "data_key_order": list(data.keys()) if data else None,
                "params_key_order": list(params.keys()) if params else None,
                # `data` is the FORM body. `json_body` should be null for every order route; if it
                # is ever populated the Rust implementation is sending the wrong content type.
                "data": dict(data) if data else None,
                "json_body": dict(json) if json else None,
                "params": dict(params) if params else None,
                # SHAPE only -- see the module docstring.
                "authorization_shape": (
                    "token <api_key>:<access_token>"
                    if auth == f"token {FAKE_API_KEY}:{FAKE_ACCESS_TOKEN}"
                    else f"UNEXPECTED: {auth!r}"
                ),
                "x_kite_version": (headers or {}).get("X-Kite-Version"),
            }
        )
        return RecordedResponse()


def build_cases(kite) -> list[tuple[str, str, callable]]:
    """The matrix, chosen to exercise the four documented subtleties and nothing decorative.

    Each entry is (case name, what it discriminates, a thunk that calls the vendor client).
    """
    return [
        (
            "place_market_regular_buy_day",
            "baseline: variety in path AND body; no price key at all",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NFO,
                tradingsymbol="NIFTY24AUG24000CE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=65,
                product=kite.PRODUCT_NRML,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
            ),
        ),
        (
            "place_limit_regular_sell_with_tag",
            "price is present; tag is present",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_SELL,
                quantity=1,
                product=kite.PRODUCT_CNC,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=1400.5,
                validity=kite.VALIDITY_DAY,
                tag="O-001",
            ),
        ),
        (
            "place_slm_regular_buy",
            "SL-M carries trigger_price and NO price",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_SLM,
                trigger_price=1450.0,
                validity=kite.VALIDITY_DAY,
            ),
        ),
        (
            "place_sl_regular_buy",
            "SL carries BOTH price and trigger_price",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_SL,
                price=1455.0,
                trigger_price=1450.0,
                validity=kite.VALIDITY_DAY,
            ),
        ),
        (
            "place_market_no_validity",
            "THE OMISSION CASE: validity absent -> the key must not appear at all",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_CNC,
                order_type=kite.ORDER_TYPE_MARKET,
            ),
        ),
        (
            "place_market_ioc",
            "IOC validity",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_MIS,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_IOC,
            ),
        ),
        (
            "place_market_amo",
            "THE PATH CASE: a different variety must change the URL, not only the body",
            lambda: kite.place_order(
                variety=kite.VARIETY_AMO,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_CNC,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
            ),
        ),
        (
            "place_symbol_with_ampersand",
            "ENCODING: a tradingsymbol containing & must not split the body",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="M&M",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=1,
                product=kite.PRODUCT_CNC,
                order_type=kite.ORDER_TYPE_MARKET,
                validity=kite.VALIDITY_DAY,
            ),
        ),
        (
            "place_with_disclosed_quantity",
            "ORDER PROOF: disclosed_quantity must land between validity and trigger_price",
            lambda: kite.place_order(
                variety=kite.VARIETY_REGULAR,
                exchange=kite.EXCHANGE_NSE,
                tradingsymbol="RELIANCE",
                transaction_type=kite.TRANSACTION_TYPE_BUY,
                quantity=100,
                product=kite.PRODUCT_CNC,
                order_type=kite.ORDER_TYPE_LIMIT,
                price=1400.5,
                validity=kite.VALIDITY_DAY,
                disclosed_quantity=10,
                trigger_price=1390.0,
                tag="O-002",
            ),
        ),
        (
            "modify_quantity_and_price",
            "ORDER PROOF: quantity precedes price in the vendor's own dict; one field alone "
            "cannot show that, and the Rust encoder had only inferred it from the signature",
            lambda: kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
                quantity=130,
                price=101.55,
            ),
        ),
        (
            "modify_price_only",
            "only the changed field travels; order_id is in the PATH",
            lambda: kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
                price=101.55,
            ),
        ),
        (
            "modify_quantity_only",
            "quantity alone",
            lambda: kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
                quantity=130,
            ),
        ),
        (
            "modify_price_and_trigger",
            "two fields together",
            lambda: kite.modify_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
                price=101.55,
                trigger_price=100.0,
            ),
        ),
        (
            "cancel_no_parent",
            "THE DELETE CASE: params go to the QUERY STRING, not a body",
            lambda: kite.cancel_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
            ),
        ),
        (
            "cancel_with_parent",
            "parent_order_id as a query parameter",
            lambda: kite.cancel_order(
                variety=kite.VARIETY_REGULAR,
                order_id="240814000123456",
                parent_order_id="240814000123455",
            ),
        ),
        (
            "orders_list",
            "read route: no variety segment",
            lambda: kite.orders(),
        ),
        (
            "order_history",
            "THE ADJACENT-PATH CASE: /orders/{order_id} has NO variety segment",
            lambda: kite.order_history(order_id="240814000123456"),
        ),
        (
            "trades_list",
            "read route",
            lambda: kite.trades(),
        ),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--out", type=Path, help="write the corpus here")
    parser.add_argument("--print", action="store_true", help="print a readable summary")
    args = parser.parse_args()

    # Import BEFORE arming the barrier: `requests` pulls in `ssl`, which subclasses `socket.socket`
    # at import time. See `_assert_no_network` for why that ordering is not optional.
    try:
        from kiteconnect import KiteConnect
        from kiteconnect.__version__ import __version__ as kc_version
    except ImportError as exc:
        print(f"kiteconnect is required and is not importable: {exc}", file=sys.stderr)
        return 1

    _assert_no_network()

    kite = KiteConnect(api_key=FAKE_API_KEY, access_token=FAKE_ACCESS_TOKEN)

    recorder = RequestRecorder()
    # THE BARRIER. Everything below this line runs against a recorder with no transport.
    kite.reqsession = recorder

    cases = build_cases(kite)
    records = []

    for name, discriminates, call in cases:
        before = len(recorder.calls)
        try:
            call()
        except Exception as exc:  # noqa: BLE001 - a vendor-side raise is a finding, not a crash
            records.append({"case": name, "discriminates": discriminates, "error": repr(exc)})
            continue

        # One vendor call must produce exactly one HTTP request. More than one would mean the
        # vendor client retries or chains, which the Rust implementation does not model.
        produced = recorder.calls[before:]
        if len(produced) != 1:
            records.append(
                {
                    "case": name,
                    "discriminates": discriminates,
                    "error": f"expected exactly 1 request, recorded {len(produced)}",
                }
            )
            continue

        records.append({"case": name, "discriminates": discriminates, **produced[0]})

    # The enum vocabulary, taken from the VENDOR CLASS rather than from the cases above.
    #
    # An enumeration is only as broad as its net. The case matrix never places a `co` order or a
    # `TTL` validity, so a vocabulary derived from the matrix would silently report those as
    # "not applicable" when the truth is "unchecked". Reading the class attributes covers every
    # constant the vendor names, including the ones nothing here exercises.
    vendor_constants = {}
    for group, prefix in (
        ("exchange", "EXCHANGE_"),
        ("product", "PRODUCT_"),
        ("variety", "VARIETY_"),
        ("transaction_type", "TRANSACTION_TYPE_"),
        ("order_type", "ORDER_TYPE_"),
        ("validity", "VALIDITY_"),
        # Only THREE of these exist. The Kite Connect v3 order lifecycle documents a dozen, but
        # the SDK names `COMPLETE`, `REJECTED` and `CANCELLED` and nothing else -- so the other
        # nine values modelled in `ZerodhaOrderStatus` have DOCUMENTATION provenance only and
        # cannot be checked here. A live `/orders` capture is the only thing that settles them.
        ("status", "STATUS_"),
    ):
        values = sorted(
            {
                getattr(KiteConnect, name)
                for name in dir(KiteConnect)
                if name.startswith(prefix) and not name.startswith("STATUS_GTT")
            }
        )
        vendor_constants[group] = values

    corpus = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "oracle": f"kiteconnect {kc_version}",
        "vendor_constants": vendor_constants,
        "vendor_constants_note": (
            "Read from the KiteConnect class attributes, not from the case matrix below. "
            "`status` is INCOMPLETE BY THE VENDOR'S OWN HAND: the SDK names only COMPLETE, "
            "REJECTED and CANCELLED, while the v3 order lifecycle has around a dozen states. "
            "ZerodhaOrderStatus models twelve; nine of them rest on documentation alone and only "
            "a live order-book capture can confirm them."
        ),
        "note": (
            "Requests kiteconnect WOULD have sent. Nothing was transmitted: the session was "
            "replaced by a recorder and socket connection was blocked. Credentials are fabricated."
        ),
        "comparing_against_the_rust_encoder": {
            "compare_textually": [
                "method",
                "url",
                "the SET of keys in `data` (this is what proves absent optionals are omitted)",
                "the ORDER of keys in `data`",
                "the SET of keys in `params`, after dropping null values",
                "every non-numeric value: variety, exchange, tradingsymbol, transaction_type, "
                "product, order_type, validity, tag",
            ],
            "compare_numerically_not_textually": [
                "price",
                "trigger_price",
                "quantity",
                "disclosed_quantity",
            ],
            "why": (
                "kiteconnect passes Python ints and floats to `requests`, which encodes them with "
                "str(). So the vendor puts `price=1450.0` on the wire where the Rust encoder puts "
                "`price=1450.00`, taken from `Price`'s own fixed-point rendering at the "
                "instrument's precision. Both parse to the same number at the venue. "
                "DO NOT 'fix' the Rust to match the vendor's text: str() on a float is the lossy "
                "side of this difference -- str(0.1 + 0.2) is '0.30000000000000004', and a 4dp CDS "
                "tick of 0.0025 has no exact binary form. The Rust approach is deliberate and is "
                "the same argument `http::parse` makes for counting precision from tick_size text "
                "rather than from a parsed f64. Compare the VALUES, not the spelling."
            ),
            "null_params_are_not_a_key": (
                "`cancel_order` passes params={'parent_order_id': None} unconditionally "
                "(connect.py:442) -- unlike place/modify it does NOT strip None first. `requests` "
                "drops null-valued query params, so the wire carries no query string at all. The "
                "Rust encoder builds the map only when the value is present, which is the same "
                "wire result by a different route."
            ),
        },
        "requests": records,
    }

    if args.print or not args.out:
        for record in records:
            if "error" in record:
                print(f"!! {record['case']}: {record['error']}")
                continue
            print(f"\n{record['case']}  ({record['discriminates']})")
            print(f"   {record['method']} {record['url']}")
            print(f"   data   = {record['data']}")
            print(f"   params = {record['params']}")
            print(f"   json   = {record['json_body']}")

    if args.out:
        # NOT sort_keys=True. See `data_key_order` -- sorting alphabetised the recorded field
        # order and quietly destroyed the corpus's whole point.
        args.out.write_text(json.dumps(corpus, indent=2) + "\n")
        print(f"\nWrote {len(records)} recorded request(s) to {args.out}")

    failures = [r for r in records if "error" in r]
    if failures:
        print(f"\n{len(failures)} case(s) failed to record", file=sys.stderr)
        return 1

    print(f"\nRecorded {len(records)} request(s); zero were sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Measures how far open interest travels from a Rust DataClient toward a Python strategy.

THE QUESTION
------------
`QuoteTick` has no open-interest field, so OI must ride a custom data type. Whether a custom type
published by a Rust `DataClient` actually REACHES anything is the open question — a Python
`DataActor` provably could not do it, and the conclusion drawn from that (2026-08-08) blamed the
producer. A Rust `DataClient` is a different producer, so the earlier result does not transfer.

WHAT THIS MEASURES, HOP BY HOP
------------------------------
Reporting only the far end is worthless: an absence there is consistent with four different causes.
So each hop is observed POSITIVELY and separately.

    hop 1  the engine dispatched it        -> the adapter logs `oi_published` and the topic string
    hop 2  a Python strategy received it   -> `on_data` fires and this script counts it
    hop 3  it is in the Cache              -> asked directly, and see the note below
    hop 4  it left via the external msgbus -> NOT measured here; needs Redis and a config

⚠️ HOP 3 IS EXPECTED TO BE NO, AND "NO" IS NOT A FAILURE OF THIS ADAPTER.
`DataEngine::handle_custom_data` publishes to a msgbus topic and never caches, and `Cache` exposes
~20 TYPED `add_*` methods with no generic slot. So there is no API by which a custom type could
arrive or be queried. That is a STRONGER and different statement than "it did not arrive", and the
script prints it that way rather than as a missing value.

⚠️ WHAT A GREEN RUN HERE DOES NOT SHOW
--------------------------------------
This runs in REPLAY MODE from a captured frame corpus. No socket is opened and no tick comes from
the venue, so auth, subscribe, mode and reconnect are NOT exercised. A green replay is not a green
session.

⚠️ IT IS NOT AN OFFLINE MODE. The instrument dump IS still fetched over the network — token
resolution needs it and the corpus carries no instrument definitions. An earlier version of this
note claimed "the venue is never contacted", which was FALSE: the same run fetched 114,870
instruments. Replay is an offline TICK SOURCE, nothing more.

Replay exists because MCX is shut for most of the week and a carriage question should not have to
wait for a market.

REQUIREMENTS
------------
  * `nautilus_trader` built from this branch, so `nautilus_trader.adapters.zerodha` exists.
  * No credentials needed for replay — but the client still CONSTRUCTS a credential, so
    ZERODHA_API_KEY / ZERODHA_ACCESS_TOKEN must be set to something. They are never used.
"""

from __future__ import annotations

import os
import time

from nautilus_trader.adapters.zerodha import (
    ZerodhaDataClientConfig,
    ZerodhaDataClientFactory,
)
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode, RoutingConfig
from nautilus_trader.model import DataType, InstrumentId, TraderId
from nautilus_trader.trading import Strategy

CORPUS = os.environ.get(
    "ZERODHA_CORPUS",
    "crates/adapters/zerodha/test_data/captured-2026-08-14-mcx-segment7.json",
)
RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "20"))

# Every exchange the adapter can produce, measured from a live dump rather than copied from the
# crate's doc comments — those list BCD (zero rows that day) and omit GLOBAL (twelve).
ZERODHA_VENUES = ["NSE", "NFO", "BSE", "BFO", "MCX", "CDS", "BCD", "NCO", "NSEIX", "GLOBAL"]


class OpenInterestProbe(Strategy):
    """Subscribes to the OI custom type and records what actually arrives."""

    def __init__(self) -> None:
        super().__init__()
        self.oi_received = 0
        self.samples: list[dict] = []
        self.other_data = 0
        self.subscribed_topic: str | None = None
        self.subscribe_error: str | None = None

    def on_start(self) -> None:
        # ⚠️ THE TOPIC MUST MATCH WHAT THE ADAPTER PUBLISHED, EXACTLY.
        #
        # A subscriber that derives a different topic string receives SILENCE — and silence is
        # indistinguishable from "the data never left the engine". That single ambiguity would make
        # a null result worthless, which is why the adapter logs its topic on the first item and why
        # this script prints the one it subscribed with. Compare the two before believing a zero.
        data_type = DataType(
            type_name="ZerodhaOpenInterest",
            metadata=None,
        )
        self.subscribed_topic = str(data_type.topic)

        try:
            self.subscribe_data(data_type)
        except Exception as e:  # noqa: BLE001 - a failed subscribe must be a reported result
            # Recorded rather than raised: "could not subscribe" and "subscribed and got nothing"
            # are different findings with different remedies, and a traceback here would look like
            # the second.
            self.subscribe_error = f"{type(e).__name__}: {e}"
            self.log.error(f"Could not subscribe to the OI custom type: {self.subscribe_error}")

    def on_data(self, data) -> None:
        """Receives every custom data item routed to this strategy.

        ⚠️ THE PAYLOAD ARRIVES WRAPPED. Python receives a `CustomData` whose `.data` holds the
        concrete type — NOT the concrete type itself. An earlier version of this method matched on
        `type(data).__name__` and therefore counted 98 genuine open-interest items as "other",
        reporting **0 received** while the adapter had published 149.

        That zero was indistinguishable from the wall this harness exists to detect, and it would
        have been reported as one. It was caught only because the same method counted the
        non-matching items instead of discarding them — a count of what you are NOT looking for is
        what separates "nothing arrived" from "something arrived and I did not recognise it".
        """
        payload = getattr(data, "data", data)
        name = type(payload).__name__

        if "OpenInterest" not in name:
            self.other_data += 1
            return

        self.oi_received += 1

        if len(self.samples) < 3:
            self.samples.append(
                {
                    "wrapper": type(data).__name__,
                    "payload": name,
                    "instrument_id": str(getattr(payload, "instrument_id", "?")),
                    "open_interest": getattr(payload, "open_interest", None),
                    "day_high": getattr(payload, "open_interest_day_high", None),
                    "day_low": getattr(payload, "open_interest_day_low", None),
                }
            )


def build_node() -> tuple[LiveNode, OpenInterestProbe]:
    builder = LiveNode.builder(
        name="zerodha-oi-hops",
        trader_id=TraderId("OIPROBE-001"),
        environment=Environment.SANDBOX,
    )
    builder = builder.add_data_client(
        name="ZERODHA",
        factory=ZerodhaDataClientFactory(),
        # Replay: an offline TICK SOURCE. The instrument dump is still fetched.
        config=ZerodhaDataClientConfig(replay_frames_path=CORPUS),
        routing=RoutingConfig(default=False, venues=ZERODHA_VENUES),
    )
    node = builder.build()
    probe = OpenInterestProbe()
    node.add_strategy(probe)
    return node, probe


def main() -> None:
    node, probe = build_node()
    polled = 0

    try:
        node.start()
        # `start()` connects clients and drives NOTHING. A sleep here would service the channel only
        # during shutdown — that cost an evening on 2026-08-14 and the tell was a timestamp, not a
        # count.
        deadline = time.monotonic() + RUN_SECONDS
        while time.monotonic() < deadline:
            events = node.poll()
            polled += events
            if events == 0:
                time.sleep(0.005)
    finally:
        node.stop()
        report(node, probe, polled)
        node.dispose()


def report(node: LiveNode, probe: OpenInterestProbe, polled: int) -> None:
    print("\n" + "=" * 68)
    print("  OPEN INTEREST — HOP BY HOP")
    print("=" * 68)
    print("\n  ⚠️  REPLAY MODE: no socket opened, no tick from the venue.")
    print("      Auth, subscribe, mode and reconnect are NOT exercised by this run.")
    print("      NOT offline: the instrument dump is still fetched over the network.")
    print(f"      corpus: {CORPUS}")

    print("\n  SUBSCRIPTION")
    print(f"    topic subscribed with : {probe.subscribed_topic}")
    print("    ^ compare against the adapter's logged topic. A mismatch produces silence that")
    print("      is indistinguishable from the data never being published.")

    if probe.subscribe_error:
        print(f"    ⚠️ SUBSCRIBE FAILED     : {probe.subscribe_error}")

    print("\n  HOP 2 — did a Python strategy receive it?")
    print(f"    engine events polled  : {polled}")
    print(f"    OI items received     : {probe.oi_received}")
    print(f"    other custom data     : {probe.other_data}")

    for sample in probe.samples:
        print(f"      {sample}")

    # HOP 3. Asked, but the honest answer is about the API rather than the value.
    print("\n  HOP 3 — is it in the Cache?")
    print("    NO, and not because it failed to arrive: the Cache exposes ~20 TYPED add_* methods")
    print("    and no generic slot, so there is NO API by which a custom type could be stored or")
    print("    queried. Nothing was asked because there is nothing to ask.")

    print("\n  HOP 4 — did it leave via the external msgbus?")
    print("    NOT MEASURED. Needs Redis and an external msgbus config; this run has neither.")
    print("    This is the hop that failed in the 2026-08-08 attempt and the only one with no")
    print("    answer from any source.")

    print("\n  VERDICT")

    if probe.subscribe_error:
        verdict = (
            "COULD NOT SUBSCRIBE — the custom type never had a chance to arrive. This is a "
            "different finding from 'published but not delivered' and needs a different fix."
        )
    elif probe.oi_received > 0:
        verdict = (
            f"OI REACHES A PYTHON STRATEGY — {probe.oi_received} items delivered through "
            f"Data::Custom from a Rust DataClient. Hops 1 and 2 are OPEN."
        )
    elif polled == 0:
        verdict = "NOTHING POLLED — the loop never ran; every number above is meaningless."
    else:
        verdict = (
            "NO OI RECEIVED. Before calling this a wall, check the adapter's logged topic against "
            "the one above, and check `oi_published` in the adapter heartbeat — a zero there means "
            "the corpus carried no full-mode packets, not that carriage failed."
        )

    print(f"    {verdict}")


if __name__ == "__main__":
    main()

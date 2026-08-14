#!/usr/bin/env python3
"""A minimal MCX paper-trading run: live Zerodha data in, simulated fills out.

The point of this file is a RESULT — an order that fills and a position that closes — not a
demonstration that the components exist. If it prints orders submitted and nothing filled, that is
a failure however healthy the tick counts look.

WHAT IS PROVEN AND WHAT IS NOT
------------------------------
Proven against the venue on 2026-08-14:
  * the REST instrument dump (auth by header), 114,851 rows across nine exchanges
  * the WebSocket (auth by query parameter), subscribe and mode messages accepted and honoured
  * full-mode ticks with five-deep depth, 89 of 89 on MCX CRUDEOIL
  * trades emitted on a cumulative-volume delta: 25 from 89 ticks, not one per tick

NOT proven, and this script is the first thing that would:
  * that the adapter's ticks reach a Nautilus engine through the Python bindings
  * that the sandbox execution client fills against them
  * that a Python strategy sees those fills

⚠️ THIS SCRIPT HAS NEVER BEEN RUN. The API calls below were read from the PyO3 bindings
(`crates/live/src/python/node.rs`, `crates/adapters/sandbox/src/config.rs`) rather than from a
working example — there is no v2 Python node example in this repo to copy. Expect the first run to
correct at least one of them.

REQUIREMENTS
------------
  * `nautilus_trader` built from THIS branch and installed (`make build-debug`), so that
    `nautilus_trader.adapters.zerodha` exists. A stock install will not have it.
  * ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN in the environment.
  * MCX open — it trades until roughly 23:30 IST. Outside that there are no ticks and no fills, and
    that is not a defect.

NO REAL ORDER IS EVER PLACED. Execution is the sandbox client: fills are simulated inside the
engine. The Zerodha credential is used for MARKET DATA ONLY. There is no execution client for
Zerodha in this build, so there is nothing here that could reach the venue's order API even by
mistake.
"""

from __future__ import annotations

import asyncio
import os

from nautilus_trader.adapters.sandbox import (
    SandboxExecutionClientConfig,
    SandboxExecutionClientFactory,
)
from nautilus_trader.adapters.zerodha import (
    ZerodhaDataClientConfig,
    ZerodhaDataClientFactory,
)
from nautilus_trader.common import Environment
from nautilus_trader.live import LiveNode
from nautilus_trader.model import (
    AccountId,
    Currency,
    InstrumentId,
    Money,
    OrderSide,
    Quantity,
    TraderId,
    Venue,
)
from nautilus_trader.trading import Strategy

# The instrument to trade. CRUDEOIL is the most liquid MCX future in the evening session and is
# what the two live smoke runs used, so its behaviour is known rather than assumed.
INSTRUMENT_ID = InstrumentId.from_str(os.environ.get("MCX_INSTRUMENT", "CRUDEOIL26AUGFUT.MCX"))

# Entry after this many quotes, exit this many quotes later. Deliberately tiny: the strategy exists
# to move an order through the engine, and anything cleverer makes a failure ambiguous.
QUOTES_BEFORE_ENTRY = 5
QUOTES_BEFORE_EXIT = 15

RUN_SECONDS = int(os.environ.get("RUN_SECONDS", "120"))


class MinimalRoundTrip(Strategy):
    """Buys one lot after a few quotes, then closes it. That is the whole strategy.

    It is not a trading idea and must never be treated as one — it has no signal, no risk control
    and no exit logic beyond a counter.
    """

    def __init__(self) -> None:
        super().__init__()
        self.quotes = 0
        self.orders_submitted = 0
        self.fills = 0
        self.entered = False
        self.exited = False
        # Set when startup found no instrument. Checked by the report so a run that could never
        # have filled says why, instead of looking like a market that was simply quiet.
        self.aborted_reason: str | None = None

    def on_start(self) -> None:
        instrument = self.cache.instrument(INSTRUMENT_ID)

        if instrument is None:
            # A missing instrument is the single most likely reason for a silent no-fill run: the
            # sandbox cannot fill what the cache cannot price. Record it and subscribe to nothing.
            #
            # ⚠️ DO NOT CALL `self.stop()` HERE. Stopping from inside `on_start` re-enters the
            # actor while the engine still holds a mutable borrow of it, and PyO3 raises
            # `RuntimeError: Already borrowed` — which then masks the real error. Observed on the
            # 2026-08-14 paper run: the useful message ("not in the cache") was buried under two
            # layers of borrow-failure traceback.
            #
            # Returning without subscribing achieves the same thing: no quotes, no orders, and the
            # report explains why.
            self.aborted_reason = (
                f"{INSTRUMENT_ID} was not in the cache at startup, so nothing could be priced "
                f"or filled"
            )
            self.log.error(f"{self.aborted_reason}. Not subscribing.")
            return

        self.lot = instrument.lot_size or Quantity.from_int(1)
        self.log.info(f"Subscribing {INSTRUMENT_ID}, lot size {self.lot}")
        self.subscribe_quote_ticks(INSTRUMENT_ID)

    def on_quote_tick(self, tick) -> None:
        self.quotes += 1

        if self.quotes == QUOTES_BEFORE_ENTRY and not self.entered:
            self.entered = True
            self._submit(OrderSide.BUY, "ENTRY")

        elif self.quotes == QUOTES_BEFORE_EXIT and self.entered and not self.exited:
            self.exited = True
            self._submit(OrderSide.SELL, "EXIT")

    def _submit(self, side: OrderSide, label: str) -> None:
        order = self.order_factory.market(
            instrument_id=INSTRUMENT_ID,
            order_side=side,
            quantity=self.lot,
        )
        self.orders_submitted += 1
        self.log.info(f"{label}: submitting {side} {self.lot} {INSTRUMENT_ID}")
        self.submit_order(order)

    def on_order_filled(self, event) -> None:
        self.fills += 1
        self.log.info(f"FILLED: {event.order_side} {event.last_qty} @ {event.last_px}")


def build_node() -> LiveNode:
    """Assembles the node. Nothing here is Zerodha-specific except the data client."""
    trader_id = TraderId("PAPER-001")
    venue = Venue("MCX")

    builder = LiveNode.builder(
        name="zerodha-mcx-paper",
        trader_id=trader_id,
        # `Environment` lives at `nautilus_trader.common` — not `.live`, not `.common.enums`.
        environment=Environment.SANDBOX,
    )

    # Credentials come from the environment; the config falls back to them when unset here.
    builder = builder.add_data_client(
        name="ZERODHA",
        factory=ZerodhaDataClientFactory(),
        config=ZerodhaDataClientConfig(),
    )

    builder = builder.add_simulated_exec_client(
        name="SANDBOX",
        factory=SandboxExecutionClientFactory(),
        config=SandboxExecutionClientConfig(
            trader_id=trader_id,
            account_id=AccountId("SANDBOX-001"),
            venue=venue,
            starting_balances=[Money(1_000_000, Currency.from_str("INR"))],
            base_currency=Currency.from_str("INR"),
            # ⚠️ bar_execution=False is the load-bearing setting. The Zerodha adapter publishes
            # QuoteTicks (from full-mode depth) and TradeTicks (on volume deltas) — it does NOT
            # publish bars. With bar_execution=True the sandbox waits for bars that never arrive,
            # NOTHING EVER FILLS, and the run looks quiet rather than broken.
            bar_execution=False,
        ),
    )

    node = builder.build()
    node.add_strategy(MinimalRoundTrip())
    return node


async def main() -> None:
    if not os.environ.get("ZERODHA_API_KEY") or not os.environ.get("ZERODHA_ACCESS_TOKEN"):
        raise SystemExit("set ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN")

    node = build_node()
    strategy = None

    try:
        await node.start()
        await asyncio.sleep(RUN_SECONDS)
    finally:
        for candidate in node.trader.strategies() if hasattr(node, "trader") else []:
            if isinstance(candidate, MinimalRoundTrip):
                strategy = candidate

        await node.stop()
        node.dispose()

    report(node, strategy)


def report(node: LiveNode, strategy: MinimalRoundTrip | None) -> None:
    """Prints the paper-trading result: what was traded, what filled, and what it made or lost.

    Counters alone are not a report. A run that submits two orders and fills neither has the same
    order count as a run that filled both, and only the money tells them apart.
    """
    venue = Venue("MCX")

    print("\n" + "=" * 62)
    print("  PAPER TRADING REPORT — Zerodha MCX via Nautilus sandbox execution")
    print("=" * 62)

    if strategy is None:
        print("  could not reach the strategy instance; check the node API")
        return

    print("\n  FEED")
    print(f"    quotes received     : {strategy.quotes}")
    print(f"    orders submitted    : {strategy.orders_submitted}")
    print(f"    fills received      : {strategy.fills}")

    # --- orders and fills, from the cache rather than the strategy's own counters ---
    # Deliberately a SECOND source. The strategy counts what it thinks it did; the cache records
    # what the engine actually processed. If those disagree, the disagreement is the finding.
    try:
        orders = node.cache.orders()
        print("\n  ORDERS (from the engine cache, not the strategy's counters)")

        for order in orders:
            print(
                f"    {order.side} {order.quantity} {order.instrument_id} "
                f"status={order.status} filled={order.filled_qty} avg_px={order.avg_px}"
            )

        if not orders:
            print("    (none)")
    except Exception as e:  # noqa: BLE001 - a reporting failure must not mask the run's result
        print(f"    could not read orders from the cache: {e}")

    # --- the money ---
    try:
        account = node.portfolio.account(venue)
        print("\n  ACCOUNT")
        print(f"    balances            : {account.balances_total() if account else 'n/a'}")

        print("\n  P&L")
        print(f"    realised            : {node.portfolio.realized_pnls(venue=venue)}")
        print(f"    unrealised          : {node.portfolio.unrealized_pnls(venue=venue)}")
        print(f"    total               : {node.portfolio.total_pnls(venue=venue)}")
        print(f"    equity              : {node.portfolio.equity(venue=venue)}")
        print(f"    net position {INSTRUMENT_ID}: {node.portfolio.net_position(INSTRUMENT_ID)}")
    except Exception as e:  # noqa: BLE001
        print(f"    could not read the portfolio: {e}")

    # The verdict separates outcomes a count cannot. "Submitted but never filled" is the likeliest
    # failure and must not read as success.
    print("\n  VERDICT")

    if strategy.aborted_reason is not None:
        # Distinguished from "no quotes" deliberately: this run could never have traded, and
        # reporting it as a quiet market would send the next person to look at the feed.
        print(f"  ABORTED AT STARTUP — {strategy.aborted_reason}")
        return

    if strategy.quotes == 0:
        verdict = "NO QUOTES — the data client never delivered. Market closed, or the adapter is not wired."
    elif strategy.orders_submitted == 0:
        verdict = "QUOTES BUT NO ORDERS — the strategy never triggered; check the counters."
    elif strategy.fills == 0:
        verdict = (
            "ORDERS BUT NO FILLS — the sandbox saw no data it could fill against. "
            "Check bar_execution and that the instrument is in the cache."
        )
    else:
        verdict = "ROUND TRIP COMPLETED — live MCX data drove a simulated fill."

    print(f"  verdict          : {verdict}")


if __name__ == "__main__":
    asyncio.run(main())

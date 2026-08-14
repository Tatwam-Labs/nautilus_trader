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

Also proven on 2026-08-14, by this script, live on MCX (runs 10-13):
  * the adapter's ticks reach a Nautilus engine through the Python bindings
  * the sandbox execution client fills against them
  * a Python strategy sees those fills
  * the engine can aggregate INTERNAL bars from these ticks (115 in 120s) -- but see the caveat
    under `on_start`: at 1-SECOND on a ~1 tick/sec feed every bar is flat and 87% carry a
    stale carried-forward close with zero volume

⚠️ WHAT IS *NOT* TESTED, WHICH IS MOST OF IT. Four runs exercised ONE PATH: quotes arrive, entry at
quote 5, exit at quote 15, both fill, flat. Nothing below has ever run:
  * ORDER REJECTION. There is no `on_order_rejected`/`on_order_denied` handler, so a rejected order
    is SILENT -- the report says "ORDERS BUT NO FILLS" and never says why. Note the recorded v1
    defect where a sandbox client silently rejected paper orders; this script could not tell you.
  * PARTIAL FILLS. `self.fills += 1` counts EVENTS, not quantity, so a partial fill is
    indistinguishable from a full one.
  * FEWER THAN 15 QUOTES. Entry fires at 5 and exit at 15, so a short or quiet run ENTERS AND NEVER
    EXITS, ending with an open position. No verdict branch calls that out.
  * three of the four verdict branches, a mid-run disconnect/reconnect, an illiquid or wide-spread
    instrument, any venue other than MCX, and any instrument other than CRUDEOIL.
There are also NO automated tests over this file, and the report's ACCOUNT section is broken --
`portfolio.account(venue)` returns None on every run so far.

This is a PLUMBING test, not a strategy test. There is no signal here to test.

REQUIREMENTS
------------
  * `nautilus_trader` built from THIS branch and installed (`make build-debug`), so that
    `nautilus_trader.adapters.zerodha` exists. A stock install will not have it.
  * ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN in the environment.
  * MCX open — it trades until roughly 23:30 IST. Outside that there are no ticks and no fills, and
    that is not a defect.

NO REAL ORDER IS EVER PLACED. Execution is the sandbox client: fills are simulated inside the
engine, and the Zerodha credential is used for MARKET DATA ONLY.

⚠️ THE REASON THAT IS SAFE HAS AN EXPIRY. It is safe because this node wires exactly ONE execution
client -- the sandbox -- and the committed tree carries no Zerodha execution client at all. It is
NOT safe because of anything in this file. A Zerodha exec client exists on the branch as unreviewed
work; the moment one is registered here, this script submits MARKET ORDERS AGAINST A REAL ACCOUNT on
a live credential. Do not add an exec client to `build_node` to "see if it works".
"""

from __future__ import annotations

import json
import os
import time

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
    BarType,
    ClientId,
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
        # The venue's own quote stream, and the order/fill events to check against it. Kept so a
        # simulated fill can be validated against what actually traded — see `on_quote`.
        self.tape: list[dict] = []
        self.submissions: list[dict] = []
        self.fill_events: list[dict] = []
        self.prints: list[dict] = []
        self.bars: list[dict] = []
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

        # Log the venue's value and the fallback SEPARATELY. A bare "lot size 1" cannot tell you
        # whether the venue said 1 or whether the field was empty and the fallback fired — and on
        # 2026-08-14 I read that line, assumed the fallback, and reported a non-existent adapter
        # defect that reached an ADR before I checked the CSV. Zerodha publishes lot_size=1 for
        # CRUDEOIL26AUGFUT; the fallback had never fired.
        self.lot = instrument.lot_size or Quantity.from_int(1)
        self.log.info(
            f"Subscribing {INSTRUMENT_ID}, venue lot_size={instrument.lot_size!r}, "
            f"using {self.lot} (fallback fired: {not instrument.lot_size})"
        )
        # ⚠️ client_id is REQUIRED here, and the reason is a real adapter limitation.
        #
        # The engine resolves a client by client_id first, then by a venue routing map, then
        # by the default client. Our data client declares `venue() -> NSE` while the adapter
        # actually serves NINE exchanges (NSE, NFO, MCX, BSE, BFO, CDS, BCD, NCO, NSEIX), so
        # an MCX subscription finds no client and the engine logs
        # `no client found for client_id=None, venue=Some("MCX")`. Observed on the
        # 2026-08-14 paper run.
        #
        # Naming the client bypasses venue routing entirely. The proper fix is on the Rust
        # side -- a multi-venue client should return None from `venue()` -- and is filed.
        self.subscribe_quotes(INSTRUMENT_ID, client_id=ClientId("ZERODHA"))
        # Trades as well as quotes, and they are NOT redundant. A quote is the BOOK (what you could
        # trade at); a trade print is what ACTUALLY TRADED. Validating a simulated fill against the
        # book shows the sandbox picked the right SIDE of the spread; validating it against prints
        # shows the price was one the venue really dealt at. The second is the stronger claim and
        # the book alone cannot make it.
        self.subscribe_trades(INSTRUMENT_ID, client_id=ClientId("ZERODHA"))
        # ⭐ THE ARCHITECTURE QUESTION, ASKED AS A MEASUREMENT.
        #
        # This adapter emits no bars, so the sandbox had to run bar_execution=False. That leaves
        # open whether a v2 paper rail fed by this adapter can be BAR-driven at all -- which decides
        # whether a bar-based differential test covers the deployed path or a configuration nobody
        # runs.
        #
        # AggregationSource.INTERNAL asks the ENGINE to build the bars from the ticks we publish,
        # rather than expecting the venue to send them. If bars arrive, in-engine aggregation works
        # over this adapter's stream and the rail CAN be bar-driven. If none arrive, it cannot --
        # and a count of zero is as useful an answer as a count of ten.
        #
        # LAST means aggregated from TRADE ticks (subscribed above), not from the book.
        self.bar_type = BarType.from_str(f"{INSTRUMENT_ID}-1-SECOND-LAST-INTERNAL")
        self.subscribe_bars(self.bar_type, client_id=ClientId("ZERODHA"))

    def on_quote(self, tick) -> None:
        self.quotes += 1

        # Every quote is retained, not just a sample. This is the INDEPENDENT PRICE REFERENCE for
        # validating a simulated fill: it comes from the venue, and neither execution path produced
        # it. A fill checked against anything the fill path itself computed is a check that cannot
        # fail. Sampling would break it too — the reference has to cover the fill's timestamp, and
        # you do not know which quote that is until afterwards.
        self.tape.append(
            {
                "ts_event": tick.ts_event,
                "ts_init": tick.ts_init,
                "bid": str(tick.bid_price),
                "ask": str(tick.ask_price),
                "bid_size": str(tick.bid_size),
                "ask_size": str(tick.ask_size),
            }
        )

        if self.quotes == QUOTES_BEFORE_ENTRY and not self.entered:
            self.entered = True
            self._submit(OrderSide.BUY, "ENTRY")

        elif self.quotes == QUOTES_BEFORE_EXIT and self.entered and not self.exited:
            self.exited = True
            self._submit(OrderSide.SELL, "EXIT")

    def on_trade(self, tick) -> None:
        # What actually traded on the venue. Neither execution path produces these, which is what
        # makes them a usable reference for a simulated fill.
        self.prints.append(
            {
                "ts_event": tick.ts_event,
                "price": str(tick.price),
                "size": str(tick.size),
                "aggressor_side": str(tick.aggressor_side),
                "trade_id": str(tick.trade_id),
            }
        )

    def on_bar(self, bar) -> None:
        self.bars.append(
            {
                "ts_event": bar.ts_event,
                "open": str(bar.open),
                "high": str(bar.high),
                "low": str(bar.low),
                "close": str(bar.close),
                "volume": str(bar.volume),
            }
        )

    def _submit(self, side: OrderSide, label: str) -> None:
        order = self.order_factory.market(
            instrument_id=INSTRUMENT_ID,
            order_side=side,
            quantity=self.lot,
        )
        self.orders_submitted += 1
        self.log.info(f"{label}: submitting {side} {self.lot} {INSTRUMENT_ID}")
        self.submissions.append(
            {
                "label": label,
                "side": str(side),
                "qty": str(self.lot),
                "ts_submitted": self.clock.timestamp_ns(),
                "quotes_seen": self.quotes,
            }
        )
        self.submit_order(order)

    def on_order_filled(self, event) -> None:
        self.fills += 1
        self.log.info(f"FILLED: {event.order_side} {event.last_qty} @ {event.last_px}")
        self.fill_events.append(
            {
                "side": str(event.order_side),
                "qty": str(event.last_qty),
                "price": str(event.last_px),
                "ts_event": event.ts_event,
                "ts_init": event.ts_init,
                "quotes_seen": self.quotes,
            }
        )


def build_node() -> tuple[LiveNode, MinimalRoundTrip]:
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
    strategy = MinimalRoundTrip()
    node.add_strategy(strategy)
    return node, strategy


def main() -> None:
    if not os.environ.get("ZERODHA_API_KEY") or not os.environ.get("ZERODHA_ACCESS_TOKEN"):
        raise SystemExit("set ZERODHA_API_KEY and ZERODHA_ACCESS_TOKEN")

    node, strategy = build_node()
    polled = 0

    try:
        node.start()
        polled = drive(node, RUN_SECONDS)
    finally:
        node.stop()
        # ⚠️ REPORT BEFORE DISPOSE. `dispose()` finalises the kernel and the cache and portfolio go
        # with it, so a report printed afterwards reads an emptied engine: no orders, no account,
        # empty P&L dicts — while the strategy's own counters still show the fills that really
        # happened. Observed on the 2026-08-14 run, which filled a full round trip and then
        # reported "(none)" against it. The two sources disagreeing is what caught it.
        report(node, strategy, polled)
        preserve(strategy)
        node.dispose()


def preserve(strategy: MinimalRoundTrip | None) -> None:
    """Writes the quote tape and the order/fill events to disk before the process exits.

    Without this the evidence dies with the process. The 2026-08-14 runs filled real orders against
    89 and 90 live quotes and retained NEITHER — the strategy logged only every twenty-fifth tick,
    so by the time the fills were worth validating there was nothing left to validate them against,
    and MCX had minutes left to run. A live venue session is not reproducible on demand.
    """
    if strategy is None or not (strategy.tape or strategy.prints):
        return

    path = os.environ.get("CAPTURE_PATH", "mcx_paper_capture.json")

    with open(path, "w") as fh:
        json.dump(
            {
                "instrument_id": str(INSTRUMENT_ID),
                "captured_by": "python/examples/zerodha_mcx_paper.py",
                # Named so a reader knows which side produced which number: the tape is the venue's,
                # the fills are the sandbox's. Conflating them is the whole risk.
                "quote_tape_source": "Zerodha WebSocket full mode, via the Rust data client",
                "fill_source": "nautilus sandbox execution client, simulated",
                "quotes": strategy.tape,
                "venue_trades": strategy.prints,
                "internal_bars": strategy.bars,
                "submissions": strategy.submissions,
                "fills": strategy.fill_events,
            },
            fh,
            indent=2,
        )

    print(f"\n  capture written: {os.path.abspath(path)} ({len(strategy.tape)} quotes)")


def drive(node: LiveNode, seconds: int) -> int:
    """Runs the node's event loop for `seconds`, then returns the events processed.

    ⚠️ THE CALLER MUST PROVIDE THE LOOP. `start()` connects the clients and starts the trader and
    then drives NOTHING — its own docstring says so: "does not consume the runner or drive channel
    receivers. Channel traffic that arrives after startup is not serviced until the caller provides
    a loop." A `time.sleep()` in place of this function is not a loop, and the failure is silent
    rather than loud:

      * the data client connects and logs success, because `start()` does do that much
      * the subscription command is ENQUEUED and the send returns Ok, because the receiver exists
      * ...and then it sits unprocessed, because nothing polls the receiver
      * ticks arrive at the socket and go nowhere
      * `stop()` finally drives the runtime, so the subscription is serviced DURING SHUTDOWN and
        one tick slips through
      * the report says "NO QUOTES — market closed, or the adapter is not wired", pointing at the
        two things that were actually fine

    Observed 2026-08-14: with `time.sleep(60)`, the subscribe logged at t+60s — to the second, the
    moment `stop()` ran — after the command was issued at t+0. The tell is that gap. A working run
    subscribes within a second of connecting.

    `poll()` rather than `run()` because `run()` owns the loop and exits on a signal, which a timed
    test cannot use: stopping it from a timer thread would touch the node off-thread, and the v2
    actors are `unsendable` — that aborts the process rather than raising.
    """
    processed = 0
    deadline = time.monotonic() + seconds

    while time.monotonic() < deadline:
        events = node.poll()
        processed += events

        # Only yield when there was nothing to do; a busy feed should never wait on this sleep.
        if events == 0:
            time.sleep(0.005)

    return processed


def report(node: LiveNode, strategy: MinimalRoundTrip | None, polled: int = 0) -> None:
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
    # Engine events processed by our own loop. Zero here means the loop never ran and every other
    # number below is meaningless — that distinction cost an evening on 2026-08-14.
    print(f"    engine events polled: {polled}")
    print(f"    quotes received     : {strategy.quotes}")
    print(f"    venue trade prints  : {len(strategy.prints)}")
    # Zero here is a real answer, not a missing measurement: it means in-engine bar aggregation
    # does NOT work over this adapter's stream, and the paper rail cannot be bar-driven.
    print(f"    INTERNAL bars built : {len(strategy.bars)}")
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
    main()

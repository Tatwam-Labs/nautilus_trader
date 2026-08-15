// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! The Zerodha execution client.
//!
//! ⚠️ **No order has ever been placed through this file.** It has not been compiled on the machine
//! it was written on, let alone run: the routes and parameter names come from `kiteconnect` 5.2.0
//! and the mappings from reading, not from a venue response. The credential this crate resolves
//! trades a **real** account with real money and there is no Zerodha sandbox for the order routes.
//!
//! # What is real and what is a stub
//!
//! | area | state |
//! |---|---|
//! | Nautilus ↔ Zerodha enum mapping ([`crate::common::enums`]) | real, unit-tested both directions |
//! | order request encoding ([`crate::http::orders`]) | real, unit-tested |
//! | `submit_order` / `modify_order` / `cancel_order` | real code, **never executed** |
//! | `cancel_all_orders`, `batch_cancel_orders` | real, fanned out over this client's own registry |
//! | `generate_account_state` | real pass-through |
//! | `generate_order_status_report`, `generate_order_status_reports` | **stub — returns an error** |
//! | `generate_fill_reports`, `generate_position_status_reports` | **stub — returns an error** |
//! | `query_account`, `query_order` | **stub — returns an error** |
//! | reconciliation as a whole | **not implemented** |
//!
//! # ⭐ The stubs return an ERROR, not an empty list, and that is the whole point
//!
//! The trait's default bodies return `Ok(Vec::new())`. For a report generator that reads "you have
//! no working orders and no positions" — which, during reconciliation, is not a missing feature but
//! a **wrong answer**: the engine would conclude the account is flat. This client overrides each of
//! them with an error naming what is missing, because a loud failure to reconcile is recoverable and
//! a confident report of a flat account is not.
//!
//! The consequence is deliberate and worth stating plainly: **a live node configured with this
//! execution client will fail its reconciliation pass.** That is the honest state of the adapter.
//!
//! # What is actually missing before reports can be built
//!
//! Two things, neither of which is a line of glue:
//!
//! 1. **Timestamps.** [`KiteOrder::order_timestamp`] is `YYYY-MM-DD HH:MM:SS` in **IST with no
//!    offset in the text**. Converting it needs a civil-datetime-to-epoch conversion, and this
//!    crate declares no date/time dependency. IST is a fixed +05:30 with no daylight saving, so the
//!    conversion is exact and cheap — but it is code that does not exist, and defaulting `ts_event`
//!    to "now" would make every reconciled order look like it was accepted this instant.
//! 2. **Precision.** [`OrderStatusReport`] holds `Price` and `Quantity`, which are fixed-point and
//!    need the instrument's precision. The venue sends bare JSON numbers. The precision is
//!    available from the cache's [`InstrumentAny`], so this is reachable — but picking a constant
//!    would round every CDS price, which is exactly the trap `http::parse` documents.
//!
//! # ⭐ Order state arrives by POSTBACK, and this client does not listen for one
//!
//! Zerodha delivers order updates as an HTTP **postback** to a webhook URL registered on the Kite
//! developer console. They do **not** arrive on the tick socket — that carries market data only, and
//! [`crate::websocket`] neither subscribes to nor decodes anything else.
//!
//! There are therefore two possible routes to order state:
//!
//! | route | what it gives | implemented here |
//! |---|---|---|
//! | postback webhook | push, per transition, with the venue's own sequencing | **no** — needs an HTTP server |
//! | polling `/orders` and `/trades` | a sample of current state | the HTTP calls exist; the reports do not |
//!
//! **The REST route is the one built.** Polling is strictly weaker even when finished: it samples,
//! so a fill that opens and closes between two polls is visible only in the aggregate, and the
//! order's `average_price` is an average rather than the sequence of individual fills.
//!
//! # ⭐ Where the order-id mapping lives, and why it does not survive a restart
//!
//! Nautilus addresses orders by [`ClientOrderId`]; Zerodha assigns a [`VenueOrderId`] string and
//! knows nothing else. Both directions live in [`ZerodhaOrderRegistry`], in **process memory**.
//!
//! The obvious way to make the link durable is the venue's `tag` field — except Kite Connect caps a
//! tag at [`KITE_MAX_TAG_LEN`] characters and a Nautilus client order id such as
//! `O-20260814-123456-001-001-1` is longer than that. So the tag is set **when it fits and omitted
//! when it does not**, never truncated: a truncated tag is a prefix, and a prefix matches other
//! orders.
//!
//! The consequence is concrete. After a restart this client cannot cancel an order it placed before
//! the restart, because cancelling needs the `variety` the order was **placed** under — a URL path
//! segment, so a wrong one is a 404 and the order stays live. Rather than guess `regular`, the
//! cancel is rejected with a reason naming the gap. Closing it is reconciliation's job, and
//! reconciliation is not implemented.
//!
//! # ⭐ A node using this client MUST configure venue routing, or only NSE orders route
//!
//! `ExecutionEngine::register_client` files a client under `client.venue()`, which for a broker
//! spanning seven exchanges can only be one of them. Overriding
//! [`ExecutionClient::handles_order_venue`] stops an `NFO` order being *denied*; it does not make
//! one *arrive*. Register this client as the node's default execution client, or call
//! `register_venue_routing` for each exchange traded. See that method's docs for the full
//! mechanism — this is a deployment requirement no test in this crate can observe.
//!
//! # ⭐ `product` is not derivable from anything Nautilus carries
//!
//! `CNC` / `MIS` / `NRML` decides margin and whether the broker force-closes the position intraday.
//! There is no Nautilus field for it and no defensible default, so it comes from
//! [`ZerodhaExecClientConfig::default_product`] — which itself has no default and fails the client's
//! construction when unset — with a per-order override through `SubmitOrder.params["product"]`.
//!
//! # This client is built on `nautilus-common` alone
//!
//! Sibling adapters compose [`ExecutionClientCore`] and `ExecutionEventEmitter` from
//! `nautilus-execution` and `nautilus-live`. Neither crate is a dependency of `nautilus-zerodha`,
//! and adding one is a manifest change outside the scope this file was written under. Everything
//! those two provide is reachable from `nautilus-common`: identity is held directly on the struct,
//! and events are generated with [`OrderEventFactory`] and dispatched on the execution event sender.
//! If this crate later takes those dependencies, the identity fields collapse into
//! `ExecutionClientCore` with no behaviour change.
//!
//! [`ExecutionClientCore`]: https://docs.rs/nautilus-execution
//! [`InstrumentAny`]: nautilus_model::instruments::InstrumentAny
//! [`KiteOrder::order_timestamp`]: crate::http::orders::KiteOrder::order_timestamp
//! [`OrderStatusReport`]: nautilus_model::reports::OrderStatusReport

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use async_trait::async_trait;
use nautilus_common::{
    cache::CacheView,
    clients::ExecutionClient,
    factories::OrderEventFactory,
    live::{get_runtime, runner::try_get_exec_event_sender, task::TaskHandles},
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReport, GenerateOrderStatusReports, GeneratePositionStatusReports,
            ModifyOrder, QueryAccount, QueryOrder, SubmitOrder,
        },
    },
};
use nautilus_core::{
    MUTEX_POISONED, Params, UUID4, UnixNanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    accounts::AccountAny,
    enums::{AccountType, OmsType, OrderSide, OrderType, TimeInForce},
    events::{OrderCancelRejected, OrderEventAny, OrderModifyRejected},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TraderId, Venue, VenueOrderId,
    },
    instruments::InstrumentAny,
    orders::{Order, OrderAny},
    reports::{FillReport, OrderStatusReport, PositionStatusReport},
    types::{AccountBalance, MarginBalance, Price, Quantity},
};
use tokio::sync::mpsc::UnboundedSender;

use crate::{
    common::{
        consts::NSE_VENUE,
        credential::credential_env_vars,
        enums::{ZerodhaExchange, ZerodhaOrderType, ZerodhaProduct, ZerodhaTransactionType,
            ZerodhaValidity, ZerodhaVariety},
    },
    config::ZerodhaExecClientConfig,
    http::{
        client::ZerodhaHttpClient,
        orders::{KITE_MAX_TAG_LEN, ModifyOrderRequest, PlaceOrderRequest},
    },
};

/// The `SubmitOrder.params` key that overrides the configured product for one order.
pub const PARAM_PRODUCT: &str = "product";

/// The `SubmitOrder.params` key that overrides the configured variety for one order.
pub const PARAM_VARIETY: &str = "variety";

/// What this client has to remember about an order it placed.
///
/// The `variety` is the load-bearing field. Modify and cancel address
/// `/orders/{variety}/{order_id}`, so the variety used to cancel must be the one used to place —
/// and a mismatch is a 404 on the path rather than a message naming the field, which means the
/// order stays live while the cancel appears to have failed for transport reasons.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZerodhaOrderContext {
    /// The Nautilus client order id.
    pub client_order_id: ClientOrderId,
    /// The venue's order id, absent until the place-order response lands.
    pub venue_order_id: Option<VenueOrderId>,
    /// The instrument.
    pub instrument_id: InstrumentId,
    /// The submitting strategy.
    pub strategy_id: StrategyId,
    /// The side, kept so `cancel_all_orders` can honour a side filter without the cache.
    pub order_side: OrderSide,
    /// The variety the order was **placed** under.
    pub variety: ZerodhaVariety,
}

/// The bidirectional map between Nautilus order ids and Zerodha order ids.
///
/// # Both stale directions are removed on re-registration
///
/// This follows [`crate::common::instruments::InstrumentRegistry`] deliberately, and for the same
/// reason: inserting into both maps without clearing the superseded key leaves a reverse entry that
/// still *resolves*, so nothing reports a problem and a cancel addresses the wrong order.
///
/// It lives in process memory and does not survive a restart. See the module docs.
#[derive(Debug, Default)]
pub struct ZerodhaOrderRegistry {
    by_client: HashMap<ClientOrderId, ZerodhaOrderContext>,
    by_venue: HashMap<VenueOrderId, ClientOrderId>,
}

impl ZerodhaOrderRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a context, replacing any previous entry for the same client order id.
    pub fn register(&mut self, context: ZerodhaOrderContext) {
        // Drop the venue id the CLIENT id used to point at, or a re-registration after a venue id
        // change leaves the old venue id resolving to this order.
        if let Some(previous) = self.by_client.get(&context.client_order_id)
            && let Some(previous_venue_id) = previous.venue_order_id
            && Some(previous_venue_id) != context.venue_order_id
        {
            self.by_venue.remove(&previous_venue_id);
        }

        if let Some(venue_order_id) = context.venue_order_id {
            self.by_venue.insert(venue_order_id, context.client_order_id);
        }

        self.by_client.insert(context.client_order_id, context);
    }

    /// Attaches the venue's order id to an already-registered order.
    ///
    /// Returns `false` when the client order id is unknown, which happens if the place-order
    /// response outlives a `stop()`.
    pub fn link_venue_order_id(
        &mut self,
        client_order_id: ClientOrderId,
        venue_order_id: VenueOrderId,
    ) -> bool {
        let Some(context) = self.by_client.get_mut(&client_order_id) else {
            return false;
        };

        if let Some(previous) = context.venue_order_id
            && previous != venue_order_id
        {
            self.by_venue.remove(&previous);
        }

        context.venue_order_id = Some(venue_order_id);
        self.by_venue.insert(venue_order_id, client_order_id);
        true
    }

    /// Resolves a Nautilus order id to its context.
    #[must_use]
    pub fn by_client_order_id(&self, client_order_id: &ClientOrderId) -> Option<ZerodhaOrderContext> {
        self.by_client.get(client_order_id).copied()
    }

    /// Resolves a venue order id back to its Nautilus order id.
    #[must_use]
    pub fn client_order_id_of(&self, venue_order_id: &VenueOrderId) -> Option<ClientOrderId> {
        self.by_venue.get(venue_order_id).copied()
    }

    /// Removes an order from both directions.
    pub fn remove(&mut self, client_order_id: &ClientOrderId) {
        if let Some(context) = self.by_client.remove(client_order_id)
            && let Some(venue_order_id) = context.venue_order_id
        {
            self.by_venue.remove(&venue_order_id);
        }
    }

    /// Returns the contexts matching an instrument, optionally filtered by side.
    ///
    /// [`OrderSide::NoOrderSide`] means "every side" — that is how [`CancelAllOrders`] spells an
    /// unfiltered request, and treating it as a literal side would match nothing.
    ///
    /// ⚠️ **THIS RETURNS TRACKED ORDERS, NOT OPEN ONES, AND THE NAME USED TO LIE ABOUT THAT.**
    ///
    /// Nothing removes an order from this registry when it FILLS. `remove` is called on exactly two
    /// paths, both failures — a refused placement and a refused cancel. So a filled order stays here
    /// for the life of the process, and this method would hand every one of them to
    /// `cancel_all_orders`, which then fires a cancel at the venue for orders that completed hours
    /// ago. Each is refused, each produces a spurious `OrderCancelRejected`, and a *genuine* cancel
    /// failure is buried in the noise.
    ///
    /// The registry cannot answer "is it open" by itself — it holds no status — so the caller must
    /// supply that. [`Self::open_for_with`] takes the predicate; this method is retained only for
    /// callers that genuinely want everything tracked, and says so in its name.
    #[must_use]
    pub fn tracked_for(
        &self,
        instrument_id: InstrumentId,
        order_side: OrderSide,
    ) -> Vec<ZerodhaOrderContext> {
        self.by_client
            .values()
            .filter(|context| {
                context.instrument_id == instrument_id
                    && (order_side == OrderSide::NoOrderSide || context.order_side == order_side)
            })
            .copied()
            .collect()
    }

    /// Returns tracked contexts for an instrument that `is_open` says are still working.
    ///
    /// The predicate is supplied by the caller because the registry has no order status of its own —
    /// the execution client reads it from the cache, which is the only component that knows. Passing
    /// it in keeps this type free of a cache dependency and keeps the openness check *somewhere*,
    /// rather than nowhere.
    #[must_use]
    pub fn open_for_with<F>(
        &self,
        instrument_id: InstrumentId,
        order_side: OrderSide,
        is_open: F,
    ) -> Vec<ZerodhaOrderContext>
    where
        F: Fn(&ClientOrderId) -> bool,
    {
        self.tracked_for(instrument_id, order_side)
            .into_iter()
            .filter(|context| is_open(&context.client_order_id))
            .collect()
    }

    /// Forgets every tracked order, in both directions.
    ///
    /// Both maps are cleared. Clearing only one would leave the other resolving to orders that are
    /// no longer tracked, which is the same corruption `register` guards against.
    pub fn clear(&mut self) {
        self.by_client.clear();
        self.by_venue.clear();
    }

    /// Returns the number of tracked orders.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_client.len()
    }

    /// Returns whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_client.is_empty()
    }
}

/// The fields of a Nautilus order the venue's place-order route needs.
///
/// This exists so the mapping is testable without constructing an [`OrderAny`]: the mapping is the
/// part most likely to be wrong in a way no compiler catches, and a test that has to build a full
/// order to exercise it is a test that will not be written.
#[derive(Clone, Copy, Debug)]
pub struct NautilusOrderRequest {
    /// The instrument, whose venue component carries the Zerodha exchange.
    pub instrument_id: InstrumentId,
    /// The Nautilus client order id, offered to the venue as a `tag` when it fits.
    pub client_order_id: ClientOrderId,
    /// Buy or sell.
    pub order_side: OrderSide,
    /// The Nautilus order type.
    pub order_type: OrderType,
    /// The quantity.
    pub quantity: Quantity,
    /// The Nautilus time in force.
    pub time_in_force: TimeInForce,
    /// The limit price.
    pub price: Option<Price>,
    /// The trigger price.
    pub trigger_price: Option<Price>,
    /// The iceberg display quantity.
    pub display_qty: Option<Quantity>,
    /// Whether the order is post-only.
    pub post_only: bool,
    /// Whether the order is reduce-only.
    pub reduce_only: bool,
}

impl NautilusOrderRequest {
    /// Extracts the needed fields from a cached order.
    #[must_use]
    pub fn from_order(order: &OrderAny) -> Self {
        Self {
            instrument_id: order.instrument_id(),
            client_order_id: order.client_order_id(),
            order_side: order.order_side(),
            order_type: order.order_type(),
            quantity: order.quantity(),
            time_in_force: order.time_in_force(),
            price: order.price(),
            trigger_price: order.trigger_price(),
            display_qty: order.display_qty(),
            post_only: order.is_post_only(),
            reduce_only: order.is_reduce_only(),
        }
    }

    /// Builds the venue request, or explains which Nautilus concept the venue cannot carry.
    ///
    /// # Every rejection here is a value the venue would have ACCEPTED
    ///
    /// None of these produce an error at Zerodha. A `reduce_only` flag simply does not exist, so
    /// dropping it places an order that can open a position the strategy meant only to close; a
    /// `post_only` flag does not exist, so dropping it lets the order cross the spread and pay the
    /// taker side; an iceberg `display_qty` needs `variety=iceberg` plus `iceberg_legs`, so dropping
    /// it shows the full size to the book. Each of those is a live order behaving differently from
    /// the one requested, with nothing raised anywhere.
    ///
    /// # Errors
    ///
    /// Returns an error if the instrument's venue is not a Zerodha routing exchange, if the order
    /// type or time in force has no Zerodha equivalent, if the quantity is not a whole number of
    /// units, if a required price is missing, or if the order carries an execution instruction
    /// Zerodha has no field for.
    pub fn to_place_request(
        &self,
        product: ZerodhaProduct,
        variety: ZerodhaVariety,
    ) -> anyhow::Result<PlaceOrderRequest> {
        if self.reduce_only {
            anyhow::bail!(
                "the order is reduce_only and Zerodha has no such flag; placing it without one \
                 would allow the order to OPEN a position the strategy meant only to close"
            );
        }

        if self.post_only {
            anyhow::bail!(
                "the order is post_only and Zerodha has no such flag; placing it without one \
                 would let the order cross the spread and take liquidity"
            );
        }

        if self.display_qty.is_some() {
            anyhow::bail!(
                "the order carries a display_qty (iceberg) and this adapter builds only the \
                 regular parameter set; Zerodha needs variety=iceberg with iceberg_legs and \
                 iceberg_quantity, and dropping the display quantity would show the full size"
            );
        }

        if !matches!(variety, ZerodhaVariety::Regular | ZerodhaVariety::Amo) {
            anyhow::bail!(
                "this adapter can only place the '{}' and '{}' varieties: '{}' requires \
                 parameters the request type does not carry (co needs a stoploss leg, iceberg \
                 needs iceberg_legs, auction needs an auction_number)",
                ZerodhaVariety::Regular.as_str(),
                ZerodhaVariety::Amo.as_str(),
                variety.as_str(),
            );
        }

        let exchange = ZerodhaExchange::from_venue_str(self.instrument_id.venue.as_str())?;
        let transaction_type = ZerodhaTransactionType::from_order_side(self.order_side)?;
        let order_type = ZerodhaOrderType::from_order_type(self.order_type)?;
        let validity = ZerodhaValidity::from_time_in_force(self.time_in_force)?;
        let quantity = quantity_to_units(self.quantity)?;

        let price = self.resolve_price(order_type)?;
        let trigger_price = self.resolve_trigger_price(order_type)?;

        let request = PlaceOrderRequest {
            variety,
            exchange,
            tradingsymbol: self.instrument_id.symbol.as_str().to_string(),
            transaction_type,
            quantity,
            product,
            order_type,
            price,
            trigger_price,
            validity: Some(validity),
            disclosed_quantity: None,
            tag: tag_for(self.client_order_id),
        };
        request.validate()?;

        Ok(request)
    }

    /// Resolves the limit price against what the venue order type expects.
    fn resolve_price(&self, order_type: ZerodhaOrderType) -> anyhow::Result<Option<String>> {
        if order_type.requires_price() {
            let price = self.price.ok_or_else(|| {
                anyhow::anyhow!(
                    "a Zerodha {} order requires a price and the Nautilus order carries none",
                    order_type.as_str(),
                )
            })?;

            return Ok(Some(price.to_string()));
        }

        // Not silently dropped. A caller who set a price believes it constrains the fill, and a
        // MARKET order that ignores it can execute anywhere.
        if self.price.is_some() {
            anyhow::bail!(
                "the Nautilus order carries a price but maps to a Zerodha {} order, which has no \
                 price field; sending it would let the caller believe a limit was applied",
                order_type.as_str(),
            );
        }

        Ok(None)
    }

    /// Resolves the trigger price against what the venue order type expects.
    fn resolve_trigger_price(
        &self,
        order_type: ZerodhaOrderType,
    ) -> anyhow::Result<Option<String>> {
        if order_type.requires_trigger_price() {
            let trigger_price = self.trigger_price.ok_or_else(|| {
                anyhow::anyhow!(
                    "a Zerodha {} order requires a trigger_price and the Nautilus order carries \
                     none",
                    order_type.as_str(),
                )
            })?;

            return Ok(Some(trigger_price.to_string()));
        }

        if self.trigger_price.is_some() {
            anyhow::bail!(
                "the Nautilus order carries a trigger_price but maps to a Zerodha {} order, which \
                 has no trigger field",
                order_type.as_str(),
            );
        }

        Ok(None)
    }
}

/// Returns the client order id as a venue tag, or `None` when it is too long to carry.
///
/// **Never truncated.** A truncated client order id is a prefix, and a prefix matches other orders
/// from the same strategy on the same day — so a "recovered" link would point at the wrong order,
/// which is worse than no link at all.
#[must_use]
pub fn tag_for(client_order_id: ClientOrderId) -> Option<String> {
    let tag = client_order_id.to_string();

    if tag.len() > KITE_MAX_TAG_LEN {
        return None;
    }

    Some(tag)
}

/// Converts a Nautilus quantity to the whole number of units Zerodha expects.
///
/// # Errors
///
/// Returns an error for a fractional or non-positive quantity. Indian equity and derivative
/// quantities are whole units, so a fraction means the caller has computed something the venue
/// cannot express — and rounding it changes the size of a real position.
pub fn quantity_to_units(quantity: Quantity) -> anyhow::Result<u64> {
    let value = quantity.as_f64();
    let rounded = value.round();

    if (value - rounded).abs() > f64::EPSILON {
        anyhow::bail!(
            "Zerodha quantities are whole units and {quantity} is fractional; rounding it would \
             change the size of a real position"
        );
    }

    if rounded < 1.0 {
        anyhow::bail!("Zerodha order quantity must be at least 1 unit, was {quantity}");
    }

    // `rounded` is finite, integral and at least 1 by the checks above, and a Nautilus `Quantity`
    // cannot exceed `u64::MAX`, so the cast cannot truncate or lose a sign.
    Ok(rounded as u64)
}

/// Builds a cancel-rejection event from raw fields.
///
/// # Why this is a free function and not a call on [`OrderEventFactory`]
///
/// The factory's `generate_order_cancel_rejected` takes an `&OrderAny`, which means reading the
/// cache — and the cache is a [`CacheView`], which is `Rc`-based and therefore not reachable from
/// the background task that discovers the rejection. `nautilus-live`'s `ExecutionEventEmitter`
/// solves this with an `emit_order_cancel_rejected_event` that constructs the event from parts
/// exactly as below; that crate is not a dependency here, so the construction is repeated.
///
/// Free rather than a method for the same reason: a spawned task holds no `&self`.
#[expect(
    clippy::too_many_arguments,
    reason = "the event carries this many identifying fields; grouping them would only move them"
)]
fn cancel_rejected_event(
    trader_id: TraderId,
    account_id: AccountId,
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    reason: &str,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderEventAny {
    OrderEventAny::CancelRejected(OrderCancelRejected::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        reason.into(),
        UUID4::new(),
        ts_event,
        ts_init,
        false,
        venue_order_id,
        Some(account_id),
    ))
}

/// Builds a modify-rejection event from raw fields.
///
/// See [`cancel_rejected_event`] for why this is not routed through the factory.
#[expect(
    clippy::too_many_arguments,
    reason = "the event carries this many identifying fields; grouping them would only move them"
)]
fn modify_rejected_event(
    trader_id: TraderId,
    account_id: AccountId,
    strategy_id: StrategyId,
    instrument_id: InstrumentId,
    client_order_id: ClientOrderId,
    venue_order_id: Option<VenueOrderId>,
    reason: &str,
    ts_event: UnixNanos,
    ts_init: UnixNanos,
) -> OrderEventAny {
    OrderEventAny::ModifyRejected(OrderModifyRejected::new(
        trader_id,
        strategy_id,
        instrument_id,
        client_order_id,
        reason.into(),
        UUID4::new(),
        ts_event,
        ts_init,
        false,
        venue_order_id,
        Some(account_id),
    ))
}

/// A Nautilus execution client for the Zerodha Kite Connect order API.
///
/// See the module documentation for what is real, what is a stub, and why the stubs raise errors
/// rather than returning empty results.
pub struct ZerodhaExecutionClient {
    client_id: ClientId,
    account_id: AccountId,
    venue: Venue,
    config: ZerodhaExecClientConfig,
    /// Resolved at construction: a client that cannot name its product must not be built.
    product: ZerodhaProduct,
    cache: CacheView,
    http: Arc<ZerodhaHttpClient>,
    events: OrderEventFactory,
    /// Taken in `start`; `None` before then, and every publish path says so rather than panicking.
    sender: Option<UnboundedSender<ExecutionEvent>>,
    orders: Arc<Mutex<ZerodhaOrderRegistry>>,
    clock: &'static AtomicTime,
    tasks: TaskHandles,
    is_connected: AtomicBool,
    is_started: AtomicBool,
}

impl Debug for ZerodhaExecutionClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Written by hand rather than derived. `config` redacts itself and so does the credential
        // inside `http`, but a derived impl here would also depend on every field's `Debug` staying
        // redacted forever -- and the crate has already had exactly that regression once.
        f.debug_struct(stringify!(ZerodhaExecutionClient))
            .field("client_id", &self.client_id)
            .field("account_id", &self.account_id)
            .field("venue", &self.venue)
            .field("product", &self.product)
            .field("default_variety", &self.config.default_variety)
            .field("is_connected", &self.is_connected())
            .field("is_started", &self.is_started.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl ZerodhaExecutionClient {
    /// Creates a new [`ZerodhaExecutionClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the credential pair cannot be resolved, if the configuration names no
    /// `default_product`, or if the HTTP client cannot be built.
    ///
    /// # The product check is HERE and not at submit time, on purpose
    ///
    /// `ExecutionClientFactory::create` is called with `?` by the live node builder, so an error
    /// here aborts the build. Deferring the check to `submit_order` would move the failure to the
    /// first order of the session — which is 09:15 on a live account.
    pub fn new(
        client_id: ClientId,
        account_id: AccountId,
        trader_id: TraderId,
        account_type: AccountType,
        cache: CacheView,
        config: ZerodhaExecClientConfig,
    ) -> anyhow::Result<Self> {
        let (key_var, token_var) = credential_env_vars();
        let credential = config.credential().ok_or_else(|| {
            anyhow::anyhow!(
                "Zerodha execution client requires both an API key and an access token \
                 (set them on the config, or via {key_var} / {token_var})"
            )
        })?;

        let product = config.default_product.ok_or_else(|| {
            anyhow::anyhow!(
                "Zerodha execution client requires `default_product` (CNC, MIS, NRML or CO). \
                 There is no defensible default: MIS has the broker force-close every position \
                 around 15:20 IST, CNC demands full delivery margin, and NRML is meaningless on \
                 an equity segment. A per-order override is available via params[\"{PARAM_PRODUCT}\"]"
            )
        })?;

        let http = ZerodhaHttpClient::new(credential, config.base_url_http.clone())?;

        // `base_currency` is `None` rather than a guessed INR. It is used only when generating
        // account state from balances, and this client never synthesises balances -- it passes
        // through whatever the caller supplies.
        let events = OrderEventFactory::new(trader_id, account_id, account_type, None);

        Ok(Self {
            client_id,
            account_id,
            venue: *NSE_VENUE,
            config,
            product,
            cache,
            http: Arc::new(http),
            events,
            sender: None,
            orders: Arc::new(Mutex::new(ZerodhaOrderRegistry::new())),
            clock: get_atomic_clock_realtime(),
            tasks: TaskHandles::default(),
            is_connected: AtomicBool::new(false),
            is_started: AtomicBool::new(false),
        })
    }

    /// Returns the shared order registry.
    #[must_use]
    pub fn orders(&self) -> Arc<Mutex<ZerodhaOrderRegistry>> {
        Arc::clone(&self.orders)
    }

    /// Returns the product this client places orders under.
    #[must_use]
    pub const fn product(&self) -> ZerodhaProduct {
        self.product
    }

    /// Resolves the product for one order, honouring a per-order override.
    ///
    /// # Errors
    ///
    /// Returns an error if the override is present but is not a product Zerodha accepts. It is not
    /// ignored in favour of the configured value: a strategy that asked for `MIS` and silently got
    /// `NRML` has different leverage from the one it sized its position against.
    fn resolve_product(&self, params: Option<&Params>) -> anyhow::Result<ZerodhaProduct> {
        match params.and_then(|p| p.get_str(PARAM_PRODUCT)) {
            Some(value) => ZerodhaProduct::from_venue_str(value),
            None => Ok(self.product),
        }
    }

    /// Resolves the variety for one order, honouring a per-order override.
    ///
    /// # Errors
    ///
    /// Returns an error if the override is not a known variety.
    fn resolve_variety(&self, params: Option<&Params>) -> anyhow::Result<ZerodhaVariety> {
        match params.and_then(|p| p.get_str(PARAM_VARIETY)) {
            Some(value) => ZerodhaVariety::from_venue_str(value),
            None => Ok(self.config.default_variety),
        }
    }

    /// Publishes an order event, or explains why it could not be published.
    fn publish(sender: Option<&UnboundedSender<ExecutionEvent>>, event: OrderEventAny) {
        let Some(sender) = sender else {
            // Not a panic and not silence. The venue may already have acted on the command, so the
            // engine's view and the venue's view have diverged and somebody has to be told.
            log::error!(
                "Cannot publish a Zerodha execution event: the client was never started, so there \
                 is no execution event sender. The venue may already have acted on the command"
            );
            return;
        };

        if sender.send(ExecutionEvent::Order(event)).is_err() {
            log::warn!("Zerodha execution event dropped: the receiver is gone");
        }
    }

    /// Spawns a fallible background task, logging any failure.
    fn spawn_task<F>(&self, description: &'static str, fut: F)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let runtime = get_runtime();

        let handle = runtime.spawn(async move {
            if let Err(e) = fut.await {
                log::warn!("Zerodha {description} failed: {e:?}");
            }
        });

        self.tasks.push(handle);
    }

    /// Returns whether the cache says this instrument is an index, or `None` if it is not cached.
    ///
    /// # ⭐ The exchange code CANNOT answer this, and that is why the check lives here
    ///
    /// An index does not have an exchange of its own — `exchange == "INDICES"` occurs **zero
    /// times** in the instrument dump (measured 2026-08-14 across all 114,851 rows). `NIFTY 50` is
    /// `exchange=NSE`, sitting among 10,036 tradable NSE rows, and is marked only by its
    /// per-row `segment`. So [`ZerodhaExchange::from_venue_str`] passes it and always will.
    ///
    /// The property does survive into the cache, though: [`crate::http::instruments`] dispatches
    /// `segment == INDICES` to [`InstrumentAny::IndexInstrument`] **before** it looks at
    /// `instrument_type`, precisely because an index row carries `instrument_type = EQ` and would
    /// otherwise parse as an equity. This reads that decision back rather than re-deriving it.
    ///
    /// # ⚠️ `None` is not "tradable" — it is "no opinion"
    ///
    /// The three states are distinct and collapsing them is how this check would fail open. An
    /// uncached instrument yields `None`, and the caller lets the order proceed: refusing every
    /// order for an instrument the cache has not seen would deny normal trading whenever the
    /// instrument provider has not run. So **the screen is exactly as good as the cache is
    /// populated**, and an order on an index the cache does not hold still reaches the venue.
    fn is_index(&self, instrument_id: &InstrumentId) -> Option<bool> {
        self.cache
            .borrow()
            .instrument(instrument_id)
            .map(|instrument| matches!(instrument, InstrumentAny::IndexInstrument(_)))
    }

    /// Reads an order out of the cache.
    ///
    /// The cache borrow is released before the value is returned, because callers publish events
    /// afterwards and a subscriber may reach for the same cache.
    fn cached_order(&self, client_order_id: &ClientOrderId) -> anyhow::Result<OrderAny> {
        let order = self.cache.borrow().try_order_owned(client_order_id)?;

        Ok(order)
    }

    /// Issues one cancel, given a context that already knows the placed variety.
    fn cancel_with_context(&self, context: ZerodhaOrderContext) {
        let Some(venue_order_id) = context.venue_order_id else {
            self.reject_cancel(
                context,
                "the venue has not yet assigned an order id, so there is nothing to address; \
                 the place-order response has not landed",
            );
            return;
        };

        let http = Arc::clone(&self.http);
        let orders = Arc::clone(&self.orders);
        let sender = self.sender.clone();
        let clock = self.clock;
        let trader_id = self.events.trader_id();
        let account_id = self.events.account_id();
        let variety = context.variety;
        let client_order_id = context.client_order_id;
        let instrument_id = context.instrument_id;
        let strategy_id = context.strategy_id;

        self.spawn_task("cancel_order", async move {
            // `parent_order_id` is `None`: it applies to cover-order legs and this adapter refuses
            // to place the `co` variety, so an order it tracks can never have a parent.
            let result = http
                .cancel_order(variety, venue_order_id.as_str(), None)
                .await;

            match result {
                Ok(_) => {
                    // The CANCELED event is not emitted here. Zerodha acknowledges the cancel
                    // request, not the cancellation -- the order can still fill in the gap before
                    // the exchange processes it. Confirming it needs the order book poll, which is
                    // reconciliation, which is not implemented. The engine keeps the order in
                    // PENDING_CANCEL, which is the honest state.
                    orders.lock().expect(MUTEX_POISONED).remove(&client_order_id);
                    log::info!("Zerodha accepted the cancel request for {client_order_id}");
                }
                // An unknown cancel outcome is not a rejected cancel. `OrderCancelRejected` asserts
                // the cancel did NOT take effect and the order is STILL LIVE — and if the request
                // actually reached the venue, that is false in the direction that leaves a strategy
                // believing it holds a position it has already closed.
                Err(e) if e.may_have_reached_venue() => {
                    // Entry retained: if the cancel did not land, this is still the only record of
                    // the variety needed to address a retry.
                    log::error!(
                        "Zerodha {client_order_id}: CANCEL OUTCOME UNKNOWN. {e} \
                         -- the order may have been cancelled, or may still be live. No \
                         OrderCancelRejected has been emitted, because that would assert it is \
                         still live. The engine keeps it in PENDING_CANCEL, which is the honest \
                         state, and the registry entry is retained so a retry can be addressed. \
                         RECONCILIATION WOULD RESOLVE THIS AND IS NOT IMPLEMENTED."
                    );
                    return Err(anyhow::anyhow!(e).context("cancel order outcome unknown"));
                }
                Err(e) => {
                    let event = cancel_rejected_event(
                        trader_id,
                        account_id,
                        strategy_id,
                        instrument_id,
                        client_order_id,
                        Some(venue_order_id),
                        &format!("cancel-order-rejected: {e}"),
                        clock.get_time_ns(),
                        clock.get_time_ns(),
                    );
                    Self::publish(sender.as_ref(), event);
                    return Err(anyhow::anyhow!(e).context("cancel order failed"));
                }
            }

            Ok(())
        });
    }

    /// Publishes a cancel rejection for an order this client cannot address.
    fn reject_cancel(&self, context: ZerodhaOrderContext, reason: &str) {
        let ts = self.clock.get_time_ns();
        let event = cancel_rejected_event(
            self.events.trader_id(),
            self.events.account_id(),
            context.strategy_id,
            context.instrument_id,
            context.client_order_id,
            context.venue_order_id,
            &format!("cancel-order-rejected: {reason}"),
            ts,
            ts,
        );

        Self::publish(self.sender.as_ref(), event);
    }
}

#[async_trait(?Send)]
impl ExecutionClient for ZerodhaExecutionClient {
    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn account_id(&self) -> AccountId {
        self.account_id
    }

    fn venue(&self) -> Venue {
        self.venue
    }

    fn oms_type(&self) -> OmsType {
        // Zerodha nets: one position per (instrument, product) per day, and there is no hedge mode
        // on the account. A HEDGING oms would let the engine hold two opposing positions the venue
        // will have already netted.
        OmsType::Netting
    }

    /// Accepts orders for every exchange Zerodha routes to, not only this client's [`Self::venue`].
    ///
    /// Zerodha is a **broker**, not an exchange: one connection reaches NSE, BSE, NFO, BFO, CDS,
    /// BCD, MCX and NCO, and the venue component of an instrument id names the exchange rather than
    /// the broker. The trait's default (`self.venue() == venue`) would therefore accept only the
    /// 10,037 NSE instruments and refuse the other 104,814 — every option and future on NFO, every
    /// commodity on MCX and NCO.
    ///
    /// The routable set comes from the instrument dump, **not** from the vendor client's
    /// `EXCHANGE_*` constants, which are stale — see [`ZerodhaExchange`]. It does not and cannot
    /// screen out indices, which ride on ordinary exchanges rather than an exchange of their own;
    /// [`Self::submit_order`] does that from the cached instrument definition.
    ///
    /// # ⚠️ THIS IS A VETO, NOT A ROUTE — and on its own it does not make multi-exchange work
    ///
    /// This override is necessary and **not sufficient**, and the gap is invisible to every test in
    /// this crate. The two halves are separate mechanisms in `ExecutionEngine`:
    ///
    /// | mechanism | where | what it does |
    /// |---|---|---|
    /// | `routing_map` | `register_client`: `routing_map.insert(client.venue(), id)` | decides WHICH client an order goes to |
    /// | this method | `execute` : `if !client.handles_order_venue(order_venue)` | decides whether that client may KEEP it |
    ///
    /// `register_client` files this client under **`NSE` alone**, because [`Self::venue`] must
    /// return a single `Venue` and NSE is the primary one. Order lookup is
    /// `routing_map.get(&venue).or(default_client_id)`, so an `NFO` order finds **no client at
    /// all** and never reaches this method. Without the override it would reach it and be denied
    /// with `OrderDeniedReason::ClientVenueMismatch`; with the override it is simply not routed.
    /// Two different failures, neither of them "it works".
    ///
    /// **So the node must ALSO do one of:**
    ///
    /// - register this client as the default (`register_default_client`), which is right when
    ///   Zerodha is the only broker in the node; or
    /// - call `register_venue_routing` once per exchange actually traded.
    ///
    /// The data client hit the same wall from the other side and answered it by returning `None`
    /// from `DataClient::venue()` — an option [`ExecutionClient`] does not offer, since its `venue`
    /// returns a bare `Venue`. That asymmetry is why this has to be handled in configuration here
    /// rather than in the client.
    fn handles_order_venue(&self, venue: Venue) -> bool {
        ZerodhaExchange::from_venue_str(venue.as_str()).is_ok()
    }

    fn get_account(&self) -> Option<AccountAny> {
        self.cache.borrow().account_owned(&self.account_id)
    }

    fn generate_account_state(
        &self,
        balances: Vec<AccountBalance>,
        margins: Vec<MarginBalance>,
        reported: bool,
        ts_event: UnixNanos,
        info: Option<Params>,
    ) -> anyhow::Result<()> {
        let state = self.events.generate_account_state(
            balances,
            margins,
            reported,
            ts_event,
            self.clock.get_time_ns(),
            info,
        );

        let Some(sender) = self.sender.as_ref() else {
            anyhow::bail!(
                "cannot publish Zerodha account state: the client was never started, so there is \
                 no execution event sender"
            );
        };

        if sender.send(ExecutionEvent::Account(state)).is_err() {
            anyhow::bail!("Zerodha account state dropped: the receiver is gone");
        }

        Ok(())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        if self.is_started.load(Ordering::Acquire) {
            return Ok(());
        }

        // `try_get_...` rather than `get_...`: the latter panics when the runner has not installed
        // a sender, which is the normal state in a unit test.
        match try_get_exec_event_sender() {
            Some(sender) => self.sender = Some(sender),
            None => log::warn!(
                "Zerodha execution client {} started with no execution event sender; order events \
                 cannot reach the engine",
                self.client_id,
            ),
        }

        self.is_started.store(true, Ordering::Release);
        log::info!(
            "Started Zerodha execution client: client_id={}, account_id={}, product={}",
            self.client_id,
            self.account_id,
            self.product.as_str(),
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        if !self.is_started.load(Ordering::Acquire) {
            return Ok(());
        }

        self.is_started.store(false, Ordering::Release);
        self.is_connected.store(false, Ordering::Release);
        self.tasks.abort_all();
        log::info!("Stopped Zerodha execution client {}", self.client_id);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.is_connected.store(false, Ordering::Release);

        // The registry is the client's only mutable state and it is per-session by nature: the
        // client order id to venue order id links it holds are not durable, so carrying them across
        // a reset would preserve entries whose venue-side orders may no longer exist.
        self.orders
            .lock()
            .map_err(|e| anyhow::anyhow!(
                // A poisoned lock is the VICTIM, not the cause: it means another thread
                // panicked while holding it. The original panic is the fault and it has
                // already been logged elsewhere -- looking at this call site finds nothing.
                "Zerodha order registry lock poisoned, which means an earlier operation \
                 panicked while holding it -- look for that panic, not for a fault here: {e}"
            ))?
            .clear();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        self.stop()
    }

    /// Connects by proving the credential against a read-only route.
    ///
    /// The execution path has no socket of its own — Zerodha's tick socket carries market data and
    /// order updates arrive by postback — so there is nothing to open. What there *is* to do is
    /// establish that the session token works, because it is flushed every morning and the
    /// alternative place to discover that is the first order of the day.
    ///
    /// `GET /orders` is used for the probe because it is read-only and unconditional. **It places
    /// nothing.**
    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        let orders = self.http.list_orders().await.map_err(|e| {
            anyhow::anyhow!(
                "Zerodha execution client could not reach the order book, so the session token is \
                 not known to work: {e}"
            )
        })?;

        self.is_connected.store(true, Ordering::Release);
        log::info!(
            "Zerodha execution client {} connected; the venue reports {} order(s) today",
            self.client_id,
            orders.len(),
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        self.is_connected.store(false, Ordering::Release);
        log::info!("Zerodha execution client {} disconnected", self.client_id);
        Ok(())
    }

    /// Submits one order.
    ///
    /// A mapping failure produces an [`OrderDenied`] rather than an `Err`. The distinction matters:
    /// an `Err` here is logged by the engine and the strategy is never told, whereas a denial
    /// reaches the order's own event stream with the reason attached.
    ///
    /// [`OrderDenied`]: nautilus_model::events::OrderDenied
    fn submit_order(&self, cmd: SubmitOrder) -> anyhow::Result<()> {
        let order = self.cached_order(&cmd.client_order_id)?;

        if order.is_closed() {
            log::warn!(
                "Not submitting {}: the order is already closed",
                order.client_order_id(),
            );
            return Ok(());
        }

        if !self.is_connected() {
            let event = self.events.generate_order_denied(
                &order,
                "the Zerodha execution client is not connected",
                self.clock.get_time_ns(),
            );
            Self::publish(self.sender.as_ref(), event);
            return Ok(());
        }

        // The index screen. This CANNOT live in `NautilusOrderRequest::to_place_request` -- that
        // function is deliberately pure so the mapping is testable without a cache, and an
        // instrument id carries nothing that distinguishes `NIFTY 50.NSE` from `RELIANCE.NSE`.
        // See `Self::is_index` for why the exchange code can never answer this.
        if self.is_index(&order.instrument_id()) == Some(true) {
            let event = self.events.generate_order_denied(
                &order,
                "this instrument is an index, which quotes but cannot be traded; the exchange \
                 code cannot show this (indices carry exchange=NSE/BSE/MCX and are marked only by \
                 segment=INDICES), so the refusal comes from the cached instrument definition",
                self.clock.get_time_ns(),
            );
            Self::publish(self.sender.as_ref(), event);
            return Ok(());
        }

        let product = match self.resolve_product(cmd.params.as_ref()) {
            Ok(product) => product,
            Err(e) => {
                let event = self.events.generate_order_denied(
                    &order,
                    &format!("invalid params[\"{PARAM_PRODUCT}\"]: {e}"),
                    self.clock.get_time_ns(),
                );
                Self::publish(self.sender.as_ref(), event);
                return Ok(());
            }
        };

        let variety = match self.resolve_variety(cmd.params.as_ref()) {
            Ok(variety) => variety,
            Err(e) => {
                let event = self.events.generate_order_denied(
                    &order,
                    &format!("invalid params[\"{PARAM_VARIETY}\"]: {e}"),
                    self.clock.get_time_ns(),
                );
                Self::publish(self.sender.as_ref(), event);
                return Ok(());
            }
        };

        let view = NautilusOrderRequest::from_order(&order);

        let request = match view.to_place_request(product, variety) {
            Ok(request) => request,
            Err(e) => {
                // This is the branch the house rule exists for. Every one of these could have been
                // "close enough" and placed a real order that behaves differently.
                let event = self.events.generate_order_denied(
                    &order,
                    &format!("Zerodha cannot express this order: {e}"),
                    self.clock.get_time_ns(),
                );
                Self::publish(self.sender.as_ref(), event);
                return Ok(());
            }
        };

        if request.tag.is_none() {
            log::warn!(
                "Placing {} without a tag: the client order id is longer than the {KITE_MAX_TAG_LEN} \
                 character Zerodha limit, so the venue will not echo it back and the link between \
                 this order and its venue id will not survive a restart",
                order.client_order_id(),
            );
        }

        self.orders
            .lock()
            .map_err(|e| anyhow::anyhow!(
                // A poisoned lock is the VICTIM, not the cause: it means another thread
                // panicked while holding it. The original panic is the fault and it has
                // already been logged elsewhere -- looking at this call site finds nothing.
                "Zerodha order registry lock poisoned, which means an earlier operation \
                 panicked while holding it -- look for that panic, not for a fault here: {e}"
            ))?
            .register(ZerodhaOrderContext {
                client_order_id: order.client_order_id(),
                venue_order_id: None,
                instrument_id: order.instrument_id(),
                strategy_id: order.strategy_id(),
                order_side: order.order_side(),
                variety,
            });

        let submitted = self
            .events
            .generate_order_submitted(&order, self.clock.get_time_ns());
        Self::publish(self.sender.as_ref(), submitted);

        let http = Arc::clone(&self.http);
        let orders = Arc::clone(&self.orders);
        let sender = self.sender.clone();
        let events = self.events.clone();
        let clock = self.clock;
        let client_order_id = order.client_order_id();

        self.spawn_task("submit_order", async move {
            match http.place_order(&request).await {
                Ok(venue_order_id) => {
                    let venue_order_id = VenueOrderId::new(&venue_order_id);
                    let linked = orders
                        .lock()
                        .expect(MUTEX_POISONED)
                        .link_venue_order_id(client_order_id, venue_order_id);

                    if !linked {
                        // The order was removed while the request was in flight, which means a
                        // stop() or reset() raced it. The venue still has the order.
                        log::warn!(
                            "Zerodha accepted {client_order_id} as {venue_order_id}, but the \
                             order is no longer tracked; it cannot be cancelled by this client"
                        );
                    }

                    let event =
                        events.generate_order_accepted(&order, venue_order_id, clock.get_time_ns(), clock.get_time_ns());
                    Self::publish(sender.as_ref(), event);
                }
                // ⭐ A REJECTION AND AN UNKNOWN OUTCOME ARE NOT THE SAME EVENT.
                //
                // `OrderRejected` asserts the venue refused and NO ORDER EXISTS. Emitting it for a
                // timeout is not a weaker claim, it is a false one, and it fails expensively: the
                // engine stops tracking a position that is live, this client drops the registry
                // entry it needs to cancel that order, and a strategy that resubmits on rejection
                // ends up with double size.
                Err(e) if e.may_have_reached_venue() => {
                    // The entry is KEPT ON PURPOSE. It is the only record of the variety this order
                    // was placed under, and cancelling addresses /orders/{variety}/{order_id} -- so
                    // discarding it would remove the one route to closing a position that may be
                    // open at the venue right now.
                    log::error!(
                        "Zerodha {client_order_id}: OUTCOME UNKNOWN, NOT REJECTED. {e} \
                         -- the order may be LIVE at the venue. No OrderRejected has been emitted, \
                         because that would tell the engine no order exists. The order stays \
                         SUBMITTED and the registry entry is retained so a cancel can still be \
                         addressed. RECONCILIATION WOULD RESOLVE THIS AND IS NOT IMPLEMENTED, so \
                         this needs a human to check the venue's order book."
                    );
                    return Err(anyhow::anyhow!(e).context("submit order outcome unknown"));
                }
                Err(e) => {
                    // Refused: the venue answered and said no. The order provably does not exist,
                    // so dropping the registry entry and reporting a rejection are both correct.
                    orders.lock().expect(MUTEX_POISONED).remove(&client_order_id);
                    let event = events.generate_order_rejected(
                        &order,
                        &format!("submit-order-rejected: {e}"),
                        clock.get_time_ns(),
                        clock.get_time_ns(),
                        false,
                    );
                    Self::publish(sender.as_ref(), event);
                    return Err(anyhow::anyhow!(e).context("submit order failed"));
                }
            }

            Ok(())
        });

        Ok(())
    }

    fn modify_order(&self, cmd: ModifyOrder) -> anyhow::Result<()> {
        let ts = self.clock.get_time_ns();
        let context = self
            .orders
            .lock()
            .map_err(|e| anyhow::anyhow!(
                // A poisoned lock is the VICTIM, not the cause: it means another thread
                // panicked while holding it. The original panic is the fault and it has
                // already been logged elsewhere -- looking at this call site finds nothing.
                "Zerodha order registry lock poisoned, which means an earlier operation \
                 panicked while holding it -- look for that panic, not for a fault here: {e}"
            ))?
            .by_client_order_id(&cmd.client_order_id);

        // The variety comes from the REGISTRY, never from the command and never from the config
        // default. Modify addresses `/orders/{variety}/{order_id}`, so a variety this process did
        // not record is a guess at a URL path.
        let Some(context) = context else {
            let event = modify_rejected_event(
                self.events.trader_id(),
                self.events.account_id(),
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                cmd.venue_order_id,
                "modify-order-rejected: this process did not place the order, so the variety it \
                 was placed under is unknown; the modify path is /orders/{variety}/{order_id} and \
                 a guessed variety is a 404. Reconciliation would close this gap and is not \
                 implemented",
                ts,
                ts,
            );
            Self::publish(self.sender.as_ref(), event);
            return Ok(());
        };

        let venue_order_id = context.venue_order_id.or(cmd.venue_order_id);

        let Some(venue_order_id) = venue_order_id else {
            let event = modify_rejected_event(
                self.events.trader_id(),
                self.events.account_id(),
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                None,
                "modify-order-rejected: the venue has not yet assigned an order id",
                ts,
                ts,
            );
            Self::publish(self.sender.as_ref(), event);
            return Ok(());
        };

        let quantity = match cmd.quantity.map(quantity_to_units).transpose() {
            Ok(quantity) => quantity,
            Err(e) => {
                let event = modify_rejected_event(
                    self.events.trader_id(),
                    self.events.account_id(),
                    cmd.strategy_id,
                    cmd.instrument_id,
                    cmd.client_order_id,
                    Some(venue_order_id),
                    &format!("modify-order-rejected: {e}"),
                    ts,
                    ts,
                );
                Self::publish(self.sender.as_ref(), event);
                return Ok(());
            }
        };

        // `order_type` and `validity` are deliberately not modified. Nautilus's ModifyOrder cannot
        // express either, so sending them would mean inventing values -- and the venue treats an
        // absent field as "leave it alone", which is exactly the requested behaviour.
        let request = ModifyOrderRequest {
            variety: context.variety,
            order_id: venue_order_id.to_string(),
            quantity,
            price: cmd.price.map(|price| price.to_string()),
            order_type: None,
            trigger_price: cmd.trigger_price.map(|price| price.to_string()),
            validity: None,
            disclosed_quantity: None,
        };

        if let Err(e) = request.validate() {
            let event = modify_rejected_event(
                self.events.trader_id(),
                self.events.account_id(),
                cmd.strategy_id,
                cmd.instrument_id,
                cmd.client_order_id,
                Some(venue_order_id),
                &format!("modify-order-rejected: {e}"),
                ts,
                ts,
            );
            Self::publish(self.sender.as_ref(), event);
            return Ok(());
        }

        let http = Arc::clone(&self.http);
        let sender = self.sender.clone();
        let clock = self.clock;
        let trader_id = self.events.trader_id();
        let account_id = self.events.account_id();
        let strategy_id = cmd.strategy_id;
        let instrument_id = cmd.instrument_id;
        let client_order_id = cmd.client_order_id;

        self.spawn_task("modify_order", async move {
            // No OrderUpdated is emitted on success. Zerodha acknowledges the modify REQUEST, and
            // the venue is free to reject it later or to fill the order at the old price first.
            // Emitting the update here would tell the engine a change had taken effect that may
            // never have; confirming it needs the order book poll, which is not implemented.
            if let Err(e) = http.modify_order(&request).await {
                // `OrderModifyRejected` asserts the order is UNCHANGED. If the request reached the
                // venue and the answer was lost, the order may now be resting at a DIFFERENT price
                // or quantity than the engine believes — which is worse than not knowing, because
                // the engine would act on a stale price it thinks is current.
                if e.may_have_reached_venue() {
                    log::error!(
                        "Zerodha {client_order_id}: MODIFY OUTCOME UNKNOWN. {e} \
                         -- the order may now be at the NEW price/quantity or the OLD one. No \
                         OrderModifyRejected has been emitted, because that would assert it is \
                         unchanged. RECONCILIATION WOULD RESOLVE THIS AND IS NOT IMPLEMENTED."
                    );
                    return Err(anyhow::anyhow!(e).context("modify order outcome unknown"));
                }

                let event = modify_rejected_event(
                    trader_id,
                    account_id,
                    strategy_id,
                    instrument_id,
                    client_order_id,
                    Some(venue_order_id),
                    &format!("modify-order-rejected: {e}"),
                    clock.get_time_ns(),
                    clock.get_time_ns(),
                );
                Self::publish(sender.as_ref(), event);
                return Err(anyhow::anyhow!(e).context("modify order failed"));
            }

            log::info!("Zerodha accepted the modify request for {client_order_id}");
            Ok(())
        });

        Ok(())
    }

    fn cancel_order(&self, cmd: CancelOrder) -> anyhow::Result<()> {
        let context = self
            .orders
            .lock()
            .map_err(|e| anyhow::anyhow!(
                // A poisoned lock is the VICTIM, not the cause: it means another thread
                // panicked while holding it. The original panic is the fault and it has
                // already been logged elsewhere -- looking at this call site finds nothing.
                "Zerodha order registry lock poisoned, which means an earlier operation \
                 panicked while holding it -- look for that panic, not for a fault here: {e}"
            ))?
            .by_client_order_id(&cmd.client_order_id);

        // Not falling back to the configured default variety, however tempting. This adapter only
        // ever places `regular` and `amo`, so a guess would usually be right -- and when it is
        // wrong the cancel 404s and the order STAYS LIVE while the failure looks like a transport
        // problem. An explicit rejection is recoverable; a live order nobody is watching is not.
        let Some(context) = context else {
            self.reject_cancel(
                ZerodhaOrderContext {
                    client_order_id: cmd.client_order_id,
                    venue_order_id: cmd.venue_order_id,
                    instrument_id: cmd.instrument_id,
                    strategy_id: cmd.strategy_id,
                    order_side: OrderSide::NoOrderSide,
                    variety: self.config.default_variety,
                },
                "this process did not place the order, so the variety it was placed under is \
                 unknown; cancelling addresses /orders/{variety}/{order_id} and a wrong variety \
                 is a 404 that leaves the order live. Reconciliation would close this gap and is \
                 not implemented",
            );
            return Ok(());
        };

        self.cancel_with_context(ZerodhaOrderContext {
            venue_order_id: context.venue_order_id.or(cmd.venue_order_id),
            ..context
        });
        Ok(())
    }

    /// Cancels every order this client is tracking for the instrument.
    ///
    /// Zerodha has no bulk-cancel route, so this fans out over the client's own registry rather
    /// than over the venue's order book. **That is a real limitation, not an implementation
    /// detail:** an order placed by another process, or by this one before a restart, is not in the
    /// registry and is therefore not cancelled. Closing that needs the order book poll, which is
    /// reconciliation, which is not implemented.
    fn cancel_all_orders(&self, cmd: CancelAllOrders) -> anyhow::Result<()> {
        let contexts = self
            .orders
            .lock()
            .map_err(|e| anyhow::anyhow!(
                // A poisoned lock is the VICTIM, not the cause: it means another thread
                // panicked while holding it. The original panic is the fault and it has
                // already been logged elsewhere -- looking at this call site finds nothing.
                "Zerodha order registry lock poisoned, which means an earlier operation \
                 panicked while holding it -- look for that panic, not for a fault here: {e}"
            ))?
            // Openness comes from the CACHE, not the registry -- the registry holds no status, and
            // nothing removes an order from it on fill. Without this predicate, cancel-all fires a
            // cancel at every order this process has ever placed for the instrument, including ones
            // that completed hours ago: each is refused, each raises a spurious cancel-rejected, and
            // a real cancel failure is lost among them.
            .open_for_with(cmd.instrument_id, cmd.order_side, |client_order_id| {
                // ⚠️ THE ABSENT CASE DEFAULTS TO OPEN, AND THAT IS THE WHOLE POINT.
                //
                // `is_order_open` alone returns FALSE for an order the cache has never seen, which
                // would silently SKIP it. Skipping an order in a "cancel everything" request is the
                // failure that leaves a position on while the operator believes it is off. So an
                // unknown order is cancelled: a needless cancel is refused harmlessly, a missed one
                // is not.
                let cache = self.cache.borrow();
                !cache.order_exists(client_order_id) || cache.is_order_open(client_order_id)
            });

        log::info!(
            "Cancelling {} OPEN tracked Zerodha order(s) for {}; already-closed tracked orders are \
             excluded, and orders this process did not place are not covered at all",
            contexts.len(),
            cmd.instrument_id,
        );

        for context in contexts {
            self.cancel_with_context(context);
        }

        Ok(())
    }

    fn batch_cancel_orders(&self, cmd: BatchCancelOrders) -> anyhow::Result<()> {
        for cancel in cmd.cancels {
            self.cancel_order(cancel)?;
        }

        Ok(())
    }

    /// Not implemented.
    ///
    /// # Errors
    ///
    /// Always. Zerodha's `/user/margins` route returns a segment-wise margin breakdown that has to
    /// become [`AccountBalance`] and [`MarginBalance`] values with currencies attached, and this
    /// adapter does not build them. Returning `Ok(())` here would look like a query that succeeded
    /// and produced no account state.
    fn query_account(&self, _cmd: QueryAccount) -> anyhow::Result<()> {
        anyhow::bail!(
            "the Zerodha execution client does not implement query_account: the /user/margins \
             response is not mapped to AccountBalance and MarginBalance values"
        )
    }

    /// Not implemented.
    ///
    /// # Errors
    ///
    /// Always. Answering a query means publishing an [`OrderStatusReport`], and building one needs
    /// the two things named in the module docs: an IST timestamp conversion and the instrument's
    /// price and size precision.
    ///
    /// [`OrderStatusReport`]: nautilus_model::reports::OrderStatusReport
    fn query_order(&self, _cmd: QueryOrder) -> anyhow::Result<()> {
        anyhow::bail!(
            "the Zerodha execution client does not implement query_order: the REST order book is \
             fetched but is not mapped to an OrderStatusReport"
        )
    }

    /// Not implemented — and returns an error rather than `None`.
    ///
    /// # Errors
    ///
    /// Always. `Ok(None)` is the trait's default and means "the venue has no such order", which
    /// during reconciliation is grounds for treating a live order as gone.
    async fn generate_order_status_report(
        &self,
        _cmd: &GenerateOrderStatusReport,
    ) -> anyhow::Result<Option<OrderStatusReport>> {
        anyhow::bail!(
            "the Zerodha execution client does not build order status reports; the REST order book \
             is fetched by `ZerodhaHttpClient::list_orders` but mapping it needs an IST timestamp \
             conversion this crate has no dependency for, and the instrument precision that Price \
             and Quantity require. Returning None would mean 'the venue has no such order'"
        )
    }

    /// Not implemented — and returns an error rather than an empty list.
    ///
    /// # Errors
    ///
    /// Always. An empty list reads as "you have no working orders", which during reconciliation is
    /// grounds for closing every order the engine believes is live.
    async fn generate_order_status_reports(
        &self,
        _cmd: &GenerateOrderStatusReports,
    ) -> anyhow::Result<Vec<OrderStatusReport>> {
        anyhow::bail!(
            "the Zerodha execution client does not build order status reports; an empty list would \
             report a venue with no working orders"
        )
    }

    /// Not implemented — and returns an error rather than an empty list.
    ///
    /// # Errors
    ///
    /// Always. `ZerodhaHttpClient::list_trades` fetches the fills, but each fill has to become a
    /// [`FillReport`] with a `Price`, a `Quantity`, a commission and a timestamp — and Zerodha does
    /// not report commission on the trade at all (it arrives on the contract note, a separate
    /// route). A zero commission is a plausible wrong number that flows straight into PnL.
    async fn generate_fill_reports(
        &self,
        _cmd: GenerateFillReports,
    ) -> anyhow::Result<Vec<FillReport>> {
        anyhow::bail!(
            "the Zerodha execution client does not build fill reports; the trade book is fetched \
             but carries no commission, and a zero commission would flow into realised PnL"
        )
    }

    /// Not implemented — and returns an error rather than an empty list.
    ///
    /// # Errors
    ///
    /// Always. An empty list reads as a flat account.
    async fn generate_position_status_reports(
        &self,
        _cmd: &GeneratePositionStatusReports,
    ) -> anyhow::Result<Vec<PositionStatusReport>> {
        anyhow::bail!(
            "the Zerodha execution client does not build position status reports; /portfolio/positions \
             is not mapped, and an empty list would report a flat account"
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    const NIFTY_OPTION: &str = "NIFTY24AUG24000CE.NFO";

    fn view(order_type: OrderType, time_in_force: TimeInForce) -> NautilusOrderRequest {
        NautilusOrderRequest {
            instrument_id: InstrumentId::from(NIFTY_OPTION),
            client_order_id: ClientOrderId::from("O-001"),
            order_side: OrderSide::Buy,
            order_type,
            quantity: Quantity::from("65"),
            time_in_force,
            price: None,
            trigger_price: None,
            display_qty: None,
            post_only: false,
            reduce_only: false,
        }
    }

    fn context(client_order_id: &str, venue_order_id: Option<&str>) -> ZerodhaOrderContext {
        ZerodhaOrderContext {
            client_order_id: ClientOrderId::from(client_order_id),
            venue_order_id: venue_order_id.map(VenueOrderId::from),
            instrument_id: InstrumentId::from(NIFTY_OPTION),
            strategy_id: StrategyId::from("S-001"),
            order_side: OrderSide::Buy,
            variety: ZerodhaVariety::Regular,
        }
    }

    #[rstest]
    fn test_a_market_order_maps_to_the_venue_request() {
        let request = view(OrderType::Market, TimeInForce::Day)
            .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular)
            .expect("a DAY market order is representable");

        assert_eq!(request.exchange, ZerodhaExchange::Nfo);
        assert_eq!(request.tradingsymbol, "NIFTY24AUG24000CE");
        assert_eq!(request.transaction_type, ZerodhaTransactionType::Buy);
        assert_eq!(request.order_type, ZerodhaOrderType::Market);
        assert_eq!(request.product, ZerodhaProduct::Nrml);
        assert_eq!(request.quantity, 65);
        assert_eq!(request.price, None);
    }

    // The instrument id splits into the two things the venue wants and nothing has to be looked up:
    // `KiteInstrument::instrument_id` builds ids as TRADINGSYMBOL.EXCHANGE, so this is that
    // construction run backwards. A registry lookup here would add a failure mode for no gain.
    #[rstest]
    #[case("RELIANCE.NSE", "RELIANCE", ZerodhaExchange::Nse)]
    #[case("NIFTY24AUG24000CE.NFO", "NIFTY24AUG24000CE", ZerodhaExchange::Nfo)]
    #[case("USDINR24AUGFUT.CDS", "USDINR24AUGFUT", ZerodhaExchange::Cds)]
    #[case("GOLDM24SEPFUT.MCX", "GOLDM24SEPFUT", ZerodhaExchange::Mcx)]
    fn test_the_instrument_id_splits_into_tradingsymbol_and_exchange(
        #[case] instrument_id: &str,
        #[case] expected_symbol: &str,
        #[case] expected_exchange: ZerodhaExchange,
    ) {
        let request = NautilusOrderRequest {
            instrument_id: InstrumentId::from(instrument_id),
            ..view(OrderType::Market, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular)
        .expect("a routable instrument");

        assert_eq!(request.tradingsymbol, expected_symbol);
        assert_eq!(request.exchange, expected_exchange);
    }

    // An exchange the venue does not route orders to is refused. Note what this does NOT prove:
    // `INDICES` never appears as an exchange in the instrument dump, so this instrument id is
    // simply malformed. It is not evidence that an INDEX is screened out -- see the test below.
    #[rstest]
    fn test_an_unroutable_exchange_is_refused() {
        let request = NautilusOrderRequest {
            instrument_id: InstrumentId::from("NIFTY50.INDICES"),
            ..view(OrderType::Market, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular);

        assert!(request.is_err(), "INDICES is not an order-routing exchange");
    }

    // ⭐ THE ENCODER DOES NOT SCREEN INDICES, AND MUST NOT. An index does not live on its own
    // exchange -- `NIFTY 50` is exchange=NSE, segment=INDICES, among 10,036 tradable NSE rows --
    // so nothing in an InstrumentId can distinguish it and this pure function cannot refuse it.
    //
    // That is the correct layering, not a gap: `to_place_request` stays cache-free so the whole
    // Nautilus-to-Zerodha mapping is testable without constructing a cache. The screen lives one
    // level up in `submit_order`, which reads `InstrumentAny::IndexInstrument` back out of the
    // cache. This test pins the boundary: if someone later moves the screen down here, they will
    // have made the mapping untestable in isolation and this is where they find out.
    #[rstest]
    fn test_the_pure_encoder_does_not_screen_indices() {
        let request = NautilusOrderRequest {
            instrument_id: InstrumentId::from("NIFTY50.NSE"),
            ..view(OrderType::Market, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Cnc, ZerodhaVariety::Regular)
        .expect("an instrument id cannot reveal that this is an index");

        assert_eq!(request.exchange, ZerodhaExchange::Nse);
        assert_eq!(
            request.tradingsymbol, "NIFTY50",
            "the refusal is submit_order's job, from the cached instrument definition",
        );
    }

    // NCO was refused until 2026-08-14 because it is absent from the vendor's EXCHANGE_* constants.
    // The dump says it carries 28,067 orderable instruments, so that refusal denied a whole
    // commodity-derivative segment. Asserted at the ORDER-BUILDING boundary, not only at the enum,
    // because that is where the refusal actually bit.
    #[rstest]
    fn test_an_nco_commodity_future_can_be_ordered() {
        let request = NautilusOrderRequest {
            instrument_id: InstrumentId::from("ALUMINI26AUGFUT.NCO"),
            ..view(OrderType::Market, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular)
        .expect("NCO carries 147 dated futures and a 27,892-strong option chain");

        assert_eq!(request.exchange, ZerodhaExchange::Nco);
        assert_eq!(request.tradingsymbol, "ALUMINI26AUGFUT");
    }

    // THE DISCRIMINATING TEST FOR reduce_only. Zerodha has no such flag, so an implementation that
    // simply does not read the field passes every other test here -- and places an order that can
    // OPEN a position the strategy meant only to close.
    #[rstest]
    fn test_a_reduce_only_order_errors_rather_than_dropping_the_flag() {
        let request = NautilusOrderRequest {
            reduce_only: true,
            ..view(OrderType::Market, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular);

        let error = match request {
            Ok(_) => panic!("a reduce_only order must not be placed without the flag"),
            Err(e) => e.to_string(),
        };

        assert!(error.contains("reduce_only"), "{error}");
    }

    #[rstest]
    fn test_a_post_only_order_errors_rather_than_dropping_the_flag() {
        let request = NautilusOrderRequest {
            post_only: true,
            price: Some(Price::from("123.45")),
            ..view(OrderType::Limit, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular);

        assert!(
            request.is_err(),
            "dropping post_only lets the order cross the spread and take liquidity",
        );
    }

    // An iceberg's whole purpose is not showing the size. Dropping `display_qty` shows all of it.
    #[rstest]
    fn test_an_iceberg_display_quantity_errors_rather_than_showing_the_full_size() {
        let request = NautilusOrderRequest {
            display_qty: Some(Quantity::from("10")),
            price: Some(Price::from("123.45")),
            ..view(OrderType::Limit, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular);

        assert!(request.is_err());
    }

    #[rstest]
    fn test_a_gtc_order_is_refused() {
        let request = view(OrderType::Limit, TimeInForce::Gtc);
        let request = NautilusOrderRequest {
            price: Some(Price::from("123.45")),
            ..request
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular);

        assert!(
            request.is_err(),
            "GTC would silently become a DAY order and be cancelled at the close",
        );
    }

    #[rstest]
    fn test_a_stop_limit_order_carries_both_prices() {
        let request = NautilusOrderRequest {
            price: Some(Price::from("123.45")),
            trigger_price: Some(Price::from("120.00")),
            ..view(OrderType::StopLimit, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Mis, ZerodhaVariety::Regular)
        .expect("a stop limit is representable");

        assert_eq!(request.order_type, ZerodhaOrderType::Sl);
        assert_eq!(request.price.as_deref(), Some("123.45"));
        assert_eq!(request.trigger_price.as_deref(), Some("120.00"));
    }

    #[rstest]
    fn test_a_stop_market_order_carries_only_the_trigger() {
        let request = NautilusOrderRequest {
            trigger_price: Some(Price::from("120.00")),
            ..view(OrderType::StopMarket, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Mis, ZerodhaVariety::Regular)
        .expect("a stop market is representable");

        assert_eq!(request.order_type, ZerodhaOrderType::Slm);
        assert_eq!(request.price, None);
        assert_eq!(request.trigger_price.as_deref(), Some("120.00"));
    }

    // The price string comes from `Price`, which already knows the instrument's precision. Deriving
    // it from an `f64` here would reintroduce the formatting problem `http::parse` solved by
    // counting decimals from `tick_size` -- 0.0025 does not survive a naive round trip.
    #[rstest]
    fn test_the_price_string_preserves_the_precision_of_the_price() {
        let request = NautilusOrderRequest {
            instrument_id: InstrumentId::from("USDINR24AUGFUT.CDS"),
            price: Some(Price::from("83.4525")),
            ..view(OrderType::Limit, TimeInForce::Day)
        }
        .to_place_request(ZerodhaProduct::Nrml, ZerodhaVariety::Regular)
        .expect("a currency future limit order");

        assert_eq!(
            request.price.as_deref(),
            Some("83.4525"),
            "a four-decimal CDS price must not be rounded to two",
        );
    }

    #[rstest]
    #[case(ZerodhaVariety::Co)]
    #[case(ZerodhaVariety::Iceberg)]
    #[case(ZerodhaVariety::Auction)]
    fn test_a_variety_this_adapter_cannot_build_is_refused(#[case] variety: ZerodhaVariety) {
        let request =
            view(OrderType::Market, TimeInForce::Day).to_place_request(ZerodhaProduct::Nrml, variety);

        assert!(
            request.is_err(),
            "'{}' needs parameters the request type does not carry",
            variety.as_str(),
        );
    }

    #[rstest]
    fn test_an_after_market_order_uses_the_regular_parameter_set() {
        let request = view(OrderType::Market, TimeInForce::Day)
            .to_place_request(ZerodhaProduct::Cnc, ZerodhaVariety::Amo)
            .expect("amo takes the same parameters as regular");

        assert_eq!(request.variety, ZerodhaVariety::Amo);
    }

    #[rstest]
    #[case("1", 1)]
    #[case("65", 65)]
    #[case("1000", 1000)]
    fn test_whole_quantities_convert_to_units(#[case] raw: &str, #[case] expected: u64) {
        assert_eq!(
            quantity_to_units(Quantity::from(raw)).expect("whole"),
            expected
        );
    }

    // A fractional quantity means the caller computed something the venue cannot express. Rounding
    // it changes the size of a real position, in one direction or the other.
    #[rstest]
    fn test_a_fractional_quantity_errors_rather_than_rounding() {
        assert!(
            quantity_to_units(Quantity::from("1.5")).is_err(),
            "rounding a fractional quantity changes the size of a real position",
        );
    }

    #[rstest]
    fn test_a_zero_quantity_errors() {
        assert!(quantity_to_units(Quantity::from("0")).is_err());
    }

    // The tag is how the client order id would survive a restart. Kite caps it at 20 characters and
    // a Nautilus id is routinely longer, so it is OMITTED rather than truncated -- a truncated id is
    // a prefix, and a prefix matches other orders from the same strategy on the same day.
    #[rstest]
    fn test_a_long_client_order_id_yields_no_tag_rather_than_a_truncated_one() {
        let long = ClientOrderId::from("O-20260814-123456-001-001-1");
        assert!(long.to_string().len() > KITE_MAX_TAG_LEN);

        assert_eq!(
            tag_for(long),
            None,
            "a truncated tag is a prefix and would match the wrong order",
        );
    }

    #[rstest]
    fn test_a_short_client_order_id_is_carried_as_the_tag() {
        assert_eq!(tag_for(ClientOrderId::from("O-001")), Some("O-001".to_string()));
    }

    #[rstest]
    fn test_the_registry_resolves_both_directions() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", Some("240814000123456")));

        assert_eq!(
            registry
                .by_client_order_id(&ClientOrderId::from("O-001"))
                .and_then(|c| c.venue_order_id),
            Some(VenueOrderId::from("240814000123456")),
        );
        assert_eq!(
            registry.client_order_id_of(&VenueOrderId::from("240814000123456")),
            Some(ClientOrderId::from("O-001")),
        );
    }

    #[rstest]
    fn test_the_registry_links_a_venue_id_after_placement() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", None));

        assert!(registry.link_venue_order_id(
            ClientOrderId::from("O-001"),
            VenueOrderId::from("240814000123456"),
        ));
        assert_eq!(
            registry.client_order_id_of(&VenueOrderId::from("240814000123456")),
            Some(ClientOrderId::from("O-001")),
        );
    }

    #[rstest]
    fn test_linking_an_unknown_client_order_id_reports_failure() {
        let mut registry = ZerodhaOrderRegistry::new();

        assert!(
            !registry.link_venue_order_id(
                ClientOrderId::from("O-999"),
                VenueOrderId::from("240814000123456"),
            ),
            "a place-order response that outlives its registration must not create an entry",
        );
    }

    // The same failure the instrument registry documents: inserting into both maps without clearing
    // the superseded key leaves a REVERSE entry that still resolves, so a cancel addresses an order
    // that has been superseded and nothing reports a problem.
    #[rstest]
    fn test_relinking_a_venue_id_drops_the_stale_reverse_entry() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", Some("OLD-ID")));
        registry.link_venue_order_id(ClientOrderId::from("O-001"), VenueOrderId::from("NEW-ID"));

        assert_eq!(
            registry.client_order_id_of(&VenueOrderId::from("OLD-ID")),
            None,
            "the superseded venue id must stop resolving",
        );
        assert_eq!(
            registry.client_order_id_of(&VenueOrderId::from("NEW-ID")),
            Some(ClientOrderId::from("O-001")),
        );
    }

    #[rstest]
    fn test_removing_an_order_clears_both_directions() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", Some("240814000123456")));
        registry.remove(&ClientOrderId::from("O-001"));

        assert!(registry.is_empty());
        assert_eq!(
            registry.client_order_id_of(&VenueOrderId::from("240814000123456")),
            None,
        );
    }

    // `CancelAllOrders` spells "every side" as `NoOrderSide`. Treating it as a literal side would
    // match nothing and silently cancel none of the orders.
    #[rstest]
    fn test_no_order_side_means_every_side_not_no_orders() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", Some("V-1")));
        registry.register(ZerodhaOrderContext {
            order_side: OrderSide::Sell,
            ..context("O-002", Some("V-2"))
        });

        let instrument_id = InstrumentId::from(NIFTY_OPTION);
        assert_eq!(
            registry.tracked_for(instrument_id, OrderSide::NoOrderSide).len(),
            2,
            "an unfiltered cancel-all must reach both sides",
        );
        assert_eq!(registry.tracked_for(instrument_id, OrderSide::Buy).len(), 1);
        assert_eq!(registry.tracked_for(instrument_id, OrderSide::Sell).len(), 1);
    }

    #[rstest]
    fn test_tracked_for_does_not_cross_instruments() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-001", Some("V-1")));
        registry.register(ZerodhaOrderContext {
            instrument_id: InstrumentId::from("RELIANCE.NSE"),
            ..context("O-002", Some("V-2"))
        });

        assert_eq!(
            registry
                .tracked_for(InstrumentId::from(NIFTY_OPTION), OrderSide::NoOrderSide)
                .len(),
            1,
        );
    }

    // ⭐ THE REGRESSION TEST FOR THE DEFECT ITSELF.
    //
    // Nothing removes an order from the registry when it FILLS, so `tracked_for` keeps returning it
    // for the life of the process. Cancel-all used to fan out over exactly that set and fire cancels
    // at orders that had completed hours earlier — each refused by the venue, each raising a
    // spurious cancel-rejected, and a GENUINE cancel failure lost among them.
    //
    // The previous tests could not catch this: both check instrument and side filtering, and neither
    // ever registers a closed order. The filter they exercise was never the one that was missing.
    #[rstest]
    fn test_open_for_with_excludes_closed_orders() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-OPEN", Some("V-1")));
        registry.register(context("O-FILLED", Some("V-2")));

        let open = registry.open_for_with(
            InstrumentId::from(NIFTY_OPTION),
            OrderSide::NoOrderSide,
            |client_order_id| client_order_id.as_str() != "O-FILLED",
        );

        assert_eq!(open.len(), 1, "a filled order must not be sent a cancel");
        assert_eq!(open[0].client_order_id, ClientOrderId::from("O-OPEN"));
    }

    // The absent case defaults to OPEN on purpose, and it is the half most likely to be "tidied"
    // later: skipping an order the cache has not seen would leave a live position on while the
    // operator believes cancel-all closed everything. A needless cancel is refused harmlessly.
    #[rstest]
    fn test_open_for_with_treats_an_unknown_order_as_open() {
        let mut registry = ZerodhaOrderRegistry::new();
        registry.register(context("O-UNKNOWN", Some("V-1")));

        let open = registry.open_for_with(
            InstrumentId::from(NIFTY_OPTION),
            OrderSide::NoOrderSide,
            // Mirrors the real predicate's absent branch: not in the cache -> cancel it anyway.
            |_| true,
        );

        assert_eq!(open.len(), 1, "an order of unknown status must still be cancelled");
    }
}

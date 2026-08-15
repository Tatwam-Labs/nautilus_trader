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

//! The Zerodha Kite order-management REST surface.
//!
//! ⚠️ **Not one request in this file has ever been sent.** Every route, parameter name and response
//! shape below is read from the vendor client `kiteconnect` 5.2.0, not observed against the venue.
//! The credential this crate resolves trades a real account, so nothing here has been exercised
//! end to end and the tests cover only the encoding and the parsing.
//!
//! # Where each shape came from
//!
//! | this file | `kiteconnect` 5.2.0 `connect.py` |
//! |---|---|
//! | [`PlaceOrderRequest`] fields and their order | `place_order`, lines 339-370 |
//! | [`ModifyOrderRequest`] fields | `modify_order`, lines 412-436 |
//! | [`ZerodhaHttpClient::cancel_order`] | `cancel_order`, lines 438-442 |
//! | [`ZerodhaHttpClient::list_orders`] | `orders`, lines 465-467 |
//! | [`ZerodhaHttpClient::order_history`] | `order_history`, lines 469-475 |
//! | [`ZerodhaHttpClient::list_trades`] | `trades`, lines 477-484 |
//! | the routes | `_routes`, lines 111-169 |
//! | the enum strings | `connect.py:45-95`, via [`crate::common::enums`] |
//!
//! # ⭐ `variety` is a PATH segment, and getting it wrong is a 404
//!
//! `_routes` (`connect.py:119-126`) is explicit about this:
//!
//! ```text
//! "order.place":  "/orders/{variety}"
//! "order.modify": "/orders/{variety}/{order_id}"
//! "order.cancel": "/orders/{variety}/{order_id}"
//! "orders":       "/orders"
//! "order.info":   "/orders/{order_id}"
//! ```
//!
//! A wrong `variety` therefore does not produce a validation message naming the field — it produces
//! a 404 on a URL that looks plausible. Worse, it is the *cancel* path that fails: an order placed
//! as `regular` and cancelled as `amo` stays live at the venue while the cancel call reports a
//! transport-level failure. That is why the placed variety is recorded per order by the execution
//! client rather than recomputed when the cancel arrives.
//!
//! Note also that `/orders/{order_id}` (order history) and `/orders/{variety}/{order_id}` (modify,
//! cancel) differ by one path segment. They are easy to build from the same pieces and wrong.
//!
//! # ⭐ The body is FORM-ENCODED, not JSON
//!
//! `_request` (`connect.py:955-964`) passes `data=params` unless `is_json` is set, and neither
//! `place_order` nor `modify_order` sets it:
//!
//! ```text
//! json=params if (method in ["POST", "PUT"] and is_json) else None,
//! data=params if (method in ["POST", "PUT"] and not is_json) else None,
//! ```
//!
//! So the body is `application/x-www-form-urlencoded`, and this file sends that `Content-Type`
//! explicitly — `requests` sets it as a side effect of `data=`, and nothing does so here.
//!
//! `DELETE` is different again: `connect.py:951-952` moves `params` into the **query string** for
//! `GET` and `DELETE`, which is why `cancel_order`'s `parent_order_id` is a query parameter rather
//! than a body field.
//!
//! # ⭐ Absent parameters are OMITTED, not sent empty
//!
//! `place_order` and `modify_order` both do this before sending (`connect.py:364-366`):
//!
//! ```text
//! for k in list(params.keys()):
//!     if params[k] is None:
//!         del (params[k])
//! ```
//!
//! An omitted key and a key with an empty value are not the same request. `price=` on a `MARKET`
//! order is a parameter the venue must interpret; no `price` key at all is the vendor's own shape.
//! [`PlaceOrderRequest::to_form_pairs`] reproduces the deletion rather than the emptiness.
//!
//! # The envelope carries the failure, and the HTTP status alone does not
//!
//! `_request` (`connect.py:980-988`) treats a body with `status == "error"` **or** any
//! `error_type` as a failure regardless of the status code, and raises an exception named by
//! `error_type`. A caller that trusts a 200 will read `data` as `None` and report an empty order
//! book. `parse_envelope` checks the envelope first for the same reason.
//!
//! # Reconciling order state: what this file can and cannot do
//!
//! Zerodha pushes order updates by **postback** — an HTTP webhook to a URL registered on the Kite
//! developer console — and *not* on the tick socket, which carries market data only. This adapter
//! has no webhook server and does not plan one here, so the only route to order state is polling
//! [`ZerodhaHttpClient::list_orders`] and [`ZerodhaHttpClient::list_trades`]. **The REST route is
//! what is implemented; the postback route is not.** Polling is strictly weaker: it samples, so a
//! fill that opens and closes between two polls is visible only in its aggregate effect, and the
//! `average_price` on the order is an average rather than the sequence of fills.
//!
//! What the execution client does with these responses — building [`OrderStatusReport`]s and
//! [`FillReport`]s — is **not implemented**, and [`crate::execution`] says which methods return an
//! error rather than an empty list.
//!
//! [`OrderStatusReport`]: nautilus_model::reports::OrderStatusReport
//! [`FillReport`]: nautilus_model::reports::FillReport

use std::collections::HashMap;

use nautilus_network::http::Method;
use serde::{Deserialize, de::DeserializeOwned};

use crate::{
    common::enums::{
        ZerodhaExchange, ZerodhaOrderType, ZerodhaProduct, ZerodhaTransactionType, ZerodhaValidity,
        ZerodhaVariety,
    },
    http::client::ZerodhaHttpClient,
};

/// Request timeout for order operations, in seconds.
///
/// Deliberately much shorter than the client's default: that default is sized for the several
/// megabyte instrument dump, and waiting a minute on a cancel is not a useful behaviour.
const ORDER_TIMEOUT_SECS: u64 = 10;

/// The `Content-Type` the venue expects on `POST` and `PUT` order bodies.
const FORM_CONTENT_TYPE: &str = "application/x-www-form-urlencoded";

/// The longest `tag` Kite Connect v3 accepts on an order.
///
/// ⚠️ **Provenance is weaker here than elsewhere in this file.** The vendor client passes `tag`
/// straight through with no validation, so this limit comes from the Kite Connect v3 HTTP API
/// documentation rather than from source that can be read. It is enforced as a *refusal to set the
/// tag*, never as a truncation: a truncated tag is a plausible-looking identifier that matches the
/// wrong order.
pub const KITE_MAX_TAG_LEN: usize = 20;

/// Percent-encodes one form value.
///
/// Everything outside the unreserved set of RFC 3986 is escaped, and a space becomes `%20` rather
/// than `+`. Both decode to a space in an `application/x-www-form-urlencoded` body, and `%20` is
/// the one that also survives being read as a URL component.
///
/// This is hand-rolled because the crate has no percent-encoding dependency and adding one for
/// eleven parameter values would be the larger change. The character set is small and closed.
fn percent_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());

    // `bytes()` rather than `as_bytes()`: iterating by value gives `u8` directly, so the match
    // arms compare against byte literals without relying on match ergonomics to deref.
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(byte as char);
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }

    encoded
}

/// Encodes name/value pairs as an `application/x-www-form-urlencoded` body.
///
/// The pairs are emitted in the order given, which is the vendor client's parameter order. Order is
/// not semantically significant to the venue, but keeping it stable makes a captured request
/// diffable against `connect.py`.
fn encode_form(pairs: &[(&'static str, String)]) -> String {
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", percent_encode(key), percent_encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// The envelope every Kite JSON response is wrapped in.
///
/// `data` is optional because an error response carries `message` and `error_type` instead —
/// modelling `data` as required would turn "your token expired" into "the response did not parse".
/// ⚠️ THE EXPLICIT `bound` IS LOAD-BEARING — without it this does not compile.
///
/// `#[serde(default)]` on a field whose type mentions a type parameter makes serde's derive infer a
/// `T: Default` bound, so `KiteEnvelope<T>: Deserialize` silently came to require `T: Default` and
/// every call site failed with E0277 — even though `Option<T>: Default` holds for ALL `T` and no
/// payload type here has any use for `Default`. The bound was an artefact of serde's conservative,
/// syntactic inference, not of anything this type needs.
///
/// Stating the bound explicitly REPLACES the inferred set with the true requirement. Keeping
/// `#[serde(default)]` is then a no-op for the three `Option<String>` fields (serde already treats a
/// missing `Option` as `None`), which is why only the generic field ever bound.
///
/// Do not "simplify" this by deleting the `#[serde(default)]` on `data`. That also compiles, but it
/// leaves the next person to rediscover why the attribute could not be there — and it makes the
/// optionality of `data` implicit when the doc comment above says it is deliberate.
#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
struct KiteEnvelope<T> {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    data: Option<T>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error_type: Option<String>,
}

/// Why an order request failed — and, crucially, **whether the venue may still have the order**.
///
/// # ⭐ A TIMEOUT IS NOT A REJECTION, AND CONFLATING THEM LOSES MONEY
///
/// Before this type existed, every failure — a venue refusal, a connection reset, a ten-second
/// timeout — collapsed into one `anyhow::Error`, and the caller emitted `OrderRejected` for all of
/// them. `OrderRejected` asserts that **the venue refused and no order exists**. For a timeout that
/// is not a weaker claim, it is a *false* one, and it fails in the expensive direction:
///
///   1. the engine believes there is no order, so the position is invisible to it
///   2. the client drops its registry entry, so it can no longer cancel the order it just placed
///   3. a strategy that resubmits on rejection now holds **double the intended size**
///
/// Zerodha at 09:15 answering in eleven seconds against a ten-second timeout is an ordinary
/// Tuesday, not a pathological case.
///
/// # The split is made where the information exists
///
/// This is classified at the point of failure — transport layer versus parsed envelope — rather
/// than recovered later by matching on an error string. A string match would be a second guess at
/// something already known once.
#[derive(Debug, thiserror::Error)]
pub enum OrderRequestError {
    /// **The venue answered and refused.** No order exists. Safe to report as rejected.
    #[error("{0}")]
    Refused(String),

    /// **No usable answer. THE ORDER MAY EXIST AT THE VENUE.**
    ///
    /// Covers every transport failure (timeout, reset, DNS), a body that is not readable JSON, and
    /// a success envelope carrying no `data`. That last one matters most on placement: the order
    /// was very likely accepted and we simply do not know its id, so it can never be addressed.
    ///
    /// Deliberately conservative: a connection *refused* is genuinely determinate, but
    /// distinguishing it from a timeout needs error-kind detail the transport does not expose.
    /// Treating all of them as indeterminate never invents a rejection; the cost is that a
    /// definitely-unsent order is investigated rather than auto-retried, which is the safe
    /// direction to be wrong in.
    #[error("{0}")]
    Indeterminate(String),
}

impl OrderRequestError {
    /// Whether the venue may still be holding this order.
    #[must_use]
    pub const fn may_have_reached_venue(&self) -> bool {
        matches!(self, Self::Indeterminate(_))
    }
}

/// Unwraps a Kite JSON envelope into its `data` payload.
///
/// # The envelope is checked BEFORE the status code, deliberately
///
/// `connect.py:981` raises on `status == "error"` **or** a present `error_type`, whatever the HTTP
/// code was. Trusting the code first turns an authentication failure into a parse failure, or —
/// worse, on `/orders` — into an empty order book, which reads as "you have no working orders".
///
/// **Confirmed by running the vendor, not only by reading it.**
/// `test_data/probe_vendor_response_handling.py` feeds `kiteconnect` 5.2.0 an error envelope on an
/// HTTP **200** and it raises `TokenException` just as it does on a 403; an envelope carrying only
/// `error_type`, with no `status` field at all, raises `GeneralException`. Both branches of the
/// condition below are therefore the vendor's own behaviour rather than defensive embellishment.
///
/// # Errors
///
/// Returns an error if the body is not JSON, if the envelope reports a failure, or if a successful
/// envelope carries no `data`.
fn parse_envelope<T: DeserializeOwned>(
    status_code: u16,
    body: &[u8],
) -> Result<T, OrderRequestError> {
    // ⚠️ INDETERMINATE, not Refused. An unreadable body means the venue answered with SOMETHING we
    // could not parse -- on a placement that is entirely consistent with the order having been
    // accepted. Calling it a refusal here would be inventing a fact about the venue's state.
    let envelope: KiteEnvelope<T> = serde_json::from_slice(body).map_err(|e| {
        // The body is NOT included. It is small and non-secret for order routes, but this helper is
        // generic over every Kite response and the habit is the thing being kept.
        OrderRequestError::Indeterminate(format!(
            "Zerodha response was not a JSON envelope (HTTP {status_code}, {} bytes): {e}",
            body.len(),
        ))
    })?;

    if envelope.error_type.is_some() || envelope.status.as_deref() == Some("error") {
        let error_type = envelope.error_type.unwrap_or_else(|| "unknown".to_string());
        let message = envelope
            .message
            .unwrap_or_else(|| "no message".to_string());

        // ⭐ THE ONE FAILURE THIS ADAPTER WILL PRODUCE MOST OFTEN, AND THE ONE MOST LIKELY TO SEND
        // SOMEONE TO THE WRONG SUBSYSTEM.
        //
        // The access token is a SESSION token flushed daily between roughly 05:00 and 07:30 IST.
        // So a system that worked perfectly at yesterday's close fails on every route the next
        // morning, with nothing having changed in the code. "Zerodha rejected the request
        // (TokenException)" alone reads as an adapter or a network fault at exactly the moment it
        // is neither, and the operator goes looking at the one part that is behaving correctly.
        //
        // Naming the flush window converts an investigation into a credential refresh. The remedy
        // is deliberately NOT stated as a code change, because there is none to make.
        if error_type == "TokenException" {
            return Err(OrderRequestError::Refused(format!(
                "Zerodha rejected the request as unauthenticated (HTTP {status_code}, \
                 {error_type}): {message}. This is almost always an EXPIRED SESSION TOKEN rather \
                 than a fault in this adapter: Zerodha flushes access tokens daily between roughly \
                 05:00 and 07:30 IST, so a process that ran yesterday fails here the next morning \
                 with no code change. Refresh the token; do not debug the transport"
            )));
        }

        return Err(OrderRequestError::Refused(format!(
            "Zerodha rejected the request (HTTP {status_code}, {error_type}): {message}"
        )));
    }

    // ⚠️ INDETERMINATE on placement specifically: a success envelope means the venue accepted, and
    // a missing `data` means we never learned the order id -- so the order very likely EXISTS and
    // can never be addressed by this client. That is the worst state to mislabel as a rejection.
    envelope.data.ok_or_else(|| {
        OrderRequestError::Indeterminate(format!(
            "Zerodha returned a success envelope with no `data` (HTTP {status_code}); \
             treating that as an empty result would report a venue with no orders"
        ))
    })
}

/// A request to place an order.
///
/// Field order follows `place_order` (`connect.py:339-356`) so the two can be read side by side.
///
/// # Prices are `String`, not `f64`, and that is load-bearing
///
/// A [`Price`] already knows its instrument's precision and renders it exactly; `0.05` does not
/// survive a round trip through `f64` formatting without a decision about how many places to print.
/// Making that decision here would re-introduce the problem `http::parse` solved by counting
/// precision from `tick_size` — so the caller passes the string the `Price` already produces.
///
/// [`Price`]: nautilus_model::types::Price
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlaceOrderRequest {
    /// The order variety, which is also the URL path segment.
    pub variety: ZerodhaVariety,
    /// The exchange to route to.
    pub exchange: ZerodhaExchange,
    /// The venue's trading symbol, e.g. `NIFTY24AUG24000CE`.
    pub tradingsymbol: String,
    /// Buy or sell.
    pub transaction_type: ZerodhaTransactionType,
    /// The quantity, in units (not lots).
    pub quantity: u64,
    /// The margin and square-off regime.
    pub product: ZerodhaProduct,
    /// The order type.
    pub order_type: ZerodhaOrderType,
    /// The limit price, already formatted to the instrument's precision.
    pub price: Option<String>,
    /// The trigger price for `SL` and `SL-M`, already formatted.
    pub trigger_price: Option<String>,
    /// The validity (Zerodha's name for time in force).
    pub validity: Option<ZerodhaValidity>,
    /// The publicly disclosed quantity.
    pub disclosed_quantity: Option<u64>,
    /// A free-form tag echoed back on the order, used here to carry the client order id.
    pub tag: Option<String>,
}

impl PlaceOrderRequest {
    /// Checks the request against the order type's own requirements.
    ///
    /// The venue enforces these too, but its rejection arrives as a message on a request that has
    /// already been sent. Failing locally keeps a malformed order off the wire entirely.
    ///
    /// # Errors
    ///
    /// Returns an error if a required price is absent, if a price is supplied where the order type
    /// has no use for one, if the quantity is zero, or if the tag exceeds [`KITE_MAX_TAG_LEN`].
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.quantity == 0 {
            anyhow::bail!("Zerodha order quantity must be greater than zero");
        }

        if self.order_type.requires_price() && self.price.is_none() {
            anyhow::bail!(
                "a Zerodha {} order requires a price",
                self.order_type.as_str(),
            );
        }

        if self.order_type.requires_trigger_price() && self.trigger_price.is_none() {
            anyhow::bail!(
                "a Zerodha {} order requires a trigger_price",
                self.order_type.as_str(),
            );
        }

        // A price on a MARKET order is not harmless: it is a parameter the venue is free to
        // interpret, and a caller who set it believes it is being honoured.
        if !self.order_type.requires_price() && self.price.is_some() {
            anyhow::bail!(
                "a Zerodha {} order takes no price, but one was supplied; sending it would let \
                 the caller believe a limit was applied",
                self.order_type.as_str(),
            );
        }

        if !self.order_type.requires_trigger_price() && self.trigger_price.is_some() {
            anyhow::bail!(
                "a Zerodha {} order takes no trigger_price, but one was supplied",
                self.order_type.as_str(),
            );
        }

        if let Some(tag) = &self.tag
            && tag.len() > KITE_MAX_TAG_LEN
        {
            anyhow::bail!(
                "the Zerodha order tag is {} characters, over the {KITE_MAX_TAG_LEN} limit; \
                 truncating it would produce an identifier that matches the wrong order",
                tag.len(),
            );
        }

        Ok(())
    }

    /// Renders the request as form parameters, omitting every absent optional.
    ///
    /// The omission mirrors `connect.py:364-366`, which deletes `None` keys before sending. An
    /// empty value is not the same request as an absent key.
    ///
    /// Note that `variety` appears here **as well as** in the path: the vendor client builds its
    /// parameter dict from `locals()` (`connect.py:361`), so `variety` is in the body too.
    #[must_use]
    pub fn to_form_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs: Vec<(&'static str, String)> = vec![
            ("variety", self.variety.as_str().to_string()),
            ("exchange", self.exchange.as_str().to_string()),
            ("tradingsymbol", self.tradingsymbol.clone()),
            (
                "transaction_type",
                self.transaction_type.as_str().to_string(),
            ),
            ("quantity", self.quantity.to_string()),
            ("product", self.product.as_str().to_string()),
            ("order_type", self.order_type.as_str().to_string()),
        ];

        if let Some(price) = &self.price {
            pairs.push(("price", price.clone()));
        }

        if let Some(validity) = self.validity {
            pairs.push(("validity", validity.as_str().to_string()));
        }

        if let Some(disclosed_quantity) = self.disclosed_quantity {
            pairs.push(("disclosed_quantity", disclosed_quantity.to_string()));
        }

        if let Some(trigger_price) = &self.trigger_price {
            pairs.push(("trigger_price", trigger_price.clone()));
        }

        if let Some(tag) = &self.tag {
            pairs.push(("tag", tag.clone()));
        }

        pairs
    }
}

/// A request to modify a working order.
///
/// Field set from `modify_order` (`connect.py:412-422`). Every field except the variety and the
/// order id is optional, and an absent field means "leave it alone".
///
/// `parent_order_id` is in the vendor signature and is absent here: it applies to cover-order legs,
/// and this adapter does not place cover orders.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModifyOrderRequest {
    /// The variety the order was PLACED under. See the module docs: this is a path segment.
    pub variety: ZerodhaVariety,
    /// The venue's order id.
    pub order_id: String,
    /// The new quantity.
    pub quantity: Option<u64>,
    /// The new limit price, already formatted to the instrument's precision.
    pub price: Option<String>,
    /// A new order type.
    pub order_type: Option<ZerodhaOrderType>,
    /// The new trigger price, already formatted.
    pub trigger_price: Option<String>,
    /// A new validity.
    pub validity: Option<ZerodhaValidity>,
    /// The new publicly disclosed quantity.
    pub disclosed_quantity: Option<u64>,
}

impl ModifyOrderRequest {
    /// Checks that the request would change something.
    ///
    /// # Errors
    ///
    /// Returns an error if every optional field is absent. Such a request is accepted by the venue
    /// and changes nothing, which is indistinguishable from a modification that was applied.
    pub fn validate(&self) -> anyhow::Result<()> {
        let changes_nothing = self.quantity.is_none()
            && self.price.is_none()
            && self.order_type.is_none()
            && self.trigger_price.is_none()
            && self.validity.is_none()
            && self.disclosed_quantity.is_none();

        if changes_nothing {
            anyhow::bail!(
                "a Zerodha modify request with no changed field is accepted by the venue and \
                 alters nothing, which cannot be told apart from a modification that was applied"
            );
        }

        Ok(())
    }

    /// Renders the request as form parameters, omitting every absent optional.
    #[must_use]
    pub fn to_form_pairs(&self) -> Vec<(&'static str, String)> {
        let mut pairs: Vec<(&'static str, String)> = vec![
            ("variety", self.variety.as_str().to_string()),
            ("order_id", self.order_id.clone()),
        ];

        if let Some(quantity) = self.quantity {
            pairs.push(("quantity", quantity.to_string()));
        }

        if let Some(price) = &self.price {
            pairs.push(("price", price.clone()));
        }

        if let Some(order_type) = self.order_type {
            pairs.push(("order_type", order_type.as_str().to_string()));
        }

        if let Some(trigger_price) = &self.trigger_price {
            pairs.push(("trigger_price", trigger_price.clone()));
        }

        if let Some(validity) = self.validity {
            pairs.push(("validity", validity.as_str().to_string()));
        }

        if let Some(disclosed_quantity) = self.disclosed_quantity {
            pairs.push(("disclosed_quantity", disclosed_quantity.to_string()));
        }

        pairs
    }
}

/// The payload every order mutation returns.
///
/// `place_order`, `modify_order` and `cancel_order` all read `["order_id"]` off the unwrapped data
/// (`connect.py:370`, `436`, `442`), so the three share one response shape.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct OrderIdResponse {
    /// The venue's order id, which is a string and **not** numeric despite looking it.
    pub order_id: String,
}

/// One entry of the order book as the venue reports it.
///
/// # ⚠️ THE REQUIRED/OPTIONAL SPLIT BELOW HAS NO VENDOR BACKING WHATSOEVER
///
/// This was checked rather than assumed, and the answer was negative:
/// `test_data/probe_vendor_response_handling.py` feeds `kiteconnect` 5.2.0 a **two-field** order
/// row — `order_id` and `status`, nothing else — and the vendor returns it intact without
/// complaint. `orders()` hands back raw dicts and validates **nothing**.
///
/// So there is no oracle for which of these fields the venue always sends. Every `#[serde(default)]`
/// below, and every field left without one, is this author's judgement. Only a live order-book
/// capture can settle it, and that capture has not been made.
///
/// The consequence is concrete and worth stating before someone meets it at 09:15: because
/// `serde_json` deserialises `Vec<KiteOrder>` **atomically**, a single row missing one of the
/// required fields fails the **entire poll**, not just that row. Every other order becomes
/// invisible.
///
/// # That atomicity is INCONSISTENT with this crate's instrument parser, deliberately
///
/// [`crate::http::parse::parse_instruments`] does the opposite: it skips unparseable rows and
/// returns a count, because one malformed row out of ~114,000 must not deny the system every other
/// instrument. The opposite choice is made here on purpose. An instrument that fails to parse is
/// one you cannot trade; an **order** that fails to parse is one you cannot SEE, and silently
/// dropping it from a reconciliation poll is how a live position becomes invisible. Loud is right
/// here and quiet is right there — but the asymmetry is a decision, not an oversight, and it should
/// be revisited with real data rather than inherited.
///
/// # Timestamps are strings here, and the vendor's own behaviour argues for that
///
/// They are `YYYY-MM-DD HH:MM:SS` in IST with no offset in the text. The same probe confirms two
/// things by execution rather than by reading:
///
/// - `_format_response` parses these fields **only when the string is exactly 19 characters**
///   (`connect.py:459`). A value carrying milliseconds — 23 characters — is left as a `str`, so the
///   vendor client would silently hand a caller a string where it expects a datetime the day
///   Zerodha adds sub-second precision. Keeping `Option<String>` absorbs that without a schema
///   change.
/// - The datetime the vendor does produce has **`tzinfo=None`**. The payload carries no zone, so
///   "IST" is knowledge from outside the response, not something in it. That is exactly why
///   [`crate::execution`] refuses to synthesise `ts_event` rather than defaulting it to now.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KiteOrder {
    /// The venue's order id.
    pub order_id: String,
    /// The parent order id, for cover-order and bracket legs.
    #[serde(default)]
    pub parent_order_id: Option<String>,
    /// The lifecycle status, e.g. `OPEN`, `COMPLETE`, `TRIGGER PENDING`.
    pub status: String,
    /// The venue's explanation, populated on rejection.
    #[serde(default)]
    pub status_message: Option<String>,
    /// The variety the order was placed under.
    #[serde(default)]
    pub variety: Option<String>,
    /// The exchange.
    pub exchange: String,
    /// The venue's trading symbol.
    pub tradingsymbol: String,
    /// The streaming instrument token.
    #[serde(default)]
    pub instrument_token: u32,
    /// The order type.
    pub order_type: String,
    /// Buy or sell.
    pub transaction_type: String,
    /// The validity.
    #[serde(default)]
    pub validity: Option<String>,
    /// The margin product.
    #[serde(default)]
    pub product: Option<String>,
    /// The ordered quantity.
    pub quantity: u64,
    /// The publicly disclosed quantity.
    #[serde(default)]
    pub disclosed_quantity: u64,
    /// The limit price.
    #[serde(default)]
    pub price: f64,
    /// The trigger price.
    #[serde(default)]
    pub trigger_price: f64,
    /// The average fill price across all fills of this order.
    #[serde(default)]
    pub average_price: f64,
    /// The executed quantity.
    #[serde(default)]
    pub filled_quantity: u64,
    /// The quantity still working.
    #[serde(default)]
    pub pending_quantity: u64,
    /// The quantity cancelled.
    #[serde(default)]
    pub cancelled_quantity: u64,
    /// The venue's own order timestamp, `YYYY-MM-DD HH:MM:SS` in IST.
    #[serde(default)]
    pub order_timestamp: Option<String>,
    /// The exchange's timestamp, absent until the exchange has seen the order.
    #[serde(default)]
    pub exchange_timestamp: Option<String>,
    /// The tag supplied at placement.
    #[serde(default)]
    pub tag: Option<String>,
}

impl KiteOrder {
    /// Returns whether any quantity of this order has executed.
    ///
    /// This is what separates a resting `OPEN` order from a partially filled one — Zerodha has no
    /// `PARTIALLY FILLED` status, so the status string alone cannot tell them apart. See
    /// [`crate::common::enums::ZerodhaOrderStatus::to_order_status`].
    #[must_use]
    pub const fn has_fills(&self) -> bool {
        self.filled_quantity > 0
    }
}

/// One executed trade.
///
/// An order fills in tranches, and each tranche is a separate trade under the same `order_id`
/// (`connect.py:480-482`).
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct KiteTrade {
    /// The venue's trade id.
    pub trade_id: String,
    /// The order this trade belongs to.
    pub order_id: String,
    /// The exchange's own order id.
    #[serde(default)]
    pub exchange_order_id: Option<String>,
    /// The venue's trading symbol.
    pub tradingsymbol: String,
    /// The exchange.
    pub exchange: String,
    /// Buy or sell.
    pub transaction_type: String,
    /// The margin product.
    #[serde(default)]
    pub product: Option<String>,
    /// The price this tranche executed at.
    #[serde(default)]
    pub average_price: f64,
    /// The quantity of this tranche.
    ///
    /// `f64` rather than an integer: equity and F&O quantities are whole, but the field is a JSON
    /// number and a non-integral value on some segment would otherwise fail the whole parse.
    #[serde(default)]
    pub quantity: f64,
    /// When the fill was recorded, `YYYY-MM-DD HH:MM:SS` in IST.
    #[serde(default)]
    pub fill_timestamp: Option<String>,
    /// The exchange's timestamp for the fill.
    #[serde(default)]
    pub exchange_timestamp: Option<String>,
}

impl ZerodhaHttpClient {
    /// Builds the headers for a form-encoded body.
    ///
    /// The authorisation header is built per call by `auth_headers` and is not retained; this only
    /// adds the content type on top.
    fn form_headers(&self) -> HashMap<String, String> {
        let mut headers = self.auth_headers();
        headers.insert("Content-Type".to_string(), FORM_CONTENT_TYPE.to_string());
        headers
    }

    /// Places an order.
    ///
    /// ⚠️ **This sends a real order to a real account.** There is no test mode on this route.
    ///
    /// Returns the venue's order id, which is a **string**: it looks numeric and is not one, and
    /// parsing it to an integer loses leading zeros the venue is entitled to send.
    ///
    /// From `place_order` (`connect.py:339-370`), which posts to `/orders/{variety}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails local validation, if the transport fails, or if the
    /// venue's envelope reports a failure.
    pub async fn place_order(
        &self,
        request: &PlaceOrderRequest,
    ) -> Result<String, OrderRequestError> {
        // Refused, not Indeterminate: this fails BEFORE anything is sent, so the venue provably
        // does not have the order.
        request
            .validate()
            .map_err(|e| OrderRequestError::Refused(e.to_string()))?;

        let url = format!("{}/orders/{}", self.base_url(), request.variety.as_str());
        let body = encode_form(&request.to_form_pairs());

        let response = self
            .transport()
            .request(
                Method::POST,
                url,
                None,
                Some(self.form_headers()),
                Some(body.into_bytes()),
                Some(ORDER_TIMEOUT_SECS),
                None,
            )
            .await
            // ⚠️ INDETERMINATE. A transport failure here is exactly the case that used to be
            // reported as a rejection: the request may well have been accepted and the answer lost.
            .map_err(|e| {
                OrderRequestError::Indeterminate(format!(
                    "Zerodha place_order got no usable answer, so THE ORDER MAY EXIST at the \
                     venue and this client cannot address it: {e}"
                ))
            })?;

        // `as_u16` rather than `{}` on the status: `HttpStatus` derives only `Clone` and `Debug`,
        // so it has no `Display` and formatting it directly does not compile.
        let payload: OrderIdResponse =
            parse_envelope(response.status.as_u16(), &response.body)?;

        Ok(payload.order_id)
    }

    /// Modifies a working order.
    ///
    /// `variety` must be the one the order was **placed** under: it is a path segment, so a
    /// mismatch is a 404 rather than a validation error.
    ///
    /// From `modify_order` (`connect.py:412-436`), a `PUT` to `/orders/{variety}/{order_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request changes nothing, if the transport fails, or if the venue's
    /// envelope reports a failure.
    pub async fn modify_order(
        &self,
        request: &ModifyOrderRequest,
    ) -> Result<String, OrderRequestError> {
        // Refused, not Indeterminate: nothing has been sent, so the venue provably did not act.
        request
            .validate()
            .map_err(|e| OrderRequestError::Refused(e.to_string()))?;

        let url = format!(
            "{}/orders/{}/{}",
            self.base_url(),
            request.variety.as_str(),
            request.order_id,
        );
        let body = encode_form(&request.to_form_pairs());

        let response = self
            .transport()
            .request(
                Method::PUT,
                url,
                None,
                Some(self.form_headers()),
                Some(body.into_bytes()),
                Some(ORDER_TIMEOUT_SECS),
                None,
            )
            .await
            // INDETERMINATE: the modify may have been applied and the answer lost.
            .map_err(|e| {
                OrderRequestError::Indeterminate(format!(
                    "Zerodha modify_order got no usable answer, so THE MODIFY MAY HAVE BEEN \
                     APPLIED and this client cannot tell: {e}"
                ))
            })?;

        let payload: OrderIdResponse =
            parse_envelope(response.status.as_u16(), &response.body)?;

        Ok(payload.order_id)
    }

    /// Cancels a working order.
    ///
    /// `variety` must be the one the order was **placed** under, for the same reason as
    /// [`Self::modify_order`] — and here the consequence is worse, because a failed cancel leaves a
    /// live order at the venue.
    ///
    /// From `cancel_order` (`connect.py:438-442`), a `DELETE` to `/orders/{variety}/{order_id}`.
    /// `parent_order_id` travels as a **query parameter**, not a body field: `connect.py:951-952`
    /// moves `params` into the query string for `GET` and `DELETE`.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails or if the venue's envelope reports a failure.
    pub async fn cancel_order(
        &self,
        variety: ZerodhaVariety,
        order_id: &str,
        parent_order_id: Option<&str>,
    ) -> Result<String, OrderRequestError> {
        let url = format!(
            "{}/orders/{}/{order_id}",
            self.base_url(),
            variety.as_str(),
        );

        // Built only when present. The vendor client passes `parent_order_id=None` through to
        // `requests`, which drops `None` query values -- so an absent key is the shape on the wire.
        let params = parent_order_id.map(|parent| {
            let mut map: HashMap<String, Vec<String>> = HashMap::new();
            map.insert("parent_order_id".to_string(), vec![parent.to_string()]);
            map
        });

        let response = self
            .transport()
            .request(
                Method::DELETE,
                url,
                params.as_ref(),
                Some(self.auth_headers()),
                None,
                Some(ORDER_TIMEOUT_SECS),
                None,
            )
            .await
            // INDETERMINATE, and this is the dangerous direction for a cancel: the order may have
            // been cancelled OR may still be live. Reporting "cancel rejected" asserts it is live.
            .map_err(|e| {
                OrderRequestError::Indeterminate(format!(
                    "Zerodha cancel_order got no usable answer, so THE ORDER MAY OR MAY NOT still \
                     be live at the venue: {e}"
                ))
            })?;

        let payload: OrderIdResponse =
            parse_envelope(response.status.as_u16(), &response.body)?;

        Ok(payload.order_id)
    }

    /// Fetches the day's order book.
    ///
    /// From `orders` (`connect.py:465-467`), a `GET` on `/orders`. The book covers the current
    /// trading day only; there is no history route that spans days.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails or if the venue's envelope reports a failure.
    pub async fn list_orders(&self) -> anyhow::Result<Vec<KiteOrder>> {
        let rows: Vec<serde_json::Value> = self.get_json("/orders", "list_orders").await?;
        Ok(Self::parse_rows(rows, "list_orders"))
    }

    /// Parses order rows INDIVIDUALLY so one bad row cannot hide the whole book.
    ///
    /// # ⚠️ Why this is not a plain `Vec<KiteOrder>` deserialisation
    ///
    /// It used to be, and that made the poll ATOMIC: one row missing a field this crate happens to
    /// declare required failed the entire response, and every other working order became invisible
    /// at the same moment. For a reconciliation poll that is the worst possible failure — you lose
    /// sight of the orders you *can* parse in order to be strict about the one you cannot.
    ///
    /// And the required/optional split is JUDGEMENT, not vendor-backed. The vendor's own probe
    /// accepts a two-field order row without complaint and hands back raw dicts, so every
    /// `#[serde(default)]` in [`KiteOrder`] is this crate's guess about what Zerodha always sends.
    /// A guess that fails closed over the whole book is a guess with far too much leverage.
    ///
    /// **A skipped row is logged at ERROR, not warn, and the count is returned to the caller's log
    /// line.** An unparseable INSTRUMENT is one you cannot trade; an unparseable ORDER is a LIVE
    /// POSITION YOU CANNOT SEE. Both deserve to be loud, but only the second can lose money, so it
    /// must never be silent — the risk of per-row parsing is that it quietly degrades into a
    /// partial book that reads like a complete one.
    fn parse_rows<T: DeserializeOwned>(rows: Vec<serde_json::Value>, operation: &str) -> Vec<T> {
        let total = rows.len();
        let mut parsed = Vec::with_capacity(total);
        let mut skipped = 0usize;

        for row in rows {
            // Pulled out BEFORE the parse attempt so a failure can still name WHICH row it was.
            // "a row failed" is nearly useless; "order 240814000123456 failed" is actionable.
            // Trades carry `trade_id`, orders carry `order_id`; try both rather than assume.
            let row_id = row
                .get("order_id")
                .or_else(|| row.get("trade_id"))
                .and_then(|v| v.as_str())
                .unwrap_or("<no id>")
                .to_string();

            match serde_json::from_value::<T>(row) {
                Ok(order) => parsed.push(order),
                Err(e) => {
                    skipped += 1;
                    log::error!(
                        "Zerodha {operation}: order {order_id} could not be parsed and is                          INVISIBLE to this client -- it may be a live position: {e}"
                    );
                }
            }
        }

        if skipped > 0 {
            log::error!(
                "⚠️ Zerodha {operation} returned an INCOMPLETE order book: {} of {total} row(s)                  parsed, {skipped} skipped. Do NOT treat this as the full set of working orders.",
                parsed.len(),
            );
        }

        parsed
    }

    /// Fetches the state transitions of one order.
    ///
    /// From `order_history` (`connect.py:469-475`), a `GET` on `/orders/{order_id}`. Note the
    /// **absence** of a variety segment here: this path and the modify/cancel path differ by one
    /// segment and are built from the same pieces.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails or if the venue's envelope reports a failure.
    pub async fn order_history(&self, order_id: &str) -> anyhow::Result<Vec<KiteOrder>> {
        let rows: Vec<serde_json::Value> = self
            .get_json(&format!("/orders/{order_id}"), "order_history")
            .await?;
        Ok(Self::parse_rows(rows, "order_history"))
    }

    /// Fetches the day's executed trades.
    ///
    /// From `trades` (`connect.py:477-484`), a `GET` on `/trades`.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails or if the venue's envelope reports a failure.
    pub async fn list_trades(&self) -> anyhow::Result<Vec<KiteTrade>> {
        let rows: Vec<serde_json::Value> = self.get_json("/trades", "list_trades").await?;
        Ok(Self::parse_rows(rows, "list_trades"))
    }

    /// Issues an authenticated `GET` and unwraps the Kite envelope.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport fails or if the venue's envelope reports a failure.
    async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        operation: &str,
    ) -> anyhow::Result<T> {
        let url = format!("{}{path}", self.base_url());

        let response = self
            .transport()
            .request(
                Method::GET,
                url,
                None,
                Some(self.auth_headers()),
                None,
                Some(ORDER_TIMEOUT_SECS),
                None,
            )
            .await
            .map_err(|e| anyhow::anyhow!("Zerodha {operation} request failed: {e}"))?;

        // `Ok(..?)` rather than returning bare: this helper reports `anyhow`, and the `?` is what
        // performs the OrderRequestError -> anyhow conversion. Returning the value directly asks
        // the compiler to unify two different error types. The read paths do not need the
        // determinate/indeterminate distinction -- a failed poll is retried, not acted on.
        Ok(parse_envelope(response.status.as_u16(), &response.body)?)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn market_request() -> PlaceOrderRequest {
        PlaceOrderRequest {
            variety: ZerodhaVariety::Regular,
            exchange: ZerodhaExchange::Nfo,
            tradingsymbol: "NIFTY24AUG24000CE".to_string(),
            transaction_type: ZerodhaTransactionType::Buy,
            quantity: 65,
            product: ZerodhaProduct::Nrml,
            order_type: ZerodhaOrderType::Market,
            price: None,
            trigger_price: None,
            validity: Some(ZerodhaValidity::Day),
            disclosed_quantity: None,
            tag: None,
        }
    }

    fn limit_request() -> PlaceOrderRequest {
        PlaceOrderRequest {
            order_type: ZerodhaOrderType::Limit,
            price: Some("123.45".to_string()),
            ..market_request()
        }
    }

    #[rstest]
    #[case("RELIANCE", "RELIANCE")]
    #[case("NIFTY 50", "NIFTY%2050")]
    #[case("M&M", "M%26M")]
    #[case("A+B", "A%2BB")]
    #[case("a-b_c.d~e", "a-b_c.d~e")]
    fn test_form_values_are_percent_encoded(#[case] raw: &str, #[case] expected: &str) {
        assert_eq!(percent_encode(raw), expected);
    }

    // `+` decodes to a SPACE in a form body. A tradingsymbol containing a literal `+` that was
    // passed through unencoded would reach the venue as a different symbol -- and `M&M` is worse
    // still, because the `&` splits one parameter into two and everything after it shifts.
    #[rstest]
    fn test_an_ampersand_in_a_value_cannot_split_the_body() {
        let request = PlaceOrderRequest {
            tradingsymbol: "M&M".to_string(),
            ..market_request()
        };

        let body = encode_form(&request.to_form_pairs());

        assert!(body.contains("tradingsymbol=M%26M"));
        assert_eq!(
            body.matches('&').count(),
            request.to_form_pairs().len() - 1,
            "the only unencoded ampersands may be the separators between parameters",
        );
    }

    // THE DISCRIMINATING TEST FOR OMISSION. The vendor client DELETES absent keys
    // (connect.py:364-366); it does not send them empty. `price=` on a MARKET order is a parameter
    // the venue must interpret, and an implementation that emits every field unconditionally
    // produces a body that looks correct and is not the vendor's shape.
    #[rstest]
    fn test_absent_optionals_are_omitted_not_sent_empty() {
        let body = encode_form(&market_request().to_form_pairs());

        assert!(!body.contains("price"), "an absent price must not appear: {body}");
        assert!(
            !body.contains("trigger_price"),
            "an absent trigger_price must not appear: {body}",
        );
        assert!(!body.contains("tag"), "an absent tag must not appear: {body}");
        assert!(
            !body.contains("=&") && !body.ends_with('='),
            "no parameter may be sent with an empty value: {body}",
        );
    }

    #[rstest]
    fn test_a_limit_order_carries_its_price() {
        let body = encode_form(&limit_request().to_form_pairs());

        assert!(body.contains("order_type=LIMIT"), "{body}");
        assert!(body.contains("price=123.45"), "{body}");
    }

    // The variety is in the body AS WELL AS the path, because the vendor builds its parameter dict
    // from `locals()` (connect.py:361) and `variety` is one of them.
    #[rstest]
    fn test_variety_appears_in_the_body_as_well_as_the_path() {
        let body = encode_form(&market_request().to_form_pairs());

        assert!(body.contains("variety=regular"), "{body}");
    }

    #[rstest]
    fn test_a_market_order_with_a_price_is_rejected() {
        let request = PlaceOrderRequest {
            price: Some("100.00".to_string()),
            ..market_request()
        };

        let error = request
            .validate()
            .expect_err("a MARKET order takes no price")
            .to_string();

        assert!(error.contains("MARKET"), "{error}");
    }

    #[rstest]
    fn test_a_limit_order_without_a_price_is_rejected() {
        let request = PlaceOrderRequest {
            order_type: ZerodhaOrderType::Limit,
            price: None,
            ..market_request()
        };

        assert!(request.validate().is_err());
    }

    #[rstest]
    #[case(ZerodhaOrderType::Sl)]
    #[case(ZerodhaOrderType::Slm)]
    fn test_a_stop_order_without_a_trigger_price_is_rejected(
        #[case] order_type: ZerodhaOrderType,
    ) {
        let request = PlaceOrderRequest {
            order_type,
            price: if order_type.requires_price() {
                Some("100.00".to_string())
            } else {
                None
            },
            trigger_price: None,
            ..market_request()
        };

        let error = request
            .validate()
            .expect_err("a stop order requires a trigger")
            .to_string();

        assert!(error.contains("trigger_price"), "{error}");
    }

    #[rstest]
    fn test_a_zero_quantity_is_rejected() {
        let request = PlaceOrderRequest {
            quantity: 0,
            ..market_request()
        };

        assert!(request.validate().is_err());
    }

    // A tag is how a client order id would survive a restart. Truncating an over-long one produces
    // a prefix that can match a DIFFERENT order -- so the request is refused and the execution
    // client omits the tag instead, which loses the link honestly rather than forging one.
    #[rstest]
    fn test_an_over_long_tag_is_rejected_rather_than_truncated() {
        let request = PlaceOrderRequest {
            tag: Some("O-20260814-123456-001-001-1".to_string()),
            ..market_request()
        };

        let error = request
            .validate()
            .expect_err("the tag is over the limit")
            .to_string();

        assert!(error.contains("20"), "the error should name the limit; was: {error}");
    }

    #[rstest]
    fn test_a_tag_at_exactly_the_limit_is_accepted() {
        let tag = "X".repeat(KITE_MAX_TAG_LEN);
        let request = PlaceOrderRequest {
            tag: Some(tag),
            ..market_request()
        };

        assert!(request.validate().is_ok());
    }

    #[rstest]
    fn test_a_modify_that_changes_nothing_is_rejected() {
        let request = ModifyOrderRequest {
            variety: ZerodhaVariety::Regular,
            order_id: "240814000123456".to_string(),
            quantity: None,
            price: None,
            order_type: None,
            trigger_price: None,
            validity: None,
            disclosed_quantity: None,
        };

        assert!(
            request.validate().is_err(),
            "the venue accepts a no-op modify, which cannot be told apart from one that applied",
        );
    }

    #[rstest]
    fn test_a_modify_carries_only_the_changed_fields() {
        let request = ModifyOrderRequest {
            variety: ZerodhaVariety::Regular,
            order_id: "240814000123456".to_string(),
            quantity: None,
            price: Some("101.55".to_string()),
            order_type: None,
            trigger_price: None,
            validity: None,
            disclosed_quantity: None,
        };

        let body = encode_form(&request.to_form_pairs());

        assert!(body.contains("price=101.55"), "{body}");
        assert!(body.contains("order_id=240814000123456"), "{body}");
        assert!(!body.contains("quantity"), "an unchanged quantity must not be sent: {body}");
        assert!(!body.contains("validity"), "an unchanged validity must not be sent: {body}");
    }

    #[rstest]
    fn test_a_success_envelope_yields_its_data() {
        let body = br#"{"status":"success","data":{"order_id":"240814000123456"}}"#;

        let payload: OrderIdResponse = parse_envelope(200, body).expect("a success envelope");

        assert_eq!(payload.order_id, "240814000123456");
    }

    // The venue's order id LOOKS numeric. It is a string, and parsing it as an integer would drop
    // a leading zero the venue is entitled to send -- producing an id that addresses nothing.
    #[rstest]
    fn test_a_leading_zero_survives_the_order_id() {
        let body = br#"{"status":"success","data":{"order_id":"024081400012345"}}"#;

        let payload: OrderIdResponse = parse_envelope(200, body).expect("a success envelope");

        assert_eq!(payload.order_id, "024081400012345");
    }

    // THE DISCRIMINATING TEST FOR THE ENVELOPE. Kite answers an expired token with a JSON error
    // document; an implementation that checks `response.status.is_success()` first and only then
    // reads `data` reports an EMPTY ORDER BOOK on a 200-shaped error -- which reads as "you have no
    // working orders" at exactly the moment the session has died.
    #[rstest]
    fn test_an_error_envelope_is_an_error_even_on_a_success_status() {
        let body = br#"{"status":"error","message":"Incorrect `api_key` or `access_token`.","error_type":"TokenException"}"#;

        let error = match parse_envelope::<Vec<KiteOrder>>(200, body) {
            Ok(orders) => panic!("an error envelope must not parse as {} orders", orders.len()),
            Err(e) => e.to_string(),
        };

        assert!(error.contains("TokenException"), "{error}");
        assert!(error.contains("Incorrect"), "{error}");
    }

    // ⭐ THE MESSAGE THAT FIRES EVERY MORNING. The token is flushed daily around 05:00-07:30 IST,
    // so this is the failure an operator meets most often -- and it arrives on a system that
    // worked at yesterday's close with no code change, which reads as an adapter or network fault
    // at precisely the moment it is neither. A message naming the wrong subsystem closes the route
    // to the real one, so the flush window has to be IN the error, not in a doc comment nobody
    // reads at 07:00.
    #[rstest]
    fn test_a_token_exception_names_the_daily_flush_not_the_transport() {
        let body = br#"{"status":"error","message":"Incorrect `api_key` or `access_token`.","error_type":"TokenException"}"#;

        let error = parse_envelope::<Vec<KiteOrder>>(403, body)
            .expect_err("an expired token is a failure")
            .to_string();

        assert!(error.contains("EXPIRED SESSION TOKEN"), "{error}");
        assert!(
            error.contains("07:30"),
            "the error must name the flush window, which is the actionable part; was: {error}",
        );
        assert!(
            error.contains("do not debug the transport"),
            "the error must steer away from the subsystem that is behaving correctly; was: {error}",
        );
    }

    // The discriminating half: a NON-token error must NOT claim the token expired. Attaching the
    // credential story to every failure would be its own misdirection, and a message that cries
    // "expired token" at an InputException sends the operator to refresh a credential that is fine.
    #[rstest]
    fn test_a_non_token_error_does_not_blame_the_credential() {
        let body = br#"{"status":"error","message":"Invalid variety","error_type":"InputException"}"#;

        let error = parse_envelope::<Vec<KiteOrder>>(400, body)
            .expect_err("an input error is a failure")
            .to_string();

        assert!(error.contains("InputException"), "{error}");
        assert!(error.contains("Invalid variety"), "{error}");
        assert!(
            !error.contains("EXPIRED SESSION TOKEN"),
            "only a TokenException may blame the credential; was: {error}",
        );
    }

    #[rstest]
    fn test_a_success_envelope_with_no_data_is_an_error() {
        let body = br#"{"status":"success"}"#;

        assert!(
            parse_envelope::<Vec<KiteOrder>>(200, body).is_err(),
            "an absent `data` must not be reported as an empty order book",
        );
    }

    #[rstest]
    fn test_a_non_json_body_is_an_error_that_does_not_echo_it() {
        let body = b"<html><body>502 Bad Gateway</body></html>";

        let error = parse_envelope::<Vec<KiteOrder>>(502, body)
            .expect_err("HTML is not an envelope")
            .to_string();

        assert!(error.contains("502"), "{error}");
        assert!(
            !error.contains("Bad Gateway"),
            "the body must not be echoed into the error: {error}",
        );
    }

    // ⭐ THE REGRESSION TEST FOR "ONE BAD ROW HID THE WHOLE BOOK".
    //
    // Deserialisation used to be atomic over the response: a single row missing a field this crate
    // declares required failed the ENTIRE poll, and every working order became invisible at the
    // same moment. For a reconciliation poll that is the worst available failure — you lose sight
    // of the orders you CAN read in order to be strict about the one you cannot.
    //
    // The fixture deliberately contains a row that CANNOT parse (no `order_id`, no `status`), so
    // this test would have failed before the change rather than passing for a new reason.
    #[rstest]
    fn test_one_unparseable_row_does_not_hide_the_others() {
        let rows: Vec<serde_json::Value> = serde_json::from_str(
            r#"[
                {"order_id":"240814000000001","status":"OPEN","exchange":"NSE",
                 "tradingsymbol":"RELIANCE","order_type":"LIMIT","transaction_type":"BUY",
                 "quantity":10},
                {"garbage":"this row cannot become a KiteOrder"},
                {"order_id":"240814000000002","status":"COMPLETE","exchange":"NSE",
                 "tradingsymbol":"INFY","order_type":"MARKET","transaction_type":"SELL",
                 "quantity":5}
            ]"#,
        )
        .expect("fixture is valid json");

        let parsed: Vec<KiteOrder> = ZerodhaHttpClient::parse_rows(rows, "test");

        assert_eq!(
            parsed.len(),
            2,
            "the two good orders must survive a bad neighbour; atomically failing the poll would \
             make live positions invisible",
        );
        assert_eq!(parsed[0].order_id, "240814000000001");
        assert_eq!(
            parsed[1].order_id, "240814000000002",
            "the row AFTER the bad one must also survive -- a parser that stops at the first \
             failure is only marginally better than one that fails the batch",
        );
    }

    // An empty response is not an error and must not be confused with a skipped row.
    #[rstest]
    fn test_an_empty_order_book_yields_nothing_without_complaint() {
        let parsed: Vec<KiteOrder> = ZerodhaHttpClient::parse_rows(Vec::new(), "test");
        assert!(parsed.is_empty(), "no orders is a valid state, not a parse failure");
    }

    #[rstest]
    fn test_an_order_book_row_parses() {
        let body = br#"{"status":"success","data":[{
            "order_id":"240814000123456",
            "status":"OPEN",
            "exchange":"NFO",
            "tradingsymbol":"NIFTY24AUG24000CE",
            "order_type":"LIMIT",
            "transaction_type":"BUY",
            "variety":"regular",
            "validity":"DAY",
            "product":"NRML",
            "quantity":65,
            "filled_quantity":0,
            "pending_quantity":65,
            "price":123.45,
            "average_price":0,
            "order_timestamp":"2026-08-14 09:15:04",
            "tag":"abc"
        }]}"#;

        let orders: Vec<KiteOrder> = parse_envelope(200, body).expect("an order book");

        assert_eq!(orders.len(), 1);
        assert_eq!(orders[0].order_id, "240814000123456");
        assert_eq!(orders[0].tradingsymbol, "NIFTY24AUG24000CE");
        assert_eq!(orders[0].exchange, "NFO");
        assert!(!orders[0].has_fills());
    }

    // A rejected order carries no exchange timestamp and no tag. Modelling those as required would
    // fail the WHOLE poll on the first rejection in the book -- and a rejection is exactly the row
    // an operator most needs to see.
    #[rstest]
    fn test_a_rejected_order_with_missing_optional_fields_still_parses() {
        let body = br#"{"status":"success","data":[{
            "order_id":"240814000123457",
            "status":"REJECTED",
            "status_message":"Insufficient funds",
            "exchange":"NSE",
            "tradingsymbol":"RELIANCE",
            "order_type":"MARKET",
            "transaction_type":"SELL",
            "quantity":1
        }]}"#;

        let orders: Vec<KiteOrder> = parse_envelope(200, body).expect("a rejected order");

        assert_eq!(orders[0].status, "REJECTED");
        assert_eq!(orders[0].status_message.as_deref(), Some("Insufficient funds"));
        assert!(orders[0].exchange_timestamp.is_none());
        assert!(orders[0].tag.is_none());
        assert!(
            orders[0].price.abs() < f64::EPSILON,
            "an absent price defaults to zero rather than failing the row",
        );
    }

    #[rstest]
    fn test_a_partially_filled_order_reports_fills() {
        let body = br#"{"status":"success","data":[{
            "order_id":"240814000123458",
            "status":"OPEN",
            "exchange":"NSE",
            "tradingsymbol":"RELIANCE",
            "order_type":"LIMIT",
            "transaction_type":"BUY",
            "quantity":100,
            "filled_quantity":40,
            "pending_quantity":60,
            "price":1400.5,
            "average_price":1400.25
        }]}"#;

        let orders: Vec<KiteOrder> = parse_envelope(200, body).expect("an order book");

        assert!(
            orders[0].has_fills(),
            "the status is still OPEN; only filled_quantity shows the partial fill",
        );
    }

    #[rstest]
    fn test_a_trade_row_parses() {
        let body = br#"{"status":"success","data":[{
            "trade_id":"10000000",
            "order_id":"240814000123456",
            "exchange_order_id":"1300000001234567",
            "tradingsymbol":"NIFTY24AUG24000CE",
            "exchange":"NFO",
            "transaction_type":"BUY",
            "product":"NRML",
            "average_price":121.5,
            "quantity":65,
            "fill_timestamp":"2026-08-14 09:15:05"
        }]}"#;

        let trades: Vec<KiteTrade> = parse_envelope(200, body).expect("a trade book");

        assert_eq!(trades.len(), 1);
        assert_eq!(trades[0].trade_id, "10000000");
        assert_eq!(trades[0].order_id, "240814000123456");
        assert!((trades[0].quantity - 65.0).abs() < f64::EPSILON);
    }

    // An order book row missing an IDENTIFYING field is a schema change, not a sparse row, and it
    // must fail rather than yield a row of defaults that looks like a real order.
    #[rstest]
    fn test_an_order_row_without_an_order_id_fails_to_parse() {
        let body = br#"{"status":"success","data":[{
            "status":"OPEN",
            "exchange":"NSE",
            "tradingsymbol":"RELIANCE",
            "order_type":"LIMIT",
            "transaction_type":"BUY",
            "quantity":1
        }]}"#;

        assert!(
            parse_envelope::<Vec<KiteOrder>>(200, body).is_err(),
            "a row with no order_id addresses nothing and must not parse as a default",
        );
    }
}

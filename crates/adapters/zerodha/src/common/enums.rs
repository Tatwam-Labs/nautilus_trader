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

//! Enumerations for the Zerodha adapter.
//!
//! # The streaming enums derive their strings; the ORDER enums write theirs out by hand
//!
//! [`ZerodhaSegment`] and [`ZerodhaTickMode`] are internal vocabulary: nothing outside this crate
//! reads them, so a `strum` `serialize_all` is safe there. Everything below
//! [`ZerodhaTransactionType`] is **wire vocabulary sent to a live trading endpoint**, and one of the
//! values is `SL-M` — a string no `serialize_all` rule produces from a Rust identifier. Rather than
//! have one enum's strings come from a derive rule and its neighbour's from a per-variant override,
//! all of the order enums spell their values out in a single `as_str`, next to the vendor line the
//! value came from. A `MARKET` where `SL-M` was meant is not a compile error and not a 404; it is a
//! market order.
//!
//! They expose `as_str` rather than [`std::fmt::Display`]. That is not a style choice: this module
//! already imports `strum::Display` for the two derived enums above, and importing
//! `std::fmt::Display` alongside it is a name collision. `as_str` is also `const` and returns
//! `&'static str`, which is what building a form body wants.

use nautilus_model::enums::{OrderSide, OrderStatus, OrderType, TimeInForce};
use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumIter, EnumString};

/// A Zerodha exchange segment.
///
/// The segment is the low byte of the instrument token, so it is derivable from the token alone and
/// is never sent separately. The numeric values are Zerodha's, not ours.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    Hash,
    PartialEq,
    Eq,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[strum(ascii_case_insensitive)]
#[strum(serialize_all = "UPPERCASE")]
#[serde(rename_all = "UPPERCASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.zerodha",
        eq,
        eq_int,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.zerodha")
)]
pub enum ZerodhaSegment {
    /// NSE equity.
    Nse = 1,
    /// NSE futures and options.
    Nfo = 2,
    /// NSE currency derivatives.
    Cds = 3,
    /// BSE equity.
    Bse = 4,
    /// BSE futures and options.
    Bfo = 5,
    /// BSE currency derivatives.
    Bcd = 6,
    /// MCX commodity.
    Mcx = 7,
    /// MCX-SX.
    Mcxsx = 8,
    /// Indices — quoted but not tradable.
    Indices = 9,
    /// A segment code Zerodha has not documented.
    #[default]
    Unknown = 0,
}

impl ZerodhaSegment {
    /// Returns the segment encoded in the low byte of a Zerodha `instrument_token`.
    ///
    /// An undocumented code maps to [`ZerodhaSegment::Unknown`], which takes the default price
    /// divisor. Zerodha adds segments without notice, so an unknown code must not be an error.
    #[must_use]
    pub const fn from_instrument_token(token: u32) -> Self {
        match token & 0xff {
            1 => Self::Nse,
            2 => Self::Nfo,
            3 => Self::Cds,
            4 => Self::Bse,
            5 => Self::Bfo,
            6 => Self::Bcd,
            7 => Self::Mcx,
            8 => Self::Mcxsx,
            9 => Self::Indices,
            _ => Self::Unknown,
        }
    }

    /// Returns the divisor that converts this segment's venue integers into prices.
    ///
    /// Currency derivatives are quoted to more decimal places than everything else, and the two
    /// currency segments do not agree with each other: `CDS` scales by 10^7 and `BCD` by 10^4.
    #[must_use]
    pub const fn price_divisor(self) -> f64 {
        match self {
            Self::Cds => 10_000_000.0,
            Self::Bcd => 10_000.0,
            _ => 100.0,
        }
    }

    /// Returns whether instruments in this segment can be traded.
    ///
    /// Indices are streamed but cannot be traded.
    #[must_use]
    pub const fn is_tradable(self) -> bool {
        !matches!(self, Self::Indices)
    }
}

/// The streaming mode of a decoded tick.
///
/// Zerodha does not tag the packet with its mode; the mode is implied by the packet length, so this
/// records what the length meant rather than anything the venue sent.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    Display,
    Hash,
    PartialEq,
    Eq,
    AsRefStr,
    EnumIter,
    EnumString,
    Serialize,
    Deserialize,
)]
#[strum(ascii_case_insensitive)]
#[strum(serialize_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.zerodha",
        eq,
        eq_int,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.zerodha")
)]
pub enum ZerodhaTickMode {
    /// Last traded price only (8-byte packet).
    #[default]
    Ltp,
    /// Last price, traded quantities and OHLC, with no depth (28- or 44-byte packet).
    Quote,
    /// Everything `Quote` carries, plus timestamps, open interest and five-deep market depth
    /// (32- or 184-byte packet).
    Full,
}

/// The exchange an order is routed to.
///
/// # This is the VENUE half of a Nautilus instrument id, not a lookup
///
/// `http::parse::KiteInstrument::instrument_id` builds ids as `TRADINGSYMBOL.EXCHANGE`, so the
/// venue component of every id this adapter produces already *is* the venue's own `exchange`
/// string. The execution path therefore needs no registry to recover it — but it does need to
/// reject a venue that Zerodha does not route to, which is what parsing here provides.
///
/// # ⚠️ THE VENDOR'S `EXCHANGE_*` CONSTANTS ARE STALE — this list is from the DUMP
///
/// Seven of these are `kiteconnect` 5.2.0 `connect.py:80-86`. The eighth, `NCO`, is not a vendor
/// constant at all and is here anyway, because the vendor's list is good provenance for *what the
/// order API names* and the wrong net for *what appears in an instrument id*. The dump decides
/// which venue strings this adapter is actually handed.
///
/// Measured directly from `https://api.kite.trade/instruments` on 2026-08-14 — 114,851 rows,
/// **unauthenticated**, no credential of any kind, so anyone can re-derive this in one `curl`:
///
/// | exchange | rows | |
/// |---|---|---|
/// | `NFO` | 35,605 | |
/// | **`NCO`** | **28,067** | **not a vendor constant** |
/// | `MCX` | 16,298 | |
/// | `BSE` | 12,774 | |
/// | `NSE` | 10,037 | |
/// | `CDS` | 7,801 | |
/// | `BFO` | 4,256 | |
/// | `GLOBAL` | 12 | not a vendor constant; 100% quote-only |
/// | `NSEIX` | 1 | not a vendor constant; 100% quote-only |
/// | `BCD` | **0** | a vendor constant with no instruments that day |
///
/// `NCO` breaks down as 13,946 `CE` + 13,946 `PE` on `NCO-OPT`, 147 `FUT` on `NCO-FUT` and 28 `EQ`,
/// across 29 underlyings — a balanced option chain with strikes and expiries plus dated futures
/// (`ALUMINI26AUGFUT`, expiry 2026-08-31, lot 1, tick 0.05). Those are orderable instruments, so
/// rejecting `.NCO` refused ~28,000 of them.
///
/// ⚠️ Still unobserved: no order has been placed on an `NCO` instrument, so "the order endpoint
/// accepts `exchange=NCO`" is inferred from the dump rather than seen. It is a strong inference —
/// the dump is the venue's own statement of an instrument's exchange code, and there is no other
/// value that could be correct — but it is an inference.
///
/// # ⭐ THIS ENUM CANNOT TELL A TRADABLE INSTRUMENT FROM AN INDEX, and nothing else does either
///
/// The obvious reading of the table above is that quote-only instruments live on their own
/// exchange. They do not. **`exchange == "INDICES"` occurs zero times in the dump.** Indices ride
/// on ordinary exchanges and are marked by their `segment`:
///
/// | | `segment == INDICES` rows |
/// |---|---|
/// | `NSE` | 136 — `NIFTY 50`, `NIFTY BANK`, `NIFTY 100`, … |
/// | `BSE` | 73 |
/// | `MCX` | 11 |
/// | `GLOBAL` | 12 (all of them) |
/// | `NSEIX` | 1 (all of it) |
///
/// So `NIFTY 50.NSE` passes this check and always will: an `InstrumentId` carries a symbol and a
/// venue, and neither distinguishes an index from the 10,036 tradable rows beside it.
///
/// **The screen therefore is not here — it is in [`crate::execution::ZerodhaExecutionClient`],**
/// which reads [`InstrumentAny::IndexInstrument`] back out of the cache. That works because
/// [`crate::http::instruments`] dispatches `segment == INDICES` **before** `instrument_type`, so
/// the property survives into the instrument definition even though it cannot survive into an id.
/// It is only as good as the cache is populated, which that method documents.
///
/// `GLOBAL` and `NSEIX` are refused below *because they are wholly quote-only*. That is a coarse,
/// per-exchange approximation of a per-row property — worth having because it catches the two
/// cases where the exchange really does determine the answer, and not a substitute for the cache
/// check. See [`ZerodhaSegment::is_tradable`] for the underlying property.
///
/// [`InstrumentAny::IndexInstrument`]: nautilus_model::instruments::InstrumentAny::IndexInstrument
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZerodhaExchange {
    /// NSE equity.
    Nse,
    /// BSE equity.
    Bse,
    /// NSE futures and options.
    Nfo,
    /// BSE futures and options.
    Bfo,
    /// NSE currency derivatives.
    Cds,
    /// BSE currency derivatives.
    ///
    /// A vendor constant that carried **zero instruments** on 2026-08-14. It parses because the
    /// venue names it; that is not the same as being tradable today.
    Bcd,
    /// MCX commodities.
    Mcx,
    /// NSE commodity derivatives — 28,067 instruments, and **not** a vendor `EXCHANGE_*` constant.
    ///
    /// See the type documentation: the vendor's constant list is stale and the dump is the
    /// authority on what an instrument id can carry.
    Nco,
}

impl ZerodhaExchange {
    /// Returns the venue's own string for this exchange.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nse => "NSE",
            Self::Bse => "BSE",
            Self::Nfo => "NFO",
            Self::Bfo => "BFO",
            Self::Cds => "CDS",
            Self::Bcd => "BCD",
            Self::Mcx => "MCX",
            Self::Nco => "NCO",
        }
    }

    /// Parses the venue's own string.
    ///
    /// # The quote-only exchanges are named separately, and that is not cosmetic
    ///
    /// `GLOBAL` and `NSEIX` are in the dump and are 100% `segment=INDICES`, so an order can never
    /// be routed to one. Folding them into the generic "not an exchange" arm would tell an operator
    /// their instrument id is malformed when it is perfectly well formed, sending them to look for
    /// a typo that does not exist. A message that closes the route to discovery is worse than no
    /// message.
    ///
    /// Note there is **no `INDICES` arm**: `exchange == "INDICES"` occurs zero times in the dump.
    /// An earlier revision had one, which was unreachable and, worse, implied this function screens
    /// out indices. It does not — see the type docs.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not an exchange this adapter routes orders to.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "NSE" => Ok(Self::Nse),
            "BSE" => Ok(Self::Bse),
            "NFO" => Ok(Self::Nfo),
            "BFO" => Ok(Self::Bfo),
            "CDS" => Ok(Self::Cds),
            "BCD" => Ok(Self::Bcd),
            "MCX" => Ok(Self::Mcx),
            "NCO" => Ok(Self::Nco),
            // Wholly quote-only: every row on these two carries `segment=INDICES`, measured
            // 2026-08-14. This is a coarse version of the right check -- the right one reads the
            // per-row segment, which an InstrumentId does not carry.
            "GLOBAL" | "NSEIX" => anyhow::bail!(
                "'{value}' carries only index instruments, which quote but cannot be traded, so no \
                 order can be routed to one. Your instrument id is well formed -- this is the \
                 venue's design, not a typo and not a configuration problem"
            ),
            other => anyhow::bail!(
                "'{other}' is not a Zerodha order-routing exchange; this adapter routes NSE, BSE, \
                 NFO, BFO, CDS, BCD, MCX and NCO. Note that the exchange set comes from the \
                 instrument dump rather than from the vendor client's EXCHANGE_* constants, which \
                 are stale, so check ZerodhaExchange's documentation before concluding the \
                 instrument id is malformed"
            ),
        }
    }
}

/// The margin and square-off regime an order is placed under.
///
/// Values from `kiteconnect` 5.2.0 `connect.py:45-48`.
///
/// # ⚠️ There is NO Nautilus field this can be derived from, and the choice is not cosmetic
///
/// `product` decides how much margin the order consumes and whether the broker force-closes the
/// resulting position:
///
/// | product | margin | broker auto-square-off |
/// |---|---|---|
/// | `CNC`  | full value, delivery | never — the position is delivered |
/// | `MIS`  | intraday leverage | **yes, around 15:20 IST** |
/// | `NRML` | span + exposure, carry-forward | never |
/// | `CO`   | cover order, mandatory stop-loss leg | yes |
///
/// A `MIS` order sent where `NRML` was meant is squared off by the broker the same afternoon; an
/// `NRML` order sent where `MIS` was meant consumes several times the margin and may be rejected
/// for insufficient funds. Neither failure is visible in the order response, so this adapter never
/// picks a value: see `ZerodhaExecClientConfig::default_product`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.zerodha",
        eq,
        eq_int,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.zerodha")
)]
pub enum ZerodhaProduct {
    /// Cash and carry — delivery, full margin, no auto square-off.
    Cnc,
    /// Margin intraday square-off — leveraged, **force-closed by the broker intraday**.
    Mis,
    /// Normal — carry-forward derivatives margin.
    Nrml,
    /// Cover order — bundled mandatory stop-loss leg.
    Co,
}

impl ZerodhaProduct {
    /// Returns the venue's own string for this product.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cnc => "CNC",
            Self::Mis => "MIS",
            Self::Nrml => "NRML",
            Self::Co => "CO",
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not one of the four products.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "CNC" => Ok(Self::Cnc),
            "MIS" => Ok(Self::Mis),
            "NRML" => Ok(Self::Nrml),
            "CO" => Ok(Self::Co),
            other => anyhow::bail!(
                "'{other}' is not a Zerodha product; expected CNC, MIS, NRML or CO. \
                 The product decides margin and whether the broker squares the position off \
                 intraday, so it is never inferred"
            ),
        }
    }
}

/// The order variety.
///
/// Values from `kiteconnect` 5.2.0 `connect.py:60-64`.
///
/// # ⭐ The variety is part of the URL PATH, not a form field
///
/// `place_order` posts to `/orders/{variety}` (`connect.py:123`, `connect.py:369`), and modify and
/// cancel address `/orders/{variety}/{order_id}` (`connect.py:124-125`). A variety that does not
/// match the one the order was placed under therefore produces a **404 on a path**, not a
/// validation message naming the field — and cancelling an order under the wrong variety leaves it
/// live at the venue.
///
/// That is why the placed variety is recorded per order rather than recomputed at cancel time.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(
        module = "nautilus_trader.adapters.zerodha",
        eq,
        eq_int,
        from_py_object,
        rename_all = "SCREAMING_SNAKE_CASE"
    )
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass_enum(module = "nautilus_trader.adapters.zerodha")
)]
pub enum ZerodhaVariety {
    /// A regular order.
    #[default]
    Regular,
    /// A cover order.
    Co,
    /// An after-market order.
    Amo,
    /// An iceberg order, disclosed in legs.
    Iceberg,
    /// An auction order.
    Auction,
}

impl ZerodhaVariety {
    /// Returns the venue's own string, which is also the URL path segment.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Regular => "regular",
            Self::Co => "co",
            Self::Amo => "amo",
            Self::Iceberg => "iceberg",
            Self::Auction => "auction",
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not a known variety. The comparison is case-sensitive
    /// because the value is interpolated into a URL path, where the venue's own casing is the only
    /// one known to resolve.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "regular" => Ok(Self::Regular),
            "co" => Ok(Self::Co),
            "amo" => Ok(Self::Amo),
            "iceberg" => Ok(Self::Iceberg),
            "auction" => Ok(Self::Auction),
            other => anyhow::bail!(
                "'{other}' is not a Zerodha order variety; expected one of \
                 regular, co, amo, iceberg, auction (lowercase — the value is a URL path segment)"
            ),
        }
    }
}

/// The side of an order, in the venue's vocabulary.
///
/// Values from `kiteconnect` 5.2.0 `connect.py:67-68`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZerodhaTransactionType {
    /// A buy.
    Buy,
    /// A sell.
    Sell,
}

impl ZerodhaTransactionType {
    /// Returns the venue's own string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }

    /// Converts from a Nautilus [`OrderSide`].
    ///
    /// # Errors
    ///
    /// Currently infallible -- `OrderSide` is `Buy | Sell` and both map. The `Result` is retained
    /// because callers already handle it and because the venue may yet gain a side this cannot
    /// express.
    ///
    /// HISTORY, because the guard that used to live here was load-bearing: until v2.0.0rc4
    /// `OrderSide` had a third variant, `NoOrderSide`, which was its `Default`. A partially-built
    /// order therefore reached this function with a side that *looked* valid, and mapping it to
    /// `BUY` would have placed a real trade in a direction nobody asked for -- so this bailed.
    /// Upstream deleted that variant, which makes the unset state unrepresentable and retires the
    /// guard at the type level rather than at runtime. **Do not reintroduce a defaulted side.**
    pub fn from_order_side(side: OrderSide) -> anyhow::Result<Self> {
        match side {
            OrderSide::Buy => Ok(Self::Buy),
            OrderSide::Sell => Ok(Self::Sell),
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is neither `BUY` nor `SELL`.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            other => anyhow::bail!("'{other}' is not a Zerodha transaction_type; expected BUY or SELL"),
        }
    }

    /// Converts to a Nautilus [`OrderSide`].
    #[must_use]
    pub const fn to_order_side(self) -> OrderSide {
        match self {
            Self::Buy => OrderSide::Buy,
            Self::Sell => OrderSide::Sell,
        }
    }
}

/// The order type, in the venue's vocabulary.
///
/// Values from `kiteconnect` 5.2.0 `connect.py:51-54`.
///
/// # Zerodha has FOUR order types; Nautilus has nine
///
/// The five without a Zerodha equivalent are rejected rather than approximated. Each of them has a
/// tempting wrong answer:
///
/// | Nautilus | tempting wrong answer | why it is wrong |
/// |---|---|---|
/// | `MarketToLimit` | `MARKET` | the unfilled remainder must rest as a limit; `MARKET` sweeps it |
/// | `MarketIfTouched` | `SL-M` | `SL-M` triggers away from the market, MIT triggers toward it |
/// | `LimitIfTouched` | `SL` | same inversion, with a limit leg attached |
/// | `TrailingStopMarket` | `SL-M` | the trigger would be frozen at its initial value |
/// | `TrailingStopLimit` | `SL` | same, plus a frozen limit |
///
/// Every one of those places a real order that behaves differently from the one requested, and
/// none of them raises anything at the venue.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZerodhaOrderType {
    /// Execute at the best available price.
    Market,
    /// Rest at a stated price or better.
    Limit,
    /// Stop-loss limit: becomes a `LIMIT` at `price` once `trigger_price` is hit.
    Sl,
    /// Stop-loss market: becomes a `MARKET` once `trigger_price` is hit.
    Slm,
}

impl ZerodhaOrderType {
    /// Returns the venue's own string.
    ///
    /// `SL-M` carries a hyphen, which is why these strings are written out rather than derived.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Market => "MARKET",
            Self::Limit => "LIMIT",
            Self::Sl => "SL",
            Self::Slm => "SL-M",
        }
    }

    /// Converts from a Nautilus [`OrderType`].
    ///
    /// # Errors
    ///
    /// Returns an error naming the order type when Zerodha's regular order rail cannot express it.
    pub fn from_order_type(order_type: OrderType) -> anyhow::Result<Self> {
        match order_type {
            OrderType::Market => Ok(Self::Market),
            OrderType::Limit => Ok(Self::Limit),
            OrderType::StopMarket => Ok(Self::Slm),
            OrderType::StopLimit => Ok(Self::Sl),
            other => anyhow::bail!(
                "Zerodha has no order type for {other}; its regular order rail offers only \
                 MARKET, LIMIT, SL and SL-M. Downgrading {other} to one of those places a real \
                 order with different behaviour and the venue reports nothing"
            ),
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not one of the four order types.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "MARKET" => Ok(Self::Market),
            "LIMIT" => Ok(Self::Limit),
            "SL" => Ok(Self::Sl),
            "SL-M" => Ok(Self::Slm),
            other => anyhow::bail!(
                "'{other}' is not a Zerodha order_type; expected MARKET, LIMIT, SL or SL-M"
            ),
        }
    }

    /// Converts to a Nautilus [`OrderType`].
    #[must_use]
    pub const fn to_order_type(self) -> OrderType {
        match self {
            Self::Market => OrderType::Market,
            Self::Limit => OrderType::Limit,
            Self::Sl => OrderType::StopLimit,
            Self::Slm => OrderType::StopMarket,
        }
    }

    /// Returns whether this order type requires a `trigger_price`.
    #[must_use]
    pub const fn requires_trigger_price(self) -> bool {
        matches!(self, Self::Sl | Self::Slm)
    }

    /// Returns whether this order type requires a `price`.
    #[must_use]
    pub const fn requires_price(self) -> bool {
        matches!(self, Self::Limit | Self::Sl)
    }
}

/// The order validity, which is Zerodha's name for time in force.
///
/// Values from `kiteconnect` 5.2.0 `connect.py:71-73`.
///
/// # ⚠️ There is no `GTC` on an Indian exchange, and `IOC` is not `FOK`
///
/// Indian exchanges discard the order book nightly, so nothing rests past the session: `DAY` is the
/// longest validity a regular order has. Zerodha does sell a "GTT" (good-till-triggered) product,
/// but it is a *separate* resource (`/gtt/triggers`, `connect.py:159-163`) that places a fresh
/// order when a trigger fires — it is not a validity on this order, and this adapter does not
/// implement it.
///
/// `FOK` is the subtler trap: `IOC` cancels the *unfilled remainder*, so a 100-lot `IOC` that finds
/// 30 lots leaves 30 lots executed. A `FOK` that finds 30 lots must execute **nothing**. Mapping
/// `Fok` to `IOC` therefore turns "all or nothing" into "whatever is available", which is a
/// position the strategy never asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZerodhaValidity {
    /// Valid for the remainder of the trading session.
    Day,
    /// Immediate-or-cancel: the unfilled remainder is cancelled.
    Ioc,
    /// Time-to-live in minutes, used with iceberg orders (`validity_ttl`).
    Ttl,
}

impl ZerodhaValidity {
    /// Returns the venue's own string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Day => "DAY",
            Self::Ioc => "IOC",
            Self::Ttl => "TTL",
        }
    }

    /// Converts from a Nautilus [`TimeInForce`].
    ///
    /// # Errors
    ///
    /// Returns an error for every time in force Zerodha cannot express, naming both the value and
    /// what silently accepting it would have changed.
    pub fn from_time_in_force(time_in_force: TimeInForce) -> anyhow::Result<Self> {
        match time_in_force {
            TimeInForce::Day => Ok(Self::Day),
            TimeInForce::Ioc => Ok(Self::Ioc),
            TimeInForce::Gtc => anyhow::bail!(
                "Zerodha has no GTC validity: Indian exchanges clear the book nightly, so DAY is \
                 the longest a regular order lives. Accepting GTC as DAY would silently cancel \
                 the order at the close. The GTT product is a separate resource this adapter \
                 does not implement"
            ),
            TimeInForce::Fok => anyhow::bail!(
                "Zerodha has no FOK validity. IOC is not equivalent: IOC executes what it can and \
                 cancels the remainder, so an all-or-nothing order would become a partial position"
            ),
            TimeInForce::Gtd => anyhow::bail!(
                "Zerodha has no GTD validity. Its TTL is a minute count for iceberg orders, not an \
                 expiry timestamp, so a GTD expire_time cannot be carried across"
            ),
            other => anyhow::bail!(
                "Zerodha has no validity for {other}; its regular order rail offers DAY and IOC"
            ),
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` is not one of the three validities.
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "DAY" => Ok(Self::Day),
            "IOC" => Ok(Self::Ioc),
            "TTL" => Ok(Self::Ttl),
            other => anyhow::bail!("'{other}' is not a Zerodha validity; expected DAY, IOC or TTL"),
        }
    }

    /// Converts to a Nautilus [`TimeInForce`].
    ///
    /// # Errors
    ///
    /// Returns an error for `TTL`, which is a minute count rather than a time in force and has no
    /// Nautilus counterpart.
    pub fn to_time_in_force(self) -> anyhow::Result<TimeInForce> {
        match self {
            Self::Day => Ok(TimeInForce::Day),
            Self::Ioc => Ok(TimeInForce::Ioc),
            Self::Ttl => anyhow::bail!(
                "Zerodha TTL validity is a minute count carried in `validity_ttl`, not a Nautilus \
                 TimeInForce; reporting it as GTD would invent an expiry timestamp"
            ),
        }
    }
}

/// The lifecycle status a Zerodha order reports.
///
/// # The vendor client names only THREE of these
///
/// `connect.py:93-95` defines `STATUS_COMPLETE`, `STATUS_REJECTED` and `STATUS_CANCELLED` and
/// nothing else, so unlike every other enum here the full set is **not** taken from the vendor
/// source — the remaining values are the ones Kite Connect v3 documents for the order lifecycle.
/// That is a weaker provenance than the rest of this file and is stated rather than hidden.
///
/// An unrecognised status is an error, not a fallback: a status this adapter has not seen before is
/// exactly the case where guessing "probably still open" could leave a filled order looking live.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ZerodhaOrderStatus {
    /// The venue received the request but has not validated it.
    PutOrderReqReceived,
    /// Validation in progress.
    ValidationPending,
    /// Accepted internally, not yet at the exchange.
    OpenPending,
    /// A modification is being validated.
    ModifyValidationPending,
    /// A modification is in progress.
    ModifyPending,
    /// An after-market order request was received.
    AmoReqReceived,
    /// A cancellation is in progress.
    CancelPending,
    /// Resting at the exchange, awaiting its stop trigger.
    TriggerPending,
    /// Working at the exchange.
    Open,
    /// Fully executed.
    Complete,
    /// Cancelled.
    Cancelled,
    /// Rejected.
    Rejected,
}

impl ZerodhaOrderStatus {
    /// Returns the venue's own string.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PutOrderReqReceived => "PUT ORDER REQ RECEIVED",
            Self::ValidationPending => "VALIDATION PENDING",
            Self::OpenPending => "OPEN PENDING",
            Self::ModifyValidationPending => "MODIFY VALIDATION PENDING",
            Self::ModifyPending => "MODIFY PENDING",
            Self::AmoReqReceived => "AMO REQ RECEIVED",
            Self::CancelPending => "CANCEL PENDING",
            Self::TriggerPending => "TRIGGER PENDING",
            Self::Open => "OPEN",
            Self::Complete => "COMPLETE",
            Self::Cancelled => "CANCELLED",
            Self::Rejected => "REJECTED",
        }
    }

    /// Parses the venue's own string.
    ///
    /// # Errors
    ///
    /// Returns an error for any status not listed. Zerodha adds lifecycle states without notice,
    /// and the safe response to one is to say so rather than to assume it means "open".
    pub fn from_venue_str(value: &str) -> anyhow::Result<Self> {
        match value {
            "PUT ORDER REQ RECEIVED" => Ok(Self::PutOrderReqReceived),
            "VALIDATION PENDING" => Ok(Self::ValidationPending),
            "OPEN PENDING" => Ok(Self::OpenPending),
            "MODIFY VALIDATION PENDING" => Ok(Self::ModifyValidationPending),
            "MODIFY PENDING" => Ok(Self::ModifyPending),
            "AMO REQ RECEIVED" => Ok(Self::AmoReqReceived),
            "CANCEL PENDING" => Ok(Self::CancelPending),
            "TRIGGER PENDING" => Ok(Self::TriggerPending),
            "OPEN" => Ok(Self::Open),
            "COMPLETE" => Ok(Self::Complete),
            "CANCELLED" => Ok(Self::Cancelled),
            "REJECTED" => Ok(Self::Rejected),
            other => anyhow::bail!(
                "'{other}' is not a Zerodha order status this adapter recognises; treating an \
                 unknown status as open could leave a filled or dead order looking live"
            ),
        }
    }

    /// Converts to a Nautilus [`OrderStatus`].
    ///
    /// # ⭐ `has_fills` is not optional, and omitting it loses partial fills
    ///
    /// Zerodha does not have a `PARTIALLY FILLED` status. A half-executed order stays `OPEN` and
    /// reports the executed amount in `filled_quantity`, so the status string **alone** cannot
    /// distinguish a resting order from one that is 40% done. The caller supplies that from the
    /// quantity fields; there is nowhere else it can come from.
    #[must_use]
    pub const fn to_order_status(self, has_fills: bool) -> OrderStatus {
        match self {
            Self::PutOrderReqReceived
            | Self::ValidationPending
            | Self::OpenPending
            | Self::ModifyValidationPending
            | Self::ModifyPending
            | Self::AmoReqReceived => OrderStatus::Submitted,
            Self::CancelPending => OrderStatus::PendingCancel,
            // A stop order accepted and resting until its trigger is hit is ACCEPTED, not
            // TRIGGERED -- `Triggered` means the trigger already fired.
            Self::TriggerPending => OrderStatus::Accepted,
            Self::Open => {
                if has_fills {
                    OrderStatus::PartiallyFilled
                } else {
                    OrderStatus::Accepted
                }
            }
            Self::Complete => OrderStatus::Filled,
            Self::Cancelled => OrderStatus::Canceled,
            Self::Rejected => OrderStatus::Rejected,
        }
    }

    /// Returns whether an order in this status can still be modified or cancelled.
    #[must_use]
    pub const fn is_open(self) -> bool {
        !matches!(self, Self::Complete | Self::Cancelled | Self::Rejected)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(OrderType::Market, "MARKET")]
    #[case(OrderType::Limit, "LIMIT")]
    #[case(OrderType::StopMarket, "SL-M")]
    #[case(OrderType::StopLimit, "SL")]
    fn test_representable_order_types_map_to_the_venue_string(
        #[case] order_type: OrderType,
        #[case] expected: &str,
    ) {
        let mapped = ZerodhaOrderType::from_order_type(order_type).expect("representable");

        assert_eq!(mapped.as_str(), expected);
    }

    // THE DISCRIMINATING TEST FOR ORDER TYPES. An implementation with a `_ => Market` arm passes
    // every case above and fails all five of these -- by placing a live market order where a
    // conditional one was requested. Each of these five has a plausible Zerodha type that behaves
    // differently, so "closest match" is the wrong instinct here.
    #[rstest]
    #[case(OrderType::MarketToLimit)]
    #[case(OrderType::MarketIfTouched)]
    #[case(OrderType::LimitIfTouched)]
    #[case(OrderType::TrailingStopMarket)]
    #[case(OrderType::TrailingStopLimit)]
    fn test_an_unrepresentable_order_type_errors_rather_than_downgrading(
        #[case] order_type: OrderType,
    ) {
        let error = match ZerodhaOrderType::from_order_type(order_type) {
            Ok(mapped) => panic!(
                "{order_type} must not silently become {}",
                mapped.as_str()
            ),
            Err(e) => e.to_string(),
        };

        assert!(
            error.contains(&order_type.to_string()),
            "the error must name the order type that could not be sent; was: {error}"
        );
    }

    #[rstest]
    #[case(TimeInForce::Day, "DAY")]
    #[case(TimeInForce::Ioc, "IOC")]
    fn test_representable_validities_map_to_the_venue_string(
        #[case] time_in_force: TimeInForce,
        #[case] expected: &str,
    ) {
        let mapped = ZerodhaValidity::from_time_in_force(time_in_force).expect("representable");

        assert_eq!(mapped.as_str(), expected);
    }

    // THE DISCRIMINATING TEST FOR TIME IN FORCE. `Fok -> IOC` is the one a reviewer waves through:
    // both are "immediate", both are single words, and the venue accepts IOC without complaint.
    // The difference only shows up when liquidity is thin -- an all-or-nothing order comes back
    // as a partial position, which is the exact case FOK exists to prevent.
    #[rstest]
    #[case(TimeInForce::Fok)]
    #[case(TimeInForce::Gtc)]
    #[case(TimeInForce::Gtd)]
    #[case(TimeInForce::AtTheOpen)]
    #[case(TimeInForce::AtTheClose)]
    fn test_an_unrepresentable_time_in_force_errors_rather_than_downgrading(
        #[case] time_in_force: TimeInForce,
    ) {
        assert!(
            ZerodhaValidity::from_time_in_force(time_in_force).is_err(),
            "{time_in_force} has no Zerodha validity and must not be approximated"
        );
    }

    #[rstest]
    fn test_fok_error_names_the_partial_fill_consequence() {
        let error = ZerodhaValidity::from_time_in_force(TimeInForce::Fok)
            .expect_err("FOK is not representable")
            .to_string();

        assert!(
            error.contains("IOC"),
            "the error should say why IOC is not a substitute; was: {error}"
        );
    }

    #[rstest]
    #[case(OrderSide::Buy, "BUY")]
    #[case(OrderSide::Sell, "SELL")]
    fn test_order_side_maps_to_transaction_type(#[case] side: OrderSide, #[case] expected: &str) {
        let mapped = ZerodhaTransactionType::from_order_side(side).expect("representable");

        assert_eq!(mapped.as_str(), expected);
    }

    // `NoOrderSide` is the `Default` for `OrderSide`, so it arrives looking like a legitimate
    // value rather than like an omission. Mapping it to BUY would place a real trade.
    #[rstest]
    fn test_no_order_side_errors_rather_than_defaulting_to_buy() {
        assert!(
            ZerodhaTransactionType::from_order_side(OrderSide::NoOrderSide).is_err(),
            "an unset side must not become a BUY"
        );
    }

    #[rstest]
    #[case("NSE")]
    #[case("BSE")]
    #[case("NFO")]
    #[case("BFO")]
    #[case("CDS")]
    #[case("BCD")]
    #[case("MCX")]
    #[case("NCO")]
    fn test_exchange_round_trips_through_its_venue_string(#[case] exchange: &str) {
        let parsed = ZerodhaExchange::from_venue_str(exchange).expect("routable exchange");

        assert_eq!(parsed.as_str(), exchange);
    }

    // ⭐ NCO IS ROUTABLE, and this test is the one that would have caught the original mistake.
    //
    // NCO is NOT one of the vendor client's EXCHANGE_* constants, and the enum was originally
    // built from that list -- so `.NCO` was refused. The instrument dump says otherwise:
    // 28,067 rows, of which 13,946 CE + 13,946 PE on NCO-OPT and 147 dated futures on NCO-FUT,
    // across 29 underlyings with real strikes, expiries, lot sizes and tick sizes. Refusing them
    // denied roughly 28,000 orderable instruments on the strength of a stale constant list.
    #[rstest]
    fn test_nco_is_routable_despite_not_being_a_vendor_constant() {
        let parsed = ZerodhaExchange::from_venue_str("NCO")
            .expect("NCO carries 28,067 orderable instruments in the dump");

        assert_eq!(parsed, ZerodhaExchange::Nco);
        assert_eq!(parsed.as_str(), "NCO");
    }

    // `exchange == "INDICES"` occurs ZERO times in the dump, so this arrives here only as a
    // malformed value -- which is why it sits with the typos rather than in a branch of its own.
    // An earlier revision had a dedicated INDICES arm that was unreachable AND implied this
    // function screens out indices. It does not: see the quote-only test below.
    #[rstest]
    #[case("INDICES")]
    #[case("nse")]
    #[case("NSE_EQ")]
    #[case("")]
    fn test_an_unroutable_exchange_is_rejected(#[case] exchange: &str) {
        assert!(
            ZerodhaExchange::from_venue_str(exchange).is_err(),
            "'{exchange}' must not be accepted as an order-routing exchange"
        );
    }

    // THE DISCRIMINATING TEST FOR A WELL-FORMED-BUT-UNTRADABLE VENUE. GLOBAL (12 rows) and NSEIX
    // (1 row) are in the dump and are 100% `segment=INDICES`, so an instrument id naming one is
    // perfectly well formed and simply cannot be ordered. Rejecting it with the generic
    // "not an exchange" message would send an operator hunting for a typo that does not exist.
    #[rstest]
    #[case("GLOBAL")]
    #[case("NSEIX")]
    fn test_a_quote_only_exchange_is_rejected_as_untradable_not_as_a_typo(#[case] exchange: &str) {
        let error = ZerodhaExchange::from_venue_str(exchange)
            .expect_err("index-only exchanges cannot take orders")
            .to_string();

        assert!(
            error.contains("well formed"),
            "the error must not read as a malformed instrument id; was: {error}"
        );
        assert!(
            error.contains(exchange),
            "the error must name the exchange the caller actually holds; was: {error}"
        );
    }

    // ⭐ THIS ENUM CANNOT SCREEN INDICES, AND MUST NOT TRY.
    //
    // An index is not identified by its exchange: `exchange == "INDICES"` never appears in the
    // dump. `NIFTY 50` carries exchange=NSE with segment=INDICES, alongside 10,036 tradable NSE
    // rows. Refusing NSE to catch 136 index rows would deny the other 10,036.
    //
    // The screen lives in the execution client, which reads InstrumentAny::IndexInstrument from
    // the cache. This test pins the boundary so nobody "fixes" it here and breaks NSE entirely.
    #[rstest]
    fn test_a_tradable_exchange_is_routable_even_though_it_carries_indices() {
        assert!(
            ZerodhaExchange::from_venue_str("NSE").is_ok(),
            "NSE must route -- it carries 10,037 rows, of which only 136 are indices",
        );
        assert!(
            ZerodhaExchange::from_venue_str("BSE").is_ok(),
            "BSE likewise -- 73 of its 12,774 rows are indices",
        );
    }

    // The BCD trap, stated as a test so it cannot quietly rot back. BCD IS a vendor constant and
    // therefore parses -- but it carried ZERO instruments in the 2026-08-14 dump, so "BCD is
    // supported" and "you can trade BCD today" are different claims. Suggesting it in a failure
    // message would send an operator somewhere with nothing in it.
    #[rstest]
    fn test_bcd_parses_but_is_not_advertised_as_a_remedy() {
        assert!(
            ZerodhaExchange::from_venue_str("BCD").is_ok(),
            "BCD is a vendor EXCHANGE_* constant and must still parse",
        );

        let error = ZerodhaExchange::from_venue_str("GLOBAL")
            .expect_err("GLOBAL is index-only")
            .to_string();
        assert!(
            !error.contains("BCD"),
            "an exchange with no instruments must not be offered as the remedy; was: {error}",
        );
    }

    #[rstest]
    #[case(ZerodhaVariety::Regular, "regular")]
    #[case(ZerodhaVariety::Co, "co")]
    #[case(ZerodhaVariety::Amo, "amo")]
    #[case(ZerodhaVariety::Iceberg, "iceberg")]
    #[case(ZerodhaVariety::Auction, "auction")]
    fn test_variety_is_lowercase_because_it_is_a_url_path_segment(
        #[case] variety: ZerodhaVariety,
        #[case] expected: &str,
    ) {
        assert_eq!(variety.as_str(), expected);
        assert_eq!(
            ZerodhaVariety::from_venue_str(expected).expect("known variety"),
            variety,
        );
    }

    // `/orders/REGULAR` is not `/orders/regular`. Since the variety is a path segment, an
    // upper-case value is a 404 rather than a validation error, so it must not parse.
    #[rstest]
    fn test_uppercase_variety_does_not_parse() {
        assert!(
            ZerodhaVariety::from_venue_str("REGULAR").is_err(),
            "the variety is a URL path segment and only the venue's own casing resolves"
        );
    }

    #[rstest]
    #[case(ZerodhaProduct::Cnc, "CNC")]
    #[case(ZerodhaProduct::Mis, "MIS")]
    #[case(ZerodhaProduct::Nrml, "NRML")]
    #[case(ZerodhaProduct::Co, "CO")]
    fn test_product_round_trips_through_its_venue_string(
        #[case] product: ZerodhaProduct,
        #[case] expected: &str,
    ) {
        assert_eq!(product.as_str(), expected);
        assert_eq!(
            ZerodhaProduct::from_venue_str(expected).expect("known product"),
            product,
        );
    }

    #[rstest]
    fn test_an_unknown_product_is_rejected() {
        assert!(ZerodhaProduct::from_venue_str("INTRADAY").is_err());
        assert!(ZerodhaProduct::from_venue_str("mis").is_err());
    }

    #[rstest]
    #[case("OPEN", OrderStatus::Accepted)]
    #[case("TRIGGER PENDING", OrderStatus::Accepted)]
    #[case("COMPLETE", OrderStatus::Filled)]
    #[case("CANCELLED", OrderStatus::Canceled)]
    #[case("REJECTED", OrderStatus::Rejected)]
    #[case("CANCEL PENDING", OrderStatus::PendingCancel)]
    #[case("VALIDATION PENDING", OrderStatus::Submitted)]
    #[case("PUT ORDER REQ RECEIVED", OrderStatus::Submitted)]
    #[case("AMO REQ RECEIVED", OrderStatus::Submitted)]
    fn test_unfilled_status_maps_to_the_nautilus_status(
        #[case] venue_status: &str,
        #[case] expected: OrderStatus,
    ) {
        let parsed = ZerodhaOrderStatus::from_venue_str(venue_status).expect("known status");

        assert_eq!(parsed.to_order_status(false), expected);
    }

    // THE DISCRIMINATING TEST FOR STATUS. Zerodha has no PARTIALLY FILLED status -- a half-done
    // order is still `OPEN` and carries the executed amount in `filled_quantity`. A mapping that
    // reads only the status string passes every case above and reports a 40%-executed order as
    // ACCEPTED, so the engine believes nothing has traded.
    #[rstest]
    fn test_open_with_fills_is_partially_filled_not_accepted() {
        let open = ZerodhaOrderStatus::from_venue_str("OPEN").expect("known status");

        assert_eq!(open.to_order_status(false), OrderStatus::Accepted);
        assert_eq!(
            open.to_order_status(true),
            OrderStatus::PartiallyFilled,
            "the status string alone cannot see a partial fill; filled_quantity has to",
        );
    }

    // TRIGGER PENDING is a stop order resting until its trigger fires. `Triggered` would say the
    // opposite -- that the trigger has already gone off and the order is live in the book.
    #[rstest]
    fn test_trigger_pending_is_accepted_not_triggered() {
        let status = ZerodhaOrderStatus::from_venue_str("TRIGGER PENDING").expect("known status");

        assert_ne!(status.to_order_status(false), OrderStatus::Triggered);
        assert_eq!(status.to_order_status(false), OrderStatus::Accepted);
    }

    #[rstest]
    fn test_an_unknown_status_errors_rather_than_assuming_open() {
        assert!(
            ZerodhaOrderStatus::from_venue_str("SOME NEW STATE").is_err(),
            "an unrecognised status must not be assumed to mean the order is still working",
        );
    }

    #[rstest]
    #[case(ZerodhaOrderType::Market, false, false)]
    #[case(ZerodhaOrderType::Limit, false, true)]
    #[case(ZerodhaOrderType::Slm, true, false)]
    #[case(ZerodhaOrderType::Sl, true, true)]
    fn test_price_and_trigger_requirements_per_order_type(
        #[case] order_type: ZerodhaOrderType,
        #[case] needs_trigger: bool,
        #[case] needs_price: bool,
    ) {
        assert_eq!(order_type.requires_trigger_price(), needs_trigger);
        assert_eq!(order_type.requires_price(), needs_price);
    }

    #[rstest]
    fn test_ttl_validity_has_no_nautilus_time_in_force() {
        assert!(
            ZerodhaValidity::Ttl.to_time_in_force().is_err(),
            "TTL is a minute count, not a time in force; reporting GTD would invent an expiry",
        );
    }
}

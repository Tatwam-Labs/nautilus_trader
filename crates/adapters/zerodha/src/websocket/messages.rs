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

//! Message types for the Zerodha Kite Connect WebSocket streaming API.
//!
//! The streaming API is a *binary* protocol: each WebSocket binary frame carries a count-prefixed
//! sequence of packets, and the packet *length* selects the payload layout. See
//! [`crate::websocket::parse`] for the decoder.

use serde::{Deserialize, Serialize};

use crate::common::enums::{ZerodhaSegment, ZerodhaTickMode};

/// Open/high/low/close prices carried by a quote or full-mode tick.
///
/// Prices are venue integers scaled down by the segment divisor (see
/// [`ZerodhaSegment::price_divisor`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct KiteOhlc {
    /// The open price for the session.
    pub open: f64,
    /// The high price for the session.
    pub high: f64,
    /// The low price for the session.
    pub low: f64,
    /// The close price of the *previous* session.
    pub close: f64,
}

/// A single level of the five-deep market depth carried by a full-mode tick.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct KiteDepthEntry {
    /// The aggregate quantity resting at this level.
    pub quantity: u32,
    /// The price of this level.
    pub price: f64,
    /// The number of orders resting at this level.
    pub orders: u16,
}

/// The five-deep bid and ask ladders carried by a full-mode tick.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct KiteDepth {
    /// The five bid levels, best first.
    pub buy: Vec<KiteDepthEntry>,
    /// The five ask levels, best first.
    pub sell: Vec<KiteDepthEntry>,
}

/// A decoded Zerodha Kite streaming tick.
///
/// Which fields are populated is determined by the packet length, not by a discriminator byte —
/// `mode` records what the length implied. LTP packets carry only `last_price`; index packets carry
/// no traded quantities; only full-mode equity/derivative packets carry depth and open interest.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct KiteTick {
    /// The Zerodha instrument token, unique per instrument per segment.
    pub instrument_token: u32,
    /// The segment decoded from the low byte of `instrument_token`.
    pub segment: ZerodhaSegment,
    /// Whether the instrument is tradable (false for indices).
    pub tradable: bool,
    /// The streaming mode implied by the packet length.
    pub mode: ZerodhaTickMode,
    /// The last traded price.
    pub last_price: f64,
    /// The quantity of the last trade (absent for LTP and index packets).
    pub last_traded_quantity: Option<u32>,
    /// The volume-weighted average traded price for the session.
    pub average_traded_price: Option<f64>,
    /// The cumulative traded volume for the session.
    pub volume_traded: Option<u32>,
    /// The total resting buy quantity across the book.
    pub total_buy_quantity: Option<u32>,
    /// The total resting sell quantity across the book.
    pub total_sell_quantity: Option<u32>,
    /// The session OHLC (absent for LTP packets).
    pub ohlc: Option<KiteOhlc>,
    /// The percentage change of `last_price` against the previous close.
    pub change: f64,
    /// The exchange timestamp, as Unix epoch **seconds**.
    pub exchange_timestamp: Option<u32>,
    /// The time of the last trade, as Unix epoch **seconds**.
    pub last_trade_time: Option<u32>,
    /// Open interest (derivatives, full mode only).
    pub oi: Option<u32>,
    /// The session high of open interest.
    pub oi_day_high: Option<u32>,
    /// The session low of open interest.
    pub oi_day_low: Option<u32>,
    /// The five-deep market depth (full mode only).
    pub depth: Option<KiteDepth>,
}

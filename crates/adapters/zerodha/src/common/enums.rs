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

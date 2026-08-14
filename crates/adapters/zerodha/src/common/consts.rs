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

//! Constants for the Zerodha adapter.

use std::sync::LazyLock;

use nautilus_model::identifiers::{ClientId, Venue};

/// The name of the Zerodha adapter.
pub const ZERODHA: &str = "ZERODHA";

/// The default WebSocket streaming endpoint.
pub const ZERODHA_WS_URL: &str = "wss://ws.kite.trade";

/// The default REST endpoint.
pub const ZERODHA_HTTP_URL: &str = "https://api.kite.trade";

/// The client ID for the Zerodha adapter.
pub static ZERODHA_CLIENT_ID: LazyLock<ClientId> = LazyLock::new(|| ClientId::new(ZERODHA));

/// The National Stock Exchange of India.
pub static NSE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("NSE"));

/// The Bombay Stock Exchange.
pub static BSE_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new("BSE"));

/// The instrument token of the NIFTY 50 index.
///
/// Index tokens are constants rather than lookups, because the index itself is not returned by the
/// derivative-segment instrument dumps.
pub const NIFTY_INDEX_TOKEN: u32 = 256_265;

/// The instrument token of the SENSEX index.
///
/// Retrieved from the `BSE` segment dump, **not** `INDICES` — the latter returns `AccessDenied`.
pub const SENSEX_INDEX_TOKEN: u32 = 265;

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

//! Error types for the Zerodha WebSocket client.

use thiserror::Error;

/// An error raised while decoding or transporting Zerodha streaming data.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum ZerodhaWsError {
    /// A field ran past the end of the buffer.
    ///
    /// Carries the position rather than only the fact, because a truncated frame is otherwise
    /// indistinguishable from a packet of a different mode.
    #[error("Truncated frame: needed {need} bytes at offset {offset}, buffer is {len} bytes")]
    Truncated {
        /// The offset the read started at.
        offset: usize,
        /// The number of bytes the read needed.
        need: usize,
        /// The total length of the buffer.
        len: usize,
    },

    /// A packet length that corresponds to no documented layout.
    ///
    /// The packet cannot be decoded even partially, because the length *is* the layout selector.
    #[error(
        "Unknown packet length {0} (expected 8, 28, 32, 44 or 184) — the venue may have added a \
         layout"
    )]
    UnknownPacketLength(usize),

    /// A text control frame from the venue could not be parsed as JSON.
    #[error("Invalid control message: {0}")]
    InvalidControlMessage(String),

    /// The venue reported an error on the stream.
    #[error("Venue error: {0}")]
    VenueError(String),
}

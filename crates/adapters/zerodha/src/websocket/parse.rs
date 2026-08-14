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

//! Decoder for the Zerodha Kite Connect binary streaming protocol.
//!
//! # Frame layout
//!
//! Every field is **big-endian**. A binary frame is a count-prefixed sequence of packets:
//!
//! ```text
//! u16  number_of_packets
//! repeated number_of_packets times:
//!   u16  packet_length
//!   u8[packet_length] payload
//! ```
//!
//! A frame shorter than two bytes is a heartbeat and decodes to no ticks.
//!
//! # Packet layout is selected by LENGTH, not by a type tag
//!
//! There is no discriminator field. The packet length alone selects the layout, which is why an
//! unrecognised length cannot be decoded at all rather than partially:
//!
//! | bytes | layout                                          |
//! |-------|-------------------------------------------------|
//! | 8     | LTP: token + last price                          |
//! | 28    | index quote: no traded quantities                |
//! | 32    | index full: index quote + exchange timestamp     |
//! | 44    | quote: traded quantities + OHLC                  |
//! | 184   | full: quote + timestamps + open interest + depth |
//!
//! All prices arrive as integers and are divided by a segment-dependent divisor
//! ([`ZerodhaSegment::price_divisor`]).
//!
//! # Provenance
//!
//! Reimplemented from the layout published in Zerodha's Kite Connect streaming documentation and
//! cross-checked field-by-field against the reference `kiteconnect` Python client (5.2.0)
//! `KiteTicker._parse_binary`. It is a reimplementation, not a translation, and it differs from the
//! reference in three deliberate ways, each of which is a bug in the reference:
//!
//! 1. **Truncated packets are rejected, not silently mis-decoded.** The reference indexes without
//!    bounds checks, so a short frame raises an opaque `struct.error` from inside the parse loop.
//! 2. **Timestamps stay as Unix epoch seconds.** The reference calls `datetime.fromtimestamp()`
//!    with no timezone, producing a naive datetime in the *host's local zone*; read as UTC that is
//!    wrong by the host's offset (5h30m for an IST host). Callers convert explicitly here.
//! 3. **An unknown segment is decoded, not rejected.** Zerodha adds segment codes without notice.

use crate::{
    common::enums::{ZerodhaSegment, ZerodhaTickMode},
    websocket::{
        error::ZerodhaWsError,
        messages::{KiteDepth, KiteDepthEntry, KiteOhlc, KiteTick},
    },
};

/// The number of price levels per side in a full-mode depth ladder.
const DEPTH_LEVELS: usize = 5;
/// The wire size of one depth level: `u32` quantity, `u32` price, `u16` orders, 2 bytes padding.
const DEPTH_ENTRY_LEN: usize = 12;
/// The offset at which the depth ladder starts within a 184-byte full-mode packet.
const DEPTH_OFFSET: usize = 64;

/// Reads a big-endian `u16` at `offset`.
fn be_u16(buf: &[u8], offset: usize) -> Result<u16, ZerodhaWsError> {
    buf.get(offset..offset + 2)
        .and_then(|s| s.try_into().ok())
        .map(u16::from_be_bytes)
        .ok_or(ZerodhaWsError::Truncated {
            offset,
            need: 2,
            len: buf.len(),
        })
}

/// Reads a big-endian `u32` at `offset`.
fn be_u32(buf: &[u8], offset: usize) -> Result<u32, ZerodhaWsError> {
    buf.get(offset..offset + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or(ZerodhaWsError::Truncated {
            offset,
            need: 4,
            len: buf.len(),
        })
}

/// Reads a big-endian `u32` at `offset` and scales it into a price.
fn be_price(buf: &[u8], offset: usize, divisor: f64) -> Result<f64, ZerodhaWsError> {
    Ok(f64::from(be_u32(buf, offset)?) / divisor)
}

/// Splits a binary frame into its constituent packets.
///
/// A frame under two bytes is a heartbeat and yields no packets.
///
/// # Errors
///
/// Returns [`ZerodhaWsError::Truncated`] if a declared packet length runs past the end of the
/// frame. The reference client walks off the end and yields short packets instead, which then
/// decode as a different mode than the venue sent.
pub fn split_packets(frame: &[u8]) -> Result<Vec<&[u8]>, ZerodhaWsError> {
    if frame.len() < 2 {
        return Ok(Vec::new()); // Heartbeat
    }

    let count = be_u16(frame, 0)? as usize;
    let mut packets = Vec::with_capacity(count);
    let mut cursor = 2usize;

    for _ in 0..count {
        let len = be_u16(frame, cursor)? as usize;
        cursor += 2;
        let packet = frame
            .get(cursor..cursor + len)
            .ok_or(ZerodhaWsError::Truncated {
                offset: cursor,
                need: len,
                len: frame.len(),
            })?;
        packets.push(packet);
        cursor += len;
    }

    Ok(packets)
}

/// Decodes a binary frame into zero or more ticks.
///
/// # Errors
///
/// Returns an error if the frame is truncated or if a packet has a length that does not correspond
/// to any documented layout.
pub fn parse_binary(frame: &[u8]) -> Result<Vec<KiteTick>, ZerodhaWsError> {
    split_packets(frame)?.into_iter().map(parse_packet).collect()
}

/// Decodes a single packet, selecting the layout from its length.
///
/// # Errors
///
/// Returns [`ZerodhaWsError::UnknownPacketLength`] for a length with no documented layout, or
/// [`ZerodhaWsError::Truncated`] if a field runs past the end.
pub fn parse_packet(packet: &[u8]) -> Result<KiteTick, ZerodhaWsError> {
    let instrument_token = be_u32(packet, 0)?;
    let segment = ZerodhaSegment::from_instrument_token(instrument_token);
    let divisor = segment.price_divisor();

    let mut tick = KiteTick {
        instrument_token,
        segment,
        tradable: segment.is_tradable(),
        last_price: be_price(packet, 4, divisor)?,
        ..Default::default()
    };

    match packet.len() {
        8 => {
            tick.mode = ZerodhaTickMode::Ltp;
        }
        // Index packets carry OHLC but no traded quantities.
        28 | 32 => {
            tick.mode = if packet.len() == 28 {
                ZerodhaTickMode::Quote
            } else {
                ZerodhaTickMode::Full
            };
            tick.ohlc = Some(KiteOhlc {
                high: be_price(packet, 8, divisor)?,
                low: be_price(packet, 12, divisor)?,
                open: be_price(packet, 16, divisor)?,
                close: be_price(packet, 20, divisor)?,
            });
            if packet.len() == 32 {
                tick.exchange_timestamp = Some(be_u32(packet, 28)?);
            }
        }
        // Tradable instrument packets. Note the OHLC field ORDER differs from the index layout
        // above: open/high/low/close here, high/low/open/close there.
        44 | 184 => {
            tick.mode = if packet.len() == 44 {
                ZerodhaTickMode::Quote
            } else {
                ZerodhaTickMode::Full
            };
            tick.last_traded_quantity = Some(be_u32(packet, 8)?);
            tick.average_traded_price = Some(be_price(packet, 12, divisor)?);
            tick.volume_traded = Some(be_u32(packet, 16)?);
            tick.total_buy_quantity = Some(be_u32(packet, 20)?);
            tick.total_sell_quantity = Some(be_u32(packet, 24)?);
            tick.ohlc = Some(KiteOhlc {
                open: be_price(packet, 28, divisor)?,
                high: be_price(packet, 32, divisor)?,
                low: be_price(packet, 36, divisor)?,
                close: be_price(packet, 40, divisor)?,
            });

            if packet.len() == 184 {
                tick.last_trade_time = Some(be_u32(packet, 44)?);
                tick.oi = Some(be_u32(packet, 48)?);
                tick.oi_day_high = Some(be_u32(packet, 52)?);
                tick.oi_day_low = Some(be_u32(packet, 56)?);
                tick.exchange_timestamp = Some(be_u32(packet, 60)?);
                tick.depth = Some(parse_depth(packet, divisor)?);
            }
        }
        len => return Err(ZerodhaWsError::UnknownPacketLength(len)),
    }

    // The venue does not send `change`; it is derived. A zero previous close (a freshly listed
    // instrument, or an index before its first close) would divide by zero.
    if let Some(ohlc) = tick.ohlc
        && ohlc.close != 0.0
    {
        tick.change = (tick.last_price - ohlc.close) * 100.0 / ohlc.close;
    }

    Ok(tick)
}

/// Decodes the five-deep bid and ask ladders from a 184-byte full-mode packet.
fn parse_depth(packet: &[u8], divisor: f64) -> Result<KiteDepth, ZerodhaWsError> {
    let mut depth = KiteDepth {
        buy: Vec::with_capacity(DEPTH_LEVELS),
        sell: Vec::with_capacity(DEPTH_LEVELS),
    };

    for level in 0..DEPTH_LEVELS * 2 {
        let offset = DEPTH_OFFSET + level * DEPTH_ENTRY_LEN;
        let entry = KiteDepthEntry {
            quantity: be_u32(packet, offset)?,
            price: be_price(packet, offset + 4, divisor)?,
            orders: be_u16(packet, offset + 8)?,
        };
        // The first five entries are bids, the next five asks.
        if level < DEPTH_LEVELS {
            depth.buy.push(entry);
        } else {
            depth.sell.push(entry);
        }
    }

    Ok(depth)
}

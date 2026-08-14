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

//! A per-instrument feed watchdog that asserts the **shape** of the tick stream, not its flow.
//!
//! # What the transport's `idle_timeout_ms` already covers, and what it cannot
//!
//! [`crate::websocket::client`] sets an inbound idle timeout because Kite emits a one-byte
//! heartbeat every ~3 seconds (measured 2.894s–3.105s across 79 heartbeats, live capture
//! 2026-08-14). That timer answers one question: *have any bytes arrived?* A connection carrying
//! nothing but heartbeats satisfies it indefinitely. **It detects a dead socket; it cannot detect
//! a dead feed.** This module covers the second case, and only the second case.
//!
//! # Why counting ticks would be useless here
//!
//! Full mode is **throttled**. A live capture on 2026-08-14 saw 21 ticks in 20 seconds with the
//! exchange timestamp incrementing by exactly 1 — the venue publishes a snapshot roughly once per
//! second and does *not* stream every change. The consequence is the whole reason this module is
//! shaped the way it is:
//!
//! - **Cadence is not a proxy for market activity.** ~1/sec is what a frantically trading
//!   instrument produces and also what a completely idle one produces. A watchdog that treats
//!   "ticks are arriving" as healthy asserts nothing.
//! - **`last_price` is not a proxy for liveness.** The same capture held `last=7811` across three
//!   consecutive ticks while the best ask moved 7814 → 7815. A staleness check keyed on the last
//!   traded price reports a live instrument as dead every time the book moves without a trade —
//!   which, on an option strike, is most of the day.
//!
//! So the only two things worth asserting are: **did anything arrive at all**, and **did what
//! arrived have the shape we subscribed for**.
//!
//! # The failure this exists for: a silent shape downgrade
//!
//! We subscribe [`ZerodhaTickMode::Full`] and expect the 184-byte packet carrying the five-deep
//! ladder. If the venue silently downgrades us to 44-byte `quote` packets, then:
//!
//! - ticks keep arriving at the same ~1/sec cadence,
//! - every counter keeps incrementing,
//! - the socket stays healthy and the idle timer never fires,
//! - and **depth disappears, so [`crate::data::parse::quote_tick_from`] can no longer construct a
//!   single `QuoteTick`** — the strategy goes blind while every liveness signal stays green.
//!
//! A flow-based watchdog is green throughout that. [`FeedHealth::Downgraded`] is the finding.
//!
//! # What this deliberately does NOT report
//!
//! - **An all-zero depth ladder.** Observed on 1 of 4 instruments in a single captured frame — it
//!   is real and it is not rare. A ladder with no non-zero level on a side is a fact about that
//!   instrument's book, not about the feed, and it is indistinguishable at this layer from a
//!   contract nobody is quoting. Alarming on it would produce daily noise on illiquid strikes.
//!   It is **counted** ([`FeedStatus::empty_ladder_ticks`]) so it stays observable, and it never
//!   changes the verdict.
//! - **An index carrying no depth.** The 32-byte index packet decodes to
//!   [`ZerodhaTickMode::Full`] and has no ladder at all — see `PacketLayout::IndexFull` in
//!   [`crate::websocket::parse`]. Asserting "full mode implies depth" unconditionally would put
//!   every index permanently in alarm. The depth assertion is therefore gated on
//!   [`KiteTick::tradable`].
//! - **A richer mode than subscribed.** Receiving `full` where `quote` was asked for costs
//!   nothing and loses nothing. Only a *strictly shallower* shape is a fault.
//! - **Anything outside the trading session** — see the section below.
//!
//! # Time and sessions are INPUTS, never read from a clock
//!
//! Nothing here reads a clock, owns a timer, or spawns a task. `now` is a parameter on every
//! method that needs it. That is not stylistic: a component that reads the clock internally cannot
//! be tested deterministically, and this component's entire value is in verdicts that are hard to
//! reproduce live.
//!
//! The session is an input for a related reason. NSE trades 09:15–15:30 IST and MCX runs to ~23:30
//! IST, so a single hard-coded window is wrong for a connection carrying both — and *any*
//! hard-coded window is wrong on the ~15 NSE holidays a year, on a muhurat session, and on an
//! early close. A watchdog that fires all weekend gets muted, **and a muted watchdog is strictly
//! worse than no watchdog, because its silence then means nothing.** Rather than encode a calendar
//! this crate has no data for, [`FeedWatchdog::report_with`] asks the caller per token. This crate
//! also carries no timezone dependency, so the arithmetic is not available here in any case.
//!
//! ⚠️ **Ceiling of the session gate**: a feed that dies at 15:29 and is still dead at 15:31 stops
//! being reported at the close. Suppression is by design, but it means an alarm must be *acted on*
//! while it is raised; the state is not retained as a verdict after hours.
//!
//! # Re-arming
//!
//! Because this never sees a clock, it cannot notice the session opening or a reconnect completing
//! — both of which produce a legitimate gap in arrivals that is not a fault.
//! [`FeedWatchdog::rearm`] resets the silence origin for every watched token without discarding
//! counters, and the caller is expected to invoke it at each session open and after each
//! successful reconnect replay. Failing to call it does not hide a real fault; it produces a
//! spurious [`FeedHealth::Stale`] shortly after the gap.

use std::collections::BTreeMap;

use nautilus_core::UnixNanos;

use crate::{
    common::enums::ZerodhaTickMode,
    websocket::messages::{KiteDepth, KiteTick},
};

/// Nanoseconds in one second.
const NANOS_PER_SEC: u64 = 1_000_000_000;

/// How long a watched instrument may produce nothing before it is called [`FeedHealth::Stale`].
///
/// # Why 60s when the measured inter-arrival is ~1s
///
/// The 2026-08-14 capture gives a **mean**: 21 ticks in 20 seconds, so ~1.05s between snapshots.
/// It does not give a **tail**. Twenty seconds of one instrument's traffic says nothing about the
/// 99.9th percentile, about the first seconds after the open, about a circuit-limit halt, or about
/// the gap a reconnect replay leaves behind. A threshold set near the mean would convert every
/// unmeasured tail event into a false alarm, and false alarms are how a watchdog gets muted.
///
/// 60s is ~57 consecutive missed throttle windows. Nothing the throttle itself does can plausibly
/// reach that, and it still catches a dead feed inside a single one-minute bar.
///
/// ⚠️ **This number is a judgement about a distribution that has not been measured.** It should be
/// re-derived from a full-session capture of the worst-behaved instrument in the subscription, not
/// from a 20-second sample. Until then it is deliberately loose in the safe direction.
///
/// ⚠️ **Load-bearing premise**: that the venue keeps emitting throttled snapshots for a *completely
/// idle* instrument. Every staleness verdict here rests on it. If the venue instead falls silent
/// on instruments with no activity, this threshold produces false alarms on illiquid strikes and
/// the check would have to be narrowed to instruments known to be trading.
pub const DEFAULT_STALE_AFTER_NS: u64 = 60 * NANOS_PER_SEC;

/// How long a shallower-than-subscribed shape must persist before it is called a downgrade.
///
/// # Why this is not zero, despite a downgrade being a shape fault rather than a rate fault
///
/// A bare `subscribe` leaves a token in `quote` mode and the requested mode arrives in a *second*
/// message — see [`crate::websocket::subscription`]. So `quote`-shaped packets for a `full`
/// subscription are **legitimate** in the window between those two messages, and on every reconnect
/// replay. Firing on a single shallow tick would alarm on correct behaviour every time we connect,
/// which is precisely the noise that gets a watchdog muted.
///
/// 10s is ~10 consecutive shallow snapshots at the measured cadence. The subscribe→mode pair is one
/// round trip on an already-open socket, and the transport's own 10s idle timeout bounds how
/// unresponsive that socket can be while still being considered alive, so 10s of *continuously*
/// shallow packets cannot be explained by the mode message still being in flight.
///
/// It is also well inside [`DEFAULT_STALE_AFTER_NS`], so a downgrade is always reported as a
/// downgrade rather than eventually being overtaken by a staleness verdict.
pub const DEFAULT_DOWNGRADE_GRACE_NS: u64 = 10 * NANOS_PER_SEC;

/// Whether an instrument's venue is currently trading, as judged by the caller.
///
/// This crate has no exchange calendar and no timezone dependency, so it cannot decide this. See
/// the module docs for why encoding a fixed window here would be actively harmful.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum SessionState {
    /// The venue is trading; assertions apply.
    Open,
    /// The venue is closed; every assertion is suppressed.
    #[default]
    Closed,
}

/// The verdict for one watched instrument.
///
/// [`Self::Stale`] and [`Self::Downgraded`] are the two alarm states and they are deliberately not
/// collapsed into one "unhealthy": they have different causes and different remedies. Stale means
/// the venue stopped sending; downgraded means it is still sending, punctually, in a shape that
/// cannot be used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedHealth {
    /// The venue is closed. No assertion is made — see the ceiling noted in the module docs.
    OutOfSession,
    /// Watched, in session, and nothing has arrived **since the token was watched or last
    /// re-armed**, with the staleness budget not yet elapsed.
    ///
    /// Distinct from [`Self::Healthy`] because nothing has been asserted about this instrument
    /// yet, and distinct from [`Self::Stale`] because a subscription that has simply not started
    /// producing is not yet evidence of anything.
    ///
    /// Note the scope: after [`FeedWatchdog::rearm`] this returns even for an instrument that
    /// delivered before the re-arm, because evidence from before a reconnect says nothing about
    /// the connection after it.
    AwaitingFirstTick,
    /// Ticks are arriving within budget, in at least the subscribed mode, with the depth the
    /// subscription implies.
    Healthy,
    /// Nothing has arrived for at least the staleness budget.
    Stale {
        /// Nanoseconds since the last arrival, or since the watch/re-arm if nothing ever arrived.
        silent_for_ns: u64,
        /// `false` when this instrument has **never** produced a tick.
        ///
        /// Worth keeping separate: a subscription that never started is the signature of the
        /// venue silently dropping tokens past its per-connection cap (see
        /// [`crate::websocket::subscription::MAX_TOKENS_PER_CONNECTION`]), whereas a feed that
        /// delivered and stopped points at the venue or the session, not at our request.
        ever_observed: bool,
    },
    /// Ticks are arriving on time but in a shallower shape than was subscribed.
    ///
    /// This is the state the module exists for. Note that `expected == observed` is possible and
    /// is **not** a contradiction: a tradable instrument subscribed in `full` mode that delivers
    /// full-mode packets carrying no ladder is a downgrade with `depth_present: false`.
    Downgraded {
        /// The mode the subscription asked for.
        expected: ZerodhaTickMode,
        /// The mode the most recent packet's length implied.
        observed: ZerodhaTickMode,
        /// Whether the most recent packet carried a depth ladder at all.
        depth_present: bool,
    },
}

impl FeedHealth {
    /// Returns whether this verdict should raise an operational alarm.
    ///
    /// [`Self::OutOfSession`] and [`Self::AwaitingFirstTick`] are states, not faults.
    #[must_use]
    pub const fn is_alarm(&self) -> bool {
        matches!(self, Self::Stale { .. } | Self::Downgraded { .. })
    }
}

/// The verdict for one instrument together with the evidence behind it.
///
/// The counters are diagnostic. `empty_ladder_ticks` in particular never influences `health` — see
/// the module docs on why an all-zero ladder is a fact about the book rather than about the feed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeedStatus {
    /// The Zerodha instrument token.
    pub token: u32,
    /// The verdict.
    pub health: FeedHealth,
    /// The mode the subscription asked for.
    pub expected_mode: ZerodhaTickMode,
    /// The mode of the most recent packet, if any has arrived.
    pub last_mode: Option<ZerodhaTickMode>,
    /// Arrival time of the most recent packet, if any has arrived.
    pub last_seen: Option<UnixNanos>,
    /// Total packets observed for this token since it was watched.
    pub ticks_observed: u64,
    /// Packets whose shape fell short of the subscription.
    pub shallow_ticks: u64,
    /// Full-mode packets whose ladder had no non-zero level on one or both sides.
    ///
    /// Diagnostic only. Never an alarm.
    pub empty_ladder_ticks: u64,
}

/// Per-token bookkeeping. Private: every field is derived from observations and none of it is
/// meaningful without the watchdog's thresholds.
#[derive(Clone, Copy, Debug)]
struct TokenState {
    expected_mode: ZerodhaTickMode,
    /// The point silence is measured from when nothing has arrived since — set at watch time and
    /// reset by [`FeedWatchdog::rearm`].
    silence_origin: UnixNanos,
    last_seen: Option<UnixNanos>,
    last_mode: Option<ZerodhaTickMode>,
    last_depth_present: bool,
    /// When the current unbroken run of shallow packets began; cleared by any adequate packet.
    shallow_since: Option<UnixNanos>,
    ticks_observed: u64,
    shallow_ticks: u64,
    empty_ladder_ticks: u64,
}

/// Ranks the streaming modes by how much shape they carry, so "shallower" is decidable.
///
/// [`ZerodhaTickMode`] deliberately does **not** derive `Ord` — [`crate::websocket::subscription`]
/// relies on that — so `<` on the enum would not compile. Ranking locally keeps the ordering an
/// implementation detail of this check rather than widening the shared enum's API.
const fn mode_rank(mode: ZerodhaTickMode) -> u8 {
    match mode {
        ZerodhaTickMode::Ltp => 0,
        ZerodhaTickMode::Quote => 1,
        ZerodhaTickMode::Full => 2,
    }
}

/// Returns whether a ladder has at least one non-zero-priced level on **both** sides.
///
/// Mirrors the rule in [`crate::data::parse::top_of_book`] — an unfilled slot arrives as a zero
/// price, and a side made entirely of zeros yields no usable top of book — but is reimplemented in
/// four lines rather than imported, to keep this module's verdicts independent of a mapping layer
/// that is free to change its API.
///
/// Only ever feeds the [`FeedStatus::empty_ladder_ticks`] counter; it never decides a verdict.
fn ladder_is_two_sided(depth: &KiteDepth) -> bool {
    let has_bid = depth.buy.iter().any(|level| level.price > 0.0);
    let has_ask = depth.sell.iter().any(|level| level.price > 0.0);

    has_bid && has_ask
}

/// Returns whether `tick` falls short of what `expected` implies.
///
/// Two ways to fall short, and they are separate faults:
///
/// 1. the packet length implied a strictly shallower mode (the 184 → 44 byte downgrade);
/// 2. a **tradable** instrument subscribed in `full` delivered a full-length packet with no ladder.
///
/// The `tradable` gate on (2) is load-bearing: the 32-byte index packet also decodes to
/// [`ZerodhaTickMode::Full`] and legitimately carries no depth, so an ungated check would hold
/// every index in permanent alarm.
fn shape_falls_short(expected: ZerodhaTickMode, tick: &KiteTick, depth_present: bool) -> bool {
    if mode_rank(tick.mode) < mode_rank(expected) {
        return true;
    }

    expected == ZerodhaTickMode::Full && tick.tradable && !depth_present
}

/// Watches the *shape* of the tick stream per subscribed instrument.
///
/// Fed observations and a current time; owns no clock and no task. Construct with [`Self::new`],
/// register with [`Self::watch`], feed [`Self::observe`], and read [`Self::report`].
///
/// # What it does not know
///
/// It has no view of the subscription request itself. The expected mode is an **input** —
/// [`crate::websocket::subscription::SubscriptionState`] already tracks token→mode and is the
/// source of truth for what was asked for. Duplicating it here would create a second source of
/// truth with nothing reconciling the two.
#[derive(Clone, Debug)]
pub struct FeedWatchdog {
    stale_after_ns: u64,
    downgrade_grace_ns: u64,
    /// A `BTreeMap` rather than a `HashMap` so [`Self::report`] is ordered by token and stable
    /// between runs — an alarm report that reshuffles itself is needlessly hard to diff.
    tokens: BTreeMap<u32, TokenState>,
}

impl Default for FeedWatchdog {
    fn default() -> Self {
        Self::new()
    }
}

impl FeedWatchdog {
    /// Creates a watchdog with [`DEFAULT_STALE_AFTER_NS`] and [`DEFAULT_DOWNGRADE_GRACE_NS`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            stale_after_ns: DEFAULT_STALE_AFTER_NS,
            downgrade_grace_ns: DEFAULT_DOWNGRADE_GRACE_NS,
            tokens: BTreeMap::new(),
        }
    }

    /// Creates a watchdog with explicit thresholds, in nanoseconds.
    ///
    /// Both defaults are judgements about distributions that have not been fully measured; a
    /// caller with a session-long capture of its own instruments should prefer its own numbers.
    #[must_use]
    pub const fn with_thresholds(stale_after_ns: u64, downgrade_grace_ns: u64) -> Self {
        Self {
            stale_after_ns,
            downgrade_grace_ns,
            tokens: BTreeMap::new(),
        }
    }

    /// Returns the staleness budget in nanoseconds.
    #[must_use]
    pub const fn stale_after_ns(&self) -> u64 {
        self.stale_after_ns
    }

    /// Returns the downgrade grace in nanoseconds.
    #[must_use]
    pub const fn downgrade_grace_ns(&self) -> u64 {
        self.downgrade_grace_ns
    }

    /// Starts watching `token`, expecting `expected_mode`, measuring silence from `now`.
    ///
    /// Watching a token that is already watched updates the expected mode and **resets** its
    /// silence origin and shallow run, discarding the counters. That matches the caller's
    /// situation exactly: re-issuing a `mode` message makes every prior observation an
    /// observation of a different subscription.
    pub fn watch(&mut self, token: u32, expected_mode: ZerodhaTickMode, now: UnixNanos) {
        self.tokens.insert(
            token,
            TokenState {
                expected_mode,
                silence_origin: now,
                last_seen: None,
                last_mode: None,
                last_depth_present: false,
                shallow_since: None,
                ticks_observed: 0,
                shallow_ticks: 0,
                empty_ladder_ticks: 0,
            },
        );
    }

    /// Stops watching `token`. Unknown tokens are ignored, matching
    /// [`crate::websocket::subscription::SubscriptionState::unsubscribe`].
    pub fn unwatch(&mut self, token: u32) {
        self.tokens.remove(&token);
    }

    /// Stops watching everything.
    pub fn clear(&mut self) {
        self.tokens.clear();
    }

    /// Resets the silence origin of every watched token to `now`, keeping modes and counters.
    ///
    /// Call this at each session open and after each successful reconnect replay. Both leave a
    /// legitimate gap in arrivals that this component cannot distinguish from a dead feed, because
    /// it deliberately cannot see a clock or the transport.
    ///
    /// The shallow run is also cleared: a replay re-enters through the subscribe→mode window where
    /// shallow packets are expected, so carrying the previous run across it would collapse the
    /// grace period that window needs.
    pub fn rearm(&mut self, now: UnixNanos) {
        for state in self.tokens.values_mut() {
            state.silence_origin = now;
            state.shallow_since = None;
        }
    }

    /// Records that `tick` arrived at `now`.
    ///
    /// `now` is the **local receive time**, not `tick.exchange_timestamp`. Using the venue stamp
    /// would make staleness unmeasurable in exactly the case that matters: a feed frozen by the
    /// venue repeats or omits its own timestamps, so a check keyed on them is keyed on the thing
    /// under suspicion.
    ///
    /// Ticks for tokens that are not watched are ignored. A tick arriving for a token we never
    /// subscribed means our subscription state and the venue's disagree, which is a real finding
    /// but belongs to the subscription layer, not here.
    pub fn observe(&mut self, tick: &KiteTick, now: UnixNanos) {
        let Some(state) = self.tokens.get_mut(&tick.instrument_token) else {
            return;
        };

        let depth_present = match tick.depth.as_ref() {
            Some(depth) => {
                if !ladder_is_two_sided(depth) {
                    state.empty_ladder_ticks = state.empty_ladder_ticks.saturating_add(1);
                }
                true
            }
            None => false,
        };

        state.ticks_observed = state.ticks_observed.saturating_add(1);
        state.last_seen = Some(now);
        state.last_mode = Some(tick.mode);
        state.last_depth_present = depth_present;

        if shape_falls_short(state.expected_mode, tick, depth_present) {
            state.shallow_ticks = state.shallow_ticks.saturating_add(1);
            state.shallow_since.get_or_insert(now);
        } else {
            state.shallow_since = None;
        }
    }

    /// Returns the verdict for `token`, or `None` if it is not watched.
    #[must_use]
    pub fn health(&self, token: u32, now: UnixNanos, session: SessionState) -> Option<FeedHealth> {
        self.tokens
            .get(&token)
            .map(|state| self.health_of(state, now, session))
    }

    /// Returns the status of every watched token, ordered by token.
    ///
    /// `session` applies to all of them. Use [`Self::report_with`] when the connection carries
    /// instruments from venues whose hours differ — NSE closes at 15:30 IST while MCX runs to
    /// ~23:30, so one flag for both is wrong for roughly eight hours a day.
    #[must_use]
    pub fn report(&self, now: UnixNanos, session: SessionState) -> Vec<FeedStatus> {
        self.report_with(now, &|_| session)
    }

    /// Returns the status of every watched token, ordered by token, asking `session` per token.
    ///
    /// Taking a closure rather than a calendar is the point: the exchange hours, the holiday list,
    /// muhurat sessions and early closes are all data this crate does not have, and a wrong
    /// hard-coded window produces exactly the weekend noise that gets a watchdog muted.
    #[must_use]
    pub fn report_with(
        &self,
        now: UnixNanos,
        session: &dyn Fn(u32) -> SessionState,
    ) -> Vec<FeedStatus> {
        self.tokens
            .iter()
            .map(|(&token, state)| FeedStatus {
                token,
                health: self.health_of(state, now, session(token)),
                expected_mode: state.expected_mode,
                last_mode: state.last_mode,
                last_seen: state.last_seen,
                ticks_observed: state.ticks_observed,
                shallow_ticks: state.shallow_ticks,
                empty_ladder_ticks: state.empty_ladder_ticks,
            })
            .collect()
    }

    /// Returns only the statuses whose verdict is an alarm, ordered by token.
    #[must_use]
    pub fn alarms(&self, now: UnixNanos, session: &dyn Fn(u32) -> SessionState) -> Vec<FeedStatus> {
        self.report_with(now, session)
            .into_iter()
            .filter(|status| status.health.is_alarm())
            .collect()
    }

    /// Returns the number of watched tokens.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns whether nothing is watched.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Decides the verdict for one token.
    ///
    /// # Ordering of the checks, which is load-bearing
    ///
    /// Session first (an alarm nobody can act on trains people to ignore alarms), then staleness,
    /// then downgrade. Staleness outranks downgrade because a feed that has gone silent is no
    /// longer in any mode; reporting it as "downgraded" would name the shape of the last packet
    /// received rather than the condition now.
    fn health_of(&self, state: &TokenState, now: UnixNanos, session: SessionState) -> FeedHealth {
        if session == SessionState::Closed {
            return FeedHealth::OutOfSession;
        }

        // Both the staleness clock and the "have we heard anything yet" question are keyed on the
        // SAME origin, and that is deliberate. Keying `AwaitingFirstTick` on `last_seen.is_none()`
        // alone would make a re-armed token report `Healthy` on the strength of an arrival from
        // before the reconnect — a green verdict backed by evidence that predates the gap.
        let seen_since_origin = match state.last_seen {
            Some(seen) => seen >= state.silence_origin,
            None => false,
        };

        let origin = match state.last_seen {
            Some(seen) if seen_since_origin => seen,
            _ => state.silence_origin,
        };

        // `duration_since` yields `None` on underflow rather than panicking, which a bare `-` on
        // `UnixNanos` would do. `now` running behind a recorded stamp is not this component's
        // problem to diagnose, so it reads as zero elapsed.
        let silent_for_ns = now.duration_since(&origin).unwrap_or(0);

        if silent_for_ns >= self.stale_after_ns {
            return FeedHealth::Stale {
                silent_for_ns,
                // Lifetime-scoped, NOT origin-scoped: a token that has never produced anything at
                // all is a different diagnosis from one that produced and then went quiet across a
                // re-arm, and re-arming must not erase that.
                ever_observed: state.last_seen.is_some(),
            };
        }

        if !seen_since_origin {
            return FeedHealth::AwaitingFirstTick;
        }

        match state.shallow_since {
            Some(since)
                if now.duration_since(&since).unwrap_or(0) >= self.downgrade_grace_ns =>
            {
                FeedHealth::Downgraded {
                    expected: state.expected_mode,
                    // `shallow_since` is only ever set inside `observe`, which sets `last_mode` on
                    // the same path, so the fallback is unreachable; it exists to keep this
                    // infallible rather than to describe a real case.
                    observed: state.last_mode.unwrap_or(state.expected_mode),
                    depth_present: state.last_depth_present,
                }
            }
            _ => FeedHealth::Healthy,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    // `KiteDepth`, `KiteTick`, `ZerodhaTickMode` and `UnixNanos` all arrive through `super::*`;
    // re-importing them here would shadow the glob and read as though they were different types.
    use super::*;
    use crate::{common::enums::ZerodhaSegment, websocket::messages::KiteDepthEntry};

    const NIFTY_50: u32 = 256_265;
    const RELIANCE: u32 = 738_561;

    fn at(seconds: u64) -> UnixNanos {
        UnixNanos::from_seconds(seconds)
    }

    fn level(price: f64, quantity: u32) -> KiteDepthEntry {
        KiteDepthEntry {
            quantity,
            price,
            orders: 1,
        }
    }

    /// A five-deep ladder with one real level a side and the rest unfilled, as the venue sends it.
    fn book(bid: f64, ask: f64) -> KiteDepth {
        KiteDepth {
            buy: vec![level(bid, 100), level(0.0, 0)],
            sell: vec![level(ask, 100), level(0.0, 0)],
        }
    }

    /// A 184-byte-shaped tick: tradable, full mode, carrying a ladder.
    fn full_tick(token: u32, last_price: f64, depth: KiteDepth) -> KiteTick {
        KiteTick {
            instrument_token: token,
            segment: ZerodhaSegment::Nse,
            tradable: true,
            mode: ZerodhaTickMode::Full,
            last_price,
            depth: Some(depth),
            ..Default::default()
        }
    }

    /// A 44-byte-shaped tick: tradable, quote mode, no ladder. This is the downgrade payload.
    fn quote_mode_tick(token: u32, last_price: f64) -> KiteTick {
        KiteTick {
            instrument_token: token,
            segment: ZerodhaSegment::Nse,
            tradable: true,
            mode: ZerodhaTickMode::Quote,
            last_price,
            depth: None,
            ..Default::default()
        }
    }

    /// A 32-byte-shaped index tick: NOT tradable, full mode, and legitimately no ladder.
    fn index_full_tick(token: u32, last_price: f64) -> KiteTick {
        KiteTick {
            instrument_token: token,
            segment: ZerodhaSegment::Indices,
            tradable: false,
            mode: ZerodhaTickMode::Full,
            last_price,
            depth: None,
            ..Default::default()
        }
    }

    fn watching_full(token: u32, from: u64) -> FeedWatchdog {
        let mut watchdog = FeedWatchdog::new();
        watchdog.watch(token, ZerodhaTickMode::Full, at(from));
        watchdog
    }

    #[rstest]
    fn test_a_fresh_watch_is_awaiting_not_stale() {
        let watchdog = watching_full(RELIANCE, 0);

        assert_eq!(
            watchdog.health(RELIANCE, at(5), SessionState::Open),
            Some(FeedHealth::AwaitingFirstTick),
            "a subscription that has not started producing is not yet evidence of a fault",
        );
    }

    #[rstest]
    fn test_a_subscription_that_never_produces_goes_stale_and_says_so() {
        let watchdog = watching_full(RELIANCE, 0);

        assert_eq!(
            watchdog.health(RELIANCE, at(60), SessionState::Open),
            Some(FeedHealth::Stale {
                silent_for_ns: 60 * NANOS_PER_SEC,
                ever_observed: false,
            }),
            "`ever_observed: false` is the signature of a token the venue silently dropped",
        );
    }

    #[rstest]
    #[case(59, false)]
    #[case(60, true)]
    fn test_the_staleness_budget_is_inclusive(#[case] elapsed: u64, #[case] expect_stale: bool) {
        let watchdog = watching_full(RELIANCE, 0);
        let health = watchdog
            .health(RELIANCE, at(elapsed), SessionState::Open)
            .expect("token is watched");

        assert_eq!(matches!(health, FeedHealth::Stale { .. }), expect_stale);
    }

    #[rstest]
    fn test_ticks_at_the_throttled_cadence_are_healthy() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(30), SessionState::Open),
            Some(FeedHealth::Healthy),
        );
    }

    // THE DISCRIMINATING TEST FOR STALENESS. `last_price` is frozen at the measured 7811 for 90
    // seconds while the best ask walks 7814 -> 7815 -> 7816..., exactly as the 2026-08-14 capture
    // showed. A watchdog keyed on the last traded price sees 90s of no change against a 60s budget
    // and calls this instrument dead. It is not dead; it is an instrument whose book is moving and
    // whose last trade has not reprinted, which on an option strike is most of the day.
    #[rstest]
    fn test_a_frozen_last_price_with_a_moving_book_is_healthy() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=90 {
            let ask = 7_814.0 + f64::from(u32::try_from(second).unwrap_or(0));
            watchdog.observe(&full_tick(RELIANCE, 7_811.0, book(7_810.0, ask)), at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(90), SessionState::Open),
            Some(FeedHealth::Healthy),
            "staleness is keyed on ARRIVAL; a price-keyed check reports this live feed as dead",
        );
    }

    // THE DISCRIMINATING TEST FOR THE DOWNGRADE. Ticks never stop and never slow: one per second
    // for the whole run, first as 184-byte full packets and then as 44-byte quote packets. Arrival
    // counters keep incrementing, the socket stays healthy, the idle timer never fires -- and no
    // `QuoteTick` can be constructed any more because there is no book. Every flow-based watchdog
    // passes this. This one must not.
    #[rstest]
    fn test_a_sustained_downgrade_is_caught_while_the_cadence_stays_perfect() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(second));
        }

        for second in 31..=60 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        let status = watchdog
            .report(at(60), SessionState::Open)
            .pop()
            .expect("one token is watched");

        assert_eq!(status.ticks_observed, 60, "flow never faltered: 60 ticks in 60s");
        assert_eq!(status.shallow_ticks, 30);
        assert_eq!(
            status.health,
            FeedHealth::Downgraded {
                expected: ZerodhaTickMode::Full,
                observed: ZerodhaTickMode::Quote,
                depth_present: false,
            },
        );
        assert!(status.health.is_alarm());
    }

    // The subscribe->mode window. A bare `subscribe` lands in `quote` mode and the requested mode
    // arrives in a second message, so shallow packets immediately after connecting are CORRECT
    // behaviour. Firing on the first one would alarm on every reconnect.
    #[rstest]
    #[case(1)]
    #[case(9)]
    fn test_a_brief_shallow_run_inside_the_grace_is_not_a_downgrade(#[case] shallow_secs: u64) {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=shallow_secs {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(shallow_secs), SessionState::Open),
            Some(FeedHealth::Healthy),
            "the mode message is still plausibly in flight at this point",
        );
    }

    #[rstest]
    fn test_a_downgrade_that_recovers_clears_the_alarm() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        assert!(
            watchdog
                .health(RELIANCE, at(30), SessionState::Open)
                .expect("token is watched")
                .is_alarm(),
        );

        watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(31));

        assert_eq!(
            watchdog.health(RELIANCE, at(31), SessionState::Open),
            Some(FeedHealth::Healthy),
            "one adequate packet ends the shallow run; the alarm must not latch",
        );
    }

    #[rstest]
    fn test_silence_after_healthy_ticks_is_stale_with_ever_observed_true() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(10));

        assert_eq!(
            watchdog.health(RELIANCE, at(75), SessionState::Open),
            Some(FeedHealth::Stale {
                silent_for_ns: 65 * NANOS_PER_SEC,
                ever_observed: true,
            }),
        );
    }

    // Staleness outranks the downgrade: a feed that has gone silent is not in any mode, and naming
    // the shape of the last packet received would describe the past rather than the condition now.
    #[rstest]
    fn test_a_stale_feed_that_was_downgraded_reports_stale() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        let health = watchdog
            .health(RELIANCE, at(120), SessionState::Open)
            .expect("token is watched");

        assert_eq!(
            health,
            FeedHealth::Stale {
                silent_for_ns: 90 * NANOS_PER_SEC,
                ever_observed: true,
            },
        );
    }

    // A muted watchdog is worse than no watchdog. Every alarm state is suppressed out of session.
    #[rstest]
    #[case(30)]
    #[case(600)]
    #[case(86_400)]
    fn test_out_of_session_suppresses_every_alarm(#[case] elapsed: u64) {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=20 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(elapsed), SessionState::Closed),
            Some(FeedHealth::OutOfSession),
        );
    }

    // THE INDEX TRAP. The 32-byte index packet decodes to FULL mode and carries no ladder at all.
    // A rule of "full mode implies depth" holds NIFTY in permanent alarm from the first tick.
    #[rstest]
    fn test_an_index_in_full_mode_is_healthy_without_any_depth() {
        let mut watchdog = watching_full(NIFTY_50, 0);

        for second in 1..=30 {
            watchdog.observe(&index_full_tick(NIFTY_50, 24_500.0), at(second));
        }

        assert_eq!(
            watchdog.health(NIFTY_50, at(30), SessionState::Open),
            Some(FeedHealth::Healthy),
            "indices carry no book; the depth assertion is gated on `tradable`",
        );
    }

    // The counterpart: a TRADABLE instrument in full mode with no ladder is a downgrade even
    // though the mode itself matches, which is why `expected == observed` is representable.
    #[rstest]
    fn test_a_tradable_full_tick_carrying_no_ladder_is_a_downgrade() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            let mut tick = full_tick(RELIANCE, 100.0, book(99.5, 100.5));
            tick.depth = None;
            watchdog.observe(&tick, at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(30), SessionState::Open),
            Some(FeedHealth::Downgraded {
                expected: ZerodhaTickMode::Full,
                observed: ZerodhaTickMode::Full,
                depth_present: false,
            }),
        );
    }

    // An all-zero ladder was seen on 1 of 4 instruments in a single captured frame. It is a fact
    // about that book, not about the feed, and alarming on it would fire daily on illiquid
    // strikes. Counted, never escalated.
    #[rstest]
    fn test_an_all_zero_ladder_is_counted_but_stays_healthy() {
        let mut watchdog = watching_full(RELIANCE, 0);
        let empty = KiteDepth {
            buy: vec![level(0.0, 0), level(0.0, 0)],
            sell: vec![level(0.0, 0), level(0.0, 0)],
        };

        for second in 1..=30 {
            watchdog.observe(&full_tick(RELIANCE, 100.0, empty.clone()), at(second));
        }

        let status = watchdog
            .report(at(30), SessionState::Open)
            .pop()
            .expect("one token is watched");

        assert_eq!(status.health, FeedHealth::Healthy);
        assert_eq!(status.empty_ladder_ticks, 30);
        assert_eq!(status.shallow_ticks, 0, "an empty book is not a shape fault");
    }

    // Receiving MORE shape than was subscribed costs nothing and loses nothing.
    #[rstest]
    #[case(ZerodhaTickMode::Ltp)]
    #[case(ZerodhaTickMode::Quote)]
    fn test_a_richer_mode_than_subscribed_is_not_a_downgrade(#[case] expected: ZerodhaTickMode) {
        let mut watchdog = FeedWatchdog::new();
        watchdog.watch(RELIANCE, expected, at(0));

        for second in 1..=30 {
            watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(30), SessionState::Open),
            Some(FeedHealth::Healthy),
        );
    }

    #[rstest]
    #[case(ZerodhaTickMode::Quote, ZerodhaTickMode::Ltp)]
    #[case(ZerodhaTickMode::Full, ZerodhaTickMode::Ltp)]
    fn test_every_strictly_shallower_mode_is_a_downgrade(
        #[case] expected: ZerodhaTickMode,
        #[case] observed: ZerodhaTickMode,
    ) {
        let mut watchdog = FeedWatchdog::new();
        watchdog.watch(RELIANCE, expected, at(0));
        let mut tick = quote_mode_tick(RELIANCE, 100.0);
        tick.mode = observed;

        for second in 1..=30 {
            watchdog.observe(&tick, at(second));
        }

        assert_eq!(
            watchdog.health(RELIANCE, at(30), SessionState::Open),
            Some(FeedHealth::Downgraded {
                expected,
                observed,
                depth_present: false,
            }),
        );
    }

    // A reconnect replay or a session open leaves a legitimate gap. Re-arming forgives the gap
    // without forgetting what was already seen.
    #[rstest]
    fn test_rearm_resets_the_silence_origin_and_keeps_the_counters() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(10));

        assert!(
            watchdog
                .health(RELIANCE, at(600), SessionState::Open)
                .expect("token is watched")
                .is_alarm(),
        );

        watchdog.rearm(at(600));

        let status = watchdog
            .report(at(610), SessionState::Open)
            .pop()
            .expect("one token is watched");

        assert_eq!(status.health, FeedHealth::AwaitingFirstTick);
        assert_eq!(status.ticks_observed, 1, "re-arming must not discard evidence");
        assert_eq!(status.last_seen, Some(at(10)));
    }

    // A re-arm must not be undone by an arrival that PRECEDED it.
    #[rstest]
    fn test_rearm_wins_over_an_older_arrival() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.observe(&full_tick(RELIANCE, 100.0, book(99.5, 100.5)), at(10));
        watchdog.rearm(at(500));

        assert_eq!(
            watchdog.health(RELIANCE, at(530), SessionState::Open),
            Some(FeedHealth::AwaitingFirstTick),
            "silence is measured from the LATER of the last arrival and the last re-arm",
        );
    }

    #[rstest]
    fn test_rearm_clears_a_shallow_run_so_the_replay_window_gets_its_grace() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        watchdog.rearm(at(30));
        watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(31));

        assert_eq!(
            watchdog.health(RELIANCE, at(31), SessionState::Open),
            Some(FeedHealth::Healthy),
            "a replay re-enters the subscribe->mode window and needs the full grace again",
        );
    }

    #[rstest]
    fn test_observing_an_unwatched_token_is_ignored() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.observe(&full_tick(NIFTY_50, 24_500.0, book(1.0, 2.0)), at(5));

        assert_eq!(watchdog.len(), 1);
        assert_eq!(watchdog.health(NIFTY_50, at(5), SessionState::Open), None);
    }

    #[rstest]
    fn test_unwatch_removes_the_token_and_is_forgiving() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.unwatch(RELIANCE);
        watchdog.unwatch(999_999);

        assert!(watchdog.is_empty());
        assert!(watchdog.report(at(0), SessionState::Open).is_empty());
    }

    #[rstest]
    fn test_clear_drops_every_watched_token() {
        let mut watchdog = watching_full(RELIANCE, 0);
        watchdog.watch(NIFTY_50, ZerodhaTickMode::Full, at(0));
        watchdog.clear();

        assert_eq!(watchdog.len(), 0);
        assert!(watchdog.report(at(600), SessionState::Open).is_empty());
    }

    #[rstest]
    fn test_rewatching_resets_the_state_for_the_new_subscription() {
        let mut watchdog = watching_full(RELIANCE, 0);

        for second in 1..=30 {
            watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(second));
        }

        watchdog.watch(RELIANCE, ZerodhaTickMode::Quote, at(30));

        let status = watchdog
            .report(at(31), SessionState::Open)
            .pop()
            .expect("one token is watched");

        assert_eq!(status.expected_mode, ZerodhaTickMode::Quote);
        assert_eq!(status.health, FeedHealth::AwaitingFirstTick);
        assert_eq!(
            status.ticks_observed, 0,
            "observations of the previous subscription say nothing about the new one",
        );
    }

    #[rstest]
    fn test_report_is_ordered_by_token_and_covers_everything_watched() {
        let mut watchdog = FeedWatchdog::new();
        watchdog.watch(RELIANCE, ZerodhaTickMode::Full, at(0));
        watchdog.watch(NIFTY_50, ZerodhaTickMode::Full, at(0));
        watchdog.watch(408_065, ZerodhaTickMode::Ltp, at(0));

        let tokens: Vec<u32> = watchdog
            .report(at(1), SessionState::Open)
            .iter()
            .map(|status| status.token)
            .collect();

        assert_eq!(tokens, vec![NIFTY_50, 408_065, RELIANCE]);
    }

    // Mixed venues on one connection: NSE has closed for the day while MCX is still trading, so a
    // single session flag would be wrong for one of them.
    #[rstest]
    fn test_report_with_asks_per_token_so_nse_and_mcx_can_differ() {
        let mut watchdog = FeedWatchdog::new();
        watchdog.watch(RELIANCE, ZerodhaTickMode::Full, at(0));
        watchdog.watch(NIFTY_50, ZerodhaTickMode::Full, at(0));

        let session = |token: u32| match token {
            RELIANCE => SessionState::Closed,
            _ => SessionState::Open,
        };

        let alarms = watchdog.alarms(at(600), &session);

        assert_eq!(alarms.len(), 1, "only the venue that is open may raise an alarm");
        assert_eq!(alarms[0].token, NIFTY_50);
    }

    #[rstest]
    #[case(FeedHealth::OutOfSession, false)]
    #[case(FeedHealth::AwaitingFirstTick, false)]
    #[case(FeedHealth::Healthy, false)]
    #[case(FeedHealth::Stale { silent_for_ns: 1, ever_observed: true }, true)]
    #[case(FeedHealth::Downgraded {
        expected: ZerodhaTickMode::Full,
        observed: ZerodhaTickMode::Quote,
        depth_present: false,
    }, true)]
    fn test_is_alarm_classifies_the_states(#[case] health: FeedHealth, #[case] expected: bool) {
        assert_eq!(health.is_alarm(), expected);
    }

    #[rstest]
    fn test_thresholds_are_configurable_and_reported() {
        let watchdog = FeedWatchdog::with_thresholds(5 * NANOS_PER_SEC, 2 * NANOS_PER_SEC);

        assert_eq!(watchdog.stale_after_ns(), 5 * NANOS_PER_SEC);
        assert_eq!(watchdog.downgrade_grace_ns(), 2 * NANOS_PER_SEC);
    }

    #[rstest]
    fn test_custom_thresholds_change_the_verdict_boundaries() {
        let mut watchdog = FeedWatchdog::with_thresholds(5 * NANOS_PER_SEC, 2 * NANOS_PER_SEC);
        watchdog.watch(RELIANCE, ZerodhaTickMode::Full, at(0));
        watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(1));
        watchdog.observe(&quote_mode_tick(RELIANCE, 100.0), at(3));

        assert_eq!(
            watchdog.health(RELIANCE, at(3), SessionState::Open),
            Some(FeedHealth::Downgraded {
                expected: ZerodhaTickMode::Full,
                observed: ZerodhaTickMode::Quote,
                depth_present: false,
            }),
            "the 2s grace has elapsed even though the 10s default would not have",
        );
    }
}

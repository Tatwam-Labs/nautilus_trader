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

//! Outbound subscription messages and the subscription state needed to replay them.
//!
//! # The wire format here was read from the vendor client, not from the docs
//!
//! Every shape below is taken from `kiteconnect` **5.2.0** `ticker.py` — the constants at
//! lines 393-396 and the three senders at 567/586/608. This matters for the same reason it
//! mattered for the decoder: a message built from *my reading* of the published documentation
//! agrees with my misreading by construction, and the venue is the only thing that can say
//! otherwise. The tests below assert the exact serialised bytes against that source.
//!
//! ⚠️ **This is still weaker evidence than the decoder has.** The decoder was checked against
//! real captured frames; these messages have **never been sent to Zerodha from this crate**. The
//! oracle is one implementation's *outbound* behaviour, and nothing here is confirmed by the
//! venue accepting it. That confirmation is the first thing a live session buys.
//!
//! # `subscribe` alone does NOT give you the mode you asked for
//!
//! The single most surprising thing in the vendor client, and it is easy to miss:
//!
//! ```text
//! def subscribe(self, instrument_tokens):
//!     ...sendMessage({"a": "subscribe", "v": instrument_tokens})
//!     for token in instrument_tokens:
//!         self.subscribed_tokens[token] = self.MODE_QUOTE   # <-- QUOTE, unconditionally
//! ```
//!
//! **A bare `subscribe` puts the token in `quote` mode.** Wanting `ltp` or `full` requires a
//! *second* message. That is why [`SubscriptionState::replay_plan`] emits a subscribe **and** a
//! mode message per group rather than just a subscribe — it is not belt-and-braces, it is the
//! only way to arrive at the mode the caller asked for.

use serde::{Deserialize, Serialize};

use crate::common::enums::ZerodhaTickMode;

/// An outbound control message on the Kite streaming socket.
///
/// Serialises to the exact shapes the vendor client sends:
///
/// ```text
/// {"a":"subscribe","v":[408065,884737]}
/// {"a":"unsubscribe","v":[408065]}
/// {"a":"mode","v":["full",[408065,884737]]}
/// ```
///
/// Note the `mode` payload is a **heterogeneous array** — a mode string followed by a token
/// array — not an object. The tests pin this; if serde's adjacent tagging ever renders it
/// differently the tests fail rather than the venue silently rejecting us.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "a", content = "v", rename_all = "lowercase")]
pub enum KiteRequest {
    /// Subscribe to a set of instrument tokens. **Leaves them in `quote` mode** — see module docs.
    Subscribe(Vec<u32>),
    /// Unsubscribe a set of instrument tokens.
    Unsubscribe(Vec<u32>),
    /// Set the streaming mode for a set of already-subscribed tokens.
    Mode(ZerodhaTickMode, Vec<u32>),
}

/// The venue's cap on instrument tokens per WebSocket connection.
///
/// Kite Connect allows **3 concurrent connections per `access_token`**, each carrying up to
/// **3,000 tokens** — 9,000 in total. A Kite app is assigned to a single retail user, so the
/// `api_key` and the `access_token` share one restriction pool; the same token may legitimately be
/// used across all three sockets. This crate holds one connection, so 3,000 is the ceiling here.
///
/// # Why this is enforced locally rather than left to the venue
///
/// Zerodha's failure mode is **silence**, not rejection. A subscription past the cap does not come
/// back as an error to be handled; the excess simply never streams, and the socket stays healthy
/// while an instrument the strategy believes it is watching produces nothing. Refusing locally
/// converts an invisible partial failure into a loud one at the call site.
pub const MAX_TOKENS_PER_CONNECTION: usize = 3_000;

/// Tracks which tokens are subscribed and in which mode, so the set can be replayed.
///
/// # Why this exists rather than the shared client's `SubscriptionState`
///
/// The shared WebSocket client tracks subscriptions as opaque keys and replays them verbatim.
/// Zerodha's model is `(token, mode)` where the mode is carried by a *separate* message from the
/// subscribe, so a verbatim replay of the subscribe messages alone would restore every token at
/// `quote` regardless of what it was. The state has to be keyed by mode to be replayable at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionState {
    /// Token to its currently-requested mode. A `BTreeMap` rather than a `HashMap` so that
    /// [`Self::replay_plan`] is deterministic — a replay that reorders between runs is
    /// needlessly hard to diff in a log when something goes wrong.
    tokens: std::collections::BTreeMap<u32, ZerodhaTickMode>,
}

impl SubscriptionState {
    /// Creates an empty [`SubscriptionState`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records `tokens` as subscribed in `mode`, overwriting any previous mode for them.
    ///
    /// # Only NEW tokens count against the cap
    ///
    /// Re-subscribing a token that is already tracked is a **mode change**, not growth. Checking
    /// `len() + tokens.len()` would refuse a legitimate mode change on a connection sitting at the
    /// cap — a refusal that looks like the cap working and is simply wrong.
    ///
    /// # Errors
    ///
    /// Returns an error if the request would take the connection past
    /// [`MAX_TOKENS_PER_CONNECTION`]. **Nothing is recorded when it does** — the state is left
    /// untouched rather than partially applied, because a half-applied subscription puts this
    /// state and the venue's out of step, and the replay after the next reconnect would then
    /// restore the wrong set.
    pub fn subscribe(&mut self, mode: ZerodhaTickMode, tokens: &[u32]) -> anyhow::Result<()> {
        let additions = tokens
            .iter()
            .filter(|token| !self.tokens.contains_key(token))
            .count();

        let total = self.tokens.len() + additions;
        anyhow::ensure!(
            total <= MAX_TOKENS_PER_CONNECTION,
            "subscription would take this connection to {total} tokens, past Zerodha's cap of \
             {MAX_TOKENS_PER_CONNECTION} per connection; the venue does not reject the excess, it \
             simply never streams it. Nothing has been subscribed. Kite allows 3 connections per \
             api_key, so a second connection is the remedy rather than a larger subscription.",
        );

        for &token in tokens {
            self.tokens.insert(token, mode);
        }
        Ok(())
    }

    /// Forgets `tokens`. Unknown tokens are ignored rather than treated as an error, matching the
    /// vendor client, which swallows the `KeyError`.
    pub fn unsubscribe(&mut self, tokens: &[u32]) {
        for token in tokens {
            self.tokens.remove(token);
        }
    }

    /// Returns the mode currently recorded for `token`, if any.
    #[must_use]
    pub fn mode_of(&self, token: u32) -> Option<ZerodhaTickMode> {
        self.tokens.get(&token).copied()
    }

    /// Returns the number of tracked tokens.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    /// Returns whether nothing is subscribed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Returns the messages that restore this state on a fresh connection, in send order.
    ///
    /// Mirrors the vendor client's `resubscribe` (ticker.py:630): group the tokens by mode, then
    /// for each group send a `subscribe` **followed by** a `mode`. Both are required — see the
    /// module docs on why a bare subscribe lands in `quote`.
    ///
    /// Returns an empty vector when nothing is subscribed, so a caller can send the result
    /// unconditionally on reconnect without a special case.
    /// Iterating [`ZerodhaTickMode`] rather than grouping through a map is deliberate: the enum
    /// derives `EnumIter` but **not** `Ord`, so a `BTreeMap` keyed by mode would not compile, and
    /// a `HashMap` would reorder the groups between runs. Declaration order gives a stable plan
    /// without widening the shared enum's API for one caller's convenience.
    #[must_use]
    pub fn replay_plan(&self) -> Vec<KiteRequest> {
        use strum::IntoEnumIterator;

        let mut plan = Vec::new();

        for mode in ZerodhaTickMode::iter() {
            // `filter_map` rather than `filter` + `map`: `filter` hands the closure a *reference*
            // to the item, so `|(_, &m)|` would bind `m: &ZerodhaTickMode` and the comparison
            // against a `ZerodhaTickMode` would not type-check. `filter_map` passes the item by
            // value and the pattern destructures cleanly.
            let tokens: Vec<u32> = self
                .tokens
                .iter()
                .filter_map(|(&token, &m)| (m == mode).then_some(token))
                .collect();

            if tokens.is_empty() {
                continue;
            }

            plan.push(KiteRequest::Subscribe(tokens.clone()));
            plan.push(KiteRequest::Mode(mode, tokens));
        }
        plan
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Expected strings are transcribed from `kiteconnect` 5.2.0 `ticker.py`, NOT from this
    // module's own output. If they are ever regenerated from `serde_json::to_string` of the type
    // under test they stop testing anything -- the assertion would compare the encoder with
    // itself.

    #[rstest]
    fn test_subscribe_matches_the_vendor_wire_format() {
        let json = serde_json::to_string(&KiteRequest::Subscribe(vec![408_065, 884_737]))
            .expect("request should serialize");
        assert_eq!(json, r#"{"a":"subscribe","v":[408065,884737]}"#);
    }

    #[rstest]
    fn test_unsubscribe_matches_the_vendor_wire_format() {
        let json = serde_json::to_string(&KiteRequest::Unsubscribe(vec![408_065]))
            .expect("request should serialize");
        assert_eq!(json, r#"{"a":"unsubscribe","v":[408065]}"#);
    }

    #[rstest]
    #[case(ZerodhaTickMode::Ltp, "ltp")]
    #[case(ZerodhaTickMode::Quote, "quote")]
    #[case(ZerodhaTickMode::Full, "full")]
    fn test_mode_payload_is_a_heterogeneous_array(
        #[case] mode: ZerodhaTickMode,
        #[case] wire: &str,
    ) {
        let json = serde_json::to_string(&KiteRequest::Mode(mode, vec![408_065]))
            .expect("request should serialize");
        assert_eq!(json, format!(r#"{{"a":"mode","v":["{wire}",[408065]]}}"#));
    }

    #[rstest]
    fn test_replay_plan_sends_subscribe_and_mode_for_each_group() {
        let mut state = SubscriptionState::new();
        state.subscribe(ZerodhaTickMode::Full, &[408_065]).expect("within the connection cap");
        state.subscribe(ZerodhaTickMode::Ltp, &[884_737, 128_083_204]).expect("within the connection cap");

        // A bare subscribe would restore BOTH groups at `quote`; the mode message is what makes
        // the replay faithful. That is the whole point of the pair.
        assert_eq!(
            state.replay_plan(),
            vec![
                KiteRequest::Subscribe(vec![884_737, 128_083_204]),
                KiteRequest::Mode(ZerodhaTickMode::Ltp, vec![884_737, 128_083_204]),
                KiteRequest::Subscribe(vec![408_065]),
                KiteRequest::Mode(ZerodhaTickMode::Full, vec![408_065]),
            ],
        );
    }

    #[rstest]
    fn test_replay_plan_of_an_empty_state_is_empty() {
        assert!(SubscriptionState::new().replay_plan().is_empty());
    }

    #[rstest]
    fn test_resubscribing_a_token_in_a_new_mode_replaces_the_old_one() {
        let mut state = SubscriptionState::new();
        state.subscribe(ZerodhaTickMode::Ltp, &[408_065]).expect("within the connection cap");
        state.subscribe(ZerodhaTickMode::Full, &[408_065]).expect("within the connection cap");

        assert_eq!(state.len(), 1, "the token must not be tracked twice");
        assert_eq!(state.mode_of(408_065), Some(ZerodhaTickMode::Full));
        assert_eq!(
            state.replay_plan(),
            vec![
                KiteRequest::Subscribe(vec![408_065]),
                KiteRequest::Mode(ZerodhaTickMode::Full, vec![408_065]),
            ],
            "a token left in two mode groups would be subscribed twice on every reconnect",
        );
    }

    #[rstest]
    fn test_unsubscribe_removes_the_token_from_the_replay() {
        let mut state = SubscriptionState::new();
        state.subscribe(ZerodhaTickMode::Full, &[408_065, 884_737]).expect("within the connection cap");
        state.unsubscribe(&[408_065]);

        assert_eq!(state.mode_of(408_065), None);
        assert_eq!(
            state.replay_plan(),
            vec![
                KiteRequest::Subscribe(vec![884_737]),
                KiteRequest::Mode(ZerodhaTickMode::Full, vec![884_737]),
            ],
        );
    }

    #[rstest]
    fn test_exactly_the_cap_is_allowed() {
        let mut state = SubscriptionState::new();
        let tokens: Vec<u32> = (1..=MAX_TOKENS_PER_CONNECTION as u32).collect();

        state
            .subscribe(ZerodhaTickMode::Full, &tokens)
            .expect("the cap is inclusive; 3,000 is allowed, 3,001 is not");
        assert_eq!(state.len(), MAX_TOKENS_PER_CONNECTION);
    }

    #[rstest]
    fn test_one_past_the_cap_is_refused_and_records_nothing() {
        let mut state = SubscriptionState::new();
        let tokens: Vec<u32> = (1..=MAX_TOKENS_PER_CONNECTION as u32 + 1).collect();

        let err = match state.subscribe(ZerodhaTickMode::Full, &tokens) {
            Ok(()) => panic!("a subscription past the venue cap must be refused"),
            Err(e) => e.to_string(),
        };

        assert!(err.contains("never streams it"), "the error should say the excess is SILENT, not rejected; was: {err}");
        assert!(
            state.is_empty(),
            "the refusal must be atomic: a partially-applied subscription leaves this state and \
             the venue out of step, and the next reconnect replays the wrong set",
        );
    }

    // THE DISCRIMINATING TEST FOR THE CAP. A naive `len() + tokens.len()` check refuses this, and
    // the refusal looks exactly like the cap doing its job. Changing the mode of an
    // already-subscribed token is not growth.
    #[rstest]
    fn test_a_mode_change_at_the_cap_is_not_growth() {
        let mut state = SubscriptionState::new();
        let tokens: Vec<u32> = (1..=MAX_TOKENS_PER_CONNECTION as u32).collect();
        state
            .subscribe(ZerodhaTickMode::Quote, &tokens)
            .expect("filling to the cap");

        state
            .subscribe(ZerodhaTickMode::Full, &tokens)
            .expect("re-subscribing tracked tokens is a MODE CHANGE and adds nothing");

        assert_eq!(state.len(), MAX_TOKENS_PER_CONNECTION);
        assert_eq!(state.mode_of(1), Some(ZerodhaTickMode::Full));
    }

    #[rstest]
    fn test_unsubscribing_frees_room_under_the_cap() {
        let mut state = SubscriptionState::new();
        let tokens: Vec<u32> = (1..=MAX_TOKENS_PER_CONNECTION as u32).collect();
        state
            .subscribe(ZerodhaTickMode::Full, &tokens)
            .expect("filling to the cap");

        state.unsubscribe(&[1]);

        state
            .subscribe(ZerodhaTickMode::Full, &[999_999])
            .expect("a slot was freed");
        assert_eq!(state.len(), MAX_TOKENS_PER_CONNECTION);
    }

    #[rstest]
    fn test_unsubscribing_an_unknown_token_is_not_an_error() {
        let mut state = SubscriptionState::new();
        state.subscribe(ZerodhaTickMode::Ltp, &[408_065]).expect("within the connection cap");
        state.unsubscribe(&[999_999]);

        assert_eq!(state.len(), 1);
    }
}

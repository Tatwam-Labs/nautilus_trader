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

//! The instrument-token registry: the bridge between Zerodha's wire identity and Nautilus's.
//!
//! # Why this has to be bidirectional
//!
//! The two directions are used by different callers on different threads, and neither can be
//! served by scanning the other's map:
//!
//! | direction | caller | when |
//! |---|---|---|
//! | [`InstrumentId`] → token | `subscribe_quotes` | issuing a subscription |
//! | token → [`InstrumentId`] | the feed task | on every single tick |
//!
//! The token→id direction runs per tick at market rates, so it must be a hash lookup rather than
//! a scan. Keeping both maps is the cost of that.
//!
//! # Precision lives here, and that is not incidental
//!
//! `Price` and `Quantity` are fixed-point and need a precision at construction. That precision is
//! a property of the *instrument*, so it belongs with the identity rather than being threaded
//! through the tick path as a parameter or — worse — defaulted to a constant that happens to be
//! right for NSE equities and wrong for CDS.
//!
//! ⚠️ **Nothing populates this from the venue yet.** The REST instrument dump is the only source
//! of tokens (the historical catalog carries none), and it is not built. Until it is, entries are
//! registered explicitly by the caller, which is enough to carry one instrument end to end but is
//! **not** a substitute for the instrument provider.

use std::collections::HashMap;

use nautilus_model::identifiers::InstrumentId;

/// What the tick path needs to know about an instrument, beyond its identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstrumentDetails {
    /// The Zerodha instrument token, unique per instrument per segment.
    pub token: u32,
    /// The Nautilus instrument identifier.
    pub instrument_id: InstrumentId,
    /// Decimal places for prices.
    pub price_precision: u8,
    /// Decimal places for quantities.
    pub size_precision: u8,
}

/// A bidirectional map between Zerodha instrument tokens and Nautilus instrument identifiers.
#[derive(Clone, Debug, Default)]
pub struct InstrumentRegistry {
    by_token: HashMap<u32, InstrumentDetails>,
    by_id: HashMap<InstrumentId, u32>,
}

impl InstrumentRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `details`, replacing any previous entry for the same token **or** the same
    /// instrument id.
    ///
    /// # Both stale directions are removed, and skipping either corrupts the map
    ///
    /// Re-registering is not rare — an instrument id can be reassigned a new token across expiry
    /// rolls, and a token can be reused by the venue. Inserting into both maps without removing
    /// the superseded keys leaves a dangling entry that resolves to the *old* pairing, and it
    /// resolves successfully, so nothing downstream reports a problem: ticks would simply be
    /// published under the wrong instrument.
    pub fn register(&mut self, details: InstrumentDetails) {
        // Remove the id that this TOKEN used to point at.
        if let Some(previous) = self.by_token.get(&details.token) {
            self.by_id.remove(&previous.instrument_id);
        }
        // Remove the token that this ID used to point at.
        if let Some(previous_token) = self.by_id.get(&details.instrument_id)
            && *previous_token != details.token
        {
            self.by_token.remove(previous_token);
        }

        self.by_id.insert(details.instrument_id, details.token);
        self.by_token.insert(details.token, details);
    }

    /// Resolves a wire token to its instrument details.
    #[must_use]
    pub fn by_token(&self, token: u32) -> Option<&InstrumentDetails> {
        self.by_token.get(&token)
    }

    /// Resolves an instrument id to its wire token.
    #[must_use]
    pub fn token_of(&self, instrument_id: &InstrumentId) -> Option<u32> {
        self.by_id.get(instrument_id).copied()
    }

    /// Returns the number of registered instruments.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_token.len()
    }

    /// Returns whether the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_token.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn details(token: u32, id: &str) -> InstrumentDetails {
        InstrumentDetails {
            token,
            instrument_id: InstrumentId::from(id),
            price_precision: 2,
            size_precision: 0,
        }
    }

    #[rstest]
    fn test_resolves_in_both_directions() {
        let mut registry = InstrumentRegistry::new();
        registry.register(details(408_065, "RELIANCE.NSE"));

        assert_eq!(
            registry.by_token(408_065).map(|d| d.instrument_id),
            Some(InstrumentId::from("RELIANCE.NSE")),
        );
        assert_eq!(
            registry.token_of(&InstrumentId::from("RELIANCE.NSE")),
            Some(408_065),
        );
    }

    #[rstest]
    fn test_unknown_lookups_are_none_in_both_directions() {
        let registry = InstrumentRegistry::new();

        assert!(registry.by_token(1).is_none());
        assert!(registry.token_of(&InstrumentId::from("NOPE.NSE")).is_none());
        assert!(registry.is_empty());
    }

    // The two tests below are the ones that matter. A `register` that inserts into both maps
    // without removing the superseded key leaves a REVERSE entry pointing at the old pairing --
    // and it resolves successfully, so ticks are published under the wrong instrument with nothing
    // reporting an error.

    #[rstest]
    fn test_reassigning_a_token_to_a_new_instrument_drops_the_stale_reverse_entry() {
        let mut registry = InstrumentRegistry::new();
        registry.register(details(408_065, "NIFTY24AUGFUT.NFO"));
        registry.register(details(408_065, "NIFTY24SEPFUT.NFO"));

        assert_eq!(registry.len(), 1, "the token must not be tracked twice");
        assert_eq!(
            registry.token_of(&InstrumentId::from("NIFTY24AUGFUT.NFO")),
            None,
            "the superseded instrument must no longer resolve to this token",
        );
        assert_eq!(
            registry.token_of(&InstrumentId::from("NIFTY24SEPFUT.NFO")),
            Some(408_065),
        );
    }

    #[rstest]
    fn test_moving_an_instrument_to_a_new_token_drops_the_stale_forward_entry() {
        let mut registry = InstrumentRegistry::new();
        registry.register(details(408_065, "RELIANCE.NSE"));
        registry.register(details(999_999, "RELIANCE.NSE"));

        assert_eq!(registry.len(), 1, "the instrument must not be tracked twice");
        assert!(
            registry.by_token(408_065).is_none(),
            "the old token must stop resolving, or a stale tick maps to a live instrument",
        );
        assert_eq!(
            registry.by_token(999_999).map(|d| d.instrument_id),
            Some(InstrumentId::from("RELIANCE.NSE")),
        );
    }

    #[rstest]
    fn test_re_registering_the_same_pairing_is_idempotent() {
        let mut registry = InstrumentRegistry::new();
        registry.register(details(408_065, "RELIANCE.NSE"));
        registry.register(details(408_065, "RELIANCE.NSE"));

        assert_eq!(registry.len(), 1);
        assert_eq!(registry.token_of(&InstrumentId::from("RELIANCE.NSE")), Some(408_065));
    }

    #[rstest]
    fn test_precision_travels_with_the_identity() {
        let mut registry = InstrumentRegistry::new();
        registry.register(InstrumentDetails {
            token: 1_234,
            instrument_id: InstrumentId::from("USDINR24AUGFUT.CDS"),
            price_precision: 4,
            size_precision: 0,
        });

        let found = registry.by_token(1_234).expect("registered");
        assert_eq!(
            found.price_precision, 4,
            "CDS quotes to 4dp; defaulting to the NSE equity 2dp would silently round every price",
        );
    }
}

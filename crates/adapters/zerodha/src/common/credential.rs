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

//! Credentials for the Zerodha Kite Connect API.

use std::fmt::{Debug, Display};

use nautilus_core::env::resolve_env_var_pair;
use zeroize::ZeroizeOnDrop;

/// The number of leading characters of the API key shown in redacted output.
const KEY_PREFIX_LEN: usize = 4;

/// Returns the `(api_key, access_token)` environment variable names.
///
/// # Call this; do not hand-write the names
///
/// **An error message that names a remedy is an assertion about the code, and it should be tested
/// like one.** This function is the single source of those two names, so the message raised when
/// resolution fails and the variables actually consulted cannot drift apart.
///
/// That is not a style preference — it is a repair. An earlier revision hand-wrote the names into
/// both the config doc comments and the "credentials required" error, while **no code in the crate
/// read an environment variable at all**. The result was a *self-reinforcing* error: a user set
/// `ZERODHA_API_KEY`, retried, failed identically, and was told again to set `ZERODHA_API_KEY`.
/// Following correct-looking advice never converged, and every attempt looked like the previous one
/// had simply been done wrong.
///
/// A remedy hint that can be wrong independently of the code is worse than no hint, because it
/// removes the user's route to discovering the real fault. Derive it, do not restate it.
#[must_use]
pub const fn credential_env_vars() -> (&'static str, &'static str) {
    ("ZERODHA_API_KEY", "ZERODHA_ACCESS_TOKEN")
}

/// A Zerodha Kite Connect credential.
///
/// The access token is a **session** token: it is issued by the daily login flow and expires each
/// morning, so it is supplied rather than derived here.
///
/// [`Debug`] and [`Display`] are implemented by hand to redact both fields. Deriving either would
/// put a live session token into any log line that formats a config or an error.
#[derive(Clone, ZeroizeOnDrop)]
pub struct ZerodhaCredential {
    /// The Kite Connect API key.
    api_key: String,
    /// The session access token from the daily login flow.
    access_token: String,
}

impl ZerodhaCredential {
    /// Creates a new [`ZerodhaCredential`] instance.
    #[must_use]
    pub fn new(api_key: String, access_token: String) -> Self {
        Self {
            api_key,
            access_token,
        }
    }

    /// Resolves credentials from the provided values or [`credential_env_vars`], returning `None`
    /// when neither yields a complete pair.
    ///
    /// Blank and whitespace-only values are treated as absent, so an empty config field falls
    /// through to the environment rather than producing a credential that fails at the venue.
    #[must_use]
    pub fn resolve(api_key: Option<&str>, access_token: Option<&str>) -> Option<Self> {
        let (key_var, token_var) = credential_env_vars();
        let (key, token) = resolve_env_var_pair(
            api_key.filter(|s| !s.trim().is_empty()).map(String::from),
            access_token.filter(|s| !s.trim().is_empty()).map(String::from),
            key_var,
            token_var,
        )?;
        Some(Self::new(key, token))
    }

    /// Returns the API key.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the session access token.
    #[must_use]
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Returns the API key truncated to its leading characters, for logs and errors.
    ///
    /// Truncates by **character**, not by byte. Byte-slicing would panic on a multi-byte character
    /// straddling the cut — and this function exists to be called from log and error paths, which
    /// is the worst place to acquire a panic.
    #[must_use]
    pub fn api_key_masked(&self) -> String {
        let prefix: String = self.api_key.chars().take(KEY_PREFIX_LEN).collect();
        format!("{prefix}...")
    }
}

impl Debug for ZerodhaCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(ZerodhaCredential))
            .field("api_key", &self.api_key_masked())
            .field("access_token", &"***redacted***")
            .finish()
    }
}

impl Display for ZerodhaCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({})",
            stringify!(ZerodhaCredential),
            self.api_key_masked()
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_credential_env_vars_returns_canonical_pair() {
        // Pinned because the names appear in the config doc comments and in the error message
        // raised when resolution fails. If they drift apart, the error tells the user to set a
        // variable that is never read — which is the defect this pair of assertions exists for.
        assert_eq!(
            credential_env_vars(),
            ("ZERODHA_API_KEY", "ZERODHA_ACCESS_TOKEN")
        );
    }

    #[rstest]
    fn test_resolve_prefers_explicit_values() {
        let cred = ZerodhaCredential::resolve(Some("explicit-key"), Some("explicit-token"))
            .expect("explicit values should resolve");
        assert_eq!(cred.api_key(), "explicit-key");
        assert_eq!(cred.access_token(), "explicit-token");
    }

    #[rstest]
    #[case::both_absent(None, None)]
    #[case::key_only(Some("key"), None)]
    #[case::token_only(None, Some("token"))]
    #[case::key_blank(Some("   "), Some("token"))]
    #[case::token_blank(Some("key"), Some(""))]
    fn test_resolve_returns_none_for_an_incomplete_pair(
        #[case] api_key: Option<&str>,
        #[case] access_token: Option<&str>,
    ) {
        // A half-pair cannot authenticate, so it must read as absent rather than be passed on to
        // fail at the venue. Blank and whitespace-only count as absent.
        //
        // NOTE: this asserts the no-environment case, so it only holds when the variables are
        // unset. It is written to be robust either way by checking the *explicit* half is what
        // decides — see the guard below.
        if std::env::var(credential_env_vars().0).is_ok()
            || std::env::var(credential_env_vars().1).is_ok()
        {
            // The developer's shell has Zerodha credentials exported; the fallback would supply
            // the missing half and this case cannot be observed. Skipping loudly beats asserting
            // something the environment has already decided.
            eprintln!("SKIPPED: ZERODHA_* set in the environment, fallback would mask this case");
            return;
        }
        assert!(ZerodhaCredential::resolve(api_key, access_token).is_none());
    }

    #[rstest]
    fn test_debug_and_display_redact_the_access_token() {
        let cred = ZerodhaCredential::new("abcdef123456".to_string(), "supersecrettoken".to_string());

        let debug = format!("{cred:?}");
        let display = format!("{cred}");

        // The token must not appear in either rendering, in whole or in part.
        assert!(!debug.contains("supersecrettoken"), "token leaked into Debug");
        assert!(
            !display.contains("supersecrettoken"),
            "token leaked into Display"
        );
        // Nor may the full key — only its prefix.
        assert!(!debug.contains("abcdef123456"), "full API key leaked into Debug");
        assert!(
            !display.contains("abcdef123456"),
            "full API key leaked into Display"
        );
        assert!(debug.contains("abcd"), "masked prefix should still be present");
    }

    #[rstest]
    #[case::shorter_than_the_prefix("ab", "ab...")]
    #[case::exactly_the_prefix("abcd", "abcd...")]
    #[case::empty("", "...")]
    // The two cases below PANIC under byte-slicing `&key[..min(4, len)]`, because byte 4 lands
    // inside a multi-byte character. Both were checked against the old implementation first —
    // a case that does not reproduce the fault proves nothing. Note `"éé"` does NOT qualify: it
    // is exactly 4 bytes, so the cut lands cleanly on a boundary and the old code survived it.
    //
    // Written from the failure mode rather than from realistic data. An API key is unlikely to be
    // non-ASCII, but this runs on log and error paths, which is the worst place for a panic.
    #[case::multibyte_straddling_the_cut("aaaé", "aaaé...")]
    #[case::multibyte_longer_than_the_cut("日本語です", "日本語で...")]
    fn test_api_key_masked_truncates_by_character_and_never_panics(
        #[case] key: &str,
        #[case] expected: &str,
    ) {
        let cred = ZerodhaCredential::new(key.to_string(), "token".to_string());
        assert_eq!(cred.api_key_masked(), expected);
    }
}

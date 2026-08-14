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

use zeroize::ZeroizeOnDrop;

/// The number of leading characters of the API key shown in redacted output.
const KEY_PREFIX_LEN: usize = 4;

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
    #[must_use]
    pub fn api_key_masked(&self) -> String {
        format!(
            "{}...",
            &self.api_key[..KEY_PREFIX_LEN.min(self.api_key.len())]
        )
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

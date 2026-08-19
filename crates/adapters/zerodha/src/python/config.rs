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

//! Python bindings for the Zerodha client configuration.
//!
//! The field getters are generated separately by `impl_pyo3_config_getters!` in
//! [`crate::config`], which deliberately exposes only the non-credential fields. Two
//! `#[pymethods]` blocks on one type are legal here because the workspace enables pyo3's
//! `multiple-pymethods` feature (`Cargo.toml:211`); `coinbase` is arranged the same way.

use pyo3::pymethods;

use crate::{
    common::enums::{ZerodhaProduct, ZerodhaVariety},
    config::{ZerodhaDataClientConfig, ZerodhaExecClientConfig},
};

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaDataClientConfig {
    /// Configuration for the Zerodha live data client.
    #[new]
    // Eight arguments, one over clippy's threshold of seven, reached by adding
    // `replay_frames_path`. `betfair` and `architect_ax` carry the same attribute on their own
    // `py_new` for the same reason.
    //
    // Note this could NOT have been added pre-emptively: at exactly seven arguments the lint does
    // not fire, and an unfulfilled `#[expect]` is itself a warning. The correct code at 7 args and
    // the correct code at 8 differ, and nothing warns on the way past.
    #[expect(clippy::too_many_arguments)]
    #[pyo3(signature = (
        api_key = None,
        access_token = None,
        base_url_http = None,
        base_url_ws = None,
        http_timeout_secs = None,
        ws_timeout_secs = None,
        update_instruments_interval_mins = None,
        replay_frames_path = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        access_token: Option<String>,
        base_url_http: Option<String>,
        base_url_ws: Option<String>,
        http_timeout_secs: Option<u64>,
        ws_timeout_secs: Option<u64>,
        update_instruments_interval_mins: Option<u64>,
        replay_frames_path: Option<String>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            access_token,
            base_url_http,
            base_url_ws,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            ws_timeout_secs: ws_timeout_secs.unwrap_or(defaults.ws_timeout_secs),
            update_instruments_interval_mins: update_instruments_interval_mins
                .unwrap_or(defaults.update_instruments_interval_mins),
            // ⭐ EXPOSED DELIBERATELY, REVERSING AN EARLIER DECISION TO WITHHOLD IT.
            //
            // The reasoning for withholding was sound in intent -- no Python caller should be able
            // to silently divert a client away from the venue -- but it does not achieve that, and
            // it costs the thing replay exists for.
            //
            // It does not achieve it because this config derives `Deserialize` with
            // `#[serde(default)]`, so `replay_frames_path` is ALREADY settable by any config that
            // arrives serialised. Withholding it from `py_new` closes the EXPLICIT, VISIBLE,
            // named-argument route while leaving the implicit one open -- which is backwards: the
            // dangerous path is the one nobody can see at the call site.
            //
            // What actually guards against a client silently replaying is not an absent parameter:
            //   * the name is explicit at the call site -- `replay_frames_path="corpus.json"`
            //   * `connect()` logs a WARN naming replay mode and stating no socket is open
            //   * `Debug` prints the field UNREDACTED, so a dumped config shows it
            // Those make a replaying client visible. An absent parameter only made it undrivable.
            //
            // And the cost was total: the question replay exists to answer is whether data reaches
            // a PYTHON strategy, which requires a Python-driven node.
            replay_frames_path,
        }
    }

    /// Routed through the hand-written `Debug`, which redacts both credential fields.
    ///
    /// `coinbase` returns a bare type name here. Going through `Debug` instead keeps the
    /// non-secret fields visible AND puts the Python repr behind the same redaction the
    /// `config::tests` regression tests already assert on — one guarded path rather than a second
    /// formatting surface that nothing checks.
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

#[pymethods]
#[pyo3_stub_gen::derive::gen_stub_pymethods]
impl ZerodhaExecClientConfig {
    /// Configuration for the Zerodha execution client.
    ///
    /// `default_product` has no default and the omission is load-bearing: `MIS` silently arms the
    /// broker to square off every position around 15:20 IST, `CNC` silently demands full delivery
    /// margin, and `NRML` is meaningless on an equity segment. A client configured without one
    /// fails to construct, which surfaces at node build time rather than at 09:15 on the first
    /// order. Passing `None` here reproduces that failure deliberately — it does not pick a value.
    #[new]
    #[pyo3(signature = (
        api_key = None,
        access_token = None,
        base_url_http = None,
        http_timeout_secs = None,
        default_product = None,
        default_variety = None,
    ))]
    fn py_new(
        api_key: Option<String>,
        access_token: Option<String>,
        base_url_http: Option<String>,
        http_timeout_secs: Option<u64>,
        default_product: Option<ZerodhaProduct>,
        default_variety: Option<ZerodhaVariety>,
    ) -> Self {
        let defaults = Self::default();
        Self {
            api_key,
            access_token,
            base_url_http,
            http_timeout_secs: http_timeout_secs.unwrap_or(defaults.http_timeout_secs),
            default_product,
            default_variety: default_variety.unwrap_or(defaults.default_variety),
        }
    }

    /// Routed through the hand-written `Debug`, which redacts `api_key` and `access_token`.
    ///
    /// Same reasoning as the data config: one guarded formatting path rather than a second surface
    /// that nothing asserts on.
    fn __repr__(&self) -> String {
        format!("{self:?}")
    }
}

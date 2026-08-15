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

//! Tests that the PyO3 registration actually takes effect.
//!
//! # Why this file exists
//!
//! **Building and registering are different claims**, and only the first was covered. The crate
//! compiles into the extension module and `crates/pyo3/src/lib.rs` wraps this adapter's `#[pymodule]`
//! — but nothing verified that `python::zerodha()` succeeds in putting the factory and config
//! extractors into the **global PyO3 registry**, or that what comes back out is usable.
//!
//! That gap sits on one of the thirteen registration surfaces, eleven of which have no CI hook and
//! **fail silently**. A missing registration would not turn anything red: the crate would build, the
//! module would import, and `add_data_client()` would simply fail to find an extractor at runtime.
//!
//! 18 of the 19 in-tree adapters carry a `tests/python.rs` doing exactly this. This one was missing.
//!
//! # Hermetic by construction
//!
//! Credentials are supplied **in the config**, not via the environment. `ZerodhaDataClient::new`
//! refuses to construct without them, and `ZerodhaDataClientConfig::credential()` falls back to
//! `ZERODHA_API_KEY` / `ZERODHA_ACCESS_TOKEN` — so a test relying on that fallback would pass or
//! fail according to the developer's shell. These values are placeholders; nothing connects.

#![cfg(feature = "python")]

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::Cache, clock::TestClock, live::runner::set_data_event_sender, messages::DataEvent,
};
use nautilus_model::identifiers::ClientId;
use nautilus_system::get_global_pyo3_registry;
use nautilus_zerodha::{
    common::consts::ZERODHA, config::ZerodhaDataClientConfig, factories::ZerodhaDataClientFactory,
    python,
};
use pyo3::{Py, Python, types::{PyAnyMethods, PyModule}};
use rstest::rstest;

fn register_zerodha_python_module(py: Python<'_>) {
    let module = PyModule::new(py, "zerodha").expect("Zerodha module should be created");
    python::zerodha(py, &module).expect("Zerodha Python module should register");
}

fn setup_data_event_sender() {
    let (sender, _receiver) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(sender);
}

#[rstest]
fn test_zerodha_python_factory_extracts_from_registry() {
    setup_data_event_sender();
    Python::initialize();

    Python::attach(|py| {
        register_zerodha_python_module(py);

        // The registration path under test: a Python-side factory and config object must come back
        // out of the GLOBAL registry as usable Rust trait objects. If `python::zerodha` failed to
        // register its extractors, both `extract_*` calls below return an error -- which is the
        // silent failure this file exists to make loud.
        let factory = Py::new(py, ZerodhaDataClientFactory::new())
            .expect("factory should convert to a Python object")
            .into_any();
        let config = Py::new(
            py,
            ZerodhaDataClientConfig {
                // In-config, never from the environment -- see the module docs.
                api_key: Some("test-api-key".to_string()),
                access_token: Some("test-access-token".to_string()),
                http_timeout_secs: 7,
                ..ZerodhaDataClientConfig::default()
            },
        )
        .expect("config should convert to a Python object")
        .into_any();

        let registry = get_global_pyo3_registry();
        let extracted_factory = registry
            .extract_factory(py, factory)
            .expect("data factory should extract from the registry");
        let extracted_config = registry
            .extract_config(py, config)
            .expect("data config should extract from the registry");

        let zerodha_config = extracted_config
            .as_any()
            .downcast_ref::<ZerodhaDataClientConfig>()
            .expect("data config should downcast to ZerodhaDataClientConfig");

        // Round-tripping through Python must not silently drop or default a field.
        assert_eq!(zerodha_config.http_timeout_secs, 7);
        assert_eq!(zerodha_config.api_key.as_deref(), Some("test-api-key"));

        // The factory's identity is what `add_data_client(name, ...)` matches on.
        assert_eq!(extracted_factory.name(), ZERODHA);
        assert_eq!(extracted_factory.config_type(), "ZerodhaDataClientConfig");

        // And the extracted pair must actually build a client -- extraction succeeding while
        // construction fails would still leave the adapter unusable from Python.
        let cache = Rc::new(RefCell::new(Cache::default()));
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let client = extracted_factory
            .create(
                "ZERODHA-DATA-EXTRACTED",
                extracted_config.as_ref(),
                cache.into(),
                clock,
            )
            .expect("extracted factory should create a data client");

        assert_eq!(client.client_id(), ClientId::from("ZERODHA-DATA-EXTRACTED"));
    });
}

#[rstest]
fn test_factory_rejects_a_config_of_the_wrong_type() {
    // The factory downcasts a type-erased `&dyn ClientConfig`. If that downcast were ever made
    // permissive, a mismatched config would be silently accepted and its fields read as defaults.
    // Asserted here rather than assumed, because the failure is quiet.
    use nautilus_common::factories::{ClientConfig, DataClientFactory};

    #[derive(Debug)]
    struct NotZerodhaConfig;
    impl ClientConfig for NotZerodhaConfig {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    let cache = Rc::new(RefCell::new(Cache::default()));
    let clock = Rc::new(RefCell::new(TestClock::new()));
    let result = ZerodhaDataClientFactory::new().create(
        "ZERODHA-WRONG-CONFIG",
        &NotZerodhaConfig,
        cache.into(),
        clock,
    );

    // `expect_err` would require `Box<dyn DataClient>: Debug` so it could print the Ok variant, and
    // the trait does not carry that bound. This is the `coinbase/src/factories.rs:270-273` form.
    let err = match result {
        Ok(_) => panic!("a foreign config type must be rejected, not defaulted"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("ZerodhaDataClientConfig"),
        "the error should name the expected config type so the caller can fix it; was: {err}",
    );
}

// ⭐ THE GUARD FOR A FIELD THAT MUST REACH FIVE PLACES AT ONCE.
//
// Adding one config field on this branch required it to land in FIVE parallel sites, and three
// separate one-line commits were needed because each omission was invisible to the gate that had
// just passed:
//
//   struct field            -> `#[builder(default)]` rejected the bare form on an Option
//   `Self { .. }` literal   -> E0063, only under `--features python`
//   `#[pyo3(signature)]`    -> "missing signature entry", only under `--features python`
//   `impl_pyo3_config_getters!`  -> ⚠️ NO ERROR AT ALL. Silently unreadable.
//   the hand-written `Debug`     -> no error either; silently absent from diagnostics
//
// The first three now fail loudly, because `--features python` is part of the standard check set.
// **The last two fail SILENTLY**, and this test exists for them: a field that decides whether the
// client contacts the venue at all must never become write-only without something noticing.
#[rstest]
fn test_replay_frames_path_survives_the_round_trip_to_python_and_back() {
    Python::attach(|py| {
        register_zerodha_python_module(py);

        let config = Py::new(
            py,
            ZerodhaDataClientConfig {
                replay_frames_path: Some("corpus.json".to_string()),
                ..Default::default()
            },
        )
        .expect("config into Python");

        // The getter half. A missing entry in `impl_pyo3_config_getters!` compiles cleanly and
        // leaves the attribute absent, so this asserts on the VALUE rather than on `hasattr` —
        // absent and present-but-wrong are different bugs and should not share an assertion.
        let readable: Option<String> = config
            .getattr(py, "replay_frames_path")
            .expect("replay_frames_path must be READABLE from Python: a field that diverts the \
                     client away from the venue must never be write-only")
            .extract(py)
            .expect("and it must extract as Option<String>");

        assert_eq!(
            readable.as_deref(),
            Some("corpus.json"),
            "the value must survive the round trip, not merely exist",
        );

        // The constructor half. A missing `#[pyo3(signature)]` entry is a compile error, but a
        // parameter accepted and then DROPPED on the floor is not — so this checks the value
        // actually reached the struct rather than that the call was accepted.
        let built = py
            .import("nautilus_trader.adapters.zerodha")
            .and_then(|m| m.getattr("ZerodhaDataClientConfig"))
            .and_then(|c| c.call1((py.None(), py.None(), py.None(), py.None(), py.None(), py.None(), py.None(), "from-kwarg.json")))
            .expect("the constructor must accept replay_frames_path positionally");

        let round_tripped: Option<String> = built
            .getattr("replay_frames_path")
            .expect("and expose it back")
            .extract()
            .expect("as Option<String>");

        assert_eq!(
            round_tripped.as_deref(),
            Some("from-kwarg.json"),
            "a constructor that accepts the argument and discards it would pass every compile gate",
        );
    });
}

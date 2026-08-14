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

//! Factory functions for creating Zerodha clients and components.
//!
//! # The config and the cache arrive HERE, not on the client trait
//!
//! This is worth stating because it is easy to look for them in the wrong place.
//! [`nautilus_common::clients::DataClient`] carries only identity, lifecycle, subscriptions and
//! requests — it never sees a config. Construction inputs arrive on
//! [`DataClientFactory::create`] instead, and there are **four** of them:
//!
//! ```text
//! fn create(
//!     &self,
//!     name: &str,
//!     config: &dyn ClientConfig,        // type-erased; downcast to the concrete config
//!     cache: CacheView,                 // READ-ONLY view, for querying platform state
//!     clock: Rc<RefCell<dyn Clock>>,    // easy to miss when copying a signature
//! ) -> anyhow::Result<Box<dyn DataClient>>;
//! ```
//!
//! The config is type-erased, so the factory downcasts it and returns a clear error on mismatch
//! rather than panicking — `config_type()` is what lets the caller pair them up correctly.
//!
//! The cache is a *view*: adapters may query platform state during construction but cannot mutate
//! it. See `crates/common/src/factories/client.rs` for the declaration.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::consts::ZERODHA, config::ZerodhaDataClientConfig, data::ZerodhaDataClient};

impl ClientConfig for ZerodhaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Factory for creating Zerodha data clients.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.zerodha", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.zerodha")
)]
pub struct ZerodhaDataClientFactory;

impl ZerodhaDataClientFactory {
    /// Creates a new [`ZerodhaDataClientFactory`] instance.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for ZerodhaDataClientFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl DataClientFactory for ZerodhaDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let zerodha_config = config
            .as_any()
            .downcast_ref::<ZerodhaDataClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ZerodhaDataClientFactory. Expected \
                     ZerodhaDataClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client_id = ClientId::from(name);
        let client = ZerodhaDataClient::new(client_id, zerodha_config)?;
        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ZERODHA
    }

    fn config_type(&self) -> &'static str {
        "ZerodhaDataClientConfig"
    }
}

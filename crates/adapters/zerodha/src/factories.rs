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
    clients::{DataClient, ExecutionClient},
    clock::Clock,
    factories::{ClientConfig, DataClientFactory, ExecutionClientFactory},
};
use nautilus_model::{
    enums::AccountType,
    identifiers::{AccountId, ClientId, TraderId},
};

use crate::{
    common::consts::ZERODHA,
    config::{ZerodhaDataClientConfig, ZerodhaExecClientConfig},
    data::ZerodhaDataClient,
    execution::ZerodhaExecutionClient,
};

impl ClientConfig for ZerodhaDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ClientConfig for ZerodhaExecClientConfig {
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

/// Factory for creating Zerodha execution clients.
///
/// # The execution factory takes THREE construction inputs the data factory does not
///
/// [`ExecutionClientFactory::create`] receives only `(name, config, cache)` — no clock, and
/// crucially no trader or account identity. Those are properties of the *node*, not of the venue,
/// so they arrive on the factory itself and are held until `create` is called. Every sibling
/// execution factory in the tree is built the same way.
///
/// `account_type` is here rather than on the config because it is a Nautilus concept with no
/// Zerodha field behind it: an Indian broker account holds equity delivery (cash) and F&O (margin)
/// at once, and which one a given node models is the operator's decision. Putting it on the factory
/// keeps it out of a serialised config where it would look venue-derived.
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "python",
    pyo3::pyclass(module = "nautilus_trader.adapters.zerodha", from_py_object)
)]
#[cfg_attr(
    feature = "python",
    pyo3_stub_gen::derive::gen_stub_pyclass(module = "nautilus_trader.adapters.zerodha")
)]
pub struct ZerodhaExecutionClientFactory {
    trader_id: TraderId,
    account_id: AccountId,
    account_type: AccountType,
}

impl ZerodhaExecutionClientFactory {
    /// Creates a new [`ZerodhaExecutionClientFactory`] instance.
    #[must_use]
    pub const fn new(
        trader_id: TraderId,
        account_id: AccountId,
        account_type: AccountType,
    ) -> Self {
        Self {
            trader_id,
            account_id,
            account_type,
        }
    }
}

impl ExecutionClientFactory for ZerodhaExecutionClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        cache: CacheView,
    ) -> anyhow::Result<Box<dyn ExecutionClient>> {
        // `{config:?}` formats the config into this message, which is why
        // `ZerodhaExecClientConfig` implements `Debug` by hand and redacts both credential fields.
        // The data config derived `Debug` until 2026-08-14 and leaked a live session token through
        // exactly this line.
        let zerodha_config = config
            .as_any()
            .downcast_ref::<ZerodhaExecClientConfig>()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "Invalid config type for ZerodhaExecutionClientFactory. Expected \
                     ZerodhaExecClientConfig, was {config:?}",
                )
            })?
            .clone();

        let client = ZerodhaExecutionClient::new(
            ClientId::from(name),
            self.account_id,
            self.trader_id,
            self.account_type,
            cache,
            zerodha_config,
        )?;

        Ok(Box::new(client))
    }

    fn name(&self) -> &'static str {
        ZERODHA
    }

    fn config_type(&self) -> &'static str {
        "ZerodhaExecClientConfig"
    }
}

#[cfg(test)]
mod tests {
    use nautilus_common::cache::Cache;
    use rstest::rstest;

    use super::*;
    use crate::common::enums::ZerodhaProduct;

    fn cache_view() -> CacheView {
        CacheView::new(Rc::new(RefCell::new(Cache::default())))
    }

    fn factory() -> ZerodhaExecutionClientFactory {
        ZerodhaExecutionClientFactory::new(
            TraderId::from("TRADER-001"),
            AccountId::from("ZERODHA-001"),
            AccountType::Margin,
        )
    }

    fn config(product: Option<ZerodhaProduct>) -> ZerodhaExecClientConfig {
        ZerodhaExecClientConfig {
            api_key: Some("c5gabz-api-key-value".to_string()),
            access_token: Some("ldyr-live-session-token-value".to_string()),
            default_product: product,
            ..ZerodhaExecClientConfig::default()
        }
    }

    #[rstest]
    fn test_the_exec_factory_names_itself_and_its_config() {
        let factory = factory();

        assert_eq!(factory.name(), ZERODHA);
        assert_eq!(factory.config_type(), "ZerodhaExecClientConfig");
    }

    // THE DISCRIMINATING TEST FOR THE PRODUCT. A client with no product must fail to BUILD, not at
    // the first order: the live node builder calls `create` with `?`, so an error here aborts the
    // build, whereas deferring the check to `submit_order` moves the failure to 09:15 on a live
    // account.
    #[rstest]
    fn test_a_config_without_a_product_fails_to_build() {
        let error = match factory().create("ZERODHA", &config(None), cache_view()) {
            Ok(_) => panic!("a client with no product must not be constructed"),
            Err(e) => e.to_string(),
        };

        assert!(
            error.contains("default_product"),
            "the error must name the missing field; was: {error}"
        );
        assert!(
            error.contains("15:20"),
            "the error should say what a wrong default would do; was: {error}"
        );
    }

    // The wrong-config error formats `{config:?}`, which is the exact line that leaked a session
    // token from the data config. Asserted at the factory rather than only on the config, because
    // fixing one does not prove the other.
    #[rstest]
    fn test_a_wrong_config_type_error_does_not_leak_the_credential() {
        let data_config = ZerodhaDataClientConfig {
            api_key: Some("c5gabz-api-key-value".to_string()),
            access_token: Some("ldyr-live-session-token-value".to_string()),
            ..ZerodhaDataClientConfig::default()
        };

        let error = match factory().create("ZERODHA", &data_config, cache_view()) {
            Ok(_) => panic!("a data config must not build an execution client"),
            Err(e) => e.to_string(),
        };

        assert!(error.contains("ZerodhaExecClientConfig"), "{error}");
        assert!(
            !error.contains("ldyr-live-session-token-value"),
            "the access token leaked into the wrong-config error: {error}"
        );
        assert!(
            !error.contains("c5gabz-api-key-value"),
            "the api key leaked into the wrong-config error: {error}"
        );
    }

    #[rstest]
    fn test_the_data_factory_still_names_itself() {
        let factory = ZerodhaDataClientFactory::new();

        assert_eq!(factory.name(), ZERODHA);
        assert_eq!(factory.config_type(), "ZerodhaDataClientConfig");
    }
}

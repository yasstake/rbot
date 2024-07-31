#![allow(non_snake_case)]
use std::env;

use pyo3::prelude::*;
use rust_decimal_macros::dec;
use serde_derive::{Deserialize, Serialize};

use rbot_lib::common::{FeeType, MarketConfig, PriceType, SecretString, ServerConfig};

use crate::BYBIT;

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitServerConfig {
    pub production: bool,
    pub rest_server: String,
    pub public_ws: String,
    pub private_ws: String,
    pub history_web_base: String,
    api_key: SecretString,
    api_secret: SecretString,
}

#[pymethods]
impl BybitServerConfig {
    #[new]
    pub fn new(production: bool) -> Self {
        let rest_server = if production {
            "https://api.bybit.com"
        } else {
            "https://api-testnet.bybit.com"
        }
        .to_string();

        let public_ws_server = if production {
            "wss://stream.bybit.com/v5/public"
        } else {
            "wss://stream-testnet.bybit.com/v5/public"
        }
        .to_string();

        let private_ws_server = if production {
            "wss://stream.bybit.com/v5/private"
        } else {
            "wss://stream-testnet.bybit.com/v5/private"
        }
        .to_string();

        let api_key = if let Ok(key) = env::var("BYBIT_API_KEY") {
            key
        }
        else {
            println!("API KEY environment variable [BYBIT_API_KEY] is not set");
            log::warn!("API KEY environment variable [BYBIT_API_KEY] is not set");
            "".to_string()
        };

        let api_secret = if let Ok(secret) = env::var("BYBIT_API_SECRET") {
            secret
        }
        else {
            println!("API SECRET environment variable [BYBIT_API_SECRET] is not set");
            log::warn!("API SECRET environment variable [BYBIT_API_SECRET] is not set");
            "".to_string()
        };

        return BybitServerConfig {
            production,
            rest_server,
            public_ws: public_ws_server,
            private_ws: private_ws_server,
            history_web_base: "https://public.bybit.com".to_string(),
            api_key: SecretString::new(&api_key),
            api_secret: SecretString::new(&api_secret),
        };
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }
}

impl ServerConfig for BybitServerConfig {
    fn get_public_ws_server(&self) -> String {
        self.public_ws.clone()
    }

    fn get_user_ws_server(&self) -> String {
        self.private_ws.clone()
    }

    fn get_rest_server(&self) -> String {
        self.rest_server.clone()
    }

    fn get_api_key(&self) -> SecretString {
        self.api_key.clone()
    }

    fn get_api_secret(&self) -> SecretString {
        self.api_secret.clone()
    }

    fn get_historical_web_base(&self) -> String {
        self.history_web_base.clone()
    }

    fn is_production(&self) -> bool {
        self.production
    }
}

#[derive(Debug, Clone, Serialize)]
#[pyclass]
pub struct BybitConfig {}

// 取引ペアーの制限については以下を参照
// https://www.bybit.com/ja-JP/announcement-info/transact-parameters/

#[pymethods]
impl BybitConfig {
    #[new]
    pub fn new() -> Self {
        return BybitConfig {};
    }


    #[classattr]
    pub fn BTCUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "BTC",
            "USDT",
            0.1,
            PriceType::Home,
            0.001,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
            None,
        )
    }

    #[classattr]
    pub fn ETHUSDT() -> MarketConfig {
        let config = MarketConfig::new(
            BYBIT,
            "linear",
            "ETH",
            "USDT",            
            0.01,
            PriceType::Home,
            0.01,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.ETHUSDT".to_string(),
                "orderbook.200.ETHUSDT".to_string(),
            ],
            None
        );

        config
    }

    #[classattr]
    pub fn BTCUSDC() -> MarketConfig {
        let mut config = MarketConfig::new(
            BYBIT,
            "linear",
            "BTC",
            "USDC",
            0.1,
            PriceType::Home,
            0.001,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCPERP".to_string(),
                "orderbook.200.BTCPERP".to_string(),
            ],
            None,
        );

        config.trade_symbol = "BTCPERP".to_string();
        config
    }



    #[classattr]
    pub fn ETHUSDC() -> MarketConfig {
        let mut config = MarketConfig::new(
            BYBIT,
            "linear",
            "ETH",
            "USDC",            
            0.01,
            PriceType::Home,
            0.01,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.ETHPERP".to_string(),
                "orderbook.200.ETHPERP".to_string(),
            ],
            Some("ETHPERP")
        );
        config
    }

    #[classattr]
    pub fn MNTUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "MNT",
            "USDT",            
            0.0001,
            PriceType::Home,
            1.0,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.MNTUSDT".to_string(),
                "orderbook.200.MNTUSDT".to_string(),
            ],
            None,
        )
    }

    #[classattr]
    pub fn MNTUSDC() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "MNT",
            "USDT",            
            0.0001,
            PriceType::Home,
            0.1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.MNT-PERP".to_string(),
                "orderbook.200.MNT-PERP".to_string(),
            ],
            Some("MNT-PERP")
        )
    }


    #[classattr]
    pub fn SOLUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "SOL",
            "USDT",            
            0.0001,
            PriceType::Home,
            0.1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.SOLUSDT".to_string(),
                "orderbook.200.SOLUSDT".to_string(),
            ],
            None
        )
    }

    #[classattr]
    pub fn SOLUSDC() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "SOL",
            "USDC",            
            0.01,
            PriceType::Home,
            0.1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.SOLPERP".to_string(),
                "orderbook.200.SOLPERP".to_string(),
            ],
            Some("SOLPERP")
        )
    }


    #[classattr]
    pub fn USDCUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "USDC",
            "USDT",            
            0.0001,
            PriceType::Home,
            0.1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.USDCUSDT".to_string(),
                "orderbook.200.USDCUSDT".to_string(),
            ],
            None,
        )
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }
}

#[cfg(test)]
mod test_bybit_config {
    use super::*;

    #[test]
    fn test_bybit_server_config() {
        let config = BybitServerConfig::new(true);
        println!("{:?}", config);
    }

    #[test]
    fn test_bybit_config() {
        let config = BybitConfig::new();
        println!("{:?}", config);
    }

    #[test]
    fn test_create_market_config() {
        let config = BybitConfig::BTCUSDT();
        println!("{:?}", config);

        let new_config = MarketConfig::new(
            BYBIT,
            "linear",
            "BTC",
            "USDT",
            0.1,
            PriceType::Home,
            0.001,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
            None
        );

        assert_eq!(config, new_config);
    }
}

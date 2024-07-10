#![allow(non_snake_case)]
use std::env;

use pyo3::prelude::*;
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

        let api_key = env::var("BYBIT_API_KEY").unwrap_or_default();
        let api_secret = env::var("BYBIT_API_SECRET").unwrap_or_default();

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

    /*
    #[classattr]
    pub fn SPOT_BTCUSDT() -> MarketConfig {
        MarketConfig {
            price_unit: dec![0.05],
            price_scale: 3,
            size_unit: dec![0.001],
            size_scale: 4,
            maker_fee: dec![0.00_01],
            taker_fee: dec![0.00_01],
            price_type: PriceType::Home,
            fee_type: FeeType::Home,
            home_currency: "USDT".to_string(),
            foreign_currency: "BTC".to_string(),
            market_order_price_slip: dec![0.01],
            board_depth: 200,
            trade_category: "spot".to_string(),
            trade_symbol: "BTCUSDT".to_string(),
            public_subscribe_channel: vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
        }
    }
    */

    #[classattr]
    pub fn BTCUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "USDT",
            "BTC",
            1,
            PriceType::Home,
            3,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
        )
    }

    #[classattr]
    pub fn BTCUSDC() -> MarketConfig {
        let mut config = MarketConfig::new(
            BYBIT,
            "linear",
            "USDC",
            "BTC",
            1,
            PriceType::Home,
            3,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCPERP".to_string(),
                "orderbook.200.BTCPERP".to_string(),
            ],
        );

        config.trade_symbol = "BTCPERP".to_string();
        config
    }


    #[classattr]
    pub fn ETHUSDT() -> MarketConfig {
        let config = MarketConfig::new(
            BYBIT,
            "linear",
            "USDT",            
            "ETH",
            2,
            PriceType::Home,
            2,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.ETHUSDT".to_string(),
                "orderbook.200.ETHUSDT".to_string(),
            ],
        );

        config
    }

    #[classattr]
    pub fn ETHUSDC() -> MarketConfig {
        let mut config = MarketConfig::new(
            BYBIT,
            "linear",
            "USDC",            
            "ETH",
            2,
            PriceType::Home,
            2,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.ETHPERP".to_string(),
                "orderbook.200.ETHPERP".to_string(),
            ],
        );

        config.trade_symbol = "ETHPERP".to_string();
        config
    }

    #[classattr]
    pub fn MNTUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "USDT",            
            "MNT",
            1,
            PriceType::Home,
            0,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.MNTUSDT".to_string(),
                "orderbook.200.MNTUSDT".to_string(),
            ],
        )
    }


    #[classattr]
    pub fn SOLUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "USDT",            
            "SOL",
            2,
            PriceType::Home,
            1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.SOLUSDT".to_string(),
                "orderbook.200.SOLUSDT".to_string(),
            ],
        )
    }


    #[classattr]
    pub fn USDCUSDT() -> MarketConfig {
        MarketConfig::new(
            BYBIT,
            "linear",
            "USDT",            
            "USDC",
            4,
            PriceType::Home,
            1,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.USDCUSDT".to_string(),
                "orderbook.200.USDCUSDT".to_string(),
            ],
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
            "USDT",
            "BTC",
            1,
            PriceType::Home,
            3,
            200,
            0.1,
            0.00_01,
            0.00_01,
            FeeType::Home,
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
        );

        assert_eq!(config, new_config);
    }
}

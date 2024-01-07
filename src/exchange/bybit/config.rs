#![allow(non_snake_case)]
use std::env;

use pyo3::prelude::*;
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};

use crate::{
    common::{FeeType, MarketConfig, PriceType},
    fs::db_full_path, exchange::to_mask_string,
};

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitServerConfig {
    pub exchange_name: String,
    pub testnet: bool,
    pub rest_server: String,
    pub public_ws: String,
    pub private_ws: String,
    pub db_base_dir: String,
    pub history_web_base: String,
    #[serde(serialize_with = "to_mask_string")]
    pub api_key: String,
    #[serde(serialize_with = "to_mask_string")]
    pub api_secret: String,
}

#[pymethods]
impl BybitServerConfig {
    #[new]
    pub fn new(testnet: bool) -> Self {
        let rest_server = if testnet {
            "https://api-testnet.bybit.com"
        } else {
            "https://api.bybit.com"
        }
        .to_string();

        let public_ws_server = if testnet {
            "wss://stream-testnet.bybit.com/v5/public"
        } else {
            "wss://stream.bybit.com/v5/public"
        }
        .to_string();

        let private_ws_server = if testnet {
            "wss://stream-testnet.bybit.com/v5/private"
        } else {
            "wss://stream.bybit.com/v5/private"
        }
        .to_string();

        let api_key = env::var("BYBIT_API_KEY").unwrap_or_default();
        let api_secret = env::var("BYBIT_API_SECRET").unwrap_or_default();

        return BybitServerConfig {
            exchange_name: "BYBIT".to_string(),
            testnet,
            rest_server,
            public_ws: public_ws_server,
            private_ws: private_ws_server,
            db_base_dir: "".to_string(),
            history_web_base: "https://public.bybit.com".to_string(),
            api_key,
            api_secret
        };
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }

}

#[derive(Debug, Clone, Serialize)]
#[pyclass]
pub struct BybitConfig {}

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
        MarketConfig {
            price_unit: dec![0.1],
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
            trade_category: "linear".to_string(),
            trade_symbol: "BTCUSDT".to_string(),
            public_subscribe_channel: vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
        }

    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }
}

/*
#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitConfig {
    pub server_config: BybitServerConfig,
    pub market_config: MarketConfig,
    pub db_base_dir: String,
}

#[pymethods]
impl BybitConfig {
    pub fn get_db_path(&self) -> String {
        let mut exchange_name = self.exchange_name.clone();

        if self.testnet {
            exchange_name = format!("{}-TESTNET", exchange_name);
        }

        let db_path = db_full_path(&exchange_name, &self.trade_category, &self.trade_symbol, &self.db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }

    #[classattr]
    pub fn SPOT_BTCUSDT() -> Self {
        return BybitConfig {
            exchange_name: "BYBIT".to_string(),
            testnet: false,
            rest_endpoint: "https://api.bybit.com".to_string(),
            history_web_base: "https://public.bybit.com".to_string(),
            db_base_dir: "".to_string(),
            public_stream_endpoint: "wss://stream.bybit.com/v5/public/spot".to_string(),
            private_stream_endpoint: "wss://stream.bybit.com/v5/private".to_string(),
            market_config: MarketConfig {
                price_unit:dec![0.05],
                price_scale:3,
                size_unit:dec![0.001],
                size_scale:4,
                maker_fee:dec![0.00_01],
                taker_fee:dec![0.00_01],
                price_type:PriceType::Home,
                fee_type:FeeType::Home,
                home_currency:"USDT".to_string(),
                foreign_currency:"BTC".to_string(),
                market_order_price_slip:dec![0.01],
                board_depth:250,
                trade_category: "spot".to_string(),
                trade_symbol: "BTCUSDT".to_string(),
            }
            }
        };
    }
}
*/
// Copyright(c) 2022-2023. yasstake. All rights reserved.

use pyo3::{pyclass, pymethods};
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};
use serde_json::json;

use crate::{fs::db_full_path, common::MarketConfig};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[pyclass]
pub struct BinanceConfig {
    #[pyo3(set)]    
    pub test_net: bool,

    #[pyo3(set)]
    pub exchange_name: String,
    #[pyo3(set)]    
    pub trade_category: String,
    #[pyo3(set)]    
    pub trade_symbol: String,
    #[pyo3(set)]    
    pub home_currency: String,
    #[pyo3(set)]    
    pub foreign_currency: String,
    #[pyo3(set)]
    pub testnet: bool,

    // server config
    #[pyo3(set)]    
    pub rest_endpoint: String,
    #[pyo3(set)]    
    pub public_ws_endpoint: String,
    #[pyo3(set)]    
    pub private_ws_endpoint: String,
    #[pyo3(set)]    
    pub history_web_base: String,
    #[pyo3(set)]    
    pub new_order_path: String,
    #[pyo3(set)]    
    pub cancel_order_path: String,
    #[pyo3(set)]    
    pub open_orders_path: String,
    #[pyo3(set)]    
    pub account_path: String,
    #[pyo3(set)]    
    pub public_subscribe_message: String,
    #[pyo3(set)]    
    pub user_data_stream_path: String,

    // key & secret
    #[pyo3(set)]
    pub api_key: String,
    #[pyo3(set)]
    pub api_secret: String,

    #[pyo3(get)]
    pub market_config: MarketConfig,

    #[pyo3(get, set)]
    pub db_base_dir: String,
}

#[pymethods]
impl BinanceConfig {
    #[allow(non_snake_case)]
    #[classattr]
    pub fn BTCUSDT() -> Self {
        return BinanceConfig::SPOT("BTC", "USDT");
    }

    #[classattr]
    #[allow(non_snake_case)]    
    pub fn TEST_BTCUSDT() -> Self {
        let mut config = BinanceConfig::TESTSPOT("BTC", "USDT");

        config.home_currency = "USDT".to_string();
        config.foreign_currency = "BTC".to_string();

        return config;
    }

    #[allow(non_snake_case)]    
    #[staticmethod]
    pub fn TESTSPOT(foreign_symbol: &str, home_symbol: &str) -> Self {
        let mut config = BinanceConfig::SPOT(foreign_symbol, home_symbol);

        config.test_net = true;
        config.trade_category = "TESTSPOT".to_string();
        config.rest_endpoint = "https://testnet.binance.vision".to_string();
        config.public_ws_endpoint = "wss://testnet.binance.vision/ws".to_string();
        config.private_ws_endpoint = "wss://testnet.binance.vision/ws".to_string();
        config.testnet = true;
        config.market_config.market_order_price_slip = dec![0.5];

        return config;
    }

    #[allow(non_snake_case)]    
    #[staticmethod]
    pub fn SPOT(foreign_symbol: &str, home_symbol: &str) -> Self {

        let symbol = format!("{}{}", foreign_symbol, home_symbol);

        let upper_symbol = symbol.to_uppercase();
        let lower_symbol = symbol.to_lowercase();

        let api_key = if let Ok(key) = std::env::var("BINANCE_API_KEY") {
            key
        }
        else {
            log::error!("no key found in env[BINANCE_API_KEY]");
            "".to_string()
        };

        let api_secret = if let Ok(secret) = std::env::var("BINANCE_API_SECRET") {
            secret
        }
        else {
            log::error!("no secret found in env[BINANCE_API_SECRET]");
            "".to_string()
        };

        let mut market_config = MarketConfig::new(
            home_symbol,
            foreign_symbol,
            2,
            4,
        );

        market_config.taker_fee = dec![0.0001];
        market_config.maker_fee = dec![0.0001];

        return BinanceConfig {
            test_net: false,
            exchange_name: "BN".to_string(),
            trade_category: "SPOT".to_string(),
            trade_symbol: upper_symbol,

            home_currency: home_symbol.to_string(),
            foreign_currency: foreign_symbol.to_string(),

            rest_endpoint: "https://api.binance.com".to_string(),
            public_ws_endpoint: "wss://stream.binance.com:9443/ws".to_string(),
            private_ws_endpoint: "wss://stream.binance.com:9443/ws".to_string(),
            history_web_base: "https://data.binance.vision/data/spot/daily/trades".to_string(),
            new_order_path: "/api/v3/order".to_string(),
            cancel_order_path: "/api/v3/order".to_string(),
            open_orders_path: "/api/v3/openOrders".to_string(),
            account_path: "/api/v3/account".to_string(),
            user_data_stream_path: "/api/v3/userDataStream".to_string(),

            public_subscribe_message: json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        format!("{}@trade", lower_symbol),
                        format!("{}@depth@100ms", lower_symbol)
                    ],
                    "id": 1
                }
            )
            .to_string(),
            testnet: false,
            api_key,
            api_secret,
            market_config,
            db_base_dir: "".to_string(),
        };
    }

    #[getter]
    pub fn get_db_path(&self) -> String {
        let mut exchange_name = self.exchange_name.clone();

        if self.testnet {
            exchange_name = format!("{}-TESTNET", exchange_name);
        }

        let db_path = db_full_path(&exchange_name, &self.trade_category, &self.trade_symbol, &self.db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }

    pub fn __repr__(&self) -> String {
        let mut printobj = self.clone();

        if printobj.api_key.len() > 2 {
            printobj.api_key = format!("{}*******************", printobj.api_key[0..2].to_string());
        } else {
            printobj.api_key = "!! NO KEY !!".to_string();
        }

        if printobj.api_secret.len() > 2 {
            printobj.api_secret = format!(
                "{}*******************",
                printobj.api_secret[0..2].to_string()
            );
        } else {
            printobj.api_secret = "!! NO SECRET !!".to_string();
        }

        serde_json::to_string(&printobj).unwrap()
    }

    pub fn short_info(&self) -> String {
        if self.test_net {
            return format!("---TEST NET--- {}", self.trade_symbol);
        }
        else {
            return format!("*** LIVE NET *** {}", self.trade_symbol);
        }
    }
}

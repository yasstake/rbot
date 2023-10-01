use pyo3::{pyclass, pymethods};
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};
use serde_json::json;

use crate::{fs::db_full_path, common::MarketConfig};



#[derive(Clone, Debug, Serialize, Deserialize)]
#[pyclass]
pub struct BinanceConfig {
    pub exchange_name: String,
    pub trade_category: String,
    pub trade_symbol: String,
    
    pub home_currency: String,
    pub foreign_currency: String,

    pub testnet: bool,

    // server config
    pub rest_endpoint: String,
    pub public_ws_endpoint: String,
    pub private_ws_endpoint: String,
    pub history_web_base: String,
    pub new_order_path: String,
    pub cancel_order_path: String,
    pub public_subscribe_message: String,

    // key & secret
    pub api_key: String,
    pub api_secret: String,

    pub exchange_config: MarketConfig
}

#[pymethods]
impl BinanceConfig {
    #[classattr]
    pub fn BTCUSDT() -> Self {
        return BinanceConfig::SPOT("BTC", "USDT");
    }

    #[classattr]
    pub fn TEST_BTCUSDT() -> Self {
        let mut config = BinanceConfig::TESTSPOT("BTC", "USDT");

        config.home_currency = "USDT".to_string();
        config.foreign_currency = "BTC".to_string();

        return config;
    }

    #[staticmethod]
    pub fn TESTSPOT(foreign_symbol: &str, home_symbol: &str) -> Self {
        let mut config = BinanceConfig::SPOT(foreign_symbol, home_symbol);

        config.trade_category = "TESTSPOT".to_string();
        config.rest_endpoint = "https://testnet.binance.vision".to_string();
        config.public_ws_endpoint = "wss://testnet.binance.vision/ws".to_string();
        config.private_ws_endpoint = "wss://testnet.binance.vision/ws".to_string();
        config.testnet = true;

        return config;
    }

    #[staticmethod]
    pub fn SPOT(foreign_symbol: &str, home_symbol: &str) -> Self {

        let symbol = format!("{}{}", foreign_symbol, home_symbol);

        let upper_symbol = symbol.to_uppercase();
        let lower_symbol = symbol.to_lowercase();

        let api_key = std::env::var("BINANCE_API_KEY").expect("BINANCE_API_KEY is not set");
        let api_secret =
            std::env::var("BINANCE_API_SECRET").expect("BINANCE_API_SECRET is not set");

        return BinanceConfig {
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
            exchange_config: MarketConfig::new(
                home_symbol,
                foreign_symbol,
                dec!(0.01),    // in btc
                dec!(0.00001), // in btc
            ),
        };
    }

    #[getter]
    pub fn get_db_path(&self) -> String {
        let mut exchange_name = self.exchange_name.clone();

        if self.testnet {
            exchange_name = format!("{}-TESTNET", exchange_name);
        }

        let db_path = db_full_path(&exchange_name, &self.trade_category, &self.trade_symbol);

        return db_path.to_str().unwrap().to_string();
    }

    pub fn __repr__(&self) -> String {
        let mut printobj = self.clone();

        if printobj.api_key.len() > 4 {
            printobj.api_key = format!("{}*******************", printobj.api_key[0..4].to_string());
        } else {
            printobj.api_key = "!! NO KEY !!".to_string();
        }

        if printobj.api_secret.len() > 4 {
            printobj.api_secret = format!(
                "{}*******************",
                printobj.api_secret[0..4].to_string()
            );
        } else {
            printobj.api_secret = "!! NO SECRET !!".to_string();
        }

        serde_json::to_string(&printobj).unwrap()
    }
}

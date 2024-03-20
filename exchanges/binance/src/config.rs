#![allow(non_snake_case)]
// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::env;

use pyo3::{pyclass, pymethods};
use rust_decimal_macros::dec;

use rbot_lib::common::{FeeType, MarketConfig, PriceType, SecretString, ServerConfig};

use crate::BINANCE;

/// see https://binance-docs.github.io/apidocs/spot/en/#general-info

#[derive(Clone, Debug)]
#[pyclass]
pub struct BinanceServerConfig {
    pub production: bool,
    pub rest_server: String,
    pub public_ws: String,
    pub private_ws: String,
    pub history_web_base: String,
    api_key: SecretString,
    api_secret: SecretString,
}

#[pymethods]
impl BinanceServerConfig {
    #[new]
    pub fn new(production: bool) -> Self {
        let rest_server = if production {
            "https://api.binance.com"            
        } else {
            "https://testnet.binance.vision"
        }
        .to_string();

        let public_ws_server = if production {
            "wss://stream.binance.com:9443/ws"            
        } else {
            "wss://testnet.binance.vision/ws"
        }
        .to_string();

        let private_ws_server = if production {
            "wss://stream.binance.com:9443"            
        } else {
            "wss://testnet.binance.vision"
        }
        .to_string();

        let api_key = env::var("BINANCE_API_KEY").unwrap_or_default();
        let api_secret = env::var("BINANCE_API_SECRET").unwrap_or_default();

        return BinanceServerConfig {

            production,
            rest_server,
            public_ws: public_ws_server,
            private_ws: private_ws_server,
            history_web_base: "https://data.binance.vision".to_string(),
            api_key: SecretString::new(&api_key),
            api_secret: SecretString::new(&api_secret),
        };
    }
}


impl ServerConfig for BinanceServerConfig {

    fn get_rest_server(&self) -> String {
        self.rest_server.clone()
    }

    fn get_public_ws_server(&self) -> String {
        self.public_ws.clone()
    }

    
    fn get_historical_web_base(&self) -> String {
        self.history_web_base.clone()
    }
    
    fn get_user_ws_server(&self) -> String {
        self.private_ws.clone()
    }
    
    fn get_api_key(&self) -> SecretString {
        self.api_key.clone()
    }
    
    fn get_api_secret(&self) -> SecretString {
        self.api_secret.clone() 
    }
}


#[pyclass]
pub struct BinanceConfig {

}

#[pymethods]
impl BinanceConfig {
    #[new]
    pub fn new() -> Self {
        return BinanceConfig {};
    }

    #[classattr]
    pub fn BTCUSDT() -> MarketConfig {
        MarketConfig {
            exchange_name: BINANCE.to_string(),            
            price_unit: dec![0.5],
            price_scale: 2,
            size_unit: dec![0.001],
            size_scale: 4,
            maker_fee: dec![0.00_01],
            taker_fee: dec![0.00_01],
            price_type: PriceType::Home,
            fee_type: FeeType::Home,
            home_currency: "USDT".to_string(),
            foreign_currency: "BTC".to_string(),
            market_order_price_slip: dec![0.5],
            board_depth: 1000,
            trade_category: "SPOT".to_string(),
            trade_symbol: "BTCUSDT".to_string(),
            public_subscribe_channel: vec![
                "btcusdt@trade".to_string(),
                "btcusdt@depth@100ms".to_string(),
            ],
        }
    }
}


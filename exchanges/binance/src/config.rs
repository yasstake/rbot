#![allow(non_snake_case)]
// Copyright(c) 2022-2024. yasstake. All rights reserved.

use pyo3::{pyclass, pymethods};

use rbot_lib::common::MarketConfig;
use rbot_lib::common::ExchangeConfig;

use crate::BINANCE;

/// see https://binance-docs.github.io/apidocs/spot/en/#general-info

#[derive(Clone, Debug)]
#[pyclass]
pub struct BinanceServerConfig {
}

impl BinanceServerConfig {
    pub fn new(production: bool) -> ExchangeConfig {
        let rest_server = if production {
            "https://api.binance.com"            
        } else {
            "https://testnet.binance.vision"
        };

        let public_ws_server = if production {
            "wss://stream.binance.com:9443/ws"            
        } else {
            "wss://testnet.binance.vision/ws"
        };

        let private_ws_server = if production {
            "wss://stream.binance.com:9443"            
        } else {
            "wss://testnet.binance.vision"
        };

        ExchangeConfig::new(
            BINANCE,
            production,
            rest_server,
            rest_server,
            public_ws_server,
            private_ws_server,
            "https://data.binance.vision",
        )    
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
        ExchangeConfig::open_exchange_market("binance", "BTC/USDT").unwrap()

        /*
        MarketConfig {
            exchange_name:BINANCE.to_string(),
            price_unit:dec![0.5],
            size_unit:dec![0.001],
            maker_fee:dec![0.00_01],
            taker_fee:dec![0.00_01],
            fee_type:FeeType::Home,
            home_currency:"USDT".to_string(),
            foreign_currency:"BTC".to_string(),
            market_order_price_slip:dec![0.5],
            board_depth:1000,trade_category:"spot".to_string(),trade_symbol:"BTCUSDT".to_string(), 
            quote_currency: "USDT".to_string(), 
            settle_currency: Some("USDT".to_string()), 
        }
        */
    }
}


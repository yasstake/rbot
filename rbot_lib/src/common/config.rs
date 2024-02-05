// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use pyo3::pyclass;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeeType {
    Home,
    Foreign,
    Both,
}
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriceType {
    Home,
    Foreign,
    Both,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    #[pyo3(set, get)]    
    pub trade_category: String,
    #[pyo3(set, get)]    
    pub trade_symbol: String,

    #[pyo3(set)]
    pub price_unit: Decimal,
    #[pyo3(set)]    
    pub price_scale: u32,
    
    #[pyo3(set)]
    pub size_unit: Decimal,
    #[pyo3(set)]    
    pub size_scale: u32,

    #[pyo3(set)]
    pub maker_fee: Decimal,
    #[pyo3(set)]    
    pub taker_fee: Decimal,

    #[pyo3(set)]
    pub price_type: PriceType,
    #[pyo3(set)]    
    pub fee_type: FeeType,

    #[pyo3(set)]
    pub home_currency: String,
    #[pyo3(set)]    
    pub foreign_currency: String,
    
    #[pyo3(set)]
    pub market_order_price_slip: Decimal,

    #[pyo3(set)]
    pub board_depth: u32,

    #[pyo3(set)]
    pub public_subscribe_channel: Vec<String>,
}

impl MarketConfig {
    pub fn new(
        trade_category: &str,
        home_currency: &str,
        foreign_currency: &str,
        price_scale: u32,
        size_scale: u32,
        board_depth: u32,
    ) -> Self {
        Self {
            price_unit: Decimal::new(1, price_scale),
            price_scale,
            size_unit: Decimal::new(1, size_scale),
            size_scale,
            maker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            taker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            price_type: PriceType::Foreign,
            fee_type: FeeType::Home,
            home_currency: home_currency.to_string(),
            foreign_currency: foreign_currency.to_string(),
            market_order_price_slip: dec![0.0],
            board_depth: board_depth,
            trade_category: trade_category.to_string(),
            trade_symbol: format!("{}{}", foreign_currency, home_currency),
            public_subscribe_channel: vec![],
        }
    }

    pub fn new_bitflyer(
        trade_category: &str,
        home_currency: &str,
        foreign_currency: &str,
        price_scale: u32,
        size_scale: u32,
    ) -> Self {
        let symbol = if trade_category == "FX" {
            format!("FX_{}_{}", foreign_currency, home_currency)
        }
        else {
            format!("{}_{}", foreign_currency, home_currency)
        };

        Self {
            price_unit: Decimal::new(1, price_scale),
            price_scale,
            size_unit: Decimal::new(1, size_scale),
            size_scale,
            maker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            taker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            price_type: PriceType::Foreign,
            fee_type: FeeType::Home,
            home_currency: home_currency.to_string(),
            foreign_currency: foreign_currency.to_string(),
            market_order_price_slip: dec![0.0],
            board_depth: 1000,
            trade_category: trade_category.to_string(),
            trade_symbol: symbol,
            public_subscribe_channel: vec![],
        }
    }



}


pub trait ServerConfig {
    fn get_historical_web_base(&self) -> String;
    fn get_public_ws_server(&self) -> String;
    fn get_user_ws_server(&self) -> String;
    fn get_rest_server(&self) -> String;
    fn get_api_key(&self) -> String;
    fn get_api_secret(&self) -> String;
}
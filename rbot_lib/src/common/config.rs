// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use pyo3::{pyclass, pymethods};
use rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};

use crate::db::KEY::price;

use super::SecretString;


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum FeeType {
    Home,
    Foreign,
    Both,
}
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum PriceType {
    Home,
    Foreign,
    Both,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketConfig {
    #[pyo3(set)]
    pub exchange_name: String,
    #[pyo3(set, get)]    
    pub trade_category: String,
    #[pyo3(set, get)]    
    pub trade_symbol: String,

    #[pyo3(set)]
    pub price_unit: Decimal,
    
    #[pyo3(set)]
    pub size_unit: Decimal,

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

#[pymethods]
impl MarketConfig {
    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    #[new]
    pub fn new(
        exchange_name: &str,
        trade_category: &str,
        foreign_currency: &str,
        home_currency: &str,
        price_unit: Decimal,
        price_type: PriceType,
        size_unit: Decimal,
        board_depth: u32,
        market_order_price_slip: f64,
        maker_fee: f64,
        taker_fee: f64,
        fee_type: FeeType,
        public_subscribe_channel: Vec<String>,
    ) -> Self {
        let maker_fee = Decimal::from_f64(maker_fee).unwrap();
        let taker_fee = Decimal::from_f64(taker_fee).unwrap();

        Self {
            exchange_name: exchange_name.to_string(),
            price_unit: price_unit,
            size_unit: size_unit,
            maker_fee,
            taker_fee,
            price_type,
            fee_type,
            home_currency: home_currency.to_string(),
            foreign_currency: foreign_currency.to_string(),
            market_order_price_slip: Decimal::from_f64(market_order_price_slip).unwrap(),
            board_depth,
            trade_category: trade_category.to_string(),
            trade_symbol: format!("{}{}", foreign_currency, home_currency),
            public_subscribe_channel: public_subscribe_channel
        }
    }

    pub fn key_string(&self, production: bool) -> String {
        if production {
            format!("{}/{}/{}", self.exchange_name, self.trade_category, self.trade_symbol)
        }
        else {
            format!("{}/{}/{}/test", self.exchange_name, self.trade_category, self.trade_symbol)
        }
    }
}


pub trait ServerConfig : Send + Sync {
    fn get_historical_web_base(&self) -> String;
    fn get_public_ws_server(&self) -> String;
    fn get_user_ws_server(&self) -> String;
    fn get_rest_server(&self) -> String;
    fn get_api_key(&self) -> SecretString;
    fn get_api_secret(&self) -> SecretString;
    fn is_production(&self) -> bool;
}


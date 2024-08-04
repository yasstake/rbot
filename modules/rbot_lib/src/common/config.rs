// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use super::SecretString;
use anyhow::anyhow;
use pyo3::{pyclass, pymethods, types::PyAnyMethods as _, Bound, PyAny, PyResult};
use rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde_derive::{Deserialize, Serialize};
use zip::read::Config;

pub trait ServerConfig: Send + Sync {
    fn get_historical_web_base(&self) -> String;
    fn get_public_ws_server(&self) -> String;
    fn get_user_ws_server(&self) -> String;
    fn get_rest_server(&self) -> String;
    fn get_api_key(&self) -> SecretString;
    fn get_api_secret(&self) -> SecretString;
    fn is_production(&self) -> bool;
}

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

    pub price_unit: Decimal,
    pub size_unit: Decimal,

    pub maker_fee: Decimal,
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

fn round(unit: Decimal, value: Decimal) -> anyhow::Result<Decimal> {
    let scale = unit.scale();

    let v = (value / unit).floor() * unit; // price_unitで切り捨て
    let v = v.round_dp(scale);

    if v == dec![0.0] {
        log::warn!(
            "Price or size becomes zero value= {} / unit= {} => {}",
            value,
            unit,
            v
        );
        return Err(anyhow!(
            "Price or size becomes zero value= {} / unit= {} => {}",
            value,
            unit,
            v
        ));
    }
    Ok(v)
}

#[pymethods]
impl MarketConfig {
    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn round_price(&self, price: Decimal) -> anyhow::Result<Decimal> {
        round(self.price_unit, price)
    }

    pub fn round_size(&self, size: Decimal) -> anyhow::Result<Decimal> {
        round(self.size_unit, size)
    }

    #[new]
    pub fn new(
        exchange_name: &str,
        trade_category: &str,
        foreign_currency: &str,
        home_currency: &str,
        price_unit: f64,
        price_type: PriceType,
        size_unit: f64,
        board_depth: u32,
        market_order_price_slip: f64,
        maker_fee: f64,
        taker_fee: f64,
        fee_type: FeeType,
        public_subscribe_channel: Vec<String>,
        trade_symbol: Option<&str>,
    ) -> Self {
        let maker_fee = Decimal::from_f64(maker_fee).unwrap();
        let taker_fee = Decimal::from_f64(taker_fee).unwrap();

        let symbol = if let Some(symbol) = trade_symbol {
            symbol.to_string()
        } else {
            format!("{}{}", foreign_currency, home_currency)
        };

        Self {
            exchange_name: exchange_name.to_string(),
            price_unit: Decimal::from_f64(price_unit).unwrap(),
            size_unit: Decimal::from_f64(size_unit).unwrap(),
            maker_fee,
            taker_fee,
            price_type,
            fee_type,
            home_currency: home_currency.to_string(),
            foreign_currency: foreign_currency.to_string(),
            market_order_price_slip: Decimal::from_f64(market_order_price_slip).unwrap(),
            board_depth,
            trade_category: trade_category.to_string(),
            public_subscribe_channel: public_subscribe_channel,
            trade_symbol: symbol,
        }
    }

    #[setter]
    pub fn set_price_unit(&mut self, unit: f64) {
        self.price_unit = Decimal::from_f64(unit).unwrap();
    }

    #[getter]
    pub fn get_price_unit(&self) -> Decimal {
        self.price_unit.clone()
    }

    #[setter]
    pub fn set_size_unit(&mut self, unit: f64) {
        self.size_unit = Decimal::from_f64(unit).unwrap();
    }

    #[getter]
    pub fn get_size_unit(&self) -> Decimal {
        self.size_unit.clone()
    }

    #[setter]
    pub fn set_maker_fee(&mut self, fee: f64) {
        self.maker_fee = Decimal::from_f64(fee).unwrap();
    }

    #[getter]
    pub fn get_maker_fee(&mut self) -> Decimal {
        self.maker_fee.clone()
    }

    #[setter]
    pub fn set_taker_fee(&mut self, fee: f64) {
        self.taker_fee = Decimal::from_f64(fee).unwrap();
    }

    #[getter]
    pub fn get_taker_fee(&mut self) -> Decimal {
        self.taker_fee.clone()
    }

    pub fn key_string(&self, production: bool) -> String {
        if production {
            format!(
                "{}/{}/{}",
                self.exchange_name, self.trade_category, self.trade_symbol
            )
        } else {
            format!(
                "{}/{}/{}/test",
                self.exchange_name, self.trade_category, self.trade_symbol
            )
        }
    }

    fn __eq__(&self, other: &Bound<Self>) -> PyResult<bool> {
        let other = other
            .extract::<MarketConfig>()
            .expect("Expected MarketConfig");
        Ok(self.eq(&other))
    }
}

impl Default for MarketConfig {
    fn default() -> Self {
        MarketConfig::new(
            "default_exchange",
            "default_trade_category",
            "default_foreign_currency",
            "default_home_currency",
            0.01,
            PriceType::Home,
            0.01,
            10,
            0.0,
            0.0,
            0.0,
            FeeType::Home,
            vec![],
            None,
        )
    }
}

#[cfg(test)]
mod test_market_config {
    use rust_decimal_macros::dec;

    use crate::common::init_debug_log;

    use super::MarketConfig;

    #[test]
    fn round_price() -> anyhow::Result<()> {
        let mut config = MarketConfig::default();
        config.price_unit = dec![0.5];

        let price = dec![0.51];
        let round = config.round_price(price)?;
        assert_eq!(round, dec![0.5]);

        let price = dec![0.6];
        let round = config.round_price(price)?;
        assert_eq!(round, dec![0.5]);

        Ok(())
    }

    #[test]
    fn round_size() -> anyhow::Result<()> {
        init_debug_log();
        let mut config = MarketConfig::default();
        config.price_unit = dec![0.001];

        // when trunc into zero, receive err.
        let price = dec![0.0001];
        let round = config.round_price(price);
        assert!(round.is_err());

        let price = dec![0.11];
        let round = config.round_price(price)?;
        assert_eq!(round, dec![0.11]);

        Ok(())
    }

    #[test]
    fn test_price_size_unit() {
        let mut config = MarketConfig::default();

        config.set_price_unit(0.123);
        assert_eq!(config.get_price_unit(), dec![0.123]);

        config.set_size_unit(1.23);
        assert_eq!(config.get_size_unit(), dec![1.23]);
    }
}

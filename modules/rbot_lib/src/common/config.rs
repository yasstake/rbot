// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABSOLUTELY NO WARRANTY.

use super::{env_api_key, env_api_secret, get_market_config, get_server_config, list_exchange, list_symbols, SecretString};
use anyhow::anyhow;
use pyo3::{pyclass, pymethods, types::PyAnyMethods as _, Bound, PyAny, PyResult};
use rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use rust_decimal_macros::dec;
use serde_derive::{Deserialize, Serialize};
use zip::read::Config;

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    exchange_name: String,
    production: bool,
    public_api: String,
    private_api: String, 
    public_ws: String,
    private_ws: String,
    history_web_base: String,
    api_key: SecretString,
    api_secret: SecretString,
}

#[pymethods]
impl ExchangeConfig {
    #[new]
    pub fn new(exchange_name: &str, production: bool, public_api: &str, private_api: &str,
        public_ws: &str, private_ws: &str, history_web_base: &str 
        ) -> Self {
        ExchangeConfig {
            exchange_name: exchange_name.to_string(),
            production,
            public_api: public_api.to_string(),
            private_api: private_api.to_string(),
            public_ws: public_ws.to_string(),
            private_ws:private_ws.to_string(),
            history_web_base: history_web_base.to_string(),
            api_key: SecretString::new(&env_api_key(exchange_name, production)),
            api_secret: SecretString::new(&env_api_secret(exchange_name, production))
        }
    }

    #[staticmethod]
    #[pyo3 (signature=(exchange_name, production=true))]
    pub fn open(exchange_name: &str, production: bool) -> anyhow::Result<ExchangeConfig> {
        get_server_config(exchange_name, production)
    }

    #[staticmethod]
    pub fn open_exchange_market(exchange_name: &str, symbol: &str) -> anyhow::Result<MarketConfig> {
        get_market_config(exchange_name, symbol)
    }

    #[classattr]
    pub fn exchanges() -> anyhow::Result<Vec<String>> {
        list_exchange()
    }

    #[getter]
    pub fn get_symbols(&self) -> anyhow::Result<Vec<String>> {
        list_symbols(&self.exchange_name)
    }

    #[getter]
    pub fn get_markets(&self) -> anyhow::Result<Vec<MarketConfig>> {
        let mut markets: Vec<MarketConfig> = vec![];

        let symbols = list_symbols(&self.exchange_name)?;

        for m in symbols {
            markets.push(get_market_config(&self.exchange_name, &m)?);
        }

        Ok(markets)
    }

    pub fn open_market(&self, symbol: &str) -> anyhow::Result<MarketConfig>{
        get_market_config(&self.exchange_name, symbol)
    }

    pub fn get_exchange_name(&self) -> String {
        self.exchange_name.to_string()
    }

    pub fn is_production(&self) -> bool {
        self.production
    }

    pub fn get_public_api(&self) -> String {
        self.public_api.clone()
    }

    pub fn get_private_api(&self) -> String {
        self.private_api.clone()
    }

    pub fn get_public_ws_server(&self) -> String {
        self.public_ws.clone()
    }

    pub fn get_private_ws_server(&self) -> String {
        self.private_ws.clone()
    }

    pub fn get_historical_web_base(&self) -> String {
        self.history_web_base.clone()
    }

    pub fn get_api_key(&self) -> SecretString {
        self.api_key.clone()
    }

    pub fn get_api_secret(&self) -> SecretString {
        self.api_secret.clone()
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }
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
pub struct MarketConfig {
    #[pyo3(set)]
    pub exchange_name: String,
    #[pyo3(set, get)]
    pub trade_category: String,
    #[pyo3(set, get)]
    pub trade_symbol: String,


    #[pyo3(set)]
    pub fee_type: FeeType,

    #[pyo3(set)]
    pub home_currency: String,

    #[pyo3(set)]
    pub foreign_currency: String,

    #[pyo3(get)]
    pub quote_currency: String,

    #[pyo3(get)]
    pub settle_currency: String,

    pub price_unit: Decimal,
    pub size_unit: Decimal,

    pub min_size: Decimal, 

    pub maker_fee: Decimal,
    pub taker_fee: Decimal,

    #[pyo3(set)]
    pub market_order_price_slip: Decimal,
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
        let size = round(self.size_unit, size)?;

        if self.min_size != dec![0.0] && size < self.min_size {
            return Err(anyhow!("below min size size={}, min_size={}", size, self.min_size));
        }

        Ok(size)
    }

    #[new]
    pub fn new(
        exchange_name: &str,
        trade_category: &str,
        trade_symbol: &str,

        foreign_currency: &str,
        home_currency: &str,
        quote_currency: &str,
        settle_currency: &str,
    
        price_unit: f64,
        size_unit: f64,
        min_size: f64,

        maker_fee: f64,
        taker_fee: f64,
        fee_type: FeeType,
    ) -> Self {
        let maker_fee = Decimal::from_f64(maker_fee).unwrap();
        let taker_fee = Decimal::from_f64(taker_fee).unwrap();

        let price_unit = Decimal::from_f64(price_unit).unwrap();
        let size_unit =  Decimal::from_f64(size_unit).unwrap();

        let min_size = Decimal::from_f64(min_size).unwrap();

        Self {
            exchange_name:exchange_name.to_string(),
            trade_category:trade_category.to_string(),
            trade_symbol:trade_symbol.to_string(),
            price_unit:price_unit,
            size_unit:size_unit,
            min_size:min_size,
            maker_fee,
            taker_fee,
            fee_type,
            home_currency:home_currency.to_string(),
            foreign_currency:foreign_currency.to_string(),
            quote_currency:quote_currency.to_string(),
            settle_currency:settle_currency.to_string(), 
            market_order_price_slip: price_unit * dec![2.0]
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
            "default_trade_symbol",
            "default_foreign_currency",
            "default_home_currency",
            "default_quote",
            "default_settle_currency",
            0.01,
            0.01,
            0.1,
            0.0,
            0.0,
            FeeType::Home,
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
        config.size_unit = dec![0.001];

        // when trunc into zero, receive err.
        let size = dec![0.0001];
        let round = config.round_size(size);
        assert!(round.is_err());

        let size = dec![0.11];
        let round = config.round_size(size)?;
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

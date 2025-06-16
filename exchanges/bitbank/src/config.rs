use pyo3::prelude::*;
use rbot_lib::common::{ExchangeConfig, MarketConfig};

#[pyclass]
pub struct BitbankConfig {}

#[pymethods]
impl BitbankConfig {
    #[new]
    pub fn new() -> Self {
        return BitbankConfig {};
    }

    #[classattr]
    pub fn BTCJPY() -> MarketConfig {
        ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY").unwrap()
    }
} 
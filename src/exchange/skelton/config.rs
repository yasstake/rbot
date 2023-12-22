
use pyo3::prelude::*;

use crate::{common::MarketConfig, fs::db_full_path};

#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonConfig {
    pub exchange_name: String,
    pub testnet: bool,
    pub trade_category: String,
    pub trade_symbol: String,
    pub db_base_dir: String,
    pub market_config: MarketConfig,
}

#[pymethods]
impl SkeltonConfig {
    pub fn get_db_path(&self) -> String {
        let mut exchange_name = self.exchange_name.clone();

        if self.testnet {
            exchange_name = format!("{}-TESTNET", exchange_name);
        }

        let db_path = db_full_path(&exchange_name, &self.trade_category, &self.trade_symbol, &self.db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }
}


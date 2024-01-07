#![allow(non_snake_case)]
use std::env;

use pyo3::prelude::*;
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};

use crate::{
    common::{FeeType, MarketConfig, PriceType},
    fs::db_full_path, exchange::to_mask_string,
};

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitflyerServerConfig {
    pub exchange_name: String,
    pub rest_server: String,
    pub public_ws: String,
    pub private_ws: String,
    pub db_base_dir: String,
    #[serde(serialize_with = "to_mask_string")]
    pub api_key: String,
    #[serde(serialize_with = "to_mask_string")]
    pub api_secret: String,
}

#[pymethods]
impl BitflyerServerConfig {
    #[new]
    pub fn new() -> Self {
        let api_key = env::var("BITFLYER_API_KEY").unwrap_or_default();
        let api_secret = env::var("BITFLYER_API_SECRET").unwrap_or_default();

        return BitflyerServerConfig {
            exchange_name: "BITFLYER".to_string(),
            rest_server: "https://api.bitflyer.com".to_string(), 
            public_ws: "https://io.lightstream.bitflyer.com".to_string(),
            private_ws: "https://io.lightstream.bitflyer.com".to_string(),
            db_base_dir: "".to_string(),
            api_key,
            api_secret
        };
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let repr = serde_json::to_string(&self).unwrap();
        Ok(repr)
    }

}


pub mod rest;
pub mod ws;

use polars_core::export::num::FromPrimitive;
pub use rest::*;
use rust_decimal::Decimal;
use serde::{Deserializer, Deserialize, de};
pub use ws::*;

pub mod bybit;
pub mod binance;

pub mod orderbook;
pub use orderbook::*;

pub mod skelton;
pub use skelton::*;


pub fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse f64")),
    }
}

pub fn string_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(Decimal::from_f64(num).unwrap()),
        Err(_) => Err(de::Error::custom("Failed to parse f64")),
    }
}

pub fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<i64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse i64")),
    }
}

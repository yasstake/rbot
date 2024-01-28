pub mod rest;
pub mod ws;

use hmac::{Hmac, Mac};
use pyo3::PyResult;
use pyo3_polars::PyDataFrame;
use sha2::Sha256;
use polars_core::export::num::FromPrimitive;
pub use rest::*;
use rust_decimal::Decimal;
use serde::{Deserializer, Deserialize, de, Serializer};
use tokio::sync::broadcast;
pub use ws::*;

pub mod bybit;
pub mod binance;
pub mod bitflyer;
pub mod wrap;

pub mod orderbook;
pub use orderbook::*;
pub use wrap::*;


pub mod skelton;
pub use skelton::*;

use crate::{common::{Order, OrderSide}, AccountStatus, MarketConfig, MarketStream, MicroSec, OrderStatus, Trade};




pub fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "" {
        return Ok(0.0);
    }

    match s.parse::<f64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
    }
}

pub fn string_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s == "" {
        return Ok(Decimal::from_f64(0.0).unwrap());
    }

    match s.parse::<f64>() {
        Ok(num) => Ok(Decimal::from_f64(num).unwrap()),
        Err(_) => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
    }
}

pub fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s == "" {
        return Ok(0);
    }
    
    match s.parse::<i64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom(format!("Failed to parse i64 {}", s))),
    }
}


fn to_mask_string<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value == "" {
        return serializer.serialize_str("--- NO KEY ---");
    }

    let mask = format!("{}*******************", value[0..2].to_string());
    serializer.serialize_str(&mask)
}

pub fn hmac_sign(secret_key: &String, message: &String) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let mac = mac.finalize();

    hex::encode(mac.into_bytes())
}


pub trait Market {
        fn set_broadcast_message(&mut self, broadcast_message: bool);
        fn get_config(&self) -> MarketConfig;
        fn get_exchange_name(&self) -> String;
        fn get_trade_category(&self) -> String;
        fn get_trade_symbol(&self) -> String;
        fn drop_table(&mut self) -> PyResult<()>;
        fn get_cache_duration(&self) -> MicroSec;
        fn reset_cache_duration(&mut self);
        fn stop_db_thread(&mut self);
        fn cache_all_data(&mut self);
        fn select_trades(
            &mut self,
            start_time: MicroSec,
            end_time: MicroSec,
        ) -> PyResult<PyDataFrame>;
        fn ohlcvv(
            &mut self,
            start_time: MicroSec,
            end_time: MicroSec,
            window_sec: i64,
        ) -> PyResult<PyDataFrame>;
        fn ohlcv(
            &mut self,
            start_time: MicroSec,
            end_time: MicroSec,
            window_sec: i64,
        ) -> PyResult<PyDataFrame>;
        fn vap(
            &mut self,
            start_time: MicroSec,
            end_time: MicroSec,
            price_unit: i64,
        ) -> PyResult<PyDataFrame>;
        fn info(&mut self) -> String;
        fn get_board_json(&self, size: usize) -> PyResult<String>;
        fn get_board(&self) -> PyResult<(PyDataFrame, PyDataFrame)>;
        fn get_board_vec(&self) -> PyResult<(Vec<BoardItem>, Vec<BoardItem>)>;
        fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)>;
        fn get_file_name(&self) -> String;
        fn get_market_config(&self) -> MarketConfig;
        fn get_running(&self) -> bool;
        fn vacuum(&self);
        fn _repr_html_(&self) -> String;
        fn download(
            &mut self,
            ndays: i64,
            force: bool,
            verbose: bool,
            archive_only: bool,
            low_priority: bool,
        ) -> i64;
        fn download_latest(&mut self, verbose: bool) -> i64;
        fn start_market_stream(&mut self);
        fn start_user_stream(&mut self);
        fn get_channel(&mut self) -> MarketStream;
        fn limit_order(
            &self,
            side: &str,
            price: Decimal,
            size: Decimal,
            client_order_id: Option<&str>,
        ) -> PyResult<Vec<Order>>;
    
        fn market_order(
            &self,
            side: &str,
            size: Decimal,
            client_order_id: Option<&str>,
        ) -> PyResult<Vec<Order>>;
        fn dry_market_order(
            &self,
            create_time: MicroSec,
            order_id: &str,
            client_order_id: &str,
            side: OrderSide,
            size: Decimal,
            transaction_id: &str,
        ) -> Vec<Order>;
        fn cancel_order(&self, order_id: &str) -> PyResult<Order>;
        fn cancel_all_orders(&self) -> PyResult<Vec<Order>>;
        fn get_order_status(&self) -> PyResult<Vec<OrderStatus>>;
        fn get_open_orders(&self) -> PyResult<Vec<Order>>;
        fn get_trade_list(&self) -> PyResult<Vec<OrderStatus>>;
        fn get_account(&self) -> PyResult<AccountStatus>;
        fn get_recent_trades(&self) -> Vec<Trade>;
    }

// Copyright(c) 2022. yasstake. All rights reserved.

pub mod message;
pub mod rest;
pub mod ws;

use std::io::{stdout, Write};
use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal_macros::dec;

use crate::common::order::{OrderSide, TimeChunk, Trade};
use crate::common::time::NOW;
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::db::sqlite::{TradeTable, TradeTableDb, TradeTableQuery};
use crate::fs::db_full_path;

use super::{log_download, make_download_url_list, download_log, Board};

#[derive(Debug)]
#[pyclass(name = "_BinanceMarket")]
pub struct BinanceMarket {
    name: String,
    pub dummy: bool,
    pub db: TradeTable,
    pub bit: Board,
    pub ask: Board,
}

// TODO: 0.5は固定値なので、変更できるようにする。BTC以外の場合もあるはず。
const BOARD_PRICE_UNIT: f64 = 0.5;

#[pymethods]
impl BinanceMarket {
    #[new]
    pub fn new(market_name: &str, dummy: bool) -> Self {
        // TODO: SPOTにのみ対応しているのを変更する。
        let db_name = Self::db_path(&market_name).unwrap();

        println!("create TradeTable: {}", db_name);

        let db = TradeTable::open(db_name.as_str()).expect("cannot open db");

        db.create_table_if_not_exists();

        return BinanceMarket {
            name: market_name.to_string(),
            dummy,
            db,
            bit: Board::new(Decimal::from_f64(BOARD_PRICE_UNIT).unwrap(), true),
            ask: Board::new(Decimal::from_f64(BOARD_PRICE_UNIT).unwrap(), false),            
        };
    }

    #[staticmethod]
    pub fn db_path(market_name: &str) -> PyResult<String> {
        let db_name = db_full_path("BN", "SPOT", market_name);

        return Ok(db_name.as_os_str().to_str().unwrap().to_string());
    }

    #[getter]
    pub fn get_cache_duration(&self) -> MicroSec {
        return self.db.get_cache_duration();
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let days_gap = self.db.make_time_days_chunk_from_days(ndays, force);
        let urls: Vec<String> = make_download_url_list(self.name.as_str(), days_gap, Self::make_historical_data_url_timestamp);
        let tx = self.db.start_thread();
        let download_rec = download_log(urls, tx, false, BinanceMarket::rec_to_trade);

        return download_rec;
    }

    pub fn cache_all_data(&mut self) {
        self.db.update_cache_all();
    }

    pub fn select_trades_a(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_select_trades(from_time, to_time);
    }

    pub fn ohlcvv_a(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcvv(from_time, to_time, window_sec);
    }

    pub fn ohlcv_a(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcv(from_time, to_time, window_sec);
    }


    pub fn select_trades(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_select_trades_polars(from_time, to_time);
    }

    pub fn ohlcvv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcvv_polars(from_time, to_time, window_sec);
    }

    pub fn ohlcv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcv_polars(from_time, to_time, window_sec);
    }


    pub fn info(&mut self) -> String {
        return self.db.info();
    }

    #[getter]
    fn get_ask(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.ask.to_pyarray();
    }

    #[getter]
    fn get_bit(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.bit.to_pyarray();
    }

    #[getter]
    fn get_file_name(&self) -> String {
        return self.db.get_file_name();
    }

    fn vaccum(&self) {
        self.db.vaccum();
    }

    pub fn _repr_html_(&self) -> String {
        return format!("<b>Binance DB ({})</b>{}", self.name, self.db._repr_html_());
    }
}

const HISTORY_WEB_BASE: &str = "https://data.binance.vision/data/spot/daily/trades";
impl BinanceMarket {
    fn make_historical_data_url_timestamp(name: &str, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        // https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip
        return format!(
            "{}/{}/{}-trades-{:04}-{:02}-{:02}.zip",
            HISTORY_WEB_BASE, name, name, yyyy, mm, dd
        );
    }

    fn rec_to_trade(rec: &StringRecord) -> Trade {
        let id = rec.get(0).unwrap_or_default().to_string();
        let price = rec
            .get(1)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();
        let size = rec
            .get(2)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();
        let timestamp = rec
            .get(4)
            .unwrap_or_default()
            .parse::<MicroSec>()
            .unwrap_or_default()
            * 1_000;
        let is_buyer_make = rec.get(5).unwrap_or_default();
        let order_side = match is_buyer_make {
            "True" => OrderSide::Buy,
            "False" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

        let trade = Trade::new(timestamp, order_side, price, size, id);

        return trade;
    }
}

#[cfg(test)]
mod binance_test {
    use crate::common::{init_debug_log, init_log};

    use super::*;

    #[test]
    fn test_make_historical_data_url_timestamp() {
        init_log();
        let market = BinanceMarket::new("BTCBUSD", true);
        println!("{}", BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 1));
        assert_eq!(
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 1),
            "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-1970-01-01.zip"            
        );
    }

    #[test]
    fn test_download() {
        init_debug_log();
        let mut market = BinanceMarket::new("BTCBUSD", true);
        println!("{}", time_string(market.db.start_time().unwrap_or(0)));
        println!("{}", time_string(market.db.end_time().unwrap_or(0)));
        println!("Let's donwload");
        market.download(2, false);
    }

    #[test]
    fn test_db_info() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        println!("{:?}", market.db.info());
    }

    #[test]
    fn test_ohlcv() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.ohlcv(0, 0, 3600);

        println!("{:?}", market.db.ohlcv_df(0, 0, 3600));
    }


    #[test]
    fn bench_ohlcv() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.ohlcv(0, 0, 3600);
    }

}

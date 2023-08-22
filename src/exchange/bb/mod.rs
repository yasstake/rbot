
// Copyright(c) 2022. yasstake. All rights reserved.

use std::io::{stdout, Write};
use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
//use pyo3::prelude::pymethods;

use crate::common::order::{OrderSide, TimeChunk, Trade};
use crate::common::time::NOW;
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::db::sqlite::{TradeTable, TradeTableDb, TradeTableQuery};
use crate::exchange::{make_download_url_list, download_log};
use crate::fs::db_full_path;

use super::log_download;

#[derive(Debug)]
#[pyclass(name = "_ByBitMarket")]
pub struct ByBitMarket {
    name: String,
    pub dummy: bool,
    pub db: TradeTable,
}

#[pymethods]
impl ByBitMarket {
    #[new]
    pub fn new(market_name: &str, dummy: bool) -> Self {
        // TODO: SPOTにのみ対応しているのを変更する。
        let db_name = Self::db_path(&market_name).unwrap();

        println!("create TradeTable: {}", db_name);

        let db = TradeTable::open(db_name.as_str()).expect("cannot open db");

        db.create_table_if_not_exists();

        return ByBitMarket {
            name: market_name.to_string(),
            dummy,
            db,
        };
    }

    #[staticmethod]
    pub fn db_path(market_name: &str) -> PyResult<String> {
        let db_name = db_full_path("BB", "SPOT", market_name);

        return Ok(db_name.as_os_str().to_str().unwrap().to_string());
    }

    #[getter]
    pub fn get_cache_duration(&self) -> MicroSec {
        return self.db.get_cache_duration();
    }

    pub fn wal_mode(&mut self) {
        let market_name = self.name.as_str();

        let db_name = Self::db_path(&market_name).unwrap();

        TradeTableDb::set_wal_mode(db_name.as_str());
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {

        let days_gap = self.db.make_time_days_chunk_from_days(ndays, force);
        let urls: Vec<String> = make_download_url_list(self.name.as_str(), days_gap, Self::make_historical_data_url_timestamp);
        let tx = self.db.start_thread();
        let download_rec = download_log(urls, tx, ByBitMarket::rec_to_trade);

        log::debug!("download rec = {}", download_rec);

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

impl ByBitMarket {
    fn make_historical_data_url_timestamp(market_name: &str, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        return Self::make_historical_data_url(market_name, yyyy, mm, dd);
    }

    fn make_historical_data_url(market_name: &str, yyyy: i64, mm: i64, dd: i64) -> String {
        let file_name = Self::log_file_name(market_name, yyyy, mm, dd);

        return format!(
            "https://public.bybit.com/trading/{}/{}",
            market_name,
            file_name
        );
    }

    fn log_file_name(market_name: &str, yyyy: i64, mm: i64, dd: i64) -> String {
        return format!(
            "{}{:04}-{:02}-{:02}.csv.gz",
            market_name,
            yyyy,
            mm,
            dd
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
    use csv::StringRecord;

    #[test]
    fn test_make_historical_data_url() {
        let url = super::ByBitMarket::make_historical_data_url("BTCUSD", 2021, 1, 1);

        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSD/BTCUSD2021-01-01.csv.gz"
        );
    }

    #[test]
    fn test_rec_to_trade() {
        let rec = StringRecord::from(vec![
        "1609545596.157273","BTCUSD","Buy","1","29395.0","ZeroMinusTick","c5a26570-e734-57e8-a2a2-a7d972046682","3401.0","1","3.401e-05"
        ]);

        let trade = super::ByBitMarket::rec_to_trade(&rec);

        assert_eq!(trade.time, 1609545596157273);
        assert_eq!(trade.price, 2.0);
        assert_eq!(trade.size, 3.0);
        assert_eq!(trade.id, "1");
        assert_eq!(trade.order_side, super::OrderSide::Sell);
    }

}

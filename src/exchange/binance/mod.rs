// Copyright(c) 2022. yasstake. All rights reserved.

use std::io::{stdout, Write};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
//use pyo3::prelude::pymethods;

use crate::common::order::{OrderSide, TimeChunk, Trade};
use crate::common::time::NOW;
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::db::sqlite::{TradeTable, TradeTableDb, TradeTableQuery};
use crate::fs::db_full_path;

use super::log_download;

#[derive(Debug)]
#[pyclass(name = "_BinanceMarket")]
pub struct BinanceMarket {
    name: String,
    pub dummy: bool,
    pub db: TradeTable,
}

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

    pub fn wal_mode(&mut self) {
        let market_name = self.name.as_str();

        let db_name = Self::db_path(&market_name).unwrap();

        TradeTableDb::set_wal_mode(db_name.as_str());
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let mut download_rec: i64 = 0;
        let from_time = NOW() - DAYS(ndays + 1);

        println!("Start download from {}", time_string(from_time));
        let _ = stdout().flush();

        let to_time = NOW() - DAYS(2);

        let time_gap = if force {
            vec![TimeChunk {
                start: from_time,
                end: to_time,
            }]
        } else {
            let start_time = self.db.start_time().unwrap_or(NOW());
            let end_time = self.db.end_time().unwrap_or(NOW());

            let mut time_chunk: Vec<TimeChunk> = vec![];

            if from_time < start_time {
                log::debug!("download before {} {}", from_time, start_time);
                time_chunk.push(TimeChunk {
                    start: from_time,
                    end: start_time,
                });
            }

            if end_time < to_time {
                log::debug!("download after {} {}", end_time, to_time);
                time_chunk.push(TimeChunk {
                    start: end_time,
                    end: to_time,
                });
            }

            time_chunk
        };

        let days_gap = TradeTable::time_chunks_to_days(&time_gap);
        log::debug!("GAP TIME: {:?}", time_gap);
        log::debug!("GAP DAYS: {:?}", days_gap);

        let mut urls: Vec<String> = vec![];
        for day in days_gap {
            urls.push(self.make_historical_data_url_timestamp(day));
        }

        let tx = self.db.start_thread();

        for url in urls {
            log::debug!("download url = {}", url);

            let mut buffer: Vec<Trade> = vec![];

            let result = log_download(url.as_str(), false, |rec| {
                let trade = BinanceMarket::rec_to_trade(&rec);

                buffer.push(trade);

                if 2000 < buffer.len() {
                    let result = tx.send(buffer.to_vec());

                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("{:?}", e);
                        }
                    }
                    buffer.clear();
                }
            });

            if buffer.len() != 0 {
                let result = tx.send(buffer.to_vec());
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{:?}", e);
                    }
                }

                buffer.clear();
            }

            match result {
                Ok(count) => {
                    log::debug!("Downloaded rec = {} ", count);
                    println!("Downloaded rec = {} ", count);
                    download_rec += count;
                }
                Err(e) => {
                    log::error!("extract err = {}", e.as_str());
                }
            }
        }

        log::debug!("download rec = {}", download_rec);

        return download_rec;
    }

    pub fn cache_all_data(&mut self) {
        self.db.update_cache_all();
    }

    pub fn select_trades(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_select_trades(from_time, to_time);
    }

    pub fn ohlcvv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcvv(from_time, to_time, window_sec);
    }

    pub fn ohlcv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcv(from_time, to_time, window_sec);
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

const HISTORY_WEB_BASE: &str = "https://data.binance.vision/data/spot/daily/trades";
impl BinanceMarket {
    fn make_historical_data_url_timestamp(&self, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        return self.make_historical_data_url(yyyy, mm, dd);
    }

    fn make_historical_data_url(&self, yyyy: i64, mm: i64, dd: i64) -> String {
        // https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip
        return format!(
            "{}/{}/{}-trades-{:04}-{:02}-{:02}.zip",
            HISTORY_WEB_BASE, self.name, self.name, yyyy, mm, dd
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
        println!("{}", market.make_historical_data_url_timestamp(1));
        assert_eq!(
            market.make_historical_data_url_timestamp(1),
            "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-1970-01-01.zip"            
        );
    }

    #[test]
    fn test_make_historical_data_url() {
        init_log();
        let market = BinanceMarket::new("BTCBUSD", true);
        println!("{}", market.make_historical_data_url(2022, 10, 1));
        assert_eq!(
            market.make_historical_data_url(2022, 11, 19),
            "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip"                        
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
}

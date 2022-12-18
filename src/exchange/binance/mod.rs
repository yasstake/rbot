// Copyright(c) 2022. yasstake. All rights reserved.

use std::io::{stdout, Write};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use chrono::{Datelike, };
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
//use pyo3::prelude::pymethods;


use crate::common::order::{OrderSide, Trade, TimeChunk};
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::common::time::{NOW};
use crate::db::sqlite::TradeTable;
use crate::fs::db_full_path;

use super::{log_download};

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
        let db_name = db_full_path("BN", &market_name);

        let db = TradeTable::open(db_name.to_str().unwrap()).expect("cannot open db");
        db.create_table_if_not_exists();

        return BinanceMarket {
            name: market_name.to_string(),
            dummy,
            db,
        };
    }

    #[getter]
    pub fn get_cache_duration(&self) -> MicroSec {
        return self.db.get_cache_duration();
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();

        let mut download_rec: i64 = 0;
        let from_time = NOW() - DAYS(ndays+1);

        println!("Start download from {}", time_string(from_time));
        let _ = stdout().flush();

        let to_time = NOW() - DAYS(2);

        let time_gap =
            if force {
                vec![TimeChunk{
                    start: from_time,
                    end: to_time
                }]
            }
            else {
                let start_time = self.db.start_time().unwrap();
                let end_time = self.db.end_time().unwrap();

                let mut time_chunk:Vec<TimeChunk> = vec![];

                
                if from_time < start_time {
                    log::debug!("download before {} {}", from_time, start_time);
                    time_chunk.push(TimeChunk { start: from_time, end: start_time});
                }

                if end_time < to_time {
                    log::debug!("download after {} {}", end_time, to_time);
                    time_chunk.push(TimeChunk {start: end_time, end: to_time});
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

        let _handle = thread::spawn(move || {
            for url in urls {
                log::debug!("download url = {}", url);

                let mut buffer: Vec<Trade> = vec![];

                let result = log_download(url.as_str(), false, |rec| {
                    let trade = BinanceMarket::rec_to_trade(&rec);

                    buffer.push(trade);

                    if 2000 < buffer.len() {
                        let _result = tx.send(buffer.to_vec());
                        buffer.clear();
                    }
                    // TODO: check send error
                });

                if buffer.len() != 0 {
                    let _result = tx.send(buffer.to_vec());
                    buffer.clear();
                }

                match result {
                    Ok(count) => {
                        log::debug!("Downloaded rec = {} ", count);
                        download_rec += count;
                    }
                    Err(e) => {
                        log::error!("extract err = {}", e);
                    }
                }
            }

            log::debug!("download rec = {}", download_rec);
        });

        let mut insert_rec_no = 0;

        // TODO: sqlite クラスへ移動させて共通にする。
        let mut last_print_rec = 0;
        loop {
            match rx.recv() {
                Ok(trades) => {
                    let result = &self.db.insert_records(&trades);
                    match result {
                        Ok(rec_no) => {
                            insert_rec_no += rec_no;

                            if 1_000_000 < (insert_rec_no - last_print_rec) {
                                print!("\rDownloading... {:.16} / rec={:>10}", time_string(trades[0].time), insert_rec_no);
                                let _ = stdout().flush();
                                last_print_rec = insert_rec_no;
                            }
                        }
                        Err(e) => {
                            log::warn!("insert error {:?}", e);
                        }
                    }
                }
                Err(_e) => {
                    break;
                }
            }
        }

        println!("download {}[rec]", insert_rec_no);
        log::debug!("insert rec={}", insert_rec_no);

        return insert_rec_no;
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
        return format!("<b>Binance DB ({})</b>{}",self.name, self.db._repr_html_());
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
        println!("{}", time_string(market.db.start_time().unwrap()));
        println!("{}", time_string(market.db.end_time().unwrap()));        
        println!("Let's donwload");
        market.download(4, false);

        println!("force download");
        market.download(1, true);        
    }

    #[test]
    fn test_db_info() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        println!("{:?}", market.db.info());
    }
}

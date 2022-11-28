use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use chrono::{DateTime, Datelike, NaiveDateTime, NaiveTime};
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
//use pyo3::prelude::pymethods;

use crate::common::init_debug_log;
use crate::common::order::{OrderSide, Trade, TimeChunk};
use crate::common::time::SEC;
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::common::time::{HHMM, NOW};
use crate::db::sqlite::TradeTable;
use crate::fs::db_full_path;

use super::{gzip_log_download, log_download};

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

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();

        let mut download_rec: i64 = 0;
        let from_time = NOW() - DAYS(ndays+1);
        let to_time = NOW() - DAYS(1);

        let time_gap =
            if force {
                vec![TimeChunk{
                    start: from_time,
                    end: to_time
                }]
            }
            else {
            self.db
                .select_gap_chunks(NOW() - DAYS(ndays + 1), NOW() - DAYS(1), HHMM(12, 0)) 
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
        loop {
            match rx.recv() {
                Ok(trades) => {
                    let result = &self.db.insert_records(&trades);
                    match result {
                        Ok(rec_no) => {
                            insert_rec_no += rec_no;
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

        log::debug!("insert rec={}", insert_rec_no);

        return insert_rec_no;
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

    pub fn info(&mut self) -> String {
        return self.db.info();
    }

    pub fn _repr_html_(&self) -> String {
        return self.db._repr_html_();
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

    /*
    pub async fn async_download(&mut self, ndays: i32, force: bool) -> i64 {
        let market = self.name.to_string();
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();


        let url = self.make_historical_data_url_timestamp(NOW()-DAYS(1));

        let handle = tokio::spawn(async move {
            log::debug!("download log url = {}", url);
            let _result = gziped_log_download(url.as_str(), |rec| {
                // id, price, qty, quoteQty, time, isBuyerMaker, isBestMatch

                let id = rec.get(0).unwrap_or_default();
                let price = rec.get(1).unwrap_or_default().parse::<f64>().unwrap_or_default();
                let size = rec.get(2).unwrap_or_default().parse::<f64>().unwrap_or_default();
                let time = rec.get(4).unwrap_or_default().parse::<MicroSec>().unwrap_or_default();
                let is_buy = rec.get(5).unwrap_or_default().parse::<bool>().unwrap_or_default();
                let order_side = if is_buy {OrderSide::Buy} else {OrderSide::Sell};

                let trade = Trade::new(time, order_side, price, size, false, id.to_string());
                println!("{:?} / {:?}", rec, trade);
                //tx.send(vec![trade]);
                // tx.send(vec![trade]);
            }).await;

            return _result.unwrap();
        });

        /*
        if force {
            log::debug!("Force donwload for {} days", ndays);
            let _handle = thread::spawn(move || {
                /*
                download_trade_callback_ndays(market.as_str(), ndays, |trade| {
                    let _ = tx.send(trade);
                });
                */
            });
        } else {
            log::debug!("Diff donwload for {} days", ndays);
            let chunks = self
                .db
                .select_gap_chunks(NOW() - DAYS(ndays as i64), 0, SEC(20));

            log::debug!("{:?}", chunks);

            let _handle = thread::spawn(move || {
                /*
                download_trade_chunks_callback(market.as_str(), &chunks, |trade| {
                    let _ = tx.send(trade);
                });
                */
            });
        }
        */

        let log_rec_no: i64 = handle.await.unwrap();
        let mut insert_rec_no = 0;

        loop {
            match rx.recv() {
                Ok(trades) => {
                    let result = &self.db.insert_records(&trades);
                    match result {
                        Ok(rec_no) => {
                            insert_rec_no += rec_no;
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

        log::debug!("log rec={} / db rec={}",log_rec_no, insert_rec_no);

        return insert_rec_no;
    }
    */
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

    /*
    #[tokio::test]
    async fn test_download_async_function() {
        init_debug_log();
        let mut market = BinanceMarket::new("BTCUSD", true);
        let url = market.make_historical_data_url_timestamp(NOW()-DAYS(1));
        log::debug!("download log url = {}", url);

        let _handle = tokio::spawn(async move {
            let _r = gziped_log_download(url.as_str(), |rec| {
                let time = rec.get(0).unwrap();
                println!("{}", time);
            }).await;
        });

        _handle.await;
    }
    */

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

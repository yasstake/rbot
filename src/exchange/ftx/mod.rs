mod message;
mod rest;

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use pyo3::prelude::*;
use rusqlite::Statement;

use crate::common::order::Trade;
use crate::common::time::MicroSec;
use crate::common::time::DAYS;
use crate::common::time::NOW;
use crate::common::time::SEC;

use self::rest::{download_trade_callback_ndays, download_trade_chunks_callback};
use crate::db::sqlite::TradeTable;
use crate::fs::db_full_path;

use numpy::IntoPyArray;
use numpy::PyArray2;


#[derive(Debug)]
#[pyclass(name = "_FtxMarket")]
pub struct FtxMarket {
    name: String,
    pub dummy: bool,
    pub db: TradeTable,
}

#[pymethods]
impl FtxMarket {
    #[new]
    pub fn new(market_name: &str, dummy: bool) -> Self {
        let db_name = db_full_path("FTX", &market_name);

        let db = TradeTable::open(db_name.to_str().unwrap()).expect("cannot open db");
        db.create_table_if_not_exists();

        return FtxMarket {
            name: market_name.to_string(),
            dummy,
            db,
        };
    }
    
    pub fn download(&mut self, ndays: i32, force: bool) -> i64 {
        let market = self.name.to_string();
        let (tx, rx): (Sender<Vec<Trade>>, Receiver<Vec<Trade>>) = mpsc::channel();

        if force {
            log::debug!("Force donwload for {} days", ndays);
            let _handle = thread::spawn(move || {
                download_trade_callback_ndays(market.as_str(), ndays, |trade| {
                    let _ = tx.send(trade);
                });
            });
        } else {
            log::debug!("Diff donwload for {} days", ndays);
            let chunks = self
                .db
                .select_gap_chunks(NOW() - DAYS(ndays as i64), 0, SEC(20));

            log::debug!("{:?}", chunks);

            let _handle = thread::spawn(move || {
                download_trade_chunks_callback(market.as_str(), &chunks, |trade| {
                    let _ = tx.send(trade);
                });
            });
        }

        let mut insert_rec_no = 0;

        loop {
            match rx.recv() {
                Ok(trades) => {
                    let result = &self.db.insert_records(&trades);
                    match result {
                        Ok(rec_no) => {
                            insert_rec_no += rec_no;                            
                        },
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

        return insert_rec_no;
    }


    pub fn select_trades(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.db.select_array(from_time, to_time);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn ohlcvv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.db.ohlcv_array(from_time, to_time, window_sec);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn info(&mut self) -> String {
        return self.db.info();
    }

    pub fn _repr_html_(&self) -> String {
        return self.db._repr_html_();
    }
}

pub trait DbForeach {
    fn foreach_trade<F>(&mut self, from_time: MicroSec, to_time: MicroSec, f: F)where  F: FnMut(&Trade);
}

impl DbForeach for FtxMarket {
    fn foreach_trade<F>(&mut self, from_time: MicroSec, to_time: MicroSec, f: F)
    where  F: FnMut(&Trade) {
            log::debug!("foreach_trade (from={}, to={})", from_time, to_time);
            self.db.select(from_time, to_time, f);            
    }
}

impl FtxMarket {
    pub fn select_all_statement(&self) -> Statement {
        return self.db.select_all_statement();
    }
}

///////////////////////////////////////////////////////////////////////////////////////////
// TEST
///////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test_exchange_ftx {
    use crate::common::init_log;

    use super::*;
    #[test]
    fn test_download_missing_chunks() {
        init_log();

        let mut ftx = FtxMarket::new("BTC-PERP", true);

        ftx.download(60, false);
    }
}

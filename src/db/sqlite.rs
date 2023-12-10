// Copyright(c) 2022-2023. yasstake. All rights reserved.

use crossbeam_channel::unbounded;
use numpy::IntoPyArray;
use numpy::PyArray2;
use polars::prelude::DataFrame;
use polars_core::prelude::IndexOrder;
use pyo3::{Py, PyResult, Python};
use pyo3_polars::PyDataFrame;
use rusqlite::params_from_iter;
use rusqlite::{params, Connection, Error, Result, Transaction};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;

use crate::common::LogStatus;
use crate::common::OrderSide;
use crate::common::SEC;
use crate::common::flush_log;
use crate::common::{time_string, MicroSec, CEIL, DAYS, FLOOR_DAY, FLOOR_SEC, NOW};
use crate::common::{TimeChunk, Trade};
use crate::db::df::merge_df;
use crate::db::df::ohlcvv_df;
use crate::db::df::ohlcvv_from_ohlcvv_df;
use crate::db::df::select_df;
use crate::db::df::start_time_df;
use crate::db::df::TradeBuffer;
use crate::db::df::{end_time_df, make_empty_ohlcvv, ohlcv_df, ohlcv_from_ohlcvv_df};

use crate::db::df::KEY;
use polars::prelude::Float64Type;

use crossbeam_channel::Sender;
use std::thread;

use super::df::convert_timems_to_datetime;
use super::df::vap_df;

#[derive(Debug)]
pub struct TradeTableDb {
    pub file_name: String,
    connection: Connection,
}

impl TradeTableDb {
    pub fn clone_connection(&self) -> TradeTableDb {
        let conn = Connection::open(&self.file_name).unwrap();
        let db = TradeTableDb {
            file_name: self.file_name.clone(),
            connection: conn,
        };

        return db;
    }

    pub fn delete_unstable_data(tx: &Transaction, start_time: MicroSec, end_time: MicroSec) {
        let sql = r#"delete from trades where $1 <= time_stamp and time_stamp < $2 and status = "U""#;

        let result = tx.execute(sql, params![start_time, end_time]);

        match result {
            Ok(rec_size) => {
                log::debug!("delete unstable data [{}]\n", rec_size);
            }
            Err(e) => {
                log::error!("delete unstable data error {}", e);
            }
        }
    }

    // TODO: delete before insert data.
    // insert records with param transaction and trades
    pub fn insert_transaction(tx: &Transaction, trades: &Vec<Trade>) -> Result<i64, Error> {
        let mut insert_len = 0;

        let sql = r#"insert or replace into trades (time_stamp, action, price, size, status, id)
                                values (?1, ?2, ?3, ?4, ?5, ?6) "#;

        for rec in trades {
            let result = tx.execute(
                sql,
                params![
                    rec.time,
                    rec.order_side.to_string(),
                    rec.price.to_f64().unwrap(),
                    rec.size.to_f64().unwrap(),
                    rec.status.to_string(),
                    rec.id
                ],
            );

            match result {
                Ok(size) => {
                    insert_len += size;
                }
                Err(e) => {
                    log::error!("insert error {}", e);
                    return Err(e);
                }
            }
        }

        Ok(insert_len as i64)
    }

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> Result<i64, Error> {
        let trades_len = trades.len();
        let start_time = trades[0].time - SEC(5);
        let end_time = trades[trades_len - 1].time;        
        
        // create transaction with immidate mode
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .unwrap();

        // when fix data comes, delete unstable data first
        if trades_len!= 0 && trades[0].status != LogStatus::UnFix {
            Self::delete_unstable_data(&tx, start_time, end_time);
        }

        // then insert data
        let insert_len = Self::insert_transaction(&tx, trades)?;

        let result = tx.commit();

        match result {
            Ok(_) => Ok(insert_len as i64),
            Err(e) => return Err(e),
        }
    }

    pub fn is_wal_mode(name: &str) -> bool {
        let conn = Connection::open(name.to_string()).unwrap();

        let result = conn.pragma_query_value(None, "journal_mode", |row| {
            let value: String = row.get(0)?;
            log::debug!("journal_mode = {}", value);
            Ok(value)
        });

        match result {
            Ok(mode) => {
                if mode == "wal" {
                    log::debug!("wal mode already set");
                    return true;
                } else {
                    return false;
                }
            }
            Err(e) => {
                log::error!("get wal mode error = {}", e);
                return false;
            }
        }
    }

    /// Sets the Write-Ahead Logging (WAL) mode for the SQLite database located at the given file path.
    /// If the WAL mode is already set, returns false. If not set, sets the mode and returns true.
    ///
    /// # Arguments
    ///
    /// * `name` - A string slice that holds the file path of the SQLite database.
    ///
    /// # Returns
    ///
    /// A boolean value indicating whether the WAL mode was successfully set or not.
    pub fn set_wal_mode(name: &str) -> bool {
        if Self::is_wal_mode(name) {
            return false;
        }

        let conn = Connection::open(name.to_string()).unwrap();
        let result = conn.pragma_update(None, "journal_mode", "wal");

        match result {
            Ok(_result) => {
                println!("set wal mode result success");
            }
            Err(e) => {
                println!("set wal mode error = {}", e);
            }
        }

        println!("set wal mode end");

        let _ = conn.close();
        return true;
    }

    /*
    pub fn select_start_end_rec(&self, date: MicroSec) -> Result<Vec<Trade>, Error> {
        let date_start = FLOOR_DAY(date);
        let date_end = date_start + DAYS(1);

        let sql = r#"select time_stamp, action, price, size, status, id from trades where $1 < time_stamp and time_stamp < $2 and (status = "S" or status = "E")"#;

        let mut trades: Vec<Trade> = vec![];

        let statement = self.connection.prepare(sql).unwrap();

        let result = statement.query_map([date_start, date_end], |row| {
            let time_stamp: i64 = row.get(0).unwrap();
            let bs_str: String = row.get(1).unwrap();
            let bs: OrderSide = OrderSide::from(&bs_str);
            let price: f64 = row.get(2).unwrap();
            let price = Decimal::from_f64(price).unwrap();
            let size: f64 = row.get(3).unwrap();
            let size = Decimal::from_f64(size).unwrap();
            let status_str: String = row.get(4).unwrap();
            let status = LogStatus::from(status_str.as_str());
            let id: String = row.get(5).unwrap();

            let trade = Trade::new(time_stamp, bs, price, size, status, id);
                
            trades.push(trade);
        });

        if result.is_err() {
            log::error!("select start end rec error {:?}", result);
            return Err(result.unwrap_err());
        }

        Ok(trades)
    }
    */
}

impl TradeTableDb {
    fn open(name: &str) -> Result<Self, Error> {
        log::debug!("open database {}", name);

        let result = Connection::open(name);
        log::debug!("Database open path = {}", name);

        match result {
            Ok(conn) => {
                let db = TradeTableDb {
                    file_name: name.to_string(),
                    connection: conn,
                };

                Ok(db)
            }
            Err(e) => {
                log::debug!("{:?}", e);
                return Err(e);
            }
        }
    }

    fn is_table_exsit(&self) -> bool {
        let sql = "select count(*) from sqlite_master where type='table' and name='trades'";

        let result = self.connection.query_row(sql, [], |row| {
            let count: i64 = row.get(0)?;
            Ok(count)
        });

        match result {
            Ok(count) => {
                if count == 0 {
                    return false;
                } else {
                    return true;
                }
            }
            Err(e) => {
                log::error!("is_table_exsit error {:?}", e);
                return false;
            }
        }
    }

    fn create_table_if_not_exists(&self) -> Result<(), Error> {
        if self.is_table_exsit() {
            return Ok(());
        }

        let _r = self.connection.execute(
            "CREATE TABLE IF NOT EXISTS trades (
            time_stamp    INTEGER,
            action  TEXT,
            price   NUMBER,
            size    NUMBER,
            status  TEXT,
            id      TEXT primary key
        )",
            (),
        );

        let r = self.connection.execute(
            "CREATE index if not exists time_index on trades(time_stamp)",
            (),
        );
        if r.is_err() {
            log::error!("create table error {:?}", r);
            r.unwrap();
        }

        let r = self.connection.pragma_update(None, "journal_mode", "wal"); 
        if r.is_err() {
            log::error!("set wal mode error {:?}", r);
            r.unwrap();
        }

        Ok(())
    }

    fn drop_table(&self) -> Result<(), Error> {
        println!("database is dropped. To use database, restart the program.");
        println!("To delete completely, delete the database file from OS.");
        println!("DB file = {}", self.file_name);
        flush_log();

        let r = self.connection.execute("drop table trades", ());

        if r.is_err() {
            log::error!("drop table error {:?}", r);
        }

        self.vacuum()?;

        Ok(())
    }

    fn vacuum(&self) -> Result<(), Error> {
        let r = self.connection.execute("VACUUM", ());

        if r.is_err() {
            log::error!("vacuum error {:?}", r);
        }

        Ok(())
    }

    fn recreate_table(&self) -> Result<(), Error> {
        self.create_table_if_not_exists()?;
        self.drop_table()?;
        self.create_table_if_not_exists()?;

        Ok(())
    }

    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select<F>(&mut self, start_time: MicroSec, end_time: MicroSec, mut f: F)
    where
        F: FnMut(&Trade),
    {
        let sql: &str;
        let param: Vec<i64>;

        if 0 < end_time {
            sql = "select time_stamp, action, price, size, status, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![start_time, end_time];
        } else {
            //sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp order by time_stamp";
            sql = "select time_stamp, action, price, size, status, id from trades where $1 <= time_stamp order by time_stamp";
            param = vec![start_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let start_time = NOW();

        let _transaction_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs: OrderSide = bs_str.as_str().into();
                let status_str: String = row.get_unwrap(4);
                let status = LogStatus::from(status_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: Decimal::from_f64(row.get_unwrap(2)).unwrap(),
                    size: Decimal::from_f64(row.get_unwrap(3)).unwrap(),
                    order_side: bs,
                    status: status,
                    id: row.get_unwrap(5),
                })
            })
            .unwrap();

        log::debug!("create iter {} microsec", NOW() - start_time);

        for trade in _transaction_iter {
            match trade {
                Ok(t) => {
                    f(&t);
                }
                Err(e) => log::error!("{:?}", e),
            }
        }
    }

    pub fn select_query(&mut self, sql: &str, param: Vec<i64>) -> Vec<Trade> {
        let mut statement = self.connection.prepare(sql).unwrap();
        let mut trades: Vec<Trade> = vec![];

        let _transaction_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs: OrderSide = bs_str.as_str().into();
                let status_str: String = row.get_unwrap(4);
                let status = LogStatus::from(status_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: Decimal::from_f64(row.get_unwrap(2)).unwrap(),
                    size: Decimal::from_f64(row.get_unwrap(3)).unwrap(),
                    order_side: bs,
                    status: status,
                    id: row.get_unwrap(5),
                })
            })
            .unwrap();

        for trade in _transaction_iter {
            match trade {
                Ok(t) => {
                    trades.push(t);
                }
                Err(e) => log::error!("{:?}", e),
            }
        }

        return trades;
    }

    /*
    fn select_all_statement(&self) -> Statement {
        let statement = self
            .connection
            .prepare("select time_stamp, action, price, size, status, id from trades order by time_stamp")
            .unwrap();
        return statement;
    }

    fn select_statement(&self, start_time: MicroSec, end_time: MicroSec) -> (Statement, Vec<i64>) {
        let sql: &str;
        let param: Vec<i64>;

        if 0 < end_time {
            sql = "select time_stamp, action, price, size, status, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![start_time, end_time];
        } else {
            sql = "select time_stamp, action, price, size, status, id from trades where $1 <= time_stamp order by time_stamp";
            param = vec![start_time];
        }

        let statement = self.connection.prepare(sql).unwrap();

        return (statement, param);
    }

    fn start_time(&self) -> Result<MicroSec, Error> {
        let sql = "select time_stamp from trades order by time_stamp asc limit 1";

        let r = self.connection.query_row(sql, [], |row| {
            let min: i64 = row.get(0)?;
            Ok(min)
        });

        return r;
    }

    /// select max(end) time_stamp in db
    fn end_time(&self) -> Result<MicroSec, Error> {
        // let sql = "select max(time_stamp) from trades";
        let sql = "select time_stamp from trades order by time_stamp desc limit 1";

        let r = self.connection.query_row(sql, [], |row| {
            let max: i64 = row.get(0)?;
            Ok(max)
        });

        return r;
    }
    */
}

#[derive(Debug)]
pub struct TradeTable {
    file_name: String,
    pub connection: TradeTableDb,
    cache_df: DataFrame,
    cache_ohlcvv: DataFrame,
    cache_duration: MicroSec,
    tx: Option<Sender<Vec<Trade>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl TradeTable {
    const OHLCV_WINDOW_SEC: i64 = 60; // min

    pub fn start_thread(&mut self) -> Sender<Vec<Trade>> {
        // check if the thread is already started
        if self.handle.is_some() {
            let handle = self.handle.take().unwrap();

            if handle.is_finished() == false {
                log::debug!("thread is already started");

                // check self.tx is valid and return clone of self.tx
                if self.tx.is_some() {
                    return self.tx.clone().unwrap();
                }
            }
        }

        let (tx, rx) = unbounded::<Vec<Trade>>();

        let file_name = self.file_name.clone();

        self.tx = Some(tx);

        let handle = thread::spawn(move || {
            let mut db = TradeTableDb::open(file_name.as_str()).unwrap();
            loop {
                match rx.recv() {
                    Ok(trades) => {
                        let _result = db.insert_records(&trades);
                        log::debug!("recv trades: {}", trades.len());
                    }
                    Err(e) => {
                        log::error!("recv error {:?}", e);
                        break;
                    }
                }
            }
            print!("thread end");
        });

        self.handle = Some(handle);

        return self.tx.clone().unwrap();
    }

    pub fn is_thread_running(&self) -> bool {
        if let Some(handler) = self.handle.as_ref() {
            if handler.is_finished() {
                log::debug!("thread is finished");
                return false;
            } else {
                log::debug!("thread is running");
                return true;
            }
        } else {
            log::debug!("thread is not started");
            return false;
        }
    }

    pub fn get_cache_duration(&self) -> MicroSec {
        return self.cache_duration;
    }

    pub fn ohlcv_start(t: MicroSec) -> MicroSec {
        return FLOOR_SEC(t, TradeTable::OHLCV_WINDOW_SEC);
    }

    pub fn ohlcv_end(t: MicroSec) -> MicroSec {
        return CEIL(t, TradeTable::OHLCV_WINDOW_SEC);
    }

    /*
        fn stop_receiving(&mut self) {
            if let Some(handle) = self.rcv_thread.take() {
                handle.join().unwrap();
            }
        }
    */

    pub fn reset_cache_duration(&mut self) {
        self.cache_duration = 0;
    }

    pub fn select_df_from_db(&mut self, start_time: MicroSec, end_time: MicroSec) -> DataFrame {
        let mut buffer = TradeBuffer::new();

        self.select(start_time, end_time, |trade| {
            buffer.push_trade(trade);
        });

        return buffer.to_dataframe();
    }

    pub fn load_df(&mut self, start_time: MicroSec, end_time: MicroSec) {
        self.cache_df = self.select_df_from_db(start_time, end_time);
    }

    pub fn update_cache_all(&mut self) {
        self.update_cache_df(0, 0);
    }

    pub fn expire_cache_df(&mut self, forget_before: MicroSec) {
        log::debug!("Expire cache {}", time_string(forget_before));
        let cache_timing = TradeTable::ohlcv_start(forget_before);
        self.cache_df = select_df(&self.cache_df, cache_timing, 0);
        self.cache_ohlcvv = select_df(&self.cache_ohlcvv, cache_timing, 0);
    }

    pub fn update_cache_df(&mut self, start_time: MicroSec, mut end_time: MicroSec) {
        log::debug!("update_cache_df {} -> {}", start_time, end_time);

        let df_start_time: i64;

        if end_time == 0 {
            end_time = NOW();
        }

        let cache_time = end_time - start_time;
        if self.cache_duration < cache_time {
            log::debug!("update cache duration {}", self.cache_duration);
            self.cache_duration = cache_time;
        }

        match start_time_df(&self.cache_df) {
            Some(t) => {
                df_start_time = t;
            }
            _ => {
                log::debug!(
                    "cache update all {} -> {}",
                    time_string(start_time),
                    time_string(end_time)
                );
                // no cache / update all
                self.load_df(start_time, end_time);

                // update ohlcv
                self.cache_ohlcvv = ohlcvv_df(
                    &self.cache_df,
                    TradeTable::ohlcv_start(start_time),
                    end_time,
                    TradeTable::OHLCV_WINDOW_SEC,
                );
                return;
            }
        }

        let df_end_time = end_time_df(&self.cache_df).unwrap();

        // load data and merge cache
        if start_time < df_start_time {
            let df1 = &self.select_df_from_db(start_time, df_start_time);
            log::debug!(
                "load data before cache df1={:?} df2={:?}",
                df1.shape(),
                self.cache_df.shape()
            );
            self.cache_df = merge_df(&df1, &self.cache_df);

            // update ohlcv
            let ohlcv1_start = TradeTable::ohlcv_start(start_time);
            let ohlcv1_end = TradeTable::ohlcv_start(df_start_time);

            log::debug!(
                "cache update diff before {} -> {}",
                time_string(ohlcv1_start),
                time_string(ohlcv1_end)
            );
            let ohlcv1 = ohlcvv_df(
                &self.cache_df,
                ohlcv1_start,
                ohlcv1_end,
                TradeTable::OHLCV_WINDOW_SEC,
            );

            if ohlcv1.shape().0 != 0 {
                let ohlcv2 = select_df(&self.cache_ohlcvv, ohlcv1_end, 0);
                self.cache_ohlcvv = merge_df(&ohlcv1, &ohlcv2);
            }
        } else {
            // expire cache ducarion * 2
            if df_start_time < start_time - self.cache_duration * 2 {
                self.expire_cache_df(start_time - self.cache_duration);
            }
        }

        if df_end_time < end_time {
            // 2日先までキャッシュを先読み
            let df2 = &self.select_df_from_db(df_end_time, end_time + DAYS(2));

            log::debug!(
                "load data AFTER cache df1={:?} df2={:?}",
                self.cache_df.shape(),
                df2.shape()
            );
            self.cache_df = merge_df(&self.cache_df, &df2);

            // update ohlcv
            let ohlcv2_start = TradeTable::ohlcv_start(start_time);
            //let ohlcv2_end = TradeTable::ohlcv_start(to_time);

            log::debug!("cache update diff after {} ", time_string(ohlcv2_start),);
            let ohlcv1 = select_df(&self.cache_ohlcvv, 0, ohlcv2_start);
            let ohlcv2 = ohlcvv_df(
                &self.cache_df,
                ohlcv2_start,
                0,
                TradeTable::OHLCV_WINDOW_SEC,
            );

            self.cache_ohlcvv = merge_df(&ohlcv1, &ohlcv2);
        }
    }

    pub fn ohlcvv_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> DataFrame {
        self.update_cache_df(start_time, end_time);

        if time_window_sec % TradeTable::OHLCV_WINDOW_SEC == 0 {
            ohlcvv_from_ohlcvv_df(&self.cache_ohlcvv, start_time, end_time, time_window_sec)
        } else {
            ohlcvv_df(&self.cache_df, start_time, end_time, time_window_sec)
        }
    }

    pub fn ohlcvv_array(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> ndarray::Array2<f64> {
        start_time = TradeTable::ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        let df = self.ohlcvv_df(start_time, end_time, time_window_sec);

        let array: ndarray::Array2<f64> = df
            .select(&[
                KEY::time_stamp,
                KEY::order_side,
                KEY::open,
                KEY::high,
                KEY::low,
                KEY::close,
                KEY::volume,
                // KEY::count,
                KEY::start_time,
                KEY::end_time,
            ])
            .unwrap()
            .to_ndarray::<Float64Type>(IndexOrder::C)
            .unwrap();

        array
    }

    pub fn py_ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.ohlcvv_array(start_time, end_time, window_sec);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn py_ohlcvv_polars(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        start_time = TradeTable::ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        let mut df = self.ohlcvv_df(start_time, end_time, window_sec);

        let df = convert_timems_to_datetime(&mut df).clone();

        return Ok(PyDataFrame(df));
    }

    pub fn ohlcv_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> DataFrame {
        self.update_cache_df(start_time, end_time);

        if time_window_sec % TradeTable::OHLCV_WINDOW_SEC == 0 {
            ohlcv_from_ohlcvv_df(&self.cache_ohlcvv, start_time, end_time, time_window_sec)
        } else {
            ohlcv_df(&self.cache_df, start_time, end_time, time_window_sec)
        }
    }

    pub fn ohlcv_array(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> ndarray::Array2<f64> {
        start_time = TradeTable::ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        let df = self.ohlcv_df(start_time, end_time, time_window_sec);

        let array: ndarray::Array2<f64> = df
            .select(&[
                KEY::time_stamp,
                KEY::open,
                KEY::high,
                KEY::low,
                KEY::close,
                KEY::volume,
                // KEY::count,
            ])
            .unwrap()
            .to_ndarray::<Float64Type>(IndexOrder::C)
            .unwrap();

        array
    }

    pub fn py_ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.ohlcv_array(start_time, end_time, window_sec);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn py_ohlcv_polars(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        start_time = TradeTable::ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        let mut df = self.ohlcv_df(start_time, end_time, window_sec);
        let df = convert_timems_to_datetime(&mut df).clone();
        let df = PyDataFrame(df);

        return Ok(df);
    }

    pub fn py_vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> PyResult<PyDataFrame> {
        let df = self.vap(start_time, end_time, price_unit);

        let py_df = PyDataFrame(df);

        Ok(py_df)
    }

    pub fn vap(&mut self, start_time: MicroSec, end_time: MicroSec, price_unit: i64) -> DataFrame {
        self.update_cache_df(start_time, end_time);
        let df = vap_df(&self.cache_df, start_time, end_time, price_unit);

        df
    }

    pub fn py_select_trades_polars(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> PyResult<PyDataFrame> {
        let mut df = self.select_df_from_db(start_time, end_time);
        let df = convert_timems_to_datetime(&mut df).clone();
        let df = PyDataFrame(df);

        return Ok(df);
    }

    pub fn py_select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.select_array(start_time, end_time);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn select_array(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> ndarray::Array2<f64> {
        self.update_cache_df(start_time, end_time);

        let trades = self.select_df_from_db(start_time, end_time);

        let array: ndarray::Array2<f64> = trades
            .select(&[KEY::time_stamp, KEY::price, KEY::size, KEY::order_side])
            .unwrap()
            .to_ndarray::<Float64Type>(IndexOrder::C)
            .unwrap();

        array
    }

    pub fn info(&mut self) -> String {
        let min = self.start_time().unwrap_or_default();
        let max = self.end_time().unwrap_or_default();

        return format!(
            "{{\"start\": {}, \"end\": {}}}",
            time_string(min),
            time_string(max)
        );
    }

    pub fn get_file_name(&self) -> String {
        return self.file_name.clone();
    }

    pub fn _repr_html_(&self) -> String {
        let min = self.start_time().unwrap_or_default();
        let max = self.end_time().unwrap_or_default();

        return format!(
            r#"
            <table>
            <caption>Trade Database info table</caption>
            <tr><th>start</th><th>end</th></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td><b>days=</b></td><td>{}</td></tr>                
            </table>                
            "#,
            min,
            max,
            time_string(min),
            time_string(max),
            (max - min) / DAYS(1),
        );
    }

    /*
    /// select min(start) time_stamp in db
    pub fn start_time(&self) -> Result<MicroSec, Error> {
        let sql = "select min(time_stamp) from trades";

        let r = self.connection.query_row(sql, [], |row| {
            let min: i64 = row.get_unwrap(0);
            Ok(min)
        });

        return r;
    }
    */

    /// Retrieves the earliest time stamp from the trades table in the SQLite database.
    /// Returns a Result containing the earliest time stamp as a MicroSec value, or an Error if the query fails.
    pub fn start_time(&self) -> Result<MicroSec, Error> {
        let sql = "select time_stamp from trades order by time_stamp asc limit 1";

        let r = self.connection.connection.query_row(sql, [], |row| {
            let min: i64 = row.get(0)?;
            Ok(min)
        });

        return r;
    }

    /// select max(end) time_stamp in db
    pub fn end_time(&self) -> Result<MicroSec, Error> {
        // let sql = "select max(time_stamp) from trades";
        let sql = "select time_stamp from trades order by time_stamp desc limit 1";

        let r = self.connection.connection.query_row(sql, [], |row| {
            let max: i64 = row.get(0)?;
            Ok(max)
        });

        return r;
    }

    /// Find un-downloaded data time chunks.
    pub fn select_gap_chunks(
        &self,
        start_time: MicroSec,
        mut end_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        if end_time == 0 {
            end_time = NOW();
        }

        let mut chunk = self.find_time_chunk_from(start_time, end_time, allow_size);
        log::debug!("chunk before {:?}", chunk);
        // find in db
        let mut c = self.select_time_chunks_in_db(start_time, end_time, allow_size);
        chunk.append(&mut c);
        log::debug!("chunk in db {:?}", chunk);

        // find after db time
        let mut c = self.find_time_chunk_to(start_time, end_time, allow_size);
        chunk.append(&mut c);
        log::debug!("chunk after {:?}", chunk);

        return chunk;
    }

    /// Find un-downloaded data chunks before db data.
    /// If db has no data, returns []
    pub fn find_time_chunk_from(
        &self,
        start_time: MicroSec,
        _end_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let db_start_time = match self.start_time() {
            Ok(t) => t,
            Err(_e) => {
                return vec![];
            }
        };

        if start_time + allow_size <= db_start_time {
            log::debug!(
                "before db {:?}-{:?}",
                time_string(start_time),
                time_string(db_start_time)
            );
            vec![TimeChunk {
                start: start_time,
                end: db_start_time,
            }]
        } else {
            vec![]
        }
    }

    pub fn find_time_chunk_to(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let mut db_end_time = match self.end_time() {
            Ok(t) => t,
            Err(_e) => {
                return vec![];
            }
        };

        if db_end_time < start_time {
            db_end_time = start_time;
        }

        if db_end_time + allow_size < end_time {
            log::debug!(
                "after db {:?}-{:?}",
                time_string(db_end_time),
                time_string(end_time)
            );
            vec![TimeChunk {
                start: db_end_time,
                end: end_time,
            }]
        } else {
            vec![]
        }
    }

    /// TODO: if NO db data, returns []
    pub fn select_time_chunks_in_db(
        &self,
        start_time: MicroSec,
        _end_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let mut chunks: Vec<TimeChunk> = vec![];

        // find select db gaps
        let sql = r#"
        select time_stamp, sub_time from (
            select time_stamp, time_stamp - lag(time_stamp, 1, 0) OVER (order by time_stamp) sub_time  
            from trades where $1 < time_stamp) 
            where $2 < sub_time order by time_stamp
        "#;

        let mut statement = self.connection.connection.prepare(sql).unwrap();
        let param = vec![start_time, allow_size];

        let chunk_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let start_time: MicroSec = row.get_unwrap(0);
                let missing_width: MicroSec = row.get_unwrap(1);

                log::debug!("{}- gap({})", time_string(start_time), missing_width);
                Ok(TimeChunk {
                    start: start_time,
                    end: start_time + missing_width,
                })
            })
            .unwrap();

        let mut index = 0;
        for chunk in chunk_iter {
            if chunk.is_ok() {
                let c = chunk.unwrap();

                // skip first row.
                if index != 0 {
                    log::debug!("gap chunk: {}-{}", time_string(c.start), time_string(c.end));
                    chunks.push(c);
                }
                index += 1;
            }
        }

        return chunks;
    }

    /// make a list of days with in time gap chunks
    pub fn time_chunks_to_days(chunks: &Vec<TimeChunk>) -> Vec<MicroSec> {
        let mut days: Vec<MicroSec> = vec![];

        let mut current_day: MicroSec = 0;

        for chunk in chunks {
            log::debug!(
                "chunk: {} -> {}",
                time_string(chunk.start),
                time_string(chunk.end)
            );
            let mut new_day = FLOOR_DAY(chunk.start);
            if current_day != new_day {
                log::debug!("DAY: {}", time_string(new_day));
                days.push(new_day);
                current_day = new_day;

                new_day += DAYS(1);
                while new_day < chunk.end {
                    log::debug!("DAY: {}", time_string(new_day));
                    days.push(new_day);
                    current_day = new_day;
                    new_day += DAYS(1);
                }
            }
        }

        return days;
    }

    pub fn make_time_days_chunk_from_days(
        &self,
        ndays: i64,
        start_time: MicroSec,
        force: bool,
    ) -> Vec<i64> {
        let from_time = FLOOR_DAY(NOW()) - DAYS(ndays);

        let time_gap = if force {
            vec![TimeChunk {
                start: from_time,
                end: start_time,
            }]
        } else {
            let start_time = self.start_time().unwrap_or(NOW());
            let end_time = self.end_time().unwrap_or(NOW());

            let mut time_chunk: Vec<TimeChunk> = vec![];

            if from_time < start_time {
                log::debug!("download before {} {}", from_time, start_time);
                time_chunk.push(TimeChunk {
                    start: from_time,
                    end: start_time,
                });
            }

            if end_time < start_time {
                log::debug!("download after {} {}", end_time, start_time);
                time_chunk.push(TimeChunk {
                    start: end_time,
                    end: start_time,
                });
            }

            time_chunk
        };

        let days_gap = TradeTable::time_chunks_to_days(&time_gap);
        log::debug!("GAP TIME: {:?}", time_gap);
        log::debug!("GAP DAYS: {:?}", days_gap);

        return days_gap;
    }

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> Result<i64, Error> {
        return self.connection.insert_records(trades);
    }
}

impl TradeTable {
    pub fn open(name: &str) -> Result<Self, Error> {
        let result = TradeTableDb::open(name);
        log::debug!("Database open path = {}", name);

        match result {
            Ok(conn) => {
                let df = TradeBuffer::new().to_dataframe();
                // let ohlcv = ohlcv_df(&df, 0, 0, TradeTable::OHLCV_WINDOW_SEC);
                let ohlcv = make_empty_ohlcvv();

                Ok(TradeTable {
                    file_name: name.to_string(),
                    connection: conn,
                    cache_df: df,
                    cache_ohlcvv: ohlcv,
                    cache_duration: 0,
                    tx: None,
                    handle: None,
                })
            }
            Err(e) => {
                log::debug!("{:?}", e);
                return Err(e);
            }
        }
    }

    pub fn create_table_if_not_exists(&self) -> Result<(), Error> {
        self.connection.create_table_if_not_exists()
    }

    pub fn drop_table(&self) -> Result<(), Error> {
        if self.is_thread_running() {
            println!("WARNING: thread is running, may cause dead lock");
            println!("Execute drop_table() before download() or start_market_stream().");
        }

        self.connection.drop_table()
    }

    pub fn vacuum(&self) -> Result<(), Error> {
        self.connection.vacuum()
    }

    pub fn recreate_table(&self) -> Result<(), Error> {
        self.connection.recreate_table()
    }

    /// Selects trades from the database within the specified time range and applies the given closure to each trade.
    ///
    /// # Arguments
    ///
    /// * `from_time` - The start time of the time range to select trades from.
    /// * `to_time` - The end time of the time range to select trades from.
    /// * `f` - A closure that will be applied to each selected trade.
    ///
    /// # Example
    ///
    /// ```
    /// use rbot::db::sqlite::SqliteDb;
    /// use rbot::models::Trade;
    ///
    /// let mut db = SqliteDb::new(":memory:").unwrap();
    /// let from_time = 0;
    /// let to_time = 100;
    /// db.insert_trade(&Trade::new(from_time, 1.0, 1.0)).unwrap();
    /// db.insert_trade(&Trade::new(to_time + 1, 2.0, 2.0)).unwrap();
    /// let mut trades = Vec::new();
    /// db.select(from_time, to_time, |trade| trades.push(trade.clone()));
    /// assert_eq!(trades.len(), 1);
    /// assert_eq!(trades[0].timestamp, from_time);
    /// ```
    fn select<F>(&mut self, start_time: MicroSec, end_time: MicroSec, f: F)
    where
        F: FnMut(&Trade),
    {
        self.connection.select(start_time, end_time, f);
    }

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   Test Suite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test_transaction_table {
    use rust_decimal_macros::dec;

    use crate::common::init_debug_log;
    use crate::common::init_log;
    use crate::common::time_string;
    use crate::common::DAYS;
    use crate::common::NOW;
    use crate::db::df::ohlcvv_from_ohlcvv_df;
    use crate::fs::db_full_path;

    use super::*;

    #[test]
    fn test_open() {
        let result = TradeTable::open("test.db");

        assert!(result.is_ok());
    }

    #[test]
    fn test_create_table_and_drop() {
        let tr = TradeTable::open("test.db").unwrap();

        let r = tr.create_table_if_not_exists();
        assert!(r.is_ok());
        
        let r = tr.drop_table();
        assert!(r.is_ok());
    }

    #[test]
    fn test_insert_table() {
        init_debug_log();
        let mut tr = TradeTable::open("test.db").unwrap();
        let r = tr.recreate_table();
        assert!(r.is_ok());

        let rec1 = Trade::new(
            1,
            OrderSide::Buy,
            dec![10.0],
            dec![10.0],
            LogStatus::UnFix,
            "abc1".to_string(),
        );
        let rec2 = Trade::new(
            2,
            OrderSide::Buy,
            dec![10.1],
            dec![10.2],
            LogStatus::UnFix,
            "abc2".to_string(),
        );
        let rec3 = Trade::new(
            3,
            OrderSide::Buy,
            dec![10.2],
            dec![10.1],
            LogStatus::UnFix,
            "abc3".to_string(),
        );

        let r = tr.insert_records(&vec![rec1, rec2, rec3]);
        assert!(r.is_ok());
    }

    #[test]
    fn test_select_fn() {
        test_insert_table();

        let mut table = TradeTable::open("test.db").unwrap();

        table.select(0, 0, |row| println!("{:?}", row));
    }

    #[test]
    fn test_select_array() {
        init_debug_log();

        let db = TradeTable::open("test.db");
        assert!(db.is_ok());
        let mut db = db.unwrap();

        let array = db.select_array(0, 0);

        println!("{:?}", array);
    }

    #[test]
    fn test_info() {
        init_debug_log();
        let mut db = TradeTable::open("test.db").unwrap();
        println!("{}", db.info());
    }

    #[test]
    fn test_start_time() {
        init_debug_log();
        let db = TradeTable::open("test.db").unwrap();

        let start_time = db.start_time();

        let s = start_time.unwrap_or(NOW());

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_end_time() {
        init_debug_log();
        let db = TradeTable::open("test.db").unwrap();

        let end_time = db.end_time();

        let s = end_time.unwrap();

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_select_gap_chunks() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", "");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_gap_chunks(NOW() - DAYS(1), NOW(), 1_000_000 * 13);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!(
                "{:?}-{:?} ?start_time={:?}&end_time={:?}",
                time_string(c.start),
                time_string(c.end),
                (c.start / 1_000_000) as i32,
                (c.end / 1_000_000) as i32
            );
        }
    }

    #[test]
    fn test_select_time_chunk_from() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", "");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_from(NOW() - DAYS(1), NOW(), 1_000_000 * 10);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunk_to() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", "/tmp");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_to(NOW() - DAYS(1), NOW(), 1_000_000 * 120);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunks() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", "/tmp");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_time_chunks_in_db(NOW() - DAYS(1), NOW(), 1_000_000 * 10);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_time_chunks_to_days() {
        let chunks = vec![TimeChunk {
            start: DAYS(1),
            end: DAYS(1) + 100,
        }];

        assert_eq!(TradeTable::time_chunks_to_days(&chunks), vec![DAYS(1)]);

        let chunks = vec![TimeChunk {
            start: DAYS(1),
            end: DAYS(3) + 100,
        }];

        assert_eq!(
            TradeTable::time_chunks_to_days(&chunks),
            vec![DAYS(1), DAYS(2), DAYS(3)]
        );
    }

    #[test]
    fn test_select_ohlcv_df() {
        init_log();
        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", "/tmp");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start_timer = NOW();
        let now = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), now, 1);
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1);
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1);
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1);
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, NOW() - DAYS(5), NOW(), 120);
        println!("{:?}", ohlcv2);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, NOW() - DAYS(1), NOW(), 60);
        println!("{:?}", ohlcv2);
        println!("{} [us]", NOW() - start_timer);
    }

    #[test]
    fn test_select_print() {
        init_log();

        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", "/tmp");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select(NOW() - DAYS(2), NOW(), |_trade| {});

        println!("{:?} / {} microsec", ohlcv, NOW() - start);
    }

    #[test]
    fn test_select_df() {
        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", "/tmp");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let df = db.select_df_from_db(NOW() - DAYS(2), NOW());

        println!("{:?}", df);
    }

    #[test]
    fn test_update_cache() {
        init_log();
        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", "/tmp");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        db.update_cache_df(NOW() - DAYS(2), NOW());
    }

    #[test]
    fn test_start_thread() {
        let mut table =
            TradeTable::open(db_full_path("BN", "SPOT", "BTCBUSD", "/tmp").to_str().unwrap()).unwrap();
        let tx = table.start_thread();

        let v = vec![Trade {
            time: 1,
            order_side: OrderSide::Buy,
            price: dec![1.0],
            size: dec![1.0],
            status: LogStatus::UnFix,
            id: "I".to_string(),
        }];
        tx.send(v).unwrap();

        let v = vec![Trade {
            time: 1,
            order_side: OrderSide::Buy,
            price: dec![1.0],
            size: dec![1.0],
            status: LogStatus::UnFix,
            id: "I".to_string(),
        }];
        tx.send(v).unwrap();

        let tx = table.start_thread();

        let v = vec![Trade {
            time: 1,
            order_side: OrderSide::Buy,
            price: dec![1.0],
            size: dec![1.0],
            status: LogStatus::UnFix,
            id: "B".to_string(),
        }];
        tx.send(v).unwrap();

        //        sleep(Duration::from_millis(5000));
    }

    #[test]
    fn test_wal_mode() {
        //let table = TradeTable::open(db_full_path("BN", "SPOT", "BTCBUSD").to_str().unwrap()).unwrap();

        TradeTableDb::set_wal_mode(db_full_path("BN", "SPOT", "BTCBUSD", "/tmp").to_str().unwrap());
    }

    #[test]
    fn test_table_is_exsit() {
        //let db_name = db_full_path("BN", "SPOT", "BTCUSDT");
        
        let db = TradeTableDb::open("/tmp/rbottest.db").unwrap();

        if db.is_table_exsit() {
            print!("table is exist");
        }
        else {
            print!("table is not exist");
        }
    }



}

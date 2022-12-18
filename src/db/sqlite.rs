// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::order::{TimeChunk, Trade};
use crate::common::time::{
    time_string, MicroSec, CEIL, DAYS, FLOOR, FLOOR_DAY, NOW,
};
use crate::OrderSide;
use numpy::IntoPyArray;
use numpy::PyArray2;
use polars::prelude::DataFrame;
use pyo3::{Py, PyResult, Python};
use rusqlite::{params, params_from_iter, Connection, Error, Result, Statement};

use super::df::{merge_df, ohlcvv_from_ohlcvv_df};
use crate::db::df::ohlcvv_df;
use crate::db::df::select_df;
use crate::db::df::start_time_df;
use crate::db::df::TradeBuffer;
use crate::db::df::{end_time_df, make_empty_ohlcvv, ohlcv_df, ohlcv_from_ohlcvv_df};

use crate::db::df::KEY;
use polars::prelude::Float64Type;

#[derive(Debug)]
pub struct TradeTable {
    file_name: String,
    connection: Connection,
    cache_df: DataFrame,
    cache_ohlcvv: DataFrame,
    cache_duration: MicroSec,
}

impl TradeTable {
    const OHLCV_WINDOW_SEC: i64 = 60; // min

    pub fn get_cache_duration(&self) -> MicroSec {
        return self.cache_duration;
    }

    pub fn ohlcv_start(t: MicroSec) -> MicroSec {
        return FLOOR(t, TradeTable::OHLCV_WINDOW_SEC);
    }

    pub fn ohlcv_end(t: MicroSec) -> MicroSec {
        return CEIL(t, TradeTable::OHLCV_WINDOW_SEC);
    }

    pub fn open(name: &str) -> Result<Self, Error> {
        let result = Connection::open(name);
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
                })
            }
            Err(e) => {
                log::debug!("{:?}", e);
                return Err(e);
            }
        }
    }

    pub fn reset_cache_duration(&mut self) {
        self.cache_duration = 0;
    }

    pub fn create_table_if_not_exists(&self) {
        let _r = self.connection.execute(
            "CREATE TABLE IF NOT EXISTS trades (
                time_stamp    INTEGER,
                action  TEXT,
                price   NUMBER,
                size    NUMBER,
                id      TEXT primary key
            )",
            (),
        );

        let _r = self.connection.execute(
            "CREATE index if not exists time_index on trades(time_stamp)",
            (),
        );
    }

    pub fn drop_table(&self) {
        let _r = self.connection.execute("drop table trades", ());
    }

    pub fn vaccum(&self) {
        let _r = self.connection.execute("VACCUM", ());
    }

    pub fn recreate_table(&self) {
        self.create_table_if_not_exists();
        self.drop_table();
        self.create_table_if_not_exists();
    }

    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select<F>(&mut self, from_time: MicroSec, to_time: MicroSec, mut f: F)
    where
        F: FnMut(&Trade),
    {
        let sql: &str;
        let param: Vec<i64>;

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![from_time, to_time];
        } else {
            //sql = "select time_stamp, action, price, size, liquid, id from trades where $1 <= time_stamp order by time_stamp";
            sql = "select time_stamp, action, price, size, id from trades where $1 <= time_stamp order by time_stamp";
            param = vec![from_time];
        }

        let mut statement = self.connection.prepare(sql).unwrap();

        let start_time = NOW();

        let _transaction_iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs = OrderSide::from_str_default(bs_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: row.get_unwrap(2),
                    size: row.get_unwrap(3),
                    order_side: bs,
                    id: row.get_unwrap(4),
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

    pub fn select_all_statement(&self) -> Statement {
        let statement = self
            .connection
            .prepare("select time_stamp, action, price, size, id from trades order by time_stamp")
            .unwrap();
        return statement;
    }

    pub fn select_statement(&self, from_time: MicroSec, to_time: MicroSec) -> (Statement, Vec<i64>) {
        let sql: &str;
        let param: Vec<i64>;

        if 0 < to_time {
            sql = "select time_stamp, action, price, size, id from trades where $1 <= time_stamp and time_stamp < $2 order by time_stamp";
            param = vec![from_time, to_time];            
        } else {
            sql = "select time_stamp, action, price, size, id from trades where $1 <= time_stamp order by time_stamp";
            param = vec![from_time];            
        }

        let statement = self.connection.prepare(sql).unwrap();

        return (statement, param);
    }

    pub fn select_df_from_db(&mut self, from_time: MicroSec, to_time: MicroSec) -> DataFrame {
        let mut buffer = TradeBuffer::new();

        self.select(from_time, to_time, |trade| {
            buffer.push_trade(trade);
        });

        return buffer.to_dataframe();
    }

    pub fn load_df(&mut self, from_time: MicroSec, to_time: MicroSec) {
        self.cache_df = self.select_df_from_db(from_time, to_time);
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

    pub fn update_cache_df(&mut self, from_time: MicroSec, mut to_time: MicroSec) {
        log::debug!("update_cache_df {} -> {}", from_time, to_time);

        let df_start_time: i64;

        if to_time == 0 {
            to_time = NOW();
        }
        
        let cache_time = to_time - from_time;
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
                    time_string(from_time),
                    time_string(to_time)
                );
                // no cache / update all
                self.load_df(from_time, to_time);

                // update ohlcv
                self.cache_ohlcvv = ohlcvv_df(
                    &self.cache_df,
                    TradeTable::ohlcv_start(from_time),
                    to_time,
                    TradeTable::OHLCV_WINDOW_SEC,
                );
                return;
            }
        }

        let df_end_time = end_time_df(&self.cache_df).unwrap();

        // load data and merge cache
        if from_time < df_start_time {
            let df1 = &self.select_df_from_db(from_time, df_start_time);
            log::debug!(
                "load data before cache df1={:?} df2={:?}",
                df1.shape(),
                self.cache_df.shape()
            );
            self.cache_df = merge_df(&df1, &self.cache_df);

            // update ohlcv
            let ohlcv1_start = TradeTable::ohlcv_start(from_time);
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
        }
        else {
            // expire cache ducarion * 2
            if df_start_time < from_time - self.cache_duration * 2 {
                self.expire_cache_df(from_time - self.cache_duration);
            }
        }

        if df_end_time < to_time {
            // 2日先までキャッシュを先読み
            let df2 = &self.select_df_from_db(df_end_time, to_time + DAYS(2));

            log::debug!(
                "load data AFTER cache df1={:?} df2={:?}",
                self.cache_df.shape(),
                df2.shape()
            );
            self.cache_df = merge_df(&self.cache_df, &df2);

            // update ohlcv
            let ohlcv2_start = TradeTable::ohlcv_start(from_time);
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
        from_time: MicroSec,
        to_time: MicroSec,
        time_window_sec: i64,
    ) -> DataFrame {
        self.update_cache_df(from_time, to_time);

        if time_window_sec % TradeTable::OHLCV_WINDOW_SEC == 0 {
            ohlcvv_from_ohlcvv_df(&self.cache_ohlcvv, from_time, to_time, time_window_sec)
        } else {
            ohlcvv_df(&self.cache_df, from_time, to_time, time_window_sec)
        }
    }

    pub fn ohlcvv_array(
        &mut self,
        mut from_time: MicroSec,
        to_time: MicroSec,
        time_window_sec: i64,
    ) -> ndarray::Array2<f64> {
        from_time = TradeTable::ohlcv_start(from_time); // 開始tickは確定足、終了は未確定足もOK.

        let df = self.ohlcvv_df(from_time, to_time, time_window_sec);

        let array: ndarray::Array2<f64> = df
            .select(&[
                KEY::time_stamp,
                KEY::order_side,
                KEY::open,
                KEY::high,
                KEY::low,
                KEY::close,
                KEY::vol,
                // KEY::count,
                KEY::start_time,
                KEY::end_time,
            ])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        array
    }

    pub fn py_ohlcvv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.ohlcvv_array(from_time, to_time, window_sec);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn ohlcv_df(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        time_window_sec: i64,
    ) -> DataFrame {
        self.update_cache_df(from_time, to_time);

        if time_window_sec % TradeTable::OHLCV_WINDOW_SEC == 0 {
            ohlcv_from_ohlcvv_df(&self.cache_ohlcvv, from_time, to_time, time_window_sec)
        } else {
            ohlcv_df(&self.cache_df, from_time, to_time, time_window_sec)
        }
    }

    pub fn ohlcv_array(
        &mut self,
        mut from_time: MicroSec,
        to_time: MicroSec,
        time_window_sec: i64,
    ) -> ndarray::Array2<f64> {
        from_time = TradeTable::ohlcv_start(from_time); // 開始tickは確定足、終了は未確定足もOK.

        let df = self.ohlcv_df(from_time, to_time, time_window_sec);

        let array: ndarray::Array2<f64> = df
            .select(&[
                KEY::time_stamp,
                KEY::open,
                KEY::high,
                KEY::low,
                KEY::close,
                KEY::vol,
                // KEY::count,
            ])
            .unwrap()
            .to_ndarray::<Float64Type>()
            .unwrap();

        array
    }

    pub fn py_ohlcv(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.ohlcv_array(from_time, to_time, window_sec);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn py_select_trades(
        &mut self,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.select_array(from_time, to_time);

        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);

            return py_array2.to_owned();
        });

        return Ok(r);
    }

    pub fn select_array(&mut self, from_time: MicroSec, to_time: MicroSec) -> ndarray::Array2<f64> {
        self.update_cache_df(from_time, to_time);

        let trades = self.select_df_from_db(from_time, to_time);

        let array: ndarray::Array2<f64> = trades
            .select(&[KEY::time_stamp, KEY::price, KEY::size, KEY::order_side])
            .unwrap()
            .to_ndarray::<Float64Type>()
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

    pub fn start_time(&self) -> Result<MicroSec, Error> {
        let sql = "select time_stamp from trades order by time_stamp asc limit 1";

        let r = self.connection.query_row(sql, [], |row| {
            let min: i64 = row.get_unwrap(0);
            Ok(min)
        });

        return r;
    }

    /// select max(end) time_stamp in db
    pub fn end_time(&self) -> Result<MicroSec, Error> {
        // let sql = "select max(time_stamp) from trades";
        let sql = "select time_stamp from trades order by time_stamp desc limit 1";

        let r = self.connection.query_row(sql, [], |row| {
            let max: i64 = row.get_unwrap(0);
            Ok(max)
        });

        return r;
    }

    /// Find un-downloaded data time chunks.
    pub fn select_gap_chunks(
        &self,
        from_time: MicroSec,
        mut to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        if to_time == 0 {
            to_time = NOW();
        }

        let mut chunk = self.find_time_chunk_from(from_time, to_time, allow_size);
        log::debug!("chunk before {:?}", chunk);
        // find in db
        let mut c = self.select_time_chunks_in_db(from_time, to_time, allow_size);
        chunk.append(&mut c);
        log::debug!("chunk in db {:?}", chunk);

        // find after db time
        let mut c = self.find_time_chunk_to(from_time, to_time, allow_size);
        chunk.append(&mut c);
        log::debug!("chunk after {:?}", chunk);

        /*
        if chunk.len() == 0 && from_time != to_time {
            return vec![TimeChunk {
                start: from_time,
                end: to_time,
            }];
        }
        */

        return chunk;
    }

    /// Find un-downloaded data chunks before db data.
    /// If db has no data, returns []
    pub fn find_time_chunk_from(
        &self,
        from_time: MicroSec,
        _to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let db_start_time = match self.start_time() {
            Ok(t) => t,
            Err(_e) => {
                return vec![];
            }
        };

        if from_time + allow_size <= db_start_time {
            log::debug!(
                "before db {:?}-{:?}",
                time_string(from_time),
                time_string(db_start_time)
            );
            vec![TimeChunk {
                start: from_time,
                end: db_start_time,
            }]
        } else {
            vec![]
        }
    }

    pub fn find_time_chunk_to(
        &self,
        from_time: MicroSec,
        to_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let mut db_end_time = match self.end_time() {
            Ok(t) => t,
            Err(_e) => {
                return vec![];
            }
        };

        if db_end_time < from_time {
            db_end_time = from_time;
        }

        if db_end_time + allow_size < to_time {
            log::debug!(
                "after db {:?}-{:?}",
                time_string(db_end_time),
                time_string(to_time)
            );
            vec![TimeChunk {
                start: db_end_time,
                end: to_time,
            }]
        } else {
            vec![]
        }
    }

    /// TODO: if NO db data, returns []
    pub fn select_time_chunks_in_db(
        &self,
        from_time: MicroSec,
        _to_time: MicroSec,
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

        let mut statement = self.connection.prepare(sql).unwrap();
        let param = vec![from_time, allow_size];

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

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> Result<i64, Error> {
        let tx = self.connection.transaction()?;

        // let trades_len = trades.len();
        let mut insert_len = 0;

        // check_skip_time(trades);

        let sql = r#"insert or replace into trades (time_stamp, action, price, size, id)
                                values (?1, ?2, ?3, ?4, ?5) "#;

        for rec in trades {
            let result = tx.execute(
                sql,
                params![
                    rec.time,
                    rec.order_side.to_string(),
                    rec.price,
                    rec.size,
                    rec.id
                ],
            );

            match result {
                Ok(size) => {
                    insert_len += size;
                }
                Err(e) => {
                    println!("insert error {}", e);
                    return Err(e);
                }
            }
        }

        let result = tx.commit();

        match result {
            Ok(_) => Ok(insert_len as i64),
            Err(e) => return Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
///   Test Suite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test_transaction_table {
    use crate::common::init_log;
    use crate::common::time::time_string;
    use crate::common::time::DAYS;
    use crate::common::time::NOW;
    use crate::db::df::ohlcvv_from_ohlcvv_df;
    use crate::fs::db_full_path;

    use super::*;

    #[test]
    fn test_open() {
        let _result = TradeTable::open("test.db");
    }

    #[test]
    fn test_create_table_and_drop() {
        let tr = TradeTable::open("test.db").unwrap();

        tr.create_table_if_not_exists();
        tr.drop_table();
    }

    #[test]
    fn test_insert_table() {
        let mut tr = TradeTable::open("test.db").unwrap();
        tr.recreate_table();

        let rec1 = Trade::new(1, OrderSide::Buy, 10.0, 10.0, "abc1".to_string());
        let rec2 = Trade::new(2, OrderSide::Buy, 10.1, 10.2, "abc2".to_string());
        let rec3 = Trade::new(3, OrderSide::Buy, 10.2, 10.1, "abc3".to_string());

        let _r = tr.insert_records(&vec![rec1, rec2, rec3]);
    }

    #[test]
    fn test_select_fn() {
        test_insert_table();

        let mut table = TradeTable::open("test.db").unwrap();
        println!("0-0");

        table.select(0, 0, |row| println!("{:?}", row));
    }

    #[test]
    fn test_select_array() {
        let db_name = db_full_path("BN", "BTCBUSD");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let array = db.select_array(0, 0);

        println!("{:?}", array);
    }

    #[test]
    fn test_info() {
        let db_name = db_full_path("BN", "BTCBUSD");

        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();
        println!("{}", db.info());
    }

    #[test]
    fn test_start_time() {
        let db_name = db_full_path("BN", "BTCBUSD");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start_time = db.start_time();

        let s = start_time.unwrap_or(NOW());

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_end_time() {
        let db_name = db_full_path("BN", "BTCBUSD");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let end_time = db.end_time();

        let s = end_time.unwrap();

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_select_gap_chunks() {
        let db_name = db_full_path("FTX", "BTC-PERP");
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
        let db_name = db_full_path("FTX", "BTC-PERP");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_from(NOW() - DAYS(1), NOW(), 1_000_000 * 10);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunk_to() {
        let db_name = db_full_path("FTX", "BTC-PERP");
        let db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_to(NOW() - DAYS(1), NOW(), 1_000_000 * 120);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunks() {
        let db_name = db_full_path("FTX", "BTC-PERP");
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
        let db_name = db_full_path("BN", "BTCBUSD");

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

        let db_name = db_full_path("BN", "BTCBUSD");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select(NOW()-DAYS(2), NOW(), |_trade| {});

        println!("{:?} / {} microsec", ohlcv, NOW() - start);
    }



    #[test]
    fn test_select_df() {
        let db_name = db_full_path("BN", "BTCBUSD");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        let df = db.select_df_from_db(NOW()-DAYS(2), NOW());

        println!("{:?}", df);
    }

    #[test]
    fn test_update_cache() {
        init_log();
        let db_name = db_full_path("BN", "BTCBUSD");
        let mut db = TradeTable::open(db_name.to_str().unwrap()).unwrap();

        db.update_cache_df(NOW() - DAYS(2), NOW());
    }
}

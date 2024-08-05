// Copyright(c) 2022-2023. yasstake. All rights reserved.

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
//use anyhow::Result;

use polars::prelude::DataFrame;
use rusqlite::params_from_iter;
use rusqlite::{params, Connection, Transaction};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::path::PathBuf;
use tokio::task::spawn;
use tokio::task::JoinHandle;

use crate::common::MarketConfig;
use crate::common::TimeChunk;
use crate::common::FLOOR_DAY;

use crossbeam_channel::unbounded;
use crossbeam_channel::Sender;

use crate::common::LogStatus;
use crate::common::OrderSide;
use crate::common::Trade;
use crate::common::SEC;
use crate::common::{time_string, MicroSec, CEIL, DAYS, FLOOR_SEC, NOW};
use crate::db::df::TradeBuffer;

use super::db_full_path;
use super::OHLCV_WINDOW_SEC;

pub fn ohlcv_floor_fix_time(t: MicroSec, unit_sec: i64) -> MicroSec {
    return FLOOR_SEC(t, unit_sec);
}

pub fn ohlcv_start(t: MicroSec) -> MicroSec {
    return FLOOR_SEC(t, OHLCV_WINDOW_SEC);
}

pub fn ohlcv_end(t: MicroSec) -> MicroSec {
    return CEIL(t, OHLCV_WINDOW_SEC);
}

pub struct TradeDb {
    config: MarketConfig,
    production: bool,
    connection: Connection,

    first_ws_message: bool,

    tx: Option<Sender<Vec<Trade>>>,
    handle: Option<JoinHandle<()>>,
}

impl TradeDb {
    /// delete unstable data, include both edge.
    /// start_time <= (time_stamp) <= end_time
    pub fn delete_virtual_data(
        tx: &Transaction,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<i64> {
        let sql =
            r#"delete from trades where ($1 <= time_stamp) and (time_stamp < $2) and status = "V""#;

        let result = tx.execute(sql, params![start_time, end_time])?;

        Ok(result as i64)
    }

    pub fn delete_date_force(
        tx: &Transaction,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<i64> {
        let sql = r#"delete from trades where $1 <= time_stamp and time_stamp < $2"#;

        let result = tx.execute(sql, params![start_time, end_time])?;

        Ok(result as i64)
    }

    /// insert trades into database
    /// return number of inserted records
    pub fn insert_transaction(tx: &Transaction, trades: &Vec<Trade>) -> anyhow::Result<i64> {
        let mut insert_len = 0;

        let sql = r#"insert or replace into trades (time_stamp, action, price, size, status, id)
                                values (?1, ?2, ?3, ?4, ?5, ?6) "#;

        for rec in trades {
            if rec.status == LogStatus::Unknown || rec.order_side == OrderSide::Unknown {
                log::error!("Invalid rec ignored: {:?}", rec);
                continue;
            }

            let no_of_records = tx.execute(
                sql,
                params![
                    rec.time,
                    rec.order_side.to_string(),
                    rec.price.to_f64().unwrap(),
                    rec.size.to_f64().unwrap(),
                    rec.status.to_string(),
                    rec.id
                ],
            )?;

            insert_len += no_of_records;
        }

        Ok(insert_len as i64)
    }

    /// begin transaction
    fn begin_transaction(&mut self) -> anyhow::Result<Transaction> {
        let tx = self
            .connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Deferred)
            .with_context(|| format!("begin transaction error"))?;

        Ok(tx)
    }

    pub fn expire_control_message(
        start_time: MicroSec,
        end_time: MicroSec,
        force: bool,
        message: &str,
    ) -> Vec<Trade> {
        let status = if force {
            LogStatus::ExpireControlForce
        } else {
            LogStatus::ExpireControl
        };

        let start_time = if start_time < 0 {
            log::warn!("expire control message with minus time {}", start_time);
            0
        } else {
            start_time
        };

        let mut trades: Vec<Trade> = vec![];
        let t = Trade::new(
            start_time,
            OrderSide::Unknown,
            dec![0.0],
            dec![0.0],
            status,
            &format!("{} start", message),
        );
        trades.push(t);

        let t = Trade::new(
            end_time,
            OrderSide::Unknown,
            dec![0.0],
            dec![0.0],
            status,
            &format!("{} end", message),
        );
        trades.push(t);

        log::debug!("expire control: {:?}", trades);

        trades
    }

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> anyhow::Result<i64> {
        let trades_len = trades.len();
        if trades_len == 0 {
            return Ok(0);
        }

        if trades[0].time > trades[trades_len - 1].time {
            log::error!("Insert order error(order in reverse) {:?}", trades);
        }

        let start_time = trades[0].time - SEC(1);
        let end_time = trades[trades_len - 1].time;
        let log_status = trades[0].status;

        log::debug!(
            "insert_records {} {} {} {}",
            trades[0].status,
            time_string(start_time),
            time_string(end_time),
            trades_len
        );

        // TODO: now log in db is unstable data only
        if log_status == LogStatus::ExpireControl && trades_len == 2 {
            log::debug!("delete unarchived data");
            let tx = self.begin_transaction()?;
            let rec = Self::delete_virtual_data(&tx, trades[0].time, trades[1].time)?;
            tx.commit()?;
            return Ok(rec);
        } else if log_status == LogStatus::ExpireControlForce && trades_len == 2 {
            log::debug!("delete unarchived data(force");
            let tx = self.begin_transaction()?;
            let rec = Self::delete_date_force(&tx, trades[0].time, trades[1].time)?;
            tx.commit()?;
            return Ok(rec);
        }

        // create transaction with immidate mode
        let tx = self.begin_transaction()?;
        // let _ = Self::delete_unstable_data(&tx, start_time, end_time);
        // then insert data
        let insert_len = Self::insert_transaction(&tx, trades)?;
        tx.commit()?;

        Ok(insert_len as i64)
    }

    pub fn is_wal_mode(name: &str) -> anyhow::Result<bool> {
        let conn = Connection::open(name.to_string())?;

        let result = conn
            .pragma_query_value(None, "journal_mode", |row| {
                let value: String = row.get(0)?;
                log::debug!("journal_mode = {}", value);
                Ok(value)
            })
            .with_context(|| format!("is_wal_mode error"))?;

        if result == "wal" {
            return Ok(true);
        } else {
            return Ok(false);
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
    pub fn set_wal_mode(name: &str) -> anyhow::Result<bool> {
        if Self::is_wal_mode(name)? {
            return Ok(false);
        }

        let conn = Connection::open(name.to_string())?;
        conn.pragma_update(None, "journal_mode", "wal")
            .with_context(|| format!("set_wal_mode error"))?;

        let _ = conn.close();

        return Ok(true);
    }

    pub fn open(config: &MarketConfig, production: bool) -> anyhow::Result<Self> {
        let db_path = db_full_path(
            &config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            production,
        );

        let create_new = Self::is_db_file_exsist(&db_path);

        let conn = Connection::open(db_path)?;

        if create_new {
            conn.pragma_update(None, "journal_mode", "wal")?;
        }

        let mut db = TradeDb {
            config: config.clone(),
            production,

            first_ws_message: true,

            connection: conn,
            tx: None,
            handle: None,
        };

        if create_new {
            db.create_table_if_not_exists()?;
        }

        Ok(db)
    }

    pub fn open_channel(&mut self) -> anyhow::Result<Sender<Vec<Trade>>> {
        // check if the thread is already started
        // check self.tx is valid and return clone of self.tx
        log::debug!("start_thread");
        if self.tx.is_some() {
            log::info!("DB Thread is already started, reuse tx");
            return Ok(self.tx.clone().unwrap().clone());
        }

        let (tx, rx) = unbounded();

        let config = self.config.clone();
        let production = self.production;
        self.tx = Some(tx);

        let handle = spawn(async move {
            let mut db = TradeDb::open(&config, production).unwrap();
            let rx = rx; // Move rx into the closure's environment
            loop {
                match rx.recv() {
                    Ok(mut trades) => {
                        if db.first_ws_message {
                            if trades.len() != 0
                                && (trades[0].status == LogStatus::UnFix
                                    || trades[0].status == LogStatus::UnFixStart)
                            {
                                trades[0].status = LogStatus::UnFixStart;

                                db.first_ws_message = false;
                            }
                        }

                        let result = db.insert_records(&trades);

                        if result.is_err() {
                            log::error!("insert error {:?}", result);
                            continue;
                        }
                        log::debug!("recv trades: {}", trades.len());
                    }
                    Err(e) => {
                        log::error!("recv error(sender program died?) {:?}", e);
                        break;
                    }
                }
            }
        });

        self.handle = Some(handle);

        return Ok(self.tx.clone().unwrap());
    }

    /// check if database file is exsit
    fn is_db_file_exsist(path: &PathBuf) -> bool {
        return path.exists();
    }

    fn create_table_if_not_exists(&mut self) -> anyhow::Result<()> {
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS trades (
            time_stamp    INTEGER,
            action  TEXT,
            price   NUMBER,
            size    NUMBER,
            status  TEXT,
            id      TEXT primary key
        )",
            (),
        )?;

        self.connection.execute(
            "CREATE index if not exists time_index on trades(time_stamp)",
            (),
        )?;

        Ok(())
    }

    pub fn vacuum(&self) -> anyhow::Result<()> {
        log::debug!("vacuum db");

        self.connection
            .execute("VACUUM", ())
            .with_context(|| format!("database VACUUM error"))?;

        Ok(())
    }

    /// select  cachedf from database
    pub fn select_cachedf(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<DataFrame> {
        let mut buffer = TradeBuffer::new();

        self.select(start_time, end_time, |trade| {
            buffer.push_trade(trade);

            Ok(())
        })?;

        return Ok(buffer.to_dataframe());
    }

    // 時間選択は左側は含み、右側は含まない。
    // 0をいれたときは全件検索
    pub fn select<F>(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
        mut f: F,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&Trade) -> anyhow::Result<()>,
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

        let mut statement = self.connection.prepare(sql)?;

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
            .with_context(|| format!("select trade error"))?;

        log::debug!("create iter {} microsec", NOW() - start_time);

        for trade in _transaction_iter {
            match trade {
                Ok(t) => {
                    let r = f(&t);
                    if r.is_err() {
                        return Err(r.err().unwrap());
                    }
                }
                Err(e) => {
                    return Err(anyhow!("select query: SQL error {}", e));
                }
            }
        }

        Ok(())
    }

    pub fn select_query(&mut self, sql: &str, param: Vec<i64>) -> anyhow::Result<Vec<Trade>> {
        let mut statement = self.connection.prepare(sql)?;
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
            .with_context(|| format!("select query: SQL error {}", sql))?;

        for trade in _transaction_iter {
            match trade {
                Ok(t) => {
                    trades.push(t);
                }
                Err(e) => {
                    log::error!("{:?}", e);
                    return Err(e.into());
                }
            }
        }

        return Ok(trades);
    }

    /// Retrieves the earliest time stamp from the trades table in the SQLite database.
    /// Returns a Result containing the earliest time stamp as a MicroSec value, or an Error if the query fails.
    pub fn start_time(&self, since_time: MicroSec) -> MicroSec {
        let sql =
            "select time_stamp from trades where $1 < time_stamp order by time_stamp asc limit 1";

        let r = self.connection.query_row(sql, [since_time], |row| {
            let min: i64 = row.get(0)?;
            Ok(min)
        });
        let time = if r.is_ok() {
            r.unwrap()
        } else {
            log::warn!("no data in db");
            0
        };

        return time;
    }

    /// select max(end) time_stamp in db
    /// returns 0 for no data.
    pub fn end_time(&self, search_from: MicroSec) -> MicroSec {
        // let sql = "select max(time_stamp) from trades";
        let sql =
            "select time_stamp from trades where $1 < time_stamp order by time_stamp desc limit 1";

        let r = self.connection.query_row(sql, [search_from], |row| {
            let max: i64 = row.get(0)?;
            Ok(max)
        });

        let time = if r.is_ok() {
            r.unwrap()
        } else {
            log::warn!("no data in db");
            0
        };

        return time;
    }

    /// 最後のWSの起動時間を探して返す。
    /// 存在しない場合はNone
    pub fn get_last_start_up_rec(&mut self) -> Option<Trade> {
        let sql = r#"select time_stamp, action, price, size, status, id from trades where status = "Us" order by time_stamp desc limit 1"#;

        let trades = self.select_query(sql, vec![]);

        log::debug!("last rec = {:?}", trades);

        if trades.is_err() {
            return None;
        }

        let trades = trades.unwrap();

        if trades.len() != 0 {
            return Some(trades[0].clone());
        }
        None
    }

    // DBにある最新のデータを取得する
    pub fn get_latest_rec(&mut self, search_before: MicroSec) -> Option<Trade> {
        let sql = r#"select time_stamp, action, price, size, status, id from trades where time_stamp < $1 order by time_stamp desc limit 1"#;

        let trades = self.select_query(sql, vec![search_before]);

        if trades.is_err() {
            return None;
        }

        let trades = trades.unwrap();

        if trades.len() != 0 {
            return Some(trades[0].clone());
        }
        None
    }

    /// Find un-downloaded data time chunks.
    pub fn select_gap_chunks(
        &self,
        start_time: MicroSec,
        mut end_time: MicroSec,
        allow_size: MicroSec,
    ) -> anyhow::Result<Vec<TimeChunk>> {
        if end_time == 0 {
            end_time = NOW();
        }

        let mut chunk = self.find_time_chunk_from(start_time, end_time, allow_size);
        log::debug!("chunk before {:?}", chunk);
        // find in db
        let mut c = self.select_time_chunks_in_db(start_time, end_time, allow_size)?;
        chunk.append(&mut c);
        log::debug!("chunk in db {:?}", chunk);

        // find after db time
        let mut c = self.find_time_chunk_to(start_time, end_time, allow_size);
        chunk.append(&mut c);
        log::debug!("chunk after {:?}", chunk);

        return Ok(chunk);
    }

    /// Find un-downloaded data chunks before db data.
    /// If db has no data, returns []
    pub fn find_time_chunk_from(
        &self,
        start_time: MicroSec,
        _end_time: MicroSec,
        allow_size: MicroSec,
    ) -> Vec<TimeChunk> {
        let db_start_time = self.start_time(0);

        if db_start_time == 0 {
            return vec![];
        }

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
        let mut db_end_time = self.end_time(0);

        if db_end_time == 0 {
            return vec![];
        }

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
    ) -> anyhow::Result<Vec<TimeChunk>> {
        let mut chunks: Vec<TimeChunk> = vec![];

        // find select db gaps
        let sql = r#"
        select time_stamp, sub_time from (
            select time_stamp, time_stamp - lag(time_stamp, 1, 0) OVER (order by time_stamp) sub_time  
            from trades where $1 < time_stamp) 
            where $2 < sub_time order by time_stamp
        "#;

        let mut statement = self.connection.prepare(sql).unwrap();
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
            .with_context(|| format!("select_time_chunks_in_db error"))?;

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
            } else {
                log::error!("select_time_chunks_in_db error {}", chunk.err().unwrap());
            }
        }

        return Ok(chunks);
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
            let now = NOW();
            let mut start_time = self.start_time(0);
            if start_time == 0 {
                start_time = now;
            }
            let mut end_time = self.end_time(0);
            if end_time == 0 {
                end_time = now;
            }

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

        let days_gap = Self::time_chunks_to_days(&time_gap);
        log::debug!("GAP TIME: {:?}", time_gap);
        log::debug!("GAP DAYS: {:?}", days_gap);

        return days_gap;
    }
}

/*
#[cfg(test)]
mod test_transaction_table {
    use rust_decimal_macros::dec;

    use crate::common::init_debug_log;
    use crate::common::init_log;
    use crate::common::time_string;
    use crate::common::DAYS;
    use crate::common::NOW;
    use crate::db::df::ohlcvv_from_ohlcvv_df;
    use crate::db::fs::db_full_path;

    use super::*;

    #[test]
    fn test_open() {
        let result = TradeDataFrame::open("test.db");

        assert!(result.is_ok());
    }

    #[test]
    fn test_create_table_and_drop() {
        let mut tr = TradeDataFrame::open("test.db").unwrap();

        let r = tr.create_table_if_not_exists();
        assert!(r.is_ok());

        let r = tr.drop_table();
        assert!(r.is_ok());
    }

    #[test]
    fn test_insert_table() {
        init_debug_log();
        let mut tr = TradeDataFrame::open("test.db").unwrap();
        let r = tr.recreate_table();
        assert!(r.is_ok());

        let rec1 = Trade::new(
            1,
            OrderSide::Buy,
            dec![10.0],
            dec![10.0],
            LogStatus::UnFix,
            "abc1",
        );
        let rec2 = Trade::new(
            2,
            OrderSide::Buy,
            dec![10.1],
            dec![10.2],
            LogStatus::UnFix,
            "abc2",
        );
        let rec3 = Trade::new(
            3,
            OrderSide::Buy,
            dec![10.2],
            dec![10.1],
            LogStatus::UnFix,
            "abc3",
        );

        let r = tr.insert_records(&vec![rec1, rec2, rec3]);
        assert!(r.is_ok());
    }

    #[test]
    fn test_info() {
        init_debug_log();
        let db = TradeDataFrame::open("test.db").unwrap();
        println!("{}", db.info());
    }

    #[test]
    fn test_start_time() {
        init_debug_log();
        let db = TradeDataFrame::open("test.db").unwrap();

        let start_time = db.start_time();

        let s = start_time.unwrap_or(NOW());

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_end_time() {
        init_debug_log();
        let db = TradeDataFrame::open("test.db").unwrap();

        let end_time = db.end_time(0);

        let s = end_time.unwrap();

        println!("{}({})", time_string(s), s);
    }

    #[test]
    fn test_select_gap_chunks() -> anyhow::Result<()> {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", false);
        let db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_gap_chunks(NOW() - DAYS(1), NOW(), 1_000_000 * 13)?;

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
        Ok(())
    }

    #[test]
    fn test_select_time_chunk_from() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", false);
        let db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_from(NOW() - DAYS(1), NOW(), 1_000_000 * 10);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunk_to() {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", false);
        let db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.find_time_chunk_to(NOW() - DAYS(1), NOW(), 1_000_000 * 120);

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
    }

    #[test]
    fn test_select_time_chunks() -> anyhow::Result<()> {
        let db_name = db_full_path("FTX", "SPOT", "BTC-PERP", false);
        let db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let chunks = db.select_time_chunks_in_db(NOW() - DAYS(1), NOW(), 1_000_000 * 10)?;

        println!("chunks {:?}", chunks);

        for c in chunks {
            println!("{:?}-{:?}", time_string(c.start), time_string(c.end));
        }
        Ok(())
    }

    #[test]
    fn test_time_chunks_to_days() {
        let chunks = vec![TimeChunk {
            start: DAYS(1),
            end: DAYS(1) + 100,
        }];

        assert_eq!(TradeDataFrame::time_chunks_to_days(&chunks), vec![DAYS(1)]);

        let chunks = vec![TimeChunk {
            start: DAYS(1),
            end: DAYS(3) + 100,
        }];

        assert_eq!(
            TradeDataFrame::time_chunks_to_days(&chunks),
            vec![DAYS(1), DAYS(2), DAYS(3)]
        );
    }

    #[test]
    fn test_select_ohlcv_df() -> anyhow::Result<()> {
        init_log();
        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", false);

        let mut db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let start_timer = NOW();
        let now = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), now, 1)?;
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1)?;
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1)?;
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv = db.ohlcvv_df(NOW() - DAYS(5), NOW(), 1)?;
        println!("{:?}", ohlcv);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, NOW() - DAYS(5), NOW(), 120)?;
        println!("{:?}", ohlcv2);
        println!("{} [us]", NOW() - start_timer);

        let start_timer = NOW();
        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, NOW() - DAYS(1), NOW(), 60)?;
        println!("{:?}", ohlcv2);
        println!("{} [us]", NOW() - start_timer);

        Ok(())
    }

    #[test]
    fn test_select_print() {
        init_log();

        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", false);
        let mut db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        let start = NOW();
        let ohlcv = db.select(NOW() - DAYS(2), NOW(), |_trade| Ok(()));

        println!("{:?} / {} microsec", ohlcv, NOW() - start);
    }

    #[test]
    fn test_update_cache() -> anyhow::Result<()> {
        init_log();
        let db_name = db_full_path("BN", "SPOT", "BTCBUSD", false);
        let mut db = TradeDataFrame::open(db_name.to_str().unwrap()).unwrap();

        db.update_cache_df(NOW() - DAYS(2), NOW())?;

        Ok(())
    }

    #[tokio::test]
    async fn test_start_thread() {
        let mut table = TradeDataFrame::open(
            db_full_path("BN", "SPOT", "BTCBUSD", false)
                .to_str()
                .unwrap(),
        )
        .unwrap();
        let tx = table.start_thread().await;

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

        let tx = table.start_thread().await;

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
    fn test_wal_mode() -> anyhow::Result<()> {
        //let table = TradeTable::open(db_full_path("BN", "SPOT", "BTCBUSD").to_str().unwrap()).unwrap();

        TradeDb::set_wal_mode(
            db_full_path("BN", "SPOT", "BTCBUSD", false)
                .to_str()
                .unwrap(),
        )?;

        Ok(())
    }

    #[test]
    fn test_table_is_exsit() {
        //let db_name = db_full_path("BN", "SPOT", "BTCUSDT");

        let mut db = TradeDb::open("/tmp/rbottest.db").unwrap();

        if db.is_table_exsit() {
            print!("table is exist");
        } else {
            print!("table is not exist");
        }
    }

    #[test]
    fn test_get_db() {
        init_debug_log();
        let mut db = TradeDataFrame::get("/tmp/rbottest.db").unwrap();

        println!("{:?}", db);
    }
}
*/

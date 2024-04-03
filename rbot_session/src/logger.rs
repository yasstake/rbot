use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
};

use polars::{datatypes::TimeUnit, export::num::ToPrimitive, frame::DataFrame, lazy::{dsl::col, frame::IntoLazy}, prelude::NamedFrom, series::Series};
use pyo3::{pyclass, pymethods, PyResult};
use pyo3_polars::PyDataFrame;
use serde_derive::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use rbot_lib::common::{ordervec_to_dataframe, AccountPair, MicroSec, Order};

#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Indicator {
    #[serde(rename = "i")]
    pub id: i64,
    #[serde(rename = "k")]
    pub name: String,
    #[serde(rename = "ID")]
    pub order_id: Option<String>,
    #[serde(rename = "id")]
    pub transaction_id: Option<String>,
    #[serde(rename = "V")]
    pub value: f64,
    #[serde(rename = "v")]
    pub value2: Option<f64>,
}

#[pyclass]
#[skip_serializing_none]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TimeIndicator {
    #[serde(rename = "i")]
    pub log_id: i64,
    #[serde(rename = "t")]
    pub timestamp: MicroSec,
    #[serde(rename = "ID")]
    pub order_id: Option<String>,
    #[serde(rename = "id")]
    pub transaction_id: Option<String>,
    #[serde(rename = "V")]
    pub value: f64,
    #[serde(rename = "v")]
    pub value2: Option<f64>
}

pub struct TimeIndicatorVec(Vec<TimeIndicator>);

impl Default for TimeIndicatorVec {
    fn default() -> Self {
        TimeIndicatorVec(vec![])
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Position {
    #[serde(rename = "s")]
    pub size: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Profit {
    pub log_id: i64,
    pub open_position: f64,
    pub close_position: f64,
    pub position: f64,
    pub profit: f64,
    pub fee: f64,
    pub total_profit: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub enum LogMessage {
    #[serde(rename = "O")]
    Order(Order),
    #[serde(rename = "A")]
    Account(AccountPair),
    #[serde(rename = "i")]
    UserIndicator(Indicator),
    #[serde(rename = "I")]
    SystemIndicator(Indicator),
    #[serde(rename = "P")]
    Profit(Profit)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct LogRecord {
    #[serde(rename = "t")]
    pub timestamp: MicroSec,
    #[serde(rename = "d")]
    pub data: Vec<LogMessage>,
}

impl LogRecord {
    pub fn new(t: MicroSec) -> Self {
        Self {
            timestamp: t,
            data: vec![],
        }
    }

    pub fn to_string(&self) -> String {
        let r = serde_json::to_string(self);

        if r.is_err() {
            return "".to_string();
        }

        r.unwrap()
    }

    pub fn from_string(s: &str) -> Result<LogRecord, serde_json::Error> {
        serde_json::from_str(s)
    }

    pub fn append_message(&mut self, msg: &LogMessage) {
        self.data.push(msg.clone());
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct SingleLogRecord {
    pub timestamp: MicroSec,
    pub data: LogMessage,
}

impl SingleLogRecord {
    pub fn new(timestamp: MicroSec, msg: &LogMessage) -> Self {
        Self {
            timestamp,
            data: msg.clone(),
        }
    }
}

impl Into<Vec<SingleLogRecord>> for LogRecord {
    fn into(self) -> Vec<SingleLogRecord> {
        let mut result = vec![];

        for msg in self.data {
            result.push(SingleLogRecord {
                timestamp: self.timestamp,
                data: msg,
            });
        }

        result
    }
}

pub fn account_logrec_to_df(accounts: Vec<SingleLogRecord>) -> DataFrame {
    let mut timestamp = Vec::<MicroSec>::new();

    let mut home = Vec::<f64>::new();
    let mut home_free = Vec::<f64>::new();
    let mut home_locked = Vec::<f64>::new();

    let mut foreign = Vec::<f64>::new();
    let mut foreign_free = Vec::<f64>::new();
    let mut foreign_locked = Vec::<f64>::new();

    for rec in accounts {
        match rec.data {
            LogMessage::Account(account) => {
                timestamp.push(rec.timestamp);                            
                home.push(account.home.volume.to_f64().unwrap());
                home_free.push(account.home.free.to_f64().unwrap());
                home_locked.push(account.home.locked.to_f64().unwrap());
                foreign.push(account.foreign.volume.to_f64().unwrap());
                foreign_free.push(account.foreign.free.to_f64().unwrap());
                foreign_locked.push(account.foreign.locked.to_f64().unwrap());
            }
            _ => {
                panic!("not supported message type");
            }
        }
    }

    let timestamp = Series::new("timestamp", timestamp);

    let home = Series::new("home", home);
    let home_free = Series::new("home_free", home_free);
    let home_locked = Series::new("home_locked", home_locked);

    let foreign = Series::new("foreign", foreign);
    let foreign_free = Series::new("foreign_free", foreign_free);
    let foreign_locked = Series::new("foreign_locked", foreign_locked);

    let mut df = DataFrame::new(vec![
        timestamp,
        home,
        home_free,
        home_locked,
        foreign,
        foreign_free,
        foreign_locked,
    ]).unwrap();

    let time = df.column("timestamp").unwrap().i64().unwrap().clone();
    let date_time = time.into_datetime(TimeUnit::Microseconds, None);
    let df = df.with_column(date_time).unwrap();

    return df.clone();
}


impl Into<LogRecord> for Vec<SingleLogRecord> {
    fn into(self) -> LogRecord {
        let mut result = LogRecord::new(0);
        let timestamp: MicroSec = self[0].timestamp;

        for msg in self {
            if timestamp != msg.timestamp {
                log::error!("timestamp is not same");
            }
            result.append_message(&msg.data);
        }

        result
    }
}

#[pyclass]
#[derive(Debug)]
pub struct Logger {
    on_memory: bool,
    current_time: MicroSec,
    order_serial: i64,
    order: Vec<SingleLogRecord>,
    user_indicator: HashMap<String, Vec<TimeIndicator>>,
    system_indicator: HashMap<String, Vec<TimeIndicator>>,
    account: Vec<SingleLogRecord>,
    log_file: Option<File>,
    log_buffer: Option<LogRecord>,
}

#[pymethods]
impl Logger {
    #[pyo3(signature = (on_memory = true))]
    #[new]
    pub fn new(on_memory: bool) -> Self {
        log::debug!("new Logger({:?})", on_memory);

        Self {
            on_memory,
            order_serial: 0,
            current_time: 0,
            order: vec![],
            user_indicator: HashMap::new(),
            system_indicator: HashMap::new(),
            account: vec![],
            log_file: None,
            log_buffer: None,
        }
    }

    pub fn clear(&mut self) {
        log::debug!("clear");
        self.current_time = 0;
        self.on_memory = true;
        self.order.clear();
        self.user_indicator.clear();
        self.system_indicator.clear();
        self.account.clear();
    }

    pub fn open_log(&mut self, path: &str) -> Result<(), std::io::Error> {
        if self.log_file.is_some() {
            log::debug!("close log file {:?}", self.log_file);
            self.close_log()?;
        }

        let log_file = Logger::log_path(path);

        self.log_file = Some(
            OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(log_file)?,
        );

        log::debug!("open log file success. {:?}", self.log_file);

        Ok(())
    }

    pub fn close_log(&mut self) -> Result<(), std::io::Error> {
        self.flush_buffer()?;

        if self.log_file.is_some() {
            self.log_file.as_mut().unwrap().sync_all()?;
            self.log_file = None;
        }

        Ok(())
    }

    pub fn dump(&mut self, path: &str) -> Result<(), std::io::Error> {
        log::debug!("save({})", path);

        self.open_log(path)?;

        self.save_log_records(&self.order.clone())?;

        self.save_indicator(&self.user_indicator.clone(), |i| {
            LogMessage::UserIndicator(i)
        })?;
        self.save_indicator(&self.system_indicator.clone(), |i| {
            LogMessage::SystemIndicator(i)
        })?;

        // save account status
        self.save_log_records(&self.account.clone())?;

        self.flush_buffer()?;

        Ok(())
    }

    pub fn restore(&mut self, file_name: String) -> Result<(), std::io::Error> {
        self.clear();
        let file_name = Logger::log_path(&file_name);

        let file = File::open(file_name)?;
        let mut reader = BufReader::new(file);

        loop {
            let mut buf = String::new();
            let count = reader.read_line(&mut buf)?;
            if count == 0 {
                break;
            }
            let log_record = LogRecord::from_string(&buf)?;

            log::debug!("LINE: {:?}", log_record);

            for msg in log_record.data {
                log::debug!("restore: {:?}", msg);
                self.store_memory(log_record.timestamp, &msg)?;
            }
        }

        Ok(())
    }

    pub fn log_order(&mut self, timestamp: MicroSec, order: &Order) -> Result<(), std::io::Error> {
        self.log_message(timestamp, &LogMessage::Order(order.clone()))
    }


    pub fn log_account(
        &mut self,
        timestamp: MicroSec,
        status: &AccountPair,
    ) -> Result<(), std::io::Error> {
        self.log_message(timestamp, &LogMessage::Account(status.clone()))
    }

    pub fn log_position(
        &mut self,
        timestamp: MicroSec,
        log_id: i64,
        position_change: f64,                
        position: f64,
        order_id: String,        
        transaction_id: String,
    ) -> Result<(), std::io::Error> {
        self.log_system_indicator(timestamp, "position", position_change, Some(position), Some(order_id), Some(transaction_id), Some(log_id))             
    }

    #[getter]
    pub fn get_orders(&self) -> PyResult<PyDataFrame> {
        let orders = self
            .order
            .iter()
            .map(|x| match &x.data {
                LogMessage::Order(order) => order.clone(),
                _ => {
                    panic!("not supported message type");
                }
            })
            .collect();

        let orders = ordervec_to_dataframe(orders);

        let cum_orders = orders.lazy().with_columns(
            vec![
               col("total_profit").cum_sum(false).alias("sum_profit")
            ]
        ).collect().unwrap();

        Ok(PyDataFrame(cum_orders))
    }


    #[getter]
    pub fn get_account(&self) -> PyResult<PyDataFrame> {
        let df = account_logrec_to_df(self.account.clone());

        Ok(PyDataFrame(df))
    }

    pub fn __getitem__(&self, key: &str) -> PyResult<PyDataFrame> {
        let df = Self::indicator_to_df(self.user_indicator.get(key), key, None, false, false);

        Ok(PyDataFrame(df))
    }

    pub fn __repr__(&self) -> String {
        let order_json = serde_json::to_string(&self.order).unwrap();
        let system_indicator_json = serde_json::to_string(&self.system_indicator).unwrap();
        let user_indicator_json = serde_json::to_string(&self.user_indicator).unwrap();
        let account_json = serde_json::to_string(&self.account).unwrap();

        return format!(
            "Logger(order={}, system_indicator={}, user_indicator={}, account={})",
            order_json, system_indicator_json, user_indicator_json, account_json
        )
    }
}

impl Logger {
    pub fn profit_to_df(records: Vec<SingleLogRecord>) -> DataFrame {
        let mut log_id = Vec::<i64>::new();
        //let mut timestamp: Vec<MicroSec> = vec![];
        let mut open_position: Vec<f64> = vec![];
        let mut close_position: Vec<f64> = vec![];
        let mut position: Vec<f64> = vec![];
        let mut profit: Vec<f64> = vec![];
        let mut fee: Vec<f64> = vec![];
        let mut total_profit: Vec<f64> = vec![];
        let mut sum_profit_acc: f64 = 0.0;
        let mut sum_profit: Vec<f64> = vec![];

        for rec in records {
            match rec.data {
                LogMessage::Profit(p) => {
                    log_id.push(p.log_id);
        //            timestamp.push(rec.timestamp);
                    open_position.push(p.open_position);
                    close_position.push(p.close_position);
                    position.push(p.position);
                    profit.push(p.profit);
                    fee.push(p.fee);
                    total_profit.push(p.total_profit);
                    sum_profit_acc += p.total_profit;
                    sum_profit.push(sum_profit_acc);
                }
                _ => {
                    panic!("not supported message type");
                }
            }
        }

        let log_id  = Series::new("log_id", log_id);
        //let timestamp_series = Series::new("timestamp", timestamp);
        let open_position_series = Series::new("open_position", open_position);
        let close_position_series = Series::new("close_position", close_position);
        let position_series = Series::new("position", position);
        let profit_series = Series::new("profit", profit);
        let fee_series = Series::new("fee", fee);
        let total_profit_series = Series::new("total_profit", total_profit);
        let sum_profit_series = Series::new("sum_profit", sum_profit);


        let df = DataFrame::new(vec![
            log_id,
        //    timestamp_series,
            open_position_series,
            close_position_series,
            position_series,
            profit_series,
            fee_series,
            total_profit_series,
            sum_profit_series,
        ]).unwrap();

        //let time = df.column("timestamp").unwrap().i64().unwrap().clone();
        //let date_time = time.into_datetime(TimeUnit::Microseconds, None);
        //let df = df.with_column(date_time).unwrap();

        df.clone()
    }

    pub fn indicator_to_df(indicator: Option<&Vec<TimeIndicator>>, value_name: &str, value_name2: Option<&str>, has_transaction_id: bool, has_log_id: bool) -> DataFrame {
        let mut timestamp: Vec<MicroSec> = vec![];
        let mut value: Vec<f64> = vec![];
        let mut value2: Vec<f64> = vec![];
        let mut order_id: Vec<String> = vec![];        
        let mut transaction_id: Vec<String> = vec![];        
        let mut log_id:Vec<i64> = vec![];

        if indicator.is_some() {
            let indicator = indicator.unwrap();
            for i in indicator {
                timestamp.push(i.timestamp);
                value.push(i.value);

                if i.value2.is_some() {
                    value2.push(i.value2.unwrap());
                }
                else {
                    value2.push(0.0);
                }

                if has_transaction_id {
                    if i.order_id.is_some() {
                        order_id.push(i.order_id.clone().unwrap());
                    }
                    else {
                        order_id.push("".to_string());
                    }

                    if i.transaction_id.is_some() {
                        transaction_id.push(i.transaction_id.clone().unwrap());
                    }
                    else {
                        transaction_id.push("".to_string());
                    }
                }

                log_id.push(i.log_id);
            }
        }

        let timestamp_series = Series::new("timestamp", timestamp);
        let value_series = Series::new(value_name, value);

        let mut column = vec![timestamp_series, value_series];

        if value_name2.is_some() {
            let value_series2 = Series::new(value_name2.unwrap_or(""), value2);
            column.push(value_series2);
        }

        if has_transaction_id {
            let order_id_series = Series::new("order_id", order_id);
            let transaction_id_series = Series::new("transaction_id", transaction_id);

            column.push(order_id_series);
            column.push(transaction_id_series);
        }

        if has_log_id {
            let log_id_series = Series::new("log_id", log_id);
            column.push(log_id_series);
        }

        let mut df = DataFrame::new(column).unwrap();

        let time = df.column("timestamp").unwrap().i64().unwrap().clone();
        let date_time = time.into_datetime(TimeUnit::Microseconds, None);
        let df = df.with_column(date_time).unwrap();

        df.clone()
    }

    pub fn log_indicator(
        &mut self,
        timestamp: MicroSec,
        key: &str,
        value: f64,        
        value2: Option<f64>,
        order_id: Option<String>,
        transaction_id: Option<String>,
        id: Option<i64>
    ) -> Result<(), std::io::Error> {
        let indicator = Indicator {
            name: key.to_string(),
            id: id.unwrap_or_default(),
            order_id: order_id,
            transaction_id: transaction_id,
            value: value,
            value2: value2,
        };
        self.log_message(timestamp, &LogMessage::UserIndicator(indicator))
    }

    pub fn log_system_indicator(
        &mut self,
        timestamp: MicroSec,
        key: &str,
        value: f64,        
        value2: Option<f64>,
        order_id: Option<String>,
        transaction_id: Option<String>,
        id: Option<i64>
    ) -> Result<(), std::io::Error> {
        let indicator = Indicator {
            name: key.to_string(),
            id: id.unwrap_or_default(),            
            order_id: order_id,
            transaction_id: transaction_id,
            value: value,
            value2: value2,
        };
        self.log_message(timestamp, &LogMessage::SystemIndicator(indicator))
    }

    pub fn save_indicator<F>(
        &mut self,
        indicator: &HashMap<String, Vec<TimeIndicator>>,
        f: F,
    ) -> Result<(), std::io::Error>
    where
        F: Fn(Indicator) -> LogMessage,
    {
        // Save indicator
        for (key, time_indicator) in indicator {
            log::debug!("save indicator KEY= {:?}", key);
            for i in time_indicator.iter() {
                log::debug!("save indicator value= {:?}", i);

                let indicator = Indicator {
                    name: key.to_string(),
                    id: i.log_id,
                    value: i.value,
                    order_id: i.order_id.clone(),
                    transaction_id: i.transaction_id.clone(),
                    value2: i.value2,
                };

                self.write_file(i.timestamp, &f(indicator))?;
            }
        }

        Ok(())
    }

    pub fn clone(&self) -> Self {
        Self {
            on_memory: true,
            current_time: self.current_time,
            order_serial: self.order_serial,
            order: self.order.clone(),
            user_indicator: self.user_indicator.clone(),
            system_indicator: self.system_indicator.clone(),
            account: self.account.clone(),
            log_file: None,
            log_buffer: None,
        }
    }

    pub fn log_message(
        &mut self,
        timestamp: MicroSec,
        msg: &LogMessage,
    ) -> Result<(), std::io::Error> {
        log::debug!("log_message: {:?}", msg);

        if self.on_memory {
            self.store_memory(timestamp, msg)?;
        }

        if self.log_file.is_some() {
            self.write_file(timestamp, msg)?;
        }

        Ok(())
    }

    pub fn store_memory(
        &mut self,
        timestamp: MicroSec,
        msg: &LogMessage,
    ) -> Result<(), std::io::Error> {
        let log_record = SingleLogRecord::new(timestamp, msg);

        match log_record.data {
            LogMessage::Order(_) => {
                self.order.push(log_record);
            }
            LogMessage::UserIndicator(indicator) => {
                log::debug!("store user indicator: {:?}", indicator);
                let time_indicator = TimeIndicator {
                    timestamp: timestamp,
                    log_id: indicator.id,
                    order_id: indicator.order_id,                    
                    transaction_id: indicator.transaction_id,
                    value: indicator.value,
                    value2: indicator.value2,
                };

                if self.user_indicator.contains_key(&indicator.name) {
                    let indicator = self.user_indicator.get_mut(&indicator.name).unwrap();
                    indicator.push(time_indicator);
                } else {
                    let indicator_vec = vec![time_indicator];
                    self.user_indicator
                        .insert(indicator.name.clone(), indicator_vec);
                }
            }
            LogMessage::SystemIndicator(indicator) => {
                log::debug!("store SYSTEM indicator: {:?}", indicator);
                let time_indicator = TimeIndicator {
                    timestamp: timestamp,
                    log_id: indicator.id,
                    order_id: indicator.order_id,
                    transaction_id: indicator.transaction_id,
                    value: indicator.value,
                    value2: indicator.value2,                    
                };

                if self.system_indicator.contains_key(&indicator.name) {
                    let indicator = self.system_indicator.get_mut(&indicator.name).unwrap();
                    indicator.push(time_indicator);
                } else {
                    let indicator_vec = vec![time_indicator];
                    self.system_indicator
                        .insert(indicator.name.clone(), indicator_vec);
                }
            }
            LogMessage::Account(_) => {
                self.account.push(log_record);
            }
            LogMessage::Profit(_) => {
            }
            /*
              _ => {
                  log::error!("not supported message type");
              }
              */
        }

        Ok(())
    }

    pub fn write_file(
        &mut self,
        timestamp: MicroSec,
        msg: &LogMessage,
    ) -> Result<(), std::io::Error> {
        if timestamp != self.current_time {
            self.flush_buffer()?;
        }

        if self.log_buffer.is_none() {
            self.log_buffer = Some(LogRecord::new(timestamp));
            self.current_time = timestamp;
        }

        let log_buffer = self.log_buffer.as_mut().unwrap();
        log_buffer.append_message(msg);

        Ok(())
    }

    pub fn flush_buffer(&mut self) -> Result<(), std::io::Error> {
        if self.log_buffer.is_none() {
            return Ok(());
        }

        // write to file
        if self.log_file.is_some() {
            let log_file = self.log_file.as_mut().unwrap();
            let json = self.log_buffer.as_ref().unwrap().to_string();
            log_file.write_all(json.as_bytes())?;
            log_file.write_all("\n".as_bytes())?;
        }

        self.log_buffer = None;

        Ok(())
    }

    pub fn save_log_records(
        &mut self,
        records: &Vec<SingleLogRecord>,
    ) -> Result<(), std::io::Error> {
        println!("save log records");
        for rec in records.iter() {
            println!("save log record: {:?}", rec);
            self.write_file(rec.timestamp, &rec.data)?;
        }

        Ok(())
    }

    fn log_path(file_name: &str) -> String {
        let file_name = if file_name.ends_with(".log") {
            file_name.to_string()
        } else {
            file_name.to_string() + ".log"
        };

        file_name
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        let _ = self.close_log();
    }
}

#[cfg(test)]
mod logger_tests {
    use super::*;
    use rbot_lib::common::Order;
    use rbot_lib::common::OrderSide;
    use rbot_lib::common::OrderStatus;
    use rbot_lib::common::OrderType;
    use rust_decimal_macros::dec;

    #[test]
    fn test_log_record_order() {
        let order = Order::new(
            "linear",
            "BTCUSD",
            1,
            "order-1",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        let mut log_record = LogRecord::new(1);

        log_record.append_message(&LogMessage::Order(order.clone()));

        let indicator = Indicator {
            name: "test".to_string(),
            id: 1,
            order_id: Some("order-1".to_string()),
            transaction_id: Some("transaction-1".to_string()),
            value: 1.0,
            value2: None,
        };
        log_record.append_message(&LogMessage::UserIndicator(indicator));

        let indicator = Indicator {
            name: "test2".to_string(),
            id: 2,
            order_id: Some("order-1".to_string()),
            transaction_id: Some("transaction-1".to_string()),
            value: 2.0,
            value2: None,
        };
        log_record.append_message(&LogMessage::UserIndicator(indicator));

        println!("{:?}", log_record);
        println!("{}", log_record.to_string());

        let json = log_record.to_string();

        let log_record2 = LogRecord::from_string(&json).unwrap();

        assert_eq!(log_record2, log_record);

        let extract: Vec<SingleLogRecord> = log_record2.into();

        println!("{:?}", extract);

        let convert: LogRecord = extract.into();
        println!("{:?}", convert);
    }


    #[test]
    fn test_logger() {
        let mut logger = Logger::new(true);

        logger.open_log("/tmp/test").unwrap();

        let order = Order::new(
            "linear",
            "BTCUSD",
            1,
            "order-1",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(1, &order).unwrap();

        let order = Order::new(
            "linear",            
            "BTCUSD",
            2,
            "order-2",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(2, &order).unwrap();

        let order = Order::new(
            "linear",                        
            "BTCUSD",
            1,
            "order-3",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(3, &order).unwrap();

        let order = Order::new(
            "linear",                                    
            "BTCUSD",
            1,
            "order-4",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(4, &order).unwrap();

        let order = Order::new(
            "linear",                                                
            "BTCUSD",
            1,
            "order-5",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(5, &order).unwrap();

        //logger.log_indicator(5, "test-key0", Some("order01".to_string()),
            // Some("tr01".to_string()),  1.0, Some(1.0)).unwrap();

        let order = Order::new(
            "linear",                                                            
            "BTCUSD",
            1,
            "order-6",
            "clientid",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );
        logger.log_order(6, &order).unwrap();
/*
        logger.log_indicator(6, "test-key", 1.0).unwrap();
        logger.log_indicator(6, "test-key2", 1.0).unwrap();
        logger.log_indicator(7, "test-key2", 1.1).unwrap();
        logger.log_indicator(7, "test-key3", 1.1).unwrap();
        logger.log_indicator(8, "test-key-SUPER", 1.1).unwrap();
*/
        logger.dump("/tmp/dump").unwrap();
        let mut l = Logger::new(true);
        l.restore("/tmp/dump".to_string()).unwrap();

        assert_eq!(logger.order, l.order);
        log::debug!(
            "indicator: {:?} / {:?}",
            logger.user_indicator.len(),
            l.user_indicator.len()
        );
        log::debug!(
            "indicator: {:?} / {:?}",
            logger.user_indicator.keys(),
            l.user_indicator.keys()
        );
        assert!(logger.user_indicator.len() == l.user_indicator.len());
        println!("{:?}", logger.__repr__());

    }

    /*
    #[test]
    fn test_dump_restore() {
        init_debug_log();
        let mut logger = Logger::new(true);

        logger.open_log("/tmp/test").unwrap();

        logger.log_indicator(1, "test-key", 1.0).unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-6".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(1, &order).unwrap();

        logger.log_indicator(2, "test-key2", 1.0).unwrap();
        logger.log_indicator(2, "test-key3", 1.0).unwrap();
        logger.flush_buffer().unwrap();

        let mut logger2 = Logger::new(true);
        logger2.restore("/tmp/test".to_string()).unwrap();

        println!(
            "{:?} / {:?}",
            logger.user_indicator.keys(),
            logger2.user_indicator.keys()
        );
        assert!(logger.user_indicator.len() == logger2.user_indicator.len());

        logger.dump("/tmp/dump").unwrap();
        let mut logger3 = Logger::new(true);
        logger3.restore("/tmp/dump".to_string()).unwrap();

        println!(
            "{:?} / {:?}",
            logger.user_indicator.keys(),
            logger3.user_indicator.keys()
        );
        assert!(logger.user_indicator.len() == logger3.user_indicator.len());
    }
*/
    /*
        #[test]
        fn test_logger_new() {
            let logger = Logger::new(true);
            assert_eq!(logger.on_memory, true);
            assert_eq!(logger.memory, vec![]);
            assert_eq!(logger.log_file_name, "");
            assert_eq!(logger.log_file, None);
        }

        #[test]
        fn test_logger_open_log() {
            let mut logger = Logger::new(true);
            let path = "/path/to/log";
            assert!(logger.open_log(path).is_ok());
            assert_eq!(logger.log_file_name, path);
            assert!(logger.log_file.is_some());
        }

        #[test]
        fn test_logger_log_on_memory() {
            let mut logger = Logger::new(true);
            let order = Order {
                // Initialize order fields here
            };
            assert!(logger.log(&order).is_ok());
            assert_eq!(logger.memory, vec![order]);
        }

        #[test]
        fn test_logger_log_to_file() {
            let mut logger = Logger::new(false);
            let order = Order {
                // Initialize order fields here
            };
            let mut file = File::create("/path/to/log.log").unwrap();
            logger.log_file = Some(file);
            assert!(logger.log(&order).is_ok());
            // Assert that the order is written to the log file
        }

        #[test]
        fn test_logger_get() {
            let logger = Logger::new(true);
            let order = Order {
                // Initialize order fields here
            };
            assert_eq!(logger.get(), vec![]);
            logger.memory.push(order.clone());
            assert_eq!(logger.get(), vec![order]);
        }
    */
}

#[cfg(test)]
mod test_logger {}

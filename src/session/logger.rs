use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
};

use polars_core::{frame::DataFrame, series::Series, prelude::NamedFrom, datatypes::TimeUnit};
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use serde_derive::{Deserialize, Serialize};

use crate::common::{AccountStatus, MicroSec, Order, ordervec_to_dataframe};

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct Indicator {
    #[serde(rename = "k")]
    pub name: String,
    #[serde(rename = "v")]
    pub value: f64,
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct TimeIndicator {
    #[serde(rename = "t")]
    pub timestamp: MicroSec,
    #[serde(rename = "t")]
    pub value: f64,
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
pub enum LogMessage {
    #[serde(rename = "O")]
    Order(Order),
    #[serde(rename = "A")]
    Account(AccountStatus),
    #[serde(rename = "i")]
    UserIndicator(Indicator),
    #[serde(rename = "I")]    
    SystemIndicator(Indicator),
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
    order: Vec<SingleLogRecord>,
    user_indicator: HashMap<String, Vec<TimeIndicator>>,
    system_indicator: HashMap<String, Vec<TimeIndicator>>,    
    account: Vec<SingleLogRecord>,
    log_file: Option<File>,
    log_buffer: Option<LogRecord>,
}

#[pymethods]
impl Logger {
    #[new]
    pub fn new(on_memory: bool) -> Self {
        Self {
            on_memory,
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

        self.save_indicator(&self.user_indicator.clone(),|i| {LogMessage::UserIndicator(i)})?;
        self.save_indicator(&self.system_indicator.clone(), |i| {LogMessage::SystemIndicator(i)})?;

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

    pub fn log_indicator(
        &mut self,
        timestamp: MicroSec,
        key: &str,
        value: f64,
    ) -> Result<(), std::io::Error> {
        let indicator = Indicator {
            name: key.to_string(),
            value: value,
        };
        self.log_message(timestamp, &LogMessage::UserIndicator(indicator))
    }

    pub fn log_system_indicator(
        &mut self,
        timestamp: MicroSec,
        key: &str,
        value: f64
    ) -> Result<(), std::io::Error> {
        let indicator = Indicator {
            name: key.to_string(),
            value: value,
        };
        self.log_message(timestamp, &LogMessage::SystemIndicator(indicator))
    }

    pub fn log_account_status(
        &mut self,
        timestamp: MicroSec,
        status: &AccountStatus,
    ) -> Result<(), std::io::Error> {
        self.log_message(timestamp, &LogMessage::Account(status.clone()))
    }

    pub fn log_position(
        &mut self,
        timestamp: MicroSec,
        position: f64
    ) -> Result<(), std::io::Error> {
        self.log_system_indicator(timestamp, "position", position)
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

        Ok(PyDataFrame(ordervec_to_dataframe(orders)))
    }

    #[getter]
    pub fn get_position(&self) -> PyResult<PyDataFrame> {
        let df = Self::indicator_to_df(self.system_indicator.get("position"), "position");

        Ok(PyDataFrame(df))
    }

    pub fn __getitem__(&self, key: &str) -> PyResult<PyDataFrame> {
        let df = Self::indicator_to_df(self.user_indicator.get(key), key);

        Ok(PyDataFrame(df))
    }

}

impl Logger {
    pub fn indicator_to_df(indicator: Option<&Vec<TimeIndicator>>, value_name: &str) -> DataFrame {

        let mut timestamp: Vec<MicroSec> = vec![];
        let mut value: Vec<f64> = vec![];

        if indicator.is_some() {
            let indicator = indicator.unwrap();
            for i in indicator {
                timestamp.push(i.timestamp);
                value.push(i.value);
            }
        }

        let timestamp_series = Series::new("timestamp", timestamp);
        let value_series = Series::new(value_name, value);

        let mut df = DataFrame::new(vec![timestamp_series, value_series]).unwrap();

        let time = df.column("timestamp").unwrap().i64().unwrap().clone();
        let date_time = time.into_datetime(TimeUnit::Microseconds, None);
        let df = df.with_column(date_time).unwrap();

        df.clone()
    }

    pub fn save_indicator<F>(&mut self, indicator: &HashMap<String, Vec<TimeIndicator>>, f: F) -> Result<(), std::io::Error>
        where F: Fn(Indicator) -> LogMessage
        {
        // Save indicator
        for (key, time_indicator) in indicator {
            log::debug!("save indicator KEY= {:?}", key);
            for i in time_indicator.iter() {
                log::debug!("save indicator value= {:?}", i);

                let indicator = Indicator {
                    name: key.to_string(),
                    value: i.value,
                };

                self.write_file(
                    i.timestamp,
                    &f(indicator)
                )?;
            }
        }

        Ok(())
    }


    pub fn clone(&self) -> Self {
        Self {
            on_memory: true,
            current_time: self.current_time,
            order: self.order.clone(),
            user_indicator: self.user_indicator.clone(),  
            system_indicator: self.system_indicator.clone(),
            account: self.account.clone(),
            log_file: None,
            log_buffer: None,
        }
    }

    // TODO: implement
    pub fn log_message(
        &mut self,
        timestamp: MicroSec,
        msg: &LogMessage,
    ) -> Result<(), std::io::Error> {
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
                    value: indicator.value,
                };

                if self.user_indicator.contains_key(&indicator.name) {
                    let indicator = self.user_indicator.get_mut(&indicator.name).unwrap();
                    indicator.push(time_indicator);
                } else {
                    let indicator_vec = vec![time_indicator];
                    self.user_indicator.insert(indicator.name.clone(), indicator_vec);
                }
            }
            LogMessage::SystemIndicator(indicator) => {
                log::debug!("store SYSTEM indicator: {:?}", indicator);
                let time_indicator = TimeIndicator {
                    timestamp: timestamp,
                    value: indicator.value,
                };

                if self.system_indicator.contains_key(&indicator.name) {
                    let indicator = self.system_indicator.get_mut(&indicator.name).unwrap();
                    indicator.push(time_indicator);
                } else {
                    let indicator_vec = vec![time_indicator];
                    self.system_indicator.insert(indicator.name.clone(), indicator_vec);
                }
            }
            LogMessage::Account(_) => {
                self.account.push(log_record);
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
#[cfg(test)]
mod tests {
    use super::*;

    use crate::common::init_debug_log;
    use crate::common::Order;
    use crate::common::OrderSide;
    use crate::common::OrderStatus;
    use crate::common::OrderType;
    use crate::Logger;
    use rust_decimal_macros::dec;

    #[test]
    fn test_log_record_order() {
        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-1".to_string(),
            "clientid".to_string(),
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
            value: 1.0,
        };
        log_record.append_message(&LogMessage::UserIndicator(indicator));

        let indicator = Indicator {
            name: "test2".to_string(),
            value: 2.0,
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
        init_debug_log();
        let mut logger = Logger::new(true);

        logger.open_log("/tmp/test").unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-1".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(1, &order).unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            2,
            "order-2".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(2, &order).unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-3".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(3, &order).unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-4".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(4, &order).unwrap();

        let order = Order::new(
            "BTCUSD".to_string(),
            1,
            "order-5".to_string(),
            "clientid".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![10.0],
            dec![10.0],
        );

        logger.log_order(5, &order).unwrap();
        logger.log_indicator(5, "test-key0", 0.0).unwrap();

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
        logger.log_order(6, &order).unwrap();

        logger.log_indicator(6, "test-key", 1.0).unwrap();
        logger.log_indicator(6, "test-key2", 1.0).unwrap();
        logger.log_indicator(7, "test-key2", 1.1).unwrap();
        logger.log_indicator(7, "test-key3", 1.1).unwrap();
        logger.log_indicator(8, "test-key-SUPER", 1.1).unwrap();        

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
    }

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

        println!("{:?} / {:?}", logger.user_indicator.keys(), logger2.user_indicator.keys());
        assert!(logger.user_indicator.len() == logger2.user_indicator.len());

        logger.dump("/tmp/dump").unwrap();
        let mut logger3 = Logger::new(true);
        logger3.restore("/tmp/dump".to_string()).unwrap();

        println!("{:?} / {:?}", logger.user_indicator.keys(), logger3.user_indicator.keys());
        assert!(logger.user_indicator.len() == logger3.user_indicator.len());
    }
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

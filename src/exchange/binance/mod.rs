// Copyright(c) 2022. yasstake. All rights reserved.

pub mod message;
pub mod rest;
pub mod ws;

use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::{json, Value};
use std::io::{stdout, Write};
use std::sync::{Arc, Mutex};
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

use crate::common::order::{OrderSide, TimeChunk, Trade};
use crate::common::time::{time_string, DAYS};
use crate::common::time::{to_naive_datetime, MicroSec};
use crate::common::time::{HHMM, NOW, TODAY};
use crate::db::sqlite::{TradeTable, TradeTableDb, TradeTableQuery};
use crate::exchange::binance::message::{BinancePublicWsMessage, BinanceWsRespond};
use crate::fs::db_full_path;

use self::message::BinanceWsBoardUpdate;
use self::rest::insert_trade_db;

use super::{
    check_exist, download_log, log_download, make_download_url_list, AutoConnectClient, Board,
    OrderBook,
};

// TODO: 0.5は固定値なので、変更できるようにする。BTC以外の場合もあるはず。
const BOARD_PRICE_UNIT: f64 = 0.01;

#[derive(Debug)]
pub struct BinanceOrderBook {
    symbol: String,
    last_update_id: u64,
    board: OrderBook,
}

impl BinanceOrderBook {
    pub fn new(symbol: String) -> Self {
        return BinanceOrderBook {
            symbol: symbol,
            last_update_id: 0,
            board: OrderBook::new(
                "BTCBUSD".to_string(),
                Decimal::from_f64(BOARD_PRICE_UNIT).unwrap(),
            ),
        };
    }

    pub fn update(&mut self, update_data: &BinanceWsBoardUpdate) {
        if self.last_update_id == 0 {
            println!("reflesh board {} / {}->{}", self.last_update_id, update_data.u, update_data.U);
            sleep(Duration::from_secs(3));    
            self.reflesh_board();
        }

        // 4. Drop any event where u is <= lastUpdateId in the snapshot.
        if update_data.u <= self.last_update_id {
            log::debug!(
                "Drop any event where u({}) is <= lastUpdateId({}) in the snapshot.",
                update_data.u,
                self.last_update_id
            );
            println!(
                "Drop any event where u({}) is <= lastUpdateId({}) in the snapshot.",
                update_data.u, self.last_update_id
            );

            return;
        }

        // 5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
        if update_data.U <= self.last_update_id + 1 && update_data.u >= self.last_update_id + 1 {
            print!("lastupdate({}) / U({}) / u({})", self.last_update_id, update_data.U, update_data.u);
            self.board
                .update(&update_data.bids, &update_data.asks, false);
        }

        // 6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
        if update_data.U != self.last_update_id + 1 {
            log::warn!(
                "U is not equal to the previous event's u+1 {} {}",
                update_data.U,
                self.last_update_id + 1
            );
        }

        self.last_update_id = update_data.u;
    }

    fn reflesh_board(&mut self) {
        let snapshot = get_board_snapshot(self.symbol.as_str()).unwrap();

        self.last_update_id = snapshot.last_update_id;

        println!("REFLESH ID: {}", self.last_update_id);

        self.board.update(&snapshot.bids, &snapshot.asks, true);
    }
}

#[derive(Debug)]
#[pyclass(name = "_BinanceMarket")]
pub struct BinanceMarket {
    name: String,
    pub dummy: bool,
    pub db: TradeTable,
    pub board: Arc<Mutex<BinanceOrderBook>>,
    pub handler: Option<JoinHandle<()>>,
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
            board: Arc::new(Mutex::new(BinanceOrderBook::new(market_name.to_string()))),
            handler: None,
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

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let latest_date;

        match self.get_latest_archive_timestamp() {
            Ok(timestamp) => latest_date = timestamp,
            Err(e) => {
                latest_date = NOW() - DAYS(1);
            }
        }

        // download from archive
        let days_gap = self
            .db
            .make_time_days_chunk_from_days(ndays, latest_date, force);
        let urls: Vec<String> = make_download_url_list(
            self.name.as_str(),
            days_gap,
            Self::make_historical_data_url_timestamp,
        );
        let tx = self.db.start_thread();
        let download_rec = download_log(urls, tx, false, BinanceMarket::rec_to_trade);

        self.repave_today();

        return download_rec;
    }

    pub fn repave_today(&mut self) {
        let latest_date;

        match self.get_latest_archive_timestamp() {
            Ok(timestamp) => latest_date = timestamp,
            Err(e) => {
                latest_date = NOW() - DAYS(1);
            }
        }

        let tx = self.db.start_thread();
        insert_trade_db(self.name.as_str(), latest_date, tx.clone());
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
    fn get_asks(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.board.lock().unwrap().board.get_asks_pyarray();
    }

    #[getter]
    fn get_bids(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.board.lock().unwrap().board.get_bids_pyarray();
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

    pub fn start_ws(&mut self) {
        // TODO: parameterize
        let mut websocket = AutoConnectClient::new(
            "wss://stream.binance.com/ws",
            json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        "btcusdt@depth@100ms"
                    ],
                    "id": 1
                }
            ),
        );

        websocket.connect();

        let db_channel = self.db.start_thread();
        let board = self.board.clone();

        let handler = std::thread::spawn(move || loop {
            let message = websocket.receive_message().unwrap();
            let message_value: Value = serde_json::from_str(message.as_str()).unwrap();

            if message_value.is_object() {
                let o = message_value.as_object().unwrap();

                if o.contains_key("e") {
                    let message: BinancePublicWsMessage =
                        serde_json::from_str(message.as_str()).unwrap();

                    match message {
                        BinancePublicWsMessage::Trade(trade) => {
                            log::debug!("Trade: {:?}", trade);
                            db_channel.send(vec![trade.to_trade()]);
                        }
                        BinancePublicWsMessage::BoardUpdate(board_update) => {
                            board.lock().unwrap().update(&board_update);
                        }
                    }
                } else if o.contains_key("result") {
                    let message: BinanceWsRespond = serde_json::from_str(message.as_str()).unwrap();
                    log::debug!("Result: {:?}", message);
                } else {
                    continue;
                }
            }
        });

        self.handler = Some(handler);
    }
}

use crate::exchange::binance::rest::get_board_snapshot;

const HISTORY_WEB_BASE: &str = "https://data.binance.vision/data/spot/daily/trades";
impl BinanceMarket {
    fn make_historical_data_url_timestamp(name: &str, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        // https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip
        return format!(
            "{}/{}/{}-trades-{:04}-{:02}-{:02}.zip",
            HISTORY_WEB_BASE, name, name, yyyy, mm, dd
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
    Order book management.
    https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream

    1. Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.
    2. Buffer the events you receive from the stream.
    3. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .
    4. Drop any event where u is <= lastUpdateId in the snapshot.
    5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
    6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
    7. The data in each event is the absolute quantity for a price level.
    8. If the quantity is 0, remove the price level.
    9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.
    */
    /*
    pub fn update_board(&mut self, update_data: &BinanceWsBoardUpdate) {
        let mut board = self.board.lock().unwrap();

        if board.last_update_id == 0 {
            sleep(Duration::from_secs(1));
            board.reflesh_board();
        }

        // 4. Drop any event where u is <= lastUpdateId in the snapshot.
        if update_data.u <= board.last_update_id {
            return;
        }

        // 5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
        if update_data.U <= board.last_update_id + 1 && update_data.u >= board.last_update_id + 1 {
            board.update(update_data);
        }

        // 6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
        if update_data.U != board.last_update_id + 1 {
            log::warn!(
                "U is not equal to the previous event's u+1 {} {}",
                update_data.U,
                board.last_update_id + 1
            );
            board.reflesh_board();
        }

        board.last_update_id = update_data.u;
    }
    */

    fn get_latest_archive_timestamp(&self) -> Result<MicroSec, String> {
        match self.get_latest_archive_date() {
            Ok(date) => {
                return Ok(date + HHMM(23, 59));
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    fn get_latest_archive_date(&self) -> Result<MicroSec, String> {
        let mut latest = TODAY();
        let mut i = 0;

        loop {
            latest -= i * DAYS(1);

            let url = Self::make_historical_data_url_timestamp(self.name.as_str(), latest);

            if check_exist(url.as_str()) {
                println!("{} exists", url);
                return Ok(latest);
            } else {
                println!("{} does not exist", url);
            }
            i += 1;

            if i > 5 {
                log::error!("{} does not exist", url);
                println!("{} does not exist", url);
                return Err(format!("{} does not exist", url));
            }
        }
    }
}

#[cfg(test)]
mod binance_test {
    use std::{thread::sleep, time::Duration};

    use crate::common::{init_debug_log, init_log};

    use super::*;

    #[test]
    fn test_make_historical_data_url_timestamp() {
        init_log();
        let market = BinanceMarket::new("BTCBUSD", true);
        println!(
            "{}",
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 1)
        );
        assert_eq!(
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 1),
            "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-1970-01-01.zip"            
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

    #[test]
    fn test_ohlcv() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.ohlcv(0, 0, 3600);

        println!("{:?}", market.db.ohlcv_df(0, 0, 3600));
    }

    #[test]
    fn bench_ohlcv() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.ohlcv(0, 0, 3600);
    }

    #[test]
    fn test_start_ws() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.start_ws();
        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_reflesh_board() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        let update_data = get_board_snapshot("BTCBUSD").unwrap();

        // TODO: test

        println!("{:?}", update_data);
    }

    #[test]
    fn test_ws_start() {
        let mut market = BinanceMarket::new("BTCBUSD", true);

        market.start_ws();

        sleep(Duration::from_secs(10));
    }

    #[test]
    fn test_latest_archive_date() {
        let market = BinanceMarket::new("BTCBUSD", true);

        println!("{}", time_string(market.get_latest_archive_date().unwrap()));
    }

    #[test]
    fn test_latest_archive_timestamp() {
        let market = BinanceMarket::new("BTCBUSD", true);

        println!(
            "{}",
            time_string(market.get_latest_archive_timestamp().unwrap())
        );
    }
}

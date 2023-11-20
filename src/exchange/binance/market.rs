// Copyright(c) 2022. yasstake. All rights reserved.

use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde_json::Value;
use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, JoinHandle, Thread, self};
use std::time::Duration;

use crate::common::{convert_pyresult_vec, MarketMessage};
use crate::common::DAYS;
use crate::common::{convert_pyresult, MarketStream};
use crate::common::{to_naive_datetime, MicroSec};
use crate::common::{MarketConfig, MultiChannel};
use crate::common::{Order, OrderSide, Trade};
use crate::common::{HHMM, NOW, TODAY};
use crate::db::sqlite::{TradeTable, TradeTableQuery};
use crate::exchange::binance::message::{BinancePublicWsMessage, BinanceWsRespond};

use super::message::BinanceUserStreamMessage;
use super::message::{
    BinanceListOrdersResponse, BinanceOrderResponse, BinanceOrderStatus, BinanceWsBoardUpdate,
};
use super::rest::cancel_order;
use super::rest::cancell_all_orders;
use super::rest::open_orders;
use super::rest::{insert_trade_db, new_limit_order, new_market_order, order_status, trade_list};
use super::ws::listen_userdata_stream;

use crate::exchange::{
    check_exist, download_log, make_download_url_list, AutoConnectClient, OrderBook, BoardItem,
};

use crate::exchange::binance::config::BinanceConfig;

pub fn binance_to_microsec(t: u64) -> MicroSec {
    return (t as i64) * 1_000;
}

#[pyclass]
pub struct BinanceAccount {
    pub api_key: String,
    pub secret_key: String,
    pub subaccount: String,
}

// TODO: 0.5は固定値なので、変更できるようにする。BTC以外の場合もあるはず。
const BOARD_PRICE_UNIT: f64 = 0.01;

#[derive(Debug)]
pub struct BinanceOrderBook {
    config: BinanceConfig,
    last_update_id: u64,
    board: OrderBook,
}

impl BinanceOrderBook {
    pub fn new(config: &BinanceConfig) -> Self {
        return BinanceOrderBook {
            config: config.clone(),
            last_update_id: 0,
            board: OrderBook::new(
                "BTCBUSD".to_string(),
                Decimal::from_f64(BOARD_PRICE_UNIT).unwrap(),
            ),
        };
    }

    pub fn update(&mut self, update_data: &BinanceWsBoardUpdate) {
        if self.last_update_id == 0 {
            log::debug!(
                "reflesh board {} / {}->{}",
                self.last_update_id,
                update_data.u,
                update_data.U
            );
            sleep(Duration::from_millis(150)); // 100ms毎に更新されるので、150ms待つ。
            self.reflesh_board();
        }

        // 4. Drop any event where u is <= lastUpdateId in the snapshot.
        if update_data.u <= self.last_update_id {
            log::debug!(
                "Drop any event where u({}) is <= lastUpdateId({}) in the snapshot.",
                update_data.u,
                self.last_update_id
            );

            return;
        }

        // 5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
        if update_data.U <= self.last_update_id + 1 && update_data.u >= self.last_update_id + 1 {
            log::debug!(
                "lastupdate({}) / U({}) / u({})",
                self.last_update_id,
                update_data.U,
                update_data.u
            );
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
        let snapshot = get_board_snapshot(&self.config).unwrap();
        self.last_update_id = snapshot.last_update_id;

        self.board.update(&snapshot.bids, &snapshot.asks, true);
    }
}

#[derive(Debug)]
#[pyclass(name = "BinanceMarket")]
pub struct BinanceMarket {
    pub config: BinanceConfig,
    name: String,
    pub db: TradeTable,
    pub board: Arc<Mutex<BinanceOrderBook>>,
    pub public_handler: Option<JoinHandle<()>>,
    pub user_handler: Option<JoinHandle<()>>,
    pub channel: Arc<Mutex<MultiChannel>>,
}

pub trait Market {
    fn limit_order(&self);
}

impl Market for BinanceMarket {
    fn limit_order(&self) {
        // todo!()
    }
}

#[pymethods]
impl BinanceMarket {
    #[new]
    pub fn new(config: &BinanceConfig) -> Self {
        let db_name = Self::db_path(&config).unwrap();

        log::debug!("create TradeTable: {}", db_name);

        let db = TradeTable::open(db_name.as_str()).expect("cannot open db");

        db.create_table_if_not_exists();

        let name = config.trade_symbol.clone();

        return BinanceMarket {
            config: config.clone(),
            name: name.clone(),
            db,
            board: Arc::new(Mutex::new(BinanceOrderBook::new(config))),
            public_handler: None,
            user_handler: None,
            channel: Arc::new(Mutex::new(MultiChannel::new())),
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
        log::info!("log download: {} days", ndays);
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

        log::info!("downloaded: {}", download_rec);

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
        insert_trade_db(&self.config, latest_date, tx.clone());
    }

    pub fn cache_all_data(&mut self) {
        self.db.update_cache_all();
    }

    pub fn select_trades_a(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_select_trades(start_time, end_time);
    }

    pub fn ohlcvv_a(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcvv(start_time, end_time, window_sec);
    }

    pub fn ohlcv_a(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcv(start_time, end_time, window_sec);
    }

    pub fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_select_trades_polars(start_time, end_time);
    }

    pub fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcvv_polars(start_time, end_time, window_sec);
    }

    pub fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcv_polars(start_time, end_time, window_sec);
    }

    pub fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_vap(start_time, end_time, price_unit);
    }
    

    pub fn info(&mut self) -> String {
        return self.db.info();
    }

    #[getter]
    pub fn get_asks_a(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.board.lock().unwrap().board.get_asks_pyarray();
    }

    #[getter]
    pub fn get_asks(&self) -> PyResult<PyDataFrame> {
        return self.board.lock().unwrap().board.get_asks_pydataframe();
    }

    #[getter]
    pub fn get_asks_vec(&self) -> Vec<BoardItem> {
        return self.board.lock().unwrap().board.get_asks();
    }

    #[getter]
    pub fn get_bids_a(&self) -> PyResult<Py<PyArray2<f64>>> {
        return self.board.lock().unwrap().board.get_bids_pyarray();
    }

    #[getter]
    pub fn get_bids_vec(&self) -> Vec<BoardItem> {
        return self.board.lock().unwrap().board.get_bids();
    }

    #[getter]
    pub fn get_bids(&self) -> PyResult<PyDataFrame> {
        return self.board.lock().unwrap().board.get_bids_pydataframe();
    }

    #[getter]
    pub fn get_file_name(&self) -> String {
        return self.db.get_file_name();
    }

    pub fn vaccum(&self) {
        self.db.vaccum();
    }

    pub fn _repr_html_(&self) -> String {
        return format!("<b>Binance DB ({})</b>{}", self.name, self.db._repr_html_());
    }

    // TODO: implment retry logic
    pub fn start_market_stream(&mut self) {
        let endpoint = &self.config.public_ws_endpoint;
        let subscribe_message: Value =
            serde_json::from_str(&self.config.public_subscribe_message).unwrap();

        // TODO: parameterize
        let mut websocket = AutoConnectClient::new(endpoint, Some(subscribe_message));

        websocket.connect();

        let db_channel = self.db.start_thread();
        let board = self.board.clone();

        let mut agent_channel = self.channel.clone();

        let handler = std::thread::spawn(move || {loop {
            let message = websocket.receive_message();
            if message.is_err() {
                log::warn!("Error in websocket.receive_message: {:?}", message);
                continue;
            }
            let m = message.unwrap();

            let message_value = serde_json::from_str::<Value>(&m);

            if message_value.is_err() {
                log::warn!("Error in serde_json::from_str: {:?}", message_value);
                continue;
            }
            let message_value: Value = message_value.unwrap();

            if message_value.is_object() {
                let o = message_value.as_object().unwrap();

                if o.contains_key("e") {
                    log::debug!("Message: {:?}", &m);

                    let message: BinancePublicWsMessage =
                        serde_json::from_str(&m).unwrap();

                    match message.clone() {
                        BinancePublicWsMessage::Trade(trade) => {
                            log::debug!("Trade: {:?}", trade);
                            db_channel.send(vec![trade.to_trade()]);

                            let multi_agent_channel = agent_channel.borrow_mut();
                            multi_agent_channel.lock().unwrap().send(message.into());
                        }
                        BinancePublicWsMessage::BoardUpdate(board_update) => {
                            board.lock().unwrap().update(&board_update);
                        }
                    }
                } else if o.contains_key("result") {
                    let message: BinanceWsRespond = serde_json::from_str(&m).unwrap();
                    log::debug!("Result: {:?}", message);
                } else {
                    continue;
                }
            }}
        });

        self.public_handler = Some(handler);

        log::info!("start_market_stream");
    }

    // TODO: 単に待っているだけなので、終了処理を実装する。
    pub fn stop_market_stream(&mut self) {
        match self.public_handler.take() {
            Some(h) => {
                h.join().unwrap();
            }
            None => {}
        }
    }

    pub fn start_user_stream(&mut self) {
        let mut agent_channel = self.channel.clone();

        let cfg = self.config.clone();

        self.user_handler = Some(listen_userdata_stream(
            &self.config,
            move |message: BinanceUserStreamMessage| {
                log::debug!("UserStream: {:?}", message);
                let mutl_agent_channel = agent_channel.borrow_mut();
                let m = message.convert_to_market_message(&cfg);
                mutl_agent_channel.lock().unwrap().send(m);
            },
        ));

        log::info!("start_user_stream");
    }

    pub fn stop_user_stream(&mut self) {
        match self.user_handler.take() {
            Some(h) => {
                h.join().unwrap();
            }
            None => {}
        }
    }

    pub fn is_user_stream_running(&self) -> bool {
        if let Some(handler) = &self.user_handler {
            return !handler.is_finished();
        }
        return false;
    }

    pub fn is_market_stream_running(&self) -> bool {
        if let Some(handler) = &self.public_handler {
            return !handler.is_finished();
        }
        return false;
    }

    pub fn is_db_thread_running(&self) -> bool {
        return self.db.is_thread_running();
    }

    #[getter]
    pub fn get_channel(&mut self) -> MarketStream {
        self.channel.lock().unwrap().open_channel(0)
    }

    pub fn open_backtest_channel(&mut self, time_from: MicroSec, time_to: MicroSec) -> MarketStream {
        let channel = self.channel.lock().unwrap().open_channel(1_000);
        let sender = self.channel.clone();

        let mut table_db = self.db.connection.clone_connection();

        thread::spawn(move || {
            let mut channel = sender.lock().unwrap();            
            table_db.select(time_from, time_to, |trade| {
                let message: MarketMessage = trade.into();
                let r = channel.send(message);
        
                if r.is_err() {
                    log::error!("Error in channel.send: {:?}", r);
                }
            });
            channel.close();
        });

        return channel;
    }

    #[pyo3(signature = (side, price, size, client_order_id=None))]
    pub fn new_limit_order_raw(
        &self,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<BinanceOrderResponse> {
        let price_scale = self.config.market_config.price_scale;
        let size_scale = self.config.market_config.size_scale;

        let price = price.round_dp(price_scale);
        let size = size.round_dp(size_scale);

        let response = new_limit_order(&self.config, side, price, size, client_order_id);

        convert_pyresult(response)
    }

    #[pyo3(signature = (side, price, size, client_order_id=None))]
    pub fn limit_order(
        &self,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let price_scale = self.config.market_config.price_scale;
        let price_dp = price.round_dp(price_scale);

        let size_scale = self.config.market_config.size_scale;
        let size_dp = size.round_dp(size_scale);

        let response = new_limit_order(&self.config, side, price_dp, size_dp, client_order_id);

        if response.is_err() {
            log::error!(
                "limit_order: side = {:?}, price = {:?}/{:?}, size = {:?}/{:?}, id = {:?}, result={:?}",
                side,
                price,
                price_dp,
                size,
                size_dp,
                client_order_id,
                response
            );

            let err = format!(
                "limit_order({:?}, {:?}/{:?}, {:?}/{:?}, {:?}) -> {:?}",
                side,
                price,
                price_dp,
                size,
                size_dp,
                client_order_id,
                response.unwrap_err()
            );
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err));
        }

        convert_pyresult(response)
    }

    pub fn new_market_order_raw(
        &self,
        side: OrderSide,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<BinanceOrderResponse> {
        let size_scale = self.config.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let response = new_market_order(&self.config, side, size, client_order_id);

        convert_pyresult(response)
    }

    pub fn market_order(
        &self,
        side: OrderSide,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let size_scale = self.config.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let response = new_market_order(&self.config, side, size, client_order_id);

        if response.is_err() {
            log::error!(
                "market_order: side = {:?}, size = {:?}, id = {:?}, result={:?}",
                side,
                size,
                client_order_id,
                response
            );

            let err = format!(
                "market_order({:?}, {:?}, {:?}) -> {:?}",
                side,
                size,
                client_order_id,
                response.unwrap_err()
            );

            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err));
        }

        convert_pyresult(response)
    }

    pub fn cancel_order(&self, order_id: &str) -> PyResult<Order> {
        let response = cancel_order(&self.config, order_id);

        return convert_pyresult(response);
    }

    pub fn cancel_all_orders(&self) -> PyResult<Vec<Order>> {
        let response = cancell_all_orders(&self.config);

        if response.is_ok() {
            return convert_pyresult_vec(response);
        }

        return PyResult::Ok(vec![]);
    }

    #[getter]
    pub fn get_order_status(&self) -> PyResult<Vec<BinanceOrderStatus>> {
        let status = order_status(&self.config);

        convert_pyresult(status)
    }

    #[getter]
    pub fn get_open_orders(&self) -> PyResult<Vec<Order>> {
        let status = open_orders(&self.config);

        log::debug!("OpenOrder: {:?}", status);

        convert_pyresult_vec(status)
    }

    #[getter]
    pub fn get_trade_list(&self) -> PyResult<Vec<BinanceListOrdersResponse>> {
        let status = trade_list(&self.config);

        convert_pyresult(status)
    }

    #[getter]
    pub fn get_market_config(&self) -> MarketConfig {
        return self.config.market_config.clone();
    }
}

use crate::exchange::binance::rest::get_board_snapshot;

const HISTORY_WEB_BASE: &str = "https://data.binance.vision/data/spot/daily/trades";

impl BinanceMarket {
    pub fn db_path(config: &BinanceConfig) -> PyResult<String> {
        Ok(config.get_db_path())
    }

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

        let price = Decimal::from_f64(price).unwrap_or_default();

        let size = rec
            .get(2)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let size = Decimal::from_f64(size).unwrap_or_default();

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
                log::debug!("{} exists", url);
                return Ok(latest);
            } else {
                log::debug!("{} does not exist", url);
            }
            i += 1;

            if i > 5 {
                log::error!("{} does not exist", url);
                return Err(format!("{} does not exist", url));
            }
        }
    }

    fn write_message_to_channel(&mut self, message: MarketMessage) {
    }
}

#[cfg(test)]
mod binance_test {
    use std::{thread::sleep, time::Duration};

    use crate::common::{init_debug_log, init_log, time_string};

    use super::*;

    #[test]
    fn test_make_historical_data_url_timestamp() {
        init_log();
        let market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
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
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);
        println!("{}", time_string(market.db.start_time().unwrap_or(0)));
        println!("{}", time_string(market.db.end_time().unwrap_or(0)));
        println!("Let's donwload");
        market.download(2, false);
    }

    #[test]
    fn test_db_info() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        println!("{:?}", market.db.info());
    }

    #[test]
    fn test_ohlcv() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        let _ = market.ohlcv(0, 0, 3600);

        println!("{:?}", market.db.ohlcv_df(0, 0, 3600));
    }

    #[test]
    fn bench_ohlcv() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        let _ = market.ohlcv(0, 0, 3600);
    }

    #[test]
    fn test_start_ws() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        market.start_market_stream();
        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_reflesh_board() {
        let config = BinanceConfig::BTCUSDT();
        let _market = BinanceMarket::new(&config);
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        let update_data = get_board_snapshot(&config).unwrap();

        // TODO: test

        println!("{:?}", update_data);
    }

    #[test]
    fn test_ws_start() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);

        market.start_market_stream();

        sleep(Duration::from_secs(10));
    }

    #[test]
    fn test_latest_archive_date() {
        let market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        println!("{}", time_string(market.get_latest_archive_date().unwrap()));
    }

    #[test]
    fn test_latest_archive_timestamp() {
        let market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        println!(
            "{}",
            time_string(market.get_latest_archive_timestamp().unwrap())
        );
    }
}

// Copyright(c) 2022-2023. yasstake. All rights reserved.

use chrono::Datelike;
use csv::StringRecord;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_json::Value;
use std::borrow::BorrowMut;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, JoinHandle, self};
use std::time::Duration;

use crate::common::{convert_pyresult_vec, MarketMessage, time_string, OrderType, OrderStatus, LogStatus, FLOOR_DAY, flush_log};
use crate::common::DAYS;
use crate::common::{convert_pyresult, MarketStream};
use crate::common::{to_naive_datetime, MicroSec};
use crate::common::{MarketConfig, MultiChannel};
use crate::common::{Order, OrderSide, Trade};
use crate::common::{HHMM, NOW, TODAY};
use crate::db::df::KEY;
use crate::db::sqlite::TradeTable;
use crate::exchange::binance::message::{BinancePublicWsMessage, BinanceWsRespond};

use super::message::{BinanceUserStreamMessage, BinanceMessageId};
use super::message::{
    BinanceListOrdersResponse, BinanceOrderStatus, BinanceWsBoardUpdate,
    BinanceAccountInformation
};
use super::rest::{cancel_order, get_balance};
use super::rest::cancell_all_orders;
use super::rest::open_orders;
use super::rest::{new_limit_order, new_market_order, order_status, trade_list};
use super::ws::listen_userdata_stream;

use crate::exchange::{
    check_exist, AutoConnectClient, OrderBook, BoardItem, download_log};

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
            board: OrderBook::new(&config.market_config),
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

    fn get_board_vec(&self) -> Result<(Vec<BoardItem>, Vec<BoardItem>), ()> {
        let (bids, asks) = self.board.get_board_vec().unwrap();

        Ok((bids, asks))
    }

    fn get_board(&mut self) -> PyResult<(PyDataFrame, PyDataFrame)> {
        let r = self.board.get_board();
        if r.is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in get_board: {:?}",
                r
            )));
        }

        let (mut bids, mut asks) = r.unwrap();

        if bids.is_empty() || asks.is_empty() {
            return Ok((PyDataFrame(bids), PyDataFrame(asks)));
        }

        let bids_edge: f64 = bids.column(KEY::price).unwrap().max().unwrap();
        let asks_edge: f64 = asks.column(KEY::price).unwrap().min().unwrap();

        if asks_edge < bids_edge{
            log::warn!("bids_edge({}) < asks_edge({})", bids_edge, asks_edge);

            self.reflesh_board();

            (bids, asks) = self.board.get_board().unwrap();
        }

        return Ok((PyDataFrame(bids), PyDataFrame(asks)));
    }

    fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)> {
        Ok(self.board.get_edge_price())
    }
}

#[derive(Debug)]
#[pyclass(name = "BinanceMarket")]
pub struct BinanceMarket {
    pub config: BinanceConfig,
    symbol: String,
    pub db: TradeTable,
    pub board: Arc<Mutex<BinanceOrderBook>>,
    pub public_handler: Option<JoinHandle<()>>,
    pub user_handler: Option<JoinHandle<()>>,
    pub channel: Arc<Mutex<MultiChannel>>,
}

#[pymethods]
impl BinanceMarket {
    #[new]
    pub fn new(config: &BinanceConfig) -> Self {
        println!("\nBinance {:?}\n", config.short_info());

        let db_name = Self::db_path(&config).unwrap();

        log::debug!("create TradeTable: {}", db_name);

        let db = TradeTable::open(db_name.as_str()).expect("cannot open db");

        let r = db.create_table_if_not_exists();
        if r.is_err() {
            log::error!("Error in create_table_if_not_exists: {:?}", r);
        }

        let symbol = config.trade_symbol.clone();

        return BinanceMarket {
            config: config.clone(),
            symbol: symbol,
            db,
            board: Arc::new(Mutex::new(BinanceOrderBook::new(config))),
            public_handler: None,
            user_handler: None,
            channel: Arc::new(Mutex::new(MultiChannel::new())),
        };
    }

    pub fn drop_table(&mut self) -> PyResult<()> {
        match self.db.drop_table() {
            Ok(_) => Ok(()),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in drop_table: {:?}",
                e
            ))),
        }
    }
    
    #[getter]
    pub fn get_cache_duration(&self) -> MicroSec {
        return self.db.get_cache_duration();
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn download_log(&mut self, date: MicroSec, verbose: bool) -> PyResult<i64> {
        let date = FLOOR_DAY(date);

        let url = Self::make_historical_data_url_timestamp(self.symbol.as_str(), date);

        match download_log(&url, &self.db.start_thread(), false, verbose, &BinanceMarket::rec_to_trade) {
            Ok(download_rec) => {
                log::info!("downloaded: {}", download_rec);
                if verbose {
                    println!("downloaded: {}", download_rec);
                    flush_log();
                }
                Ok(download_rec)
            }
            Err(e) => {
                log::error!("Error in download_logs: {:?}", e);
                if verbose {
                    println!("Error in download_logs: {:?}", e);
                }
                Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error in download_logs: {:?}",
                    e
                )))
            }
        }
    }

    #[pyo3(signature = (ndays, *, force = false, verbose=true))]
    pub fn download(&mut self, ndays: i64, force: bool, verbose: bool) -> i64 {
        log::info!("log download: {} days", ndays);
        if verbose {
            println!("log download: {} days", ndays);
            flush_log();
        }

        let latest_date;

        match self.get_latest_archive_date() {
            Ok(timestamp) => latest_date = timestamp,
            Err(_) => {
                latest_date = NOW() - DAYS(2);
            }
        }

        log::info!("archive latest_date: {}", time_string(latest_date));
        if verbose {
            println!("archive latest_date: {}", time_string(latest_date));
            flush_log();
        }

        let mut download_rec: i64 = 0;

        for i in 0..ndays {
            let date = latest_date - i * DAYS(1);

            if ! force && self.validate_db_by_date(date) {
                log::info!("{} is valid", time_string(date));
                
                if verbose {
                    println!("{} skip download", time_string(date));
                    flush_log();
                }
                continue;
            }

            match self.download_log(date, verbose) {
                Ok(rec) => {
                    log::info!("downloaded: {}", download_rec);
                    download_rec += rec;
                },
                Err(e) => {
                    log::error!("Error in download_log: {:?}", e);
                    if verbose {
                        println!("Error in download_log: {:?}", e);
                    }
                }
            }
        }

        // download from rest API
        download_rec += self.download_latest(force, verbose);

        download_rec
    }

    #[pyo3(signature = (force=false, verbose = true))]
    pub fn download_latest(&mut self, force:bool, verbose: bool) -> i64 {
        if verbose {
            println!("start download from rest API");
            flush_log();
        }

        let start_id: u64;

        if force {
            let (fix_id, _fix_time) = self.latest_fix_time();
            start_id = fix_id + 1;
        }
        else {
            let (stable_id, _stable_time)= self.latest_stable_time(verbose);
            start_id = stable_id + 1;
        }

        if start_id == 1 {
            println!("ERROR: no record in database");
            println!(" Try to reboot your program and download(days=2, force=True).");            
            flush_log();

            return 0;
        }
        
        let ch = self.db.start_thread();

        let record_number = download_historical_trades_from_id(&BinanceConfig::BTCUSDT(), start_id, verbose,&mut |row|
        {
            ch.send(row.clone()).unwrap();

            Ok(())
        }).unwrap();

        if verbose {
            println!("\nREST downloaded: {}[rec]", record_number);
            flush_log();
        }

        return record_number;
    }

    #[pyo3(signature = (verbose = false))]
    pub fn latest_stable_time(&mut self, verbose: bool) -> (BinanceMessageId, MicroSec) {
        let sql = r#"select time_stamp, action, price, size, status, id from trades where $1 < time_stamp and (status = "E" or status = "e") order by time_stamp desc"#;

        let r = self.db.connection.select_query(sql, vec![NOW()-DAYS(4)]);

        if r.len() == 0 {
            log::warn!("no record");
            return (0, 0);
        }

        let id: BinanceMessageId = r[0].id.parse().unwrap();

        if verbose {
            println!("latest_stable_message: {:?}({:?}) / message id={:?}", r[0].time, time_string(r[0].time), r[0].id);
        }

        return (id, r[0].time);
    }

    pub fn latest_fix_time(&mut self) -> (BinanceMessageId, MicroSec) {
        let sql = r#"select time_stamp, action, price, size, status, id from trades where $1 < time_stamp and status = "E" order by time_stamp desc"#;

        let r = self.db.connection.select_query(sql, vec![NOW()-DAYS(2)]);

        if r.len() == 0 {
            log::warn!("no record");
            return (0, 0);
        }

        let id: BinanceMessageId = r[0].id.parse().unwrap();

        return (id, r[0].time);
    }

    #[pyo3(signature = (allow_gap_rec=50))]
    pub fn analyze_db(&mut self, allow_gap_rec: u64) -> i64 {
        let mut first_id: BinanceMessageId = 0;
        let mut first_time: MicroSec = 0;

        let mut last_id: BinanceMessageId = 0;
        let mut last_time: MicroSec = 0;

        let mut gap_count: i64 = 0;
        let mut record_count: i64 = 0;

        self.db.connection.select(0, 0, |trade|{
            if first_id == 0 {
                first_id = trade.id.parse::<BinanceMessageId>().unwrap();
                first_time = trade.time;
            }

            let id = trade.id.clone();
            let id = id.parse::<BinanceMessageId>().unwrap();

            let time = trade.time;

            if last_id != 0 && id + allow_gap_rec < last_id {
                println!("MISSING: FROM: {}({})  -> TO: {}({}), {}[rec]", 
                    time_string(last_time), last_id,
                    time_string(time), id,
                    last_id - id - 1);
                gap_count += 1;
            }

            last_id = id;
            last_time = time;
            record_count += 1;
        });

        println!("Database analyze / BEGIN: {}({})  -> END: {}({}), {}[rec]  / {}[gap] / total rec {}", 
            time_string(first_time), first_id,
            time_string(last_time), last_id,
            last_id - first_id + 1,
            gap_count,
            record_count
        );

        if 1 < gap_count {
            println!("WARNING database has {} gaps. Download with force option, or drop-and-create database.", gap_count);
        }

        gap_count
    }

    pub fn cache_all_data(&mut self) {
        self.db.update_cache_all();
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
    pub fn get_board(&self) -> PyResult<(PyDataFrame, PyDataFrame)> {
        self.board.lock().unwrap().get_board()
    }

    #[getter]
    pub fn get_board_vec(&self) -> PyResult<(Vec<BoardItem>, Vec<BoardItem>)> {
        Ok(self.board.lock().unwrap().get_board_vec().unwrap())
    }

    #[getter]
    pub fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)> {
        self.board.lock().unwrap().get_edge_price()
    }

    #[getter]
    pub fn get_file_name(&self) -> String {
        return self.db.get_file_name();
    }

    #[getter]
    pub fn get_market_config(&self) -> MarketConfig {
        return self.config.market_config.clone();
    }

    pub fn vacuum(&self) {
        let _ = self.db.vacuum();
    }

    pub fn _repr_html_(&self) -> String {
        return format!("<b>Binance DB ({})</b>{}", self.symbol, self.db._repr_html_());
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
                            let r = db_channel.send(vec![trade.to_trade()]);
                            
                            if r.is_err() {
                                log::error!("Error in db_channel.send: {:?} {:?}", trade, r);
                            }

                            let multi_agent_channel = agent_channel.borrow_mut();
                            
                            let r = multi_agent_channel.lock().unwrap().send(message.into());

                            if r.is_err() {
                                log::error!("Error in agent_channel.send: {:?} {:?}", trade, r);
                            }
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

    /*
    // TODO: 単に待っているだけなので、終了処理を実装する。
    pub fn stop_market_stream(&mut self) {
        match self.public_handler.take() {
            Some(h) => {
                h.join().unwrap();
            }
            None => {}
        }
    }
    */

    pub fn start_user_stream(&mut self) {
        let mut agent_channel = self.channel.clone();

        let cfg = self.config.clone();

        self.user_handler = Some(listen_userdata_stream(
            &self.config,
            move |message: BinanceUserStreamMessage| {
                log::debug!("UserStream: {:?}", message);
                let mutl_agent_channel = agent_channel.borrow_mut();
                let m = message.convert_to_market_message(&cfg);
                let _ = mutl_agent_channel.lock().unwrap().send(m);
            },
        ));

        log::info!("start_user_stream");
    }

    /*
    pub fn stop_user_stream(&mut self) {
        match self.user_handler.take() {
            Some(h) => {
                h.join().unwrap();
            }
            None => {}
        }
    }
    */

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

    /*
    #[pyo3(signature = (side, price, size, client_order_id=None))]
    pub fn new_limit_order_raw(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<BinanceOrderResponse> {
        let price_scale = self.config.market_config.price_scale;
        let size_scale = self.config.market_config.size_scale;
        let order_side = OrderSide::from(side);

        let price = price.round_dp(price_scale);
        let size = size.round_dp(size_scale);

        let response = new_limit_order(&self.config, order_side, price, size, client_order_id);

        convert_pyresult(response)
    }
    */

    #[pyo3(signature = (side, price, size, client_order_id=None))]
    pub fn limit_order(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let price_scale = self.config.market_config.price_scale;
        let price_dp = price.round_dp(price_scale);

        let size_scale = self.config.market_config.size_scale;
        let size_dp = size.round_dp(size_scale);
        let order_side = OrderSide::from(side);

        let response = new_limit_order(&self.config, order_side, price_dp, size_dp, client_order_id);

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

    /*
    pub fn new_market_order_raw(
        &self,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<BinanceOrderResponse> {
        let size_scale = self.config.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let order_side = OrderSide::from(side);

        let response = new_market_order(&self.config, order_side, size, client_order_id);

        convert_pyresult(response)
    }
    */

    pub fn market_order(
        &self,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let size_scale = self.config.market_config.size_scale;
        let size = size.round_dp(size_scale);
        
        let order_side = OrderSide::from(side);

        let response = new_market_order(&self.config, order_side, size, client_order_id);

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


    pub fn dry_market_order(
        &self,
        create_time: MicroSec,
        order_id: &str,
        client_order_id: &str,
        side: OrderSide,
        size: Decimal,
        transaction_id: &str,
    ) -> Vec<Order> {
        let (bids, asks) = self.board.lock().unwrap().get_board_vec().unwrap();

        let board = if side == OrderSide::Buy {
            asks
        } else {
            bids
        };

        let mut orders: Vec<Order> = vec![];
        let mut split_index = 0;

        let mut remain_size = size;

        // TODO: consume boards
        for item in board {
            if remain_size <= dec![0.0] {
                break;
            }

            let execute_size;
            let order_status;
            split_index += 1;

            if remain_size <= item.size {
                order_status = OrderStatus::Filled;                
                execute_size = remain_size;
                remain_size = dec![0.0];
            }
            else {
                order_status = OrderStatus::PartiallyFilled;                
                execute_size = item.size;
                remain_size -= item.size;
            }

            let mut order = Order::new(
                self.config.market_config.symbol(),
                create_time,
                order_id.to_string(),
                client_order_id.to_string(),
                side,
                OrderType::Market,
                order_status,
                dec![0.0],
                size,
            );

            order.transaction_id = format!("{}-{}", transaction_id, split_index);
            order.update_time = create_time;
            order.is_maker = false;
            order.execute_price = item.price;
            order.execute_size = execute_size;
            order.remain_size = remain_size;
            order.quote_vol = order.execute_price * order.execute_size;

            orders.push(order);
        }

        if remain_size > dec![0.0] {
            log::error!("remain_size > 0.0: {:?}", remain_size);
        }

        return orders;
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
    pub fn get_account(&self) -> PyResult<BinanceAccountInformation> {
        let status = get_balance(&self.config);

        convert_pyresult(status)
    }
}

use crate::exchange::binance::rest::{get_board_snapshot, download_historical_trades_from_id};

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

        let trade = Trade::new(timestamp, order_side, price, size, LogStatus::FixArchiveBlock, id);

        return trade;
    }

    fn get_latest_archive_date(&self) -> Result<MicroSec, String> {
        let mut latest = TODAY();
        let mut i = 0;

        loop {

            let has_archive = self.has_archive(latest);

            if has_archive.is_err() {
                log::error!("Error in has_archive: {:?}", has_archive);
                return Err(format!("Error in has_archive: {:?}", has_archive));
            }            

            let has_archive = has_archive.unwrap();            

            if has_archive {
                return Ok(latest);
            }

            latest -= DAYS(1);
            i += 1;

            if 5 < i {
                return Err(format!("get_latest_archive max retry error"));
            }
        }
    }

    fn has_archive(&self, date: MicroSec) -> Result<bool, String> {
        let url = Self::make_historical_data_url_timestamp(self.symbol.as_str(), date);

        if check_exist(url.as_str()) {
            log::debug!("{} exists", url);
            return Ok(true);
        } else {
            log::debug!("{} does not exist", url);
        }
        return Ok(false);
    }

    /// Check if database is valid at the date
    /// TODO: implement
    fn validate_db_by_date(&mut self, date: MicroSec) -> bool {
        let start_time = FLOOR_DAY(date);
        let end_time = start_time + DAYS(1);

        // startからendまでのレコードにS,Eが1つづつあるかどうかを確認する。
        let sql = r#"select time_stamp, action, price, size, status, id from trades where $1 <= time_stamp and time_stamp < $2 and (status = "S" or status = "E") order by time_stamp"#;
        let trades = self.db.connection.select_query(sql, vec![start_time, end_time]);

        if trades.len() != 2 {
            log::debug!("S,E is not 2 {}", trades.len());
            return false;
        }

        let first = trades[0].clone();
        let last = trades[1].clone();

        // Sから始まりEでおわることを確認
        if first.status != LogStatus::FixBlockStart && last.status != LogStatus::FixBlockEnd {
            log::debug!("S,E is not S,E");
            return false;
        }

        // S, Eのレコードの間が十分にあること（トラフィックにもよるが２２時間を想定）
        if last.time - first.time < HHMM(20, 0) {
            log::debug!("batch is too short");
            return false;
        }

        true
    }
}

#[cfg(test)]
mod binance_test {
    use std::{thread::sleep, time::Duration};

    use crate::{common::{init_debug_log, init_log, time_string}, exchange::binance::rest::{download_historical_trades, download_historical_trades_from_id}};

    use super::*;

    #[test]
    fn test_make_historical_data_url_timestamp() {
        init_log();
        println!(
            "{}",
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 0)
        );
        assert_eq!(
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", 0),
            "https://data.binance.vision/data/spot/daily/trades/BTCUSD/BTCUSD-trades-1970-01-01.zip"            
        );

        println!(
            "{}",
            BinanceMarket::make_historical_data_url_timestamp("BTCUSD", NOW())
        );

        println!("{} / {}", TODAY(), time_string(TODAY()));
        println!("{} / {}", DAYS(1), time_string(DAYS(1)));        
    }

    #[test]
    fn test_download() {
        init_debug_log();
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let mut market = BinanceMarket::new("BTCBUSD", true);
        println!("{}", time_string(market.db.start_time().unwrap_or(0)));
        println!("{}", time_string(market.db.end_time().unwrap_or(0)));
        println!("Let's donwload");
        market.download(2, false, true);
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
        let market = BinanceMarket::new(&BinanceConfig::TEST_BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        init_debug_log();

        println!("{}", time_string(market.get_latest_archive_date().unwrap()));
    }

    #[test]
    fn test_validate_db_by_days() {
        let mut market = BinanceMarket::new(&BinanceConfig::TEST_BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        let date = market.get_latest_archive_date().unwrap();

        println!("{}", time_string(date));
        // market.download(1, false, true);

        let valid = market.validate_db_by_date(date);

        println!("valid: {}", valid);
    }

    #[test]
    fn test_latest_fixed_db_time() {
        let mut market = BinanceMarket::new(&BinanceConfig::TEST_BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        let r = market.latest_fix_time();
        println!("FIXTIME: {:?}", time_string(r.1));

        let r = market.latest_stable_time(true);
        println!("STABLETIME: {:?}", time_string(r.1));
    }

    #[test]
    fn test_download_historical_data() {
        init_debug_log();
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
        //let market = BinanceMarket::new("BTCBUSD", true);

        let r = market.latest_stable_time(true);
        println!("STABLETIME: {:?}", time_string(r.1));

        /*
        let ((fid, ftime), (eid, etime)) = download_historical_trades(&BinanceConfig::BTCUSDT(), r.0, &mut |row|Ok({
            println!("{:?}", row);
        })).unwrap();
        */

        let ((fid, ftime), (eid, etime), rec_no) = download_historical_trades(&BinanceConfig::BTCUSDT(), 1, &mut |row|Ok({
            println!("{:?}", row);
        })).unwrap();

        println!("{} / {}", fid, time_string(ftime));
        println!("{} / {}", eid, time_string(etime));
        println!("{}", rec_no);
    }

    #[test]
    fn test_download_historical_data2() {
//        init_debug_log();
let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());
//let market = BinanceMarket::new("BTCBUSD", true);

        let (stable_id, stable_time)= market.latest_stable_time(true);
        println!("STABLETIME: {:?}", time_string(stable_time));

        let rec_no = download_historical_trades_from_id(&BinanceConfig::BTCUSDT(), stable_id, true,&mut |_row|Ok({
            // println!("{:?}", _row);
        })).unwrap();

        println!("{}", rec_no);
    }

    #[test]
    fn test_analyze_db() {
        let mut market = BinanceMarket::new(&BinanceConfig::BTCUSDT());

        let mut last_id: BinanceMessageId = 0;
        let mut last_time: MicroSec = 0;

        market.db.connection.select(0, 0, |trade|{
            let id = trade.id.clone();
            let id = id.parse::<BinanceMessageId>().unwrap();

            let time = trade.time;

            if last_id != 0 && id != last_id + 1{
                println!("{} / {}", time, time_string(time));
            }

            last_id = id;
            last_time = time;
        });
    }

}

// Copyright(c) 2022-2023. yasstake. All rights reserved.

use crossbeam_channel::Sender;
use csv::StringRecord;
use polars_core::export::num::FromPrimitive;

use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

use crate::common::{
    to_naive_datetime, LogStatus, MarketConfig, MarketMessage, MarketStream, MicroSec,
    MultiChannel, Order, OrderSide, OrderStatus, OrderType, Trade, FLOOR_DAY, flush_log, NOW, DAYS, time_string,
};
use crate::db::df::KEY;
use crate::db::sqlite::TradeTable;
use crate::exchange::bybit::rest::open_orders;
use crate::exchange::{download_log, latest_archive_date, BoardItem, OrderBook, WebSocketClient, BybitWsOpMessage};
use crate::fs::db_full_path;
use chrono::Datelike;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::BybitServerConfig;
use super::rest::cancel_order;
use super::rest::cancell_all_orders;
use super::rest::get_balance;
use super::rest::new_limit_order;
use super::rest::new_market_order;
use super::rest::order_status;
use super::rest::trade_list;

use super::message::{BybitAccountInformation, BybitWsMessage};
use super::message::BybitOrderStatus;

#[derive(Debug)]
pub struct BybitOrderBook {
    config: MarketConfig,
    last_update_id: u64,
    board: OrderBook,
}

impl BybitOrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        return BybitOrderBook {
            config: config.clone(),
            last_update_id: 0,
            board: OrderBook::new(&config),
        };
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

        if asks_edge < bids_edge {
            log::warn!("bids_edge({}) < asks_edge({})", bids_edge, asks_edge);

            self.reflesh_board();

            (bids, asks) = self.board.get_board().unwrap();
        }

        return Ok((PyDataFrame(bids), PyDataFrame(asks)));
    }

    fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)> {
        Ok(self.board.get_edge_price())
    }

    /*
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
    */

    fn reflesh_board(&mut self) {
        // TODO: reflesh board from rest api
    }
}


#[pyclass]
#[derive(Debug)]
pub struct Bybit {
    server_config: BybitServerConfig,
}

#[pymethods]
impl Bybit {
    #[new]
    #[pyo3(signature = (testnet=false))]
    pub fn new(testnet: bool) -> Self {
        let server_config = BybitServerConfig::new(testnet);

        return Bybit {
            server_config: server_config.clone(),
        };
    }

    pub fn open_market(&self, config: &MarketConfig) -> BybitMarket {
        return BybitMarket::new(&self.server_config, config);
    }
}


#[derive(Debug)]
#[pyclass]
pub struct BybitMarket {
    pub server_config: BybitServerConfig,
    pub config: MarketConfig,
    pub db: TradeTable,
    pub board: Arc<Mutex<BybitOrderBook>>,
    pub public_ws: WebSocketClient<BybitWsOpMessage>
}

#[pymethods]
impl BybitMarket {
    /*-------------------　共通実装（コピペ） ---------------------------------------*/
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

    pub fn stop_db_thread(&mut self) {
        self.db.stop_thread();
    }

    /// common implementation
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
        return self.config.clone();
    }

    #[getter]
    pub fn get_running(&self) -> bool {
        return self.db.is_running();
    }

    pub fn vacuum(&self) {
        let _ = self.db.vacuum();
    }

    pub fn _repr_html_(&self) -> String {
        return format!(
            "<b>DB ({})</b>{}",
            self.config.trade_symbol,
            self.db._repr_html_()
        );
    }

    /*--------------　ここまでコピペ　--------------------------*/

    #[pyo3(signature = (*, ndays, force = false, verbose=true, _archive_only=false))]
    pub fn download(&mut self, ndays: i64, force: bool, verbose: bool, _archive_only: bool) -> i64 {
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

        let tx = &self.db.start_thread();

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

            match self.download_log(tx, date, verbose) {
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

            // self.wait_for_settlement(tx);
        }

        download_rec
    }

    #[pyo3(signature = (force=false, verbose = true))]
    pub fn download_latest(&mut self, force: bool, verbose: bool) -> i64 {
        return 0;
        /*
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
            println!("ERROR: no record in database path= {}", self.get_file_name());
            flush_log();

            return 0;
        }

        let ch = self.db.start_thread();

        let record_number = download_historical_trades_from_id(&BinanceConfig::BTCUSDT(), start_id, verbose,&mut |row|
        {
            //TODO:
            while 100 < ch.len() {
                sleep(Duration::from_millis(100));
            }

            ch.send(row.clone()).unwrap();

            Ok(())
        }).unwrap();

        if verbose {
            println!("\nREST downloaded: {}[rec]", record_number);
            flush_log();
        }


        return record_number;
        */
    }

    /*
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
    */

    /*
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
*/

        /*
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
    */    

    pub fn start_user_stream(&mut self) {
        /*
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
        */
    }

    /*
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
    */

    /* TODO: implment */
    /*
    #[getter]
    pub fn get_channel(&mut self) -> MarketStream {
        let ch = self.public_ws.open_channel();

        let ch = self.channel.lock().unwrap().open_channel(0);
        MarketStream{reciver: ch}
    }
    */

    pub fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> MarketStream {
        self.db.connection.select_stream(time_from, time_to)
    }

    #[pyo3(signature = (side, price, size, client_order_id=None))]
    pub fn limit_order(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let price_scale = self.config.price_scale;
        let price_dp = price.round_dp(price_scale);

        let size_scale = self.config.size_scale;
        let size_dp = size.round_dp(size_scale);
        let order_side = OrderSide::from(side);

        let response =
            new_limit_order(
                &self.server_config.rest_server, &self.config, 
                order_side, price_dp, size_dp, client_order_id);

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

        // TODO: FIXconvert_pyresult(response)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
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
        let size_scale = self.config.size_scale;
        let size = size.round_dp(size_scale);

        let order_side = OrderSide::from(side);

        let response = new_market_order(
            &self.server_config.rest_server, &self.config, 
            order_side, size, client_order_id);

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

        //convert_pyresult(response)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
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

        let board = if side == OrderSide::Buy { asks } else { bids };

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
            } else {
                order_status = OrderStatus::PartiallyFilled;
                execute_size = item.size;
                remain_size -= item.size;
            }

            let mut order = Order::new(
                &self.config.trade_symbol,
                create_time,
                &order_id,
                &client_order_id.to_string(),
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
        let response = cancel_order(
            &self.server_config.rest_server, &self.config, order_id);

        // return convert_pyresult(response);
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    pub fn cancel_all_orders(&self) -> PyResult<Vec<Order>> {
        let response = cancell_all_orders(
            &self.server_config.rest_server, &self.config); 

        if response.is_ok() {
            // TODO:: FIX IMPLMENET
            // return convert_pyresult_vec(response);
        }

        return PyResult::Ok(vec![]);
    }

    #[getter]
    pub fn get_order_status(&self) -> PyResult<Vec<BybitOrderStatus>> {
        let status = order_status(
            &self.server_config.rest_server, &self.config); 


        // TODO: IMPLEMENT convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_open_orders(&self) -> PyResult<Vec<Order>> {
        let status = open_orders(
            &self.server_config.rest_server, &self.config); 


        log::debug!("OpenOrder: {:?}", status);

        // convert_pyresult_vec(status)
        // TODO: implement
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_trade_list(&self) -> PyResult<Vec<BybitOrderStatus>> {
        let status = trade_list(
            &self.server_config.rest_server, &self.config); 

        //         convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_account(&self) -> PyResult<BybitAccountInformation> {
        let status = get_balance(
            &self.server_config.rest_server, &self.config); 


        //convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }
}

impl BybitMarket {
    pub fn new(server_config: &BybitServerConfig, config: &MarketConfig) -> Self {

        let db = TradeTable::open(&Self::make_db_path(
            &server_config.exchange_name,
            &config.trade_category, 
            &config.trade_symbol,
            &server_config.db_base_dir));
        if db.is_err() {
            log::error!("Error in TradeTable::open: {:?}", db);
        }

        return BybitMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: db.unwrap(),
            board: Arc::new(Mutex::new(BybitOrderBook::new(config))),
            public_ws: WebSocketClient::new(&server_config.public_ws, config.public_subscribe_channel.clone()),
        };
    }

    pub fn make_db_path(exchange_name: &str, trade_category: &str, trade_symbol: &str, db_base_dir: &str) -> String {
        let db_path = db_full_path(&exchange_name, trade_category, trade_symbol, db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }

    fn history_web_url(&self, date: MicroSec) -> String {
        Self::make_historical_data_url_timestamp(
            &self.server_config.history_web_base,
            &self.config.trade_symbol,
            date)
    }

    fn make_historical_data_url_timestamp(history_web_base: &str, symbol: &str, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        return format!(
            "{}/trading/{}/{}{:04}-{:02}-{:02}.csv.gz",
            history_web_base, symbol, symbol, yyyy, mm, dd
        );
    }

    fn get_latest_archive_date(&self) -> Result<MicroSec, String> {
        let f = |date: MicroSec| -> String {
            Self::make_historical_data_url_timestamp(
                &self.server_config.history_web_base,
                &self.config.trade_symbol,
                date)
        };

        latest_archive_date(&f)
    }

    pub fn download_log(
        &mut self,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        verbose: bool,
    ) -> PyResult<i64> {
        let date = FLOOR_DAY(date);
        let url = self.history_web_url(date);

        match download_log(&url, tx, true, verbose, &Self::rec_to_trade) {
            Ok(download_rec) => Ok(download_rec),
            Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in download_logs: {:?}",
                e
            ))),
        }
    }

    /// timestamp,      symbol,side,size,price,  tickDirection,trdMatchID,                          grossValue,  homeNotional,foreignNotional
    /// 1620086396.8268,BTCUSDT,Buy,0.02,57199.5,ZeroMinusTick,224061a0-e105-508c-9696-b53ab4b5bb03,114399000000.0,0.02,1143.99    
    fn rec_to_trade(rec: &StringRecord) -> Trade {
        let timestamp = rec
            .get(0)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default()
            * 1_000_000.0;

        let timestamp = timestamp as MicroSec;

        let order_side = match rec.get(2).unwrap_or_default() {
            "Buy" => OrderSide::Buy,
            "Sell" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

        let id = rec.get(6).unwrap_or_default().to_string();

        let price = rec
            .get(4)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let price: Decimal = Decimal::from_f64(price).unwrap();

        let size = rec
            .get(3)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let size = Decimal::from_f64(size).unwrap();

        let trade = Trade::new(
            timestamp,
            order_side,
            price,
            size,
            LogStatus::FixArchiveBlock,
            &id,
        );

        return trade;
    }


    /// Check if database is valid at the date
    fn validate_db_by_date(&mut self, date: MicroSec) -> bool {
        self.db.connection.validate_by_date(date)
    }

    /*
    pub fn wait_for_settlement(&mut self, tx: &Sender<Vec<Trade>>) {
        while 5 < tx.len() {
            sleep(Duration::from_millis(1 * 100));
        }
    }




    pub fn download_log(&mut self, tx: &Sender<Vec<Trade>>, date: MicroSec, verbose: bool) -> PyResult<i64> {
        let date = FLOOR_DAY(date);

        let url = Self::make_historical_data_url_timestamp(&self.config, self.symbol.as_str(), date);


        match download_log(&url, tx, false, verbose, &BinanceMarket::rec_to_trade) {
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
    */
}

#[cfg(test)]
mod test_bybit_market {
    use csv::StringRecord;
    use rust_decimal_macros::dec;

    use crate::{common::time_string, exchange::bybit::config::{BybitServerConfig, BybitConfig}};

    #[test]
    fn test_make_historical_data_url_timestamp() {
        let config = BybitConfig::SPOT_BTCUSDT();

        let url = super::BybitMarket::make_historical_data_url_timestamp(
            "https://public.bybit.com/trading/",
            &config.trade_symbol,
            1620000000000_000,
        );

        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2021-05-03.csv.gz"
        );

        let url = super::BybitMarket::make_historical_data_url_timestamp(
            "https://public.bybit.com/trading/",           
            &config.trade_symbol,             
            1234,
        );

        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSDT/BTCUSDT1970-01-01.csv.gz"
        );
    }

    #[test]
    fn test_get_latest_archive_date() {
        let server_config = BybitServerConfig::new(false);    
        let config = BybitConfig::SPOT_BTCUSDT();

        let market = super::BybitMarket::new(&server_config, &config);

        let date = market.get_latest_archive_date().unwrap();
        println!("date: {}", date);
        println!("date: {}", time_string(date));
    }

    #[test]
    fn test_rec_to_trade() {
        let rec = "1692748800.279,BTCUSD,Sell,641,26027.00,ZeroMinusTick,80253109-efbb-58ca-9adc-d458b66201e9,2.462827064202559e+06,641,0.02462827064202559".to_string();
        let rec = rec.split(',').collect::<Vec<&str>>();
        let rec = StringRecord::from(rec);
        let trade = super::BybitMarket::rec_to_trade(&rec);

        assert_eq!(trade.time, 1692748800279000);
        assert_eq!(trade.order_side, super::OrderSide::Sell);
        assert_eq!(trade.price, dec![26027.0]);
        assert_eq!(trade.size, dec![641.0]);
        assert_eq!(trade.id, "80253109-efbb-58ca-9adc-d458b66201e9");
    }

}

// Copyright(c) 2022-2023. yasstake. All rights reserved.
#![allow(unused_imports)]

use crossbeam_channel::Sender;
use csv::StringRecord;
use polars_core::export::num::FromPrimitive;
use rusqlite::ffi::SQLITE_LIMIT_FUNCTION_ARG;

use std::sync::{Arc, RwLock};
use std::thread::{JoinHandle, sleep};
use std::time::Duration;

use crate::common::{
    flush_log, time_string, to_naive_datetime, LogStatus, MarketConfig, MarketMessage,
    MarketStream, MicroSec, MultiChannel, Order, OrderSide, OrderStatus, OrderType, Trade, DAYS,
    FLOOR_DAY, NOW, HHMM,
};
use crate::db::df::KEY;
use crate::db::sqlite::{TradeTable, TradeTableDb};
use crate::exchange::bybit::message::BybitUserStreamMessage;
use crate::exchange::bybit::rest::{open_orders, get_trade_kline};
use crate::exchange::bybit::ws::listen_userdata_stream;
use crate::exchange::{
    download_log, latest_archive_date, BoardItem, BybitWsOpMessage, OrderBook, OrderBookRaw,
    WebSocketClient,
};
use crate::fs::db_full_path;
use crate::net::{UdpSender, udp};
use chrono::Datelike;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::BybitServerConfig;
use super::rest::{cancel_order, get_recent_trade};
use super::rest::cancell_all_orders;
use super::rest::get_balance;
use super::rest::new_limit_order;
use super::rest::new_market_order;
use super::rest::order_status;
use super::rest::trade_list;

use super::message::BybitOrderStatus;
use super::message::{BybitAccountInformation, BybitWsMessage};


#[derive(Debug)]
pub struct BybitOrderBook {
    board: OrderBook,
}

impl BybitOrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        return BybitOrderBook {
            board: OrderBook::new(&config),
        };
    }

    fn get_board_vec(&self) -> Result<(Vec<BoardItem>, Vec<BoardItem>), ()> {
        let (bids, asks) = self.board.get_board_vec().unwrap();

        Ok((bids, asks))
    }

    fn get_board_json(&self, size: usize) -> Result<String, ()> {
        let json = self.board.get_json(size).unwrap();

        Ok(json)
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

    pub fn update(&mut self, board: &OrderBookRaw) {
        let bids = board.get_bids();
        let asks = board.get_asks();
        self.board.update(&bids, &asks, board.snapshot);
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
        // TODO: implement
        println!("reflesh board :NOT IMPLEMENTED");
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
    pub board: Arc<RwLock<BybitOrderBook>>,
    pub public_ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage>,
    pub public_handler: Option<JoinHandle<()>>,
    pub user_handler: Option<JoinHandle<()>>,
    pub agent_channel: Arc<RwLock<MultiChannel<MarketMessage>>>,
    #[pyo3(get, set)]
    pub broadcast_message: bool
}

#[pymethods]
impl BybitMarket {

    /*-------------------　共通実装（コピペ） ---------------------------------------*/
    #[getter]
    pub fn get_config(&self) -> MarketConfig {
        return self.config.clone();
    }

    #[getter]
    pub fn get_exchange_name(&self) -> String {
        return self.server_config.exchange_name.clone();
    }

    #[getter]
    pub fn get_trade_category(&self) -> String {
        return self.config.trade_category.clone();
    }

    #[getter]
    pub fn get_trade_symbol(&self) -> String {
        return self.config.trade_symbol.clone();
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

    pub fn get_board_json(&self, size: usize) -> PyResult<String> {
        let board = self.board.read().unwrap();
        return Ok(board.get_board_json(size).unwrap());
    }

    #[getter]
    pub fn get_board(&self) -> PyResult<(PyDataFrame, PyDataFrame)> {
        self.board.write().unwrap().get_board()
    }

    #[getter]
    pub fn get_board_vec(&self) -> PyResult<(Vec<BoardItem>, Vec<BoardItem>)> {
        Ok(self.board.read().unwrap().get_board_vec().unwrap())
    }

    #[getter]
    pub fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)> {
        self.board.read().unwrap().get_edge_price()
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

    #[pyo3(signature = (*, ndays, force = false, verbose=true, archive_only=false, low_priority=false))]
    pub fn download(&mut self, ndays: i64, force: bool, verbose: bool, archive_only: bool, low_priority: bool) -> i64 {
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

            if !force && self.validate_db_by_date(date) {
                log::info!("{} is valid", time_string(date));

                if verbose {
                    println!("{} skip download", time_string(date));
                    flush_log();
                }
                continue;
            }

            match self.download_log(tx, date, low_priority, verbose) {
                Ok(rec) => {
                    log::info!("downloaded: {}", download_rec);
                    download_rec += rec;
                }
                Err(e) => {
                    log::error!("Error in download_log: {:?}", e);
                    if verbose {
                        println!("Error in download_log: {:?}", e);
                    }
                }
            }
        }

        if ! archive_only {
            let rec = self.download_latest(verbose);
            download_rec += rec;
        }
        // let expire_message = self.db.connection.make_expire_control_message(now);
        // tx.send(expire_message).unwrap();

        download_rec
    }

    /// Download klines and store ohlcv cache.
    /// STEP1: 不確定データを削除する（start_market_streamで実行）
    /// STEP2: WSでデータを取得開始する。(start_market_streamで実行)
    /// STEP3: RESTでrecent tradeデータを取得する。start_market_streamで実行
    /// STEP4: RESTでklineデータを取得しキャッシュする。download_latestで実行
    #[pyo3(signature = (verbose = true))]
    pub fn download_latest(&mut self, verbose: bool) -> i64 {
        if verbose {
            println!("download_latest");
            flush_log();
        }
        let start_time = NOW() - DAYS(2) ;

        let fix_time = self.db.connection.latest_fix_time(start_time);
        let fix_time = TradeTable::ohlcv_end(fix_time);

        let unfix_time = self.db.connection.first_unfix_time(fix_time);
        let unfix_time = TradeTable::ohlcv_end(unfix_time) - 1;

        if (unfix_time - fix_time) <= HHMM(0, 1) {
            if verbose {
                println!("no need to download");
                flush_log();
            }
            return 0;
        }

        if verbose {
            print!("fill out with kline data ");
            print!("FROM: fix_time: {:?}/{:?}", time_string(fix_time), fix_time);
            println!(" TO:unfix_time: {:?}/{:?}", time_string(unfix_time), unfix_time);
            flush_log();
        }

        let klines = get_trade_kline(
            &self.server_config.rest_server,
            &self.config,
            fix_time,
            unfix_time,
        );

        if klines.is_err() {
            log::error!("Error in get_trade_kline: {:?}", klines);
            return 0;
        }
        let klines = klines.unwrap();
        let rec = klines.klines.len();

        let control_message = TradeTableDb::expire_control_message(fix_time, unfix_time);

        let trades: Vec<Trade> = klines.into();

        let tx = self.db.start_thread();
        tx.send(control_message).unwrap();
        tx.send(trades).unwrap();

        if verbose {
            println!("downloaded klines: {} rec", rec);
            flush_log();
        }        

        return rec as i64;
    }

    // TODO: implment retry logic
    pub fn start_market_stream(&mut self) {
        // if thread is working, do nothing.
        if self.public_handler.is_some() {
            println!("market stream is already running.");
            return;
        }

        // delete unstable data
        let db_channel = self.db.start_thread();

        // if the latest data is overrap with REST minute, do not delete data.
        let trades = self.get_recent_trades();
        let l = trades.len();
        if l != 0 {
            let rest_start_time = trades[l - 1].time;
            let last_time = self.db.end_time(rest_start_time);
            if last_time.is_err() {
                let now = NOW();

                log::debug!("db has gap, so delete after FIX data now={:?} / db_end={:?}", 
                        time_string(now), time_string(rest_start_time));

                let delete_message = self.db.connection.make_expire_control_message(now);

                log::debug!("delete_message: {:?}", delete_message);

                db_channel.send(delete_message).unwrap();
            }
            else {
                log::debug!("db has no gap, so continue to receive data {}", time_string(rest_start_time));
            }

            db_channel.send(trades).unwrap();
        }

        let agent_channel = self.agent_channel.clone();
        let ws_channel = self.public_ws.open_channel();

        self.public_ws.connect(|message| {
            let m = serde_json::from_str::<BybitWsMessage>(&message);

            if m.is_err() {
                log::warn!("Error in serde_json::from_str: {:?}", message);
                println!("ERR: {:?}", message);
            }

            let m = m.unwrap();

            return m.into();
        });

        let board = self.board.clone();
        let db_channel_for_after = db_channel.clone();

        let udp_sender = if self.broadcast_message {
            Some(self.open_udp())
        }
        else {
            None
        };

        let handler = std::thread::spawn(move || {
            loop {
                let message = ws_channel.recv();

                let message = message.unwrap();


                if message.trade.len() != 0 {
                    log::debug!("Trade: {:?}", message.trade);
                    let r = db_channel.send(message.trade.clone());

                    if r.is_err() {
                        log::error!("Error in db_channel.send: {:?}", r);
                    }
                }

                if message.orderbook.is_some() {
                    let orderbook = message.orderbook.clone().unwrap();
                    let mut b = board.write().unwrap();
                    b.update(&orderbook);
                    drop(b);
                }

                let messages = message.extract();

                // update board

                // send message to agent
                for m in messages {
                    if m.trade.is_some() {
                        // broadcast only trade message
                        if udp_sender.is_some() {
                            let sender = udp_sender.as_ref().unwrap();
                            let _ = sender.send_market_message(&m);
                        }

                        let mut ch = agent_channel.write().unwrap();
                        let r = ch.send(m.clone());
                        drop(ch);

                        if r.is_err() {
                            log::error!("Error in db_channel.send: {:?}", r);
                        }
                    }
                }
            }
        });

        self.public_handler = Some(handler);

        // update recent trade
        // wait for channel open
        sleep(Duration::from_millis(100));     // TODO: fix to wait for channel open
        let trade = self.get_recent_trades();
        db_channel_for_after.send(trade).unwrap();

        // TODO: store recent trade timestamp.

        log::info!("start_market_stream");
    }


    pub fn start_user_stream(&mut self) {
        let agent_channel = self.agent_channel.clone();

        let server_config = self.server_config.clone();
        let market_config = self.config.clone();


        let udp_sender = if self.broadcast_message {
            Some(self.open_udp())
        }
        else {
            None
        };

        self.user_handler = Some(listen_userdata_stream(
            &server_config,
            move |message: BybitUserStreamMessage| {
                log::debug!("UserStream: {:?}", message);

                let ms = message.convert_to_market_message(&market_config);
                
                for m in ms {
                    if udp_sender.is_some() {
                        let sender = udp_sender.as_ref().unwrap();
                        let _ = sender.send_market_message(&m);
                    }
            
                    let mut mutl_agent_channel = agent_channel.write().unwrap();                    
                    let _ = mutl_agent_channel.send(m);
                    drop(mutl_agent_channel);
                }
            },
        ));

        log::info!("start_user_stream");
    }


    /* TODO: implment */

    #[getter]
    pub fn get_channel(&mut self) -> MarketStream {
        let ch = self.agent_channel.write().unwrap().open_channel(0);

        MarketStream { reciver: ch }
    }

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

        let response = new_limit_order(
            &self.server_config,
            &self.config,
            order_side,
            price_dp,
            size_dp,
            client_order_id.clone(),
        );

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
                "limit_order({:?}, {:?}, {:?}, {:?}) -> {:?}",
                side,
                price_dp,
                size_dp,
                client_order_id,
                response.unwrap_err()
            );
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(err));
        }

        Ok(vec![response.unwrap()])
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
            &self.server_config,
            &self.config,
            order_side,
            size,
            client_order_id,
        );

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

        Ok(vec![response.unwrap()])        
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
        let (bids, asks) = self.board.read().unwrap().get_board_vec().unwrap();

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
        let response = cancel_order(&self.server_config, &self.config, order_id);

        if response.is_err() {
            log::error!("Error in cancel_order: {:?}", response);
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Error in cancel_order {:?}", response),
            ));
        }

        let order = response.unwrap();

        Ok(order)
    }

    pub fn cancel_all_orders(&self) -> PyResult<Vec<Order>> {
        let response = cancell_all_orders(&self.server_config, &self.config);

        if response.is_err() {
            log::error!("Error in cancel_all_orders: {:?}", response);
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                format!("Error in cancel_all_orders {:?}", response),
            ));
        }

        return Ok(response.unwrap());
    }

    #[getter]
    pub fn get_order_status(&self) -> PyResult<Vec<BybitOrderStatus>> {
        let status = order_status(&self.server_config.rest_server, &self.config);

        // TODO: IMPLEMENT convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_open_orders(&self) -> PyResult<Vec<Order>> {
        let orders = open_orders(&self.server_config, &self.config);

        log::debug!("OpenOrder: {:?}", orders);
        Ok(orders.unwrap())
    }

    #[getter]
    pub fn get_trade_list(&self) -> PyResult<Vec<BybitOrderStatus>> {
        let status = trade_list(&self.server_config.rest_server, &self.config);

        //         convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_account(&self) -> PyResult<BybitAccountInformation> {
        let status = get_balance(&self.server_config.rest_server, &self.config);

        //convert_pyresult(status)
        return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
            "Not implemented",
        ));
    }

    #[getter]
    pub fn get_recent_trades(&self) -> Vec<Trade> {
        let trades = get_recent_trade(&self.server_config.rest_server, &self.config);        

        if trades.is_err() {
            log::error!("Error in get_recent_trade: {:?}", trades);
            return vec![];
        }

        trades.unwrap().into()
    }
}

impl BybitMarket {
    pub fn new(server_config: &BybitServerConfig, config: &MarketConfig) -> Self {
        let db = TradeTable::open(&Self::make_db_path(
            &server_config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            &server_config.db_base_dir,
        ));
        if db.is_err() {
            log::error!("Error in TradeTable::open: {:?}", db);
        }

        return BybitMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: db.unwrap(),
            board: Arc::new(RwLock::new(BybitOrderBook::new(config))),
            public_ws: WebSocketClient::new(
                &server_config,
                &format!("{}/{}", &server_config.public_ws, config.trade_category),
                config.public_subscribe_channel.clone(),
                None
            ),
            public_handler: None,
            user_handler: None,
            agent_channel: Arc::new(RwLock::new(MultiChannel::new())),
            broadcast_message: false,
        };
    }

    pub fn open_udp(&self) -> UdpSender {
        UdpSender::open(&self.server_config.exchange_name, &self.config.trade_category, &self.config.trade_symbol)
    }

    pub fn make_db_path(
        exchange_name: &str,
        trade_category: &str,
        trade_symbol: &str,
        db_base_dir: &str,
    ) -> String {
        let db_path = db_full_path(&exchange_name, trade_category, trade_symbol, db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }

    fn history_web_url(&self, date: MicroSec) -> String {
        Self::make_historical_data_url_timestamp(
            &self.server_config.history_web_base,
            &self.config.trade_symbol,
            date,
        )
    }

    fn make_historical_data_url_timestamp(
        history_web_base: &str,
        symbol: &str,
        t: MicroSec,
    ) -> String {
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
                date,
            )
        };

        latest_archive_date(&f)
    }

    pub fn download_log(
        &mut self,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        low_priority: bool,
        verbose: bool,
    ) -> PyResult<i64> {
        let date = FLOOR_DAY(date);
        let url = self.history_web_url(date);

        match download_log(&url, tx, true, low_priority, verbose, &Self::rec_to_trade) {
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

}

#[cfg(test)]
mod test_bybit_market {
    use csv::StringRecord;
    use rust_decimal_macros::dec;

    use crate::{
        common::{time_string, NOW, DAYS},
        exchange::bybit::config::{BybitConfig, BybitServerConfig},
    };

    #[test]
    fn test_make_historical_data_url_timestamp() {
        let config = BybitConfig::BTCUSDT();

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
        let config = BybitConfig::BTCUSDT();

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

    #[test]
    fn test_make_expire_message_control() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let mut market = super::BybitMarket::new(&server_config, &config);

        let message = market.db.connection.make_expire_control_message(NOW());

        println!("{:?}", message);
    }

    #[test]
    fn test_lastest_fix_time(){
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let mut market = super::BybitMarket::new(&server_config, &config);

        let start_time = NOW() - DAYS(2) ;

        let fix_time = market.db.connection.latest_fix_time(start_time);

        println!("fix_time: {:?}/{:?}", time_string(fix_time), fix_time);

        let unfix_time = market.db.connection.first_unfix_time(fix_time);
        println!("unfix_time: {:?}/{:?}", time_string(unfix_time), unfix_time);

    }
}

// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::sync::{Arc, Mutex, RwLock};

use crate::common::{
    flush_log, time_string, AccountStatus, MarketConfig,
    MarketMessage, MarketStream, MicroSec, MultiChannel, Order, OrderSide, OrderStatus, OrderType,
    Trade, DAYS, HHMM, NOW,
};
use crate::db::df::KEY;
use crate::db::sqlite::TradeTable;
use crate::exchange::{
    BoardItem, OrderBook, OrderBookRaw,
};
use crate::fs::db_full_path;

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::BitflyerServerConfig;
use super::rest::{new_limit_order, cancel_order, get_recent_trade};
use super::rest::cancell_all_orders;
use super::rest::get_balance;
use super::rest::new_market_order;
use super::rest::order_status;

//use super::rest::{cancel_order, get_recent_trade};

#[derive(Debug)]
pub struct BitflyerOrderBook {
    board: OrderBook,
}

impl BitflyerOrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        return BitflyerOrderBook {
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

            self.refresh_board();

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


    fn refresh_board(&mut self) {
        // TODO: refresh board from rest api
    }
}

#[pyclass]
#[derive(Debug)]
pub struct Bitflyer {
    server_config: BitflyerServerConfig,
}

#[pymethods]
impl Bitflyer {
    #[new]
    pub fn new() -> Self {
        let server_config = BitflyerServerConfig::new();

        return Bitflyer {
            server_config: server_config.clone(),
        };
    }

    pub fn open_market(&self, config: &MarketConfig) -> BitflyerMarket {
        return BitflyerMarket::new(&self.server_config, config);
    }
}

#[derive(Debug)]
#[pyclass]
pub struct BitflyerMarket {
    pub server_config: BitflyerServerConfig,
    pub config: MarketConfig,
    pub db: TradeTable,
    pub board: Arc<Mutex<BitflyerOrderBook>>,
    // pub public_ws: WebSocketClient<BitflyerServerConfig, BybitWsOpMessage>,
    // pub public_handler: Option<JoinHandle<()>>,
    pub agent_channel: Arc<RwLock<MultiChannel<MarketMessage>>>,
}

#[pymethods]
impl BitflyerMarket {
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

    #[allow(unused_variables)]
    #[pyo3(signature = (*, ndays, force = false, verbose=true, archive_only=false, low_priority=false))]
    pub fn download(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        archive_only: bool,
        low_priority: bool,
    ) -> i64 {
        /*
        log::info!("log download: {} days", ndays);
        if verbose {
            println!("log download: {} days", ndays);
            flush_log();
        }

        let mut download_rec: i64 = 0;

        let tx = &self.db.start_thread();

        let now = NOW();

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
        */
        0
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
        let start_time = NOW() - DAYS(2);

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
            println!(
                " TO:unfix_time: {:?}/{:?}",
                time_string(unfix_time),
                unfix_time
            );
            flush_log();
        }

        let rec: i64 = 0;

        /*
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
        */

        return rec as i64;
    }

    // TODO: implment retry logic
    pub fn start_market_stream(&mut self) {
        /*
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

        self.public_ws.connect(|message| {
            let m = serde_json::from_str::<BybitWsMessage>(&message);

            if m.is_err() {
                log::warn!("Error in serde_json::from_str: {:?}", message);
                println!("ERR: {:?}", message);
            }

            return m.unwrap().into();
        });


        let agent_channel = self.agent_channel.clone();
        let ws_channel = self.public_ws.open_channel();

        let board = self.board.clone();

        let db_channel_for_after = db_channel.clone();

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

                let messages = message.extract();

                // update board

                // send message to agent
                for m in messages {
                    if m.trade.is_some() {
                        let mut ch = agent_channel.write().unwrap();
                        let r = ch.send(m.clone());

                        if r.is_err() {
                            log::error!("Error in db_channel.send: {:?}", r);
                        }
                    }

                    if m.orderbook.is_some() {
                        log::debug!("BoardUpdate: {:?}", m.orderbook);
                        let orderbook = m.orderbook.unwrap();
                        board.lock().unwrap().update(&orderbook);
                        board.lock().unwrap().clip_depth();
                    }
                }
            }
        });

        self.public_handler = Some(handler);

        // update recent trade

        // wait for channel open
        sleep(Duration::from_millis(1000));     // TODO: fix to wait for channel open
        let trade = self.get_recent_trades();
        db_channel_for_after.send(trade).unwrap();

        // TODO: store recent trade timestamp.

        log::info!("start_market_stream");
        */
    }

    /*
        pub fn make_kline_df(&self, from_time: MicroSec, end_time: MicroSec) -> PyResult<PyDataFrame> {
            let start_time = self.db.start_time();

            // let mut df = self.db.make_kline_df();

            if df.is_err() {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error in make_kline_df: {:?}",
                    df
                )));
            }

            let df = df.unwrap();

            return Ok(PyDataFrame(df));
        }
    */

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


    pub fn is_market_stream_running(&self) -> bool {
        if let Some(handler) = &self.public_handler {
            return !handler.is_finished();
        }
        return false;
    }

    pub fn is_db_thread_running(&self) -> bool {
        return self.db.is_thread_running();
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
        let response = cancel_order(&self.server_config, &self.config, order_id);

        if response.is_err() {
            log::error!("Error in cancel_order: {:?}", response);
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in cancel_order {:?}",
                response
            )));
        }

        let order = response.unwrap();

        Ok(order)
    }

    pub fn cancel_all_orders(&self) -> PyResult<Vec<Order>> {
        let response = cancell_all_orders(&self.server_config, &self.config);

        if response.is_err() {
            log::error!("Error in cancel_all_orders: {:?}", response);
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in cancel_all_orders {:?}",
                response
            )));
        }

        return Ok(response.unwrap());
    }

    #[getter]
    pub fn get_order_status(&self) -> PyResult<Vec<Order>> {
        let status = order_status(&self.server_config.rest_server, &self.config);

        convert_pyresult(status)
    }

    #[getter]
    pub fn get_open_orders(&self) -> PyResult<Vec<Order>> {
        let status = open_orders(&self.server_config, &self.config);

        log::debug!("OpenOrder: {:?}", status);

        convert_pyresult_vec(status)
    }

    #[getter]
    pub fn get_trade_list(&self) -> PyResult<Vec<Order>> {
        let status = trade_list(&self.server_config.rest_server, &self.config);
        convert_pyresult(status)
    }

    #[getter]
    pub fn get_account(&self) -> PyResult<AccountStatus> {
        let status = get_balance(&self.server_config.rest_server, &self.config);

        convert_pyresult(status)
    }

    #[getter]
    pub fn get_recent_trades(&self) -> Vec<Trade> {
        let trades = get_recent_trade(&self.server_config, &self.config);

        if trades.is_err() {
            log::error!("Error in get_recent_trade: {:?}", trades);
            return vec![];
        }

        // TODO: implemnt
        // trades.unwrap().into()
        return vec![];
    }
}

impl BitflyerMarket {
    pub fn new(server_config: &BitflyerServerConfig, config: &MarketConfig) -> Self {
        let db = TradeTable::open(&Self::make_db_path(
            &server_config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            &server_config.db_base_dir,
        ));
        if db.is_err() {
            log::error!("Error in TradeTable::open: {:?}", db);
        }

        return BitflyerMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: db.unwrap(),
            board: Arc::new(Mutex::new(BitflyerOrderBook::new(config))),
            /*
            public_ws: WebSocketClient::new(
                &server_config,
                &format!("{}/{}", &server_config.public_ws, config.trade_category),
                config.public_subscribe_channel.clone(),
                None
            ),
            */
            // public_handler: None,
            agent_channel: Arc::new(RwLock::new(MultiChannel::new())),
        };
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

    #[allow(dead_code)]
    /// Check if database is valid at the date
    fn validate_db_by_date(&mut self, date: MicroSec) -> bool {
        self.db.connection.validate_by_date(date)
    }
}

#[cfg(test)]
mod test_message {
    #[test]
    fn parse_bitflyer_time() {
        use chrono::prelude::*;
        let date_str = "2024-01-07T14:51:28.033";
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.3f");
        println!("timestamp: {:?}", timestamp);

        let date_str = "2024-01-07T14:51:28+09:00";
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S");
        println!("timestamp: {:?}", timestamp);

        let date_str = "2024-01-07T14:51:28Z";
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S");
        println!("timestamp: {:?}", timestamp);

        let date_str = "2024-01-07T14:51:28";
        //                  %Y-%m-%dT%H:%M:%S");
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S");
        println!("timestamp: {:?}", timestamp);

        let date_str = "2024-01-07T14:51:28.033";
        //                  %Y-%m-%dT%H:%M:%S");
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.3f");
        println!("timestamp: {:?}", timestamp);

        let date_str = "2024-01-07T14:51:28.033+0000";
        //                  %Y-%m-%dT%H:%M:%S");
        let timestamp = DateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S%.3f%z");
        println!("timestamp: {:?}", timestamp);

        let dt = DateTime::parse_from_str(
            "1983 Apr 13 12:09:14.274 +0000", "%Y %b %d %H:%M:%S%.3f %z");

        println!("dt: {:?}", dt);

    }
}

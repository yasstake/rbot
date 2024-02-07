// Copyright(c) 2022-2023. yasstake. All rights reserved.
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

use chrono::Datelike;
// use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use csv::StringRecord;

use futures::StreamExt;
use polars_core::export::num::FromPrimitive;

use std::sync::{Arc, Mutex, RwLock};

use std::thread::{sleep, JoinHandle};
use std::time::Duration;

use rbot_lib::common::{
    convert_klines_to_trades, flush_log, time_string, to_naive_datetime, AccountStatus, BoardItem, BoardTransfer, LogStatus, MarketConfig, MarketMessage, MarketStream, MicroSec, MultiMarketMessage, Order, OrderBook, OrderBookRaw, OrderSide, OrderStatus, OrderType, Trade, DAYS, FLOOR_DAY, HHMM, NOW, RUNTIME, SEC
};

use rbot_lib::common::BLOCK_ON;

use rbot_lib::db::{db_full_path, TradeTable, TradeTableDb, KEY};

use rbot_lib::net::{latest_archive_date, RestApi, UdpSender};

use rbot_market::MarketImpl;
use rbot_market::{MarketInterface, OrderInterface, OrderInterfaceImpl};

use crate::message::BybitUserWsMessage;

use crate::rest::BybitRestApi;
use crate::ws::{BybitPublicWsClient, BybitWsOpMessage};

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::BybitServerConfig;

use super::message::BybitOrderStatus;
use super::message::{BybitAccountInformation, BybitPublicWsMessage};

use anyhow::Context;

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

    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        let (bids, asks) = self.board.get_board_vec().unwrap();

        Ok((bids, asks))
    }

    fn get_board_json(&self, size: usize) -> anyhow::Result<String> {
        let json = self.board.get_json(size).unwrap();

        Ok(json)
    }

    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        let (mut bids, mut asks) = self.board.get_board()?;

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

    fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)> {
        Ok(self.board.get_edge_price())
    }

    pub fn update(&mut self, board: &OrderBookRaw) {
        let bids = board.get_bids();
        let asks = board.get_asks();
        self.board.update(&bids, &asks, board.snapshot);
    }

    fn reflesh_board(&mut self) {
        // TODO: implement
        println!("reflesh board :NOT IMPLEMENTED");
        // TODO: reflesh board from rest api
    }
}

#[pyclass]
#[derive(Debug)]
pub struct Bybit {
    enable_order: bool,
    server_config: BybitServerConfig,
}

#[pymethods]
impl Bybit {
    #[new]
    #[pyo3(signature = (production=false))]
    pub fn new(production: bool) -> Self {
        let server_config = BybitServerConfig::new(production);

        return Bybit {
            enable_order: false,
            server_config: server_config.clone(),
        };
    }

    pub fn open_market(&self, config: &MarketConfig) -> BybitMarket {
        return BybitMarket::new(&self.server_config, config);
    }

    //--- OrderInterfaceImpl ----
    #[setter]
    pub fn set_enable_order_with_my_own_risk(&mut self, enable_order: bool) {
        OrderInterfaceImpl::set_enable_order_feature(self, enable_order)
    }
    #[getter]
    pub fn get_enable_order_with_my_own_risk(&self) -> bool {
        OrderInterfaceImpl::get_enable_order_feature(self)
    }

    #[pyo3(signature = (market_config, *, side, price, size, client_order_id=None))]
    pub fn limit_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        OrderInterfaceImpl::limit_order(self, market_config, side, price, size, client_order_id)
    }

    #[pyo3(signature = (market_config, *, side, size, client_order_id=None))]
    pub fn market_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        OrderInterfaceImpl::market_order(self, market_config, side, size, client_order_id)
    }

    #[pyo3(signature = (market_config, *, order_id))]
    pub fn cancel_order(
        &self,
        market_config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order> {
        OrderInterfaceImpl::cancel_order(self, market_config, order_id)
    }

    #[pyo3(signature = (market_config))]
    pub fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        OrderInterfaceImpl::get_open_orders(self, market_config)
    }

    #[pyo3(signature = (market_config))]
    // TODO: implement and test
    pub fn get_account(&self, market_config: &MarketConfig) -> anyhow::Result<AccountStatus> {
        OrderInterfaceImpl::get_account(self, market_config)
    }
}

impl OrderInterfaceImpl<BybitRestApi, BybitServerConfig> for Bybit {
    fn set_enable_order_feature(&mut self, enable_order: bool) {
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        self.enable_order
    }

    fn get_server_config(&self) -> &BybitServerConfig {
        &self.server_config
    }

    fn start_user_stream(&mut self) {
        todo!()
    }
}

#[derive(Debug)]
#[pyclass]
pub struct BybitMarket {
    pub server_config: BybitServerConfig,
    pub config: MarketConfig,
    pub db: Arc<Mutex<TradeTable>>,
    pub board: Arc<RwLock<OrderBook>>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
    // pub user_ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage>,
    // pub user_handler: Option<tokio::task::JoinHandle<()>>,
    // pub agent_channel: Arc<RwLock<MultiChannel<MarketMessage>>>,
    // pub broadcast_message: bool,
    // pub udp_sender: Option<Arc<Mutex<UdpSender>>>,
}

#[pymethods]
impl BybitMarket {
    #[new]
    pub fn new(server_config: &BybitServerConfig, config: &MarketConfig) -> Self {
        log::debug!("open market BybitMarket::new");
        BLOCK_ON(async { Self::async_new(server_config, config).await })
    }

    fn get_config(&self) -> MarketConfig {
        MarketImpl::get_config(self)
    }

    fn get_exchange_name(&self) -> String {
        MarketImpl::get_exchange_name(self)
    }

    fn get_trade_category(&self) -> String {
        MarketImpl::get_trade_category(self)
    }

    fn get_trade_symbol(&self) -> String {
        MarketImpl::get_trade_symbol(self)
    }

    fn drop_table(&mut self) -> anyhow::Result<()> {
        MarketImpl::drop_table(self)
    }

    fn get_cache_duration(&self) -> MicroSec {
        MarketImpl::get_cache_duration(self)
    }

    fn reset_cache_duration(&mut self) {
        MarketImpl::reset_cache_duration(self)
    }

    fn stop_db_thread(&mut self) {
        MarketImpl::stop_db_thread(self)
    }

    fn cache_all_data(&mut self) -> anyhow::Result<()> {
        MarketImpl::cache_all_data(self)
    }

    fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_trades(self, start_time, end_time)
    }

    fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::ohlcvv(self, start_time, end_time, window_sec)
    }

    fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::ohlcv(self, start_time, end_time, window_sec)
    }

    fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::vap(self, start_time, end_time, price_unit)
    }

    fn info(&mut self) -> String {
        MarketImpl::info(self)
    }

    fn get_board_json(&self, size: usize) -> anyhow::Result<String> {
        MarketImpl::get_board_json(self, size)
    }

    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        MarketImpl::get_board(self)
    }

    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        MarketImpl::get_board_vec(self)
    }

    fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)> {
        MarketImpl::get_edge_price(self)
    }

    fn get_file_name(&self) -> String {
        MarketImpl::get_file_name(self)
    }

    fn get_market_config(&self) -> MarketConfig {
        MarketImpl::get_market_config(self)
    }

    fn get_running(&self) -> bool {
        MarketImpl::get_running(self)
    }

    fn vacuum(&self) {
        MarketImpl::vacuum(self)
    }

    fn _repr_html_(&self) -> String {
        MarketImpl::_repr_html_(self)
    }

    fn download_archive(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64> {
        MarketImpl::download_archives(self, ndays, force, verbose, low_priority)
    }

    fn expire_unfix_data(&mut self) -> anyhow::Result<()> {
        MarketImpl::expire_unfix_data(self)
    }

    fn find_latest_gap(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        MarketImpl::find_latest_gap(self)
    }

    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        MarketImpl::download_latest(self, verbose)
    }

    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64> {
        MarketImpl::download_gap(self, verbose)
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        MarketImpl::start_market_stream(self)
    }

    fn get_trade_list(&self) -> anyhow::Result<Vec<OrderStatus>> {
        todo!()
        //MarketImpl::get_trade_list(self)
    }

    fn get_account(&self) -> anyhow::Result<AccountStatus> {
        todo!()
        //MarketImpl::get_account(self)
    }

    fn get_recent_trades(&self) -> anyhow::Result<Vec<Trade>> {
        todo!()
        //MarketImpl::get_recent_trades(self)
    }

    fn open_realtime_channel(&mut self) -> anyhow::Result<MarketStream> {
        MarketImpl::open_realtime_channel(self)
    }

    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<MarketStream> {
        MarketImpl::open_backtest_channel(self, time_from, time_to)
    }
}

impl BybitMarket {
    pub async fn async_new(server_config: &BybitServerConfig, config: &MarketConfig) -> Self {
        let db = TradeTable::open(&Self::make_db_path(
            &server_config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            &server_config.db_base_dir,
        ));

        if db.is_err() {
            log::error!("Error in TradeTable::open: {:?}", db);
        }

        let public_ws = BybitPublicWsClient::new(&server_config, &config).await;

        return BybitMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: Arc::new(Mutex::new(db.unwrap())),
            board: Arc::new(RwLock::new(OrderBook::new(&config))),
            public_handler: None,
        };
    }
}

impl MarketImpl<BybitRestApi, BybitServerConfig> for BybitMarket {
    fn get_config(&self) -> MarketConfig {
        self.config.clone()
    }

    fn get_exchange_name(&self) -> String {
        self.server_config.exchange_name.clone()
    }

    fn get_trade_category(&self) -> String {
        self.config.trade_category.clone()
    }

    fn get_trade_symbol(&self) -> String {
        self.config.trade_symbol.clone()
    }

    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async { self.async_download_lastest().await })
    }

    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64> {
        log::debug!("[start] download_gap ");
        let (unfix_start, unfix_end) = self.find_latest_gap()?;
        log::debug!("unfix_start: {:?}, unfix_end: {:?}", unfix_start, unfix_end);

        if unfix_end - unfix_start <= HHMM(0, 1) {
            log::info!("no need to download");
            return Ok(0);
        }

        let klines = BLOCK_ON(BybitRestApi::get_trade_klines(
            &self.server_config,
            &self.get_config(),
            unfix_start,
            unfix_end,
        ))?;

        let trades: Vec<Trade> = convert_klines_to_trades(klines);
        let rec = trades.len() as i64;

        let expire_message = TradeTableDb::expire_control_message(unfix_start, unfix_end);

        let mut lock = self.db.lock().unwrap();
        let tx = lock.start_thread();
        drop(lock);

        tx.send(expire_message)?;
        tx.send(trades)?;

        Ok(rec)
    }

    fn get_db(&self) -> Arc<Mutex<TradeTable>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.history_web_base.clone()
    }

    fn open_udp(&mut self) -> Arc<Mutex<UdpSender>> {
        todo!()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    fn reflesh_order_book(&mut self) {
        todo!()
    }

    fn get_market_config(&self) -> MarketConfig {
        self.config.clone()
    }

    fn get_server_config(&self) -> BybitServerConfig {
        todo!()
    }

    fn open_realtime_channel(&mut self) -> anyhow::Result<MarketStream> {
        todo!()
    }

    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<MarketStream> {
        todo!()
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async { self.async_start_market_stream().await })
    }
}

impl BybitMarket {
    async fn async_download_lastest(&mut self) -> anyhow::Result<i64> {
        let trades = BybitRestApi::get_recent_trades(&self.server_config, &self.config).await?;
        let rec = trades.len() as i64;

        let tx = self.db.lock().unwrap().start_thread();
        tx.send(trades)?;

        Ok(rec)
    }

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()> {
        if self.public_handler.is_some() {
            log::info!("market stream is already running.");
            return Ok(());
        }

        let mut lock = self.db.lock().unwrap();
        let db_channel = lock.start_thread();
        drop(lock);

        let mut orderbook = self.board.clone();

        let server_config = self.server_config.clone();
        let config = self.config.clone();

        self.public_handler = Some(tokio::task::spawn(async move {
            let mut public_ws = BybitPublicWsClient::new(&server_config, &config).await;

            public_ws.connect().await;
            let mut ws_stream = public_ws.open_stream().await;    
            let mut ws_stream = Box::pin(ws_stream);

            loop {
                let message = ws_stream.next().await;
                if message.is_none() {
                    log::error!("Error in ws_stream.recv: {:?}", message);
                    continue;
                }

                let message = message.unwrap();

                if message.is_err() {
                    log::error!("Error in ws_stream.recv: {:?}", message);
                    continue;
                }

                let messages = message.unwrap();

                match messages {
                    MultiMarketMessage::Trade(trade) => {
                        log::debug!("Trade: {:?}", trade);
                        let r = db_channel.send(trade);

                        if r.is_err() {
                            log::error!("Error in db_channel.send: {:?}", r);
                        }
                    }
                    MultiMarketMessage::Orderbook(ob) => {
                        let board = BoardTransfer::from_orderbook(&ob);
                        let snapshot = ob.snapshot;

                        let mut b = orderbook.write().unwrap();
                        b.update(&board.bids, &board.asks, snapshot);
                        drop(b);
                    }
                    _ => {
                        log::warn!("Not implemented/unexpected message: {:?}", messages);
                    } /*
                      MultiMarketMessage::Order(_) => todo!(),
                      MultiMarketMessage::Account(_) => todo!(),
                      MultiMarketMessage::Message(_) => todo!(),
                      MultiMarketMessage::Control(_) => todo!(),
                      */
                }
            }
        }));

        Ok(())
    }
}

/*
//----------- FOR BACKUP --------------------------------------------
//#[derive(Debug)]
#[pyclass]
pub struct _BybitMarket {
    pub server_config: BybitServerConfig,
    pub config: MarketConfig,
    pub db: TradeTable,
    pub board: Arc<RwLock<BybitOrderBook>>,
    pub public_ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
    pub user_handler: Option<tokio::task::JoinHandle<()>>,
    pub agent_channel: Arc<RwLock<MultiChannel<MarketMessage>>>,
    pub broadcast_message: bool,
    pub udp_sender: Option<Arc<Mutex<UdpSender>>>,
}

#[pymethods]
impl _BybitMarket {
    #[setter]
    pub fn set_broadcast_message(&mut self, broadcast_message: bool) {
        self.broadcast_message = broadcast_message;
        self.open_udp();
    }



    pub fn start_market_stream(&mut self) {
        RUNTIME.block_on(async move { self._start_market_stream().await })
    }

    pub fn start_user_stream(&mut self) {
        let agent_channel = self.agent_channel.clone();

        let server_config = self.server_config.clone();
        let market_config = self.config.clone();

        let udp_sender = self.udp_sender.clone();

        self.user_handler = Some(listen_userdata_stream(
            &server_config,
            move |message: BybitUserStreamMessage| {
                log::debug!("UserStream: {:?}", message);

                let ms = message.convert_to_market_message(&market_config);

                for m in ms {
                    if udp_sender.is_some() {
                        let sender = udp_sender.as_ref().unwrap().as_ref();
                        sender.lock().unwrap().send_market_message(&m);
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


impl _BybitMarket {
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

        return _BybitMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: db.unwrap(),
            board: Arc::new(RwLock::new(BybitOrderBook::new(config))),
            public_ws: WebSocketClient::new(
                &server_config,
                &format!("{}/{}", &server_config.public_ws, config.trade_category),
                config.public_subscribe_channel.clone(),
                PING_INTERVAL_SEC,
                SWITCH_INTERVAL_SEC,
                SYNC_RECORDS,
                None,
            ),
            public_handler: None,
            user_handler: None,
            agent_channel: Arc::new(RwLock::new(MultiChannel::new())),
            broadcast_message: false,
            udp_sender: None,
        };
    }

    pub fn open_udp(&mut self) {
        if !self.broadcast_message {
            return;
        }

        self.udp_sender = Some(Arc::new(Mutex::new(UdpSender::open(
            &self.server_config.exchange_name,
            &self.config.trade_category,
            &self.config.trade_symbol,
        ))));
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

    pub async fn _start_market_stream(&mut self) {
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

                log::debug!(
                    "db has gap, so delete after FIX data now={:?} / db_end={:?}",
                    time_string(now),
                    time_string(rest_start_time)
                );

                let delete_message = self.db.connection.make_expire_control_message(now);

                log::debug!("delete_message: {:?}", delete_message);

                db_channel.send(delete_message).unwrap();
            } else {
                log::debug!(
                    "db has no gap, so continue to receive data {}",
                    time_string(rest_start_time)
                );
            }

            db_channel.send(trades).unwrap();
        }

        let agent_channel = self.agent_channel.clone();
        let ws_channel = self.public_ws._open_channel().await;

        self.public_ws
            ._connect(|message| {
                let m = serde_json::from_str::<BybitWsMessage>(&message);

                if m.is_err() {
                    log::warn!("Error in serde_json::from_str: {:?}", message);
                    println!("ERR: {:?}", message);
                }

                let m = m.unwrap();

                return m.into();
            })
            .await;

        let board = self.board.clone();
        let db_channel_for_after = db_channel.clone();

        let udp_sender = self.udp_sender.clone();

        let handler = tokio::task::spawn(async move {
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
                            let sender = udp_sender.as_ref().unwrap().as_ref();
                            sender.lock().unwrap().send_market_message(&m);
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
        sleep(Duration::from_millis(100)); // TODO: fix to wait for channel open
        let trade = self.get_recent_trades();
        db_channel_for_after.send(trade).unwrap();

        // TODO: store recent trade timestamp.

        log::info!("start_market_stream");
    }

    fn history_web_url(&self, date: MicroSec) -> String {
        Self::make_historical_data_url_timestamp(
            &self.server_config.history_web_base,
            &self.config.trade_symbol,
            date,
        )
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
        common::{time_string, DAYS, NOW},
        exchange::bybit::config::{BybitConfig, BybitServerConfig},
    };

    #[test]
    fn test_make_historical_data_url_timestamp() {
        let config = BybitConfig::BTCUSDT();

        let url = super::_BybitMarket::make_historical_data_url_timestamp(
            "https://public.bybit.com/trading/",
            &config.trade_symbol,
            1620000000000_000,
        );

        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2021-05-03.csv.gz"
        );

        let url = super::_BybitMarket::make_historical_data_url_timestamp(
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

        let market = super::_BybitMarket::new(&server_config, &config);

        let date = market.get_latest_archive_date().unwrap();
        println!("date: {}", date);
        println!("date: {}", time_string(date));
    }

    #[test]
    fn test_rec_to_trade() {
        let rec = "1692748800.279,BTCUSD,Sell,641,26027.00,ZeroMinusTick,80253109-efbb-58ca-9adc-d458b66201e9,2.462827064202559e+06,641,0.02462827064202559".to_string();
        let rec = rec.split(',').collect::<Vec<&str>>();
        let rec = StringRecord::from(rec);
        let trade = super::_BybitMarket::rec_to_trade(&rec);

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
    fn test_lastest_fix_time() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let mut market = super::BybitMarket::new(&server_config, &config);

        let start_time = NOW() - DAYS(2);

        let fix_time = market.db.connection.latest_fix_time(start_time);

        println!("fix_time: {:?}/{:?}", time_string(fix_time), fix_time);

        let unfix_time = market.db.connection.first_unfix_time(fix_time);
        println!("unfix_time: {:?}/{:?}", time_string(unfix_time), unfix_time);
    }
}
*/

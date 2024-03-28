// Copyright(c) 2022-2024. yasstake. All rights reserved.
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

use chrono::Datelike;
// use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use csv::StringRecord;

use futures::StreamExt;
use polars::export::num::FromPrimitive;
use pyo3::ffi::getter;

use std::sync::{Arc, Mutex, RwLock};

use std::thread::sleep;
use std::time::Duration;

use rbot_lib::common::{
    convert_klines_to_trades, flush_log, time_string, to_naive_datetime, AccountCoins, AccountPair,
    BoardItem, BoardTransfer, LogStatus, MarketConfig, MarketMessage, MarketStream, MicroSec,
    MultiMarketMessage, Order, OrderBook, OrderBookRaw, OrderSide, OrderStatus, OrderType,
    ServerConfig, Trade, DAYS, FLOOR_DAY, HHMM, MARKET_HUB, NOW, SEC,
};

use rbot_lib::db::{db_full_path, TradeTable, TradeTableDb, KEY};
use rbot_lib::net::{latest_archive_date, BroadcastMessage, RestApi, UdpSender};

use rbot_market::MarketImpl;
use rbot_market::{MarketInterface, OrderInterface, OrderInterfaceImpl};

use crate::market;
use crate::message::BybitUserWsMessage;

use crate::rest::BybitRestApi;
use crate::ws::{BybitPrivateWsClient, BybitPublicWsClient, BybitWsOpMessage};

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::config::BybitServerConfig;

use super::message::BybitOrderStatus;
use super::message::{BybitAccountInformation, BybitPublicWsMessage};

use anyhow::Context;

use rbot_blockon::BLOCK_ON;

use tokio::task::JoinHandle;

pub const BYBIT: &str = "BYBIT";

#[pyclass]
#[derive(Debug)]
pub struct Bybit {
    test_net: bool,
    enable_order: bool,
    server_config: BybitServerConfig,
    user_handler: Option<JoinHandle<()>>,
}

#[pymethods]
impl Bybit {
    #[new]
    #[pyo3(signature = (production=false))]
    pub fn new(production: bool) -> Self {
        let server_config = BybitServerConfig::new(production);

        return Bybit {
            test_net: !production,
            enable_order: false,
            server_config: server_config,
            user_handler: None,
        };
    }

    #[getter]
    fn get_production(&self) -> bool {
        self.server_config.production
    }

    pub fn open_market(&self, config: &MarketConfig) -> BybitMarket {
        return BybitMarket::new(&self.server_config, config, self.test_net);
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

    pub fn limit_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async {
            OrderInterfaceImpl::limit_order(self, market_config, side, price, size, client_order_id)
                .await
        })
    }

    pub fn market_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async {
            OrderInterfaceImpl::market_order(self, market_config, side, size, client_order_id).await
        })
    }

    pub fn cancel_order(
        &self,
        market_config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order> {
        BLOCK_ON(async { OrderInterfaceImpl::cancel_order(self, market_config, order_id).await })
    }

    pub fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async { OrderInterfaceImpl::get_open_orders(self, market_config).await })
    }

    #[getter]
    pub fn get_account(&self) -> anyhow::Result<AccountCoins> {
        BLOCK_ON(async { OrderInterfaceImpl::get_account(self).await })
    }

    pub fn start_user_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async { OrderInterfaceImpl::async_start_user_stream(self).await })
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

    async fn async_start_user_stream(&mut self) -> anyhow::Result<()> {
        let exchange_name = BYBIT.to_string();
        let server_config = self.server_config.clone();

        self.user_handler = Some(tokio::task::spawn(async move {
            let mut ws = BybitPrivateWsClient::new(&server_config).await;
            ws.connect().await;

            let mut market_channel = MARKET_HUB.open_channel();
            let mut ws_stream = Box::pin(ws.open_stream().await);

            while let Some(message) = ws_stream.next().await {
                if message.is_err() {
                    log::error!("Error in ws_stream.recv: {:?}", message);
                    continue;
                }

                let message = message.unwrap();
                match message {
                    MultiMarketMessage::Order(order) => {
                        for o in order {
                            market_channel.send(BroadcastMessage {
                                exchange: exchange_name.clone(),
                                category: o.category.clone(),
                                symbol: o.symbol.clone(),
                                msg: MarketMessage::Order(o.clone()),
                            });
                            log::debug!("Order: {:?}", o);
                        }
                    }
                    MultiMarketMessage::Account(account) => {
                        market_channel.send(BroadcastMessage {
                            exchange: exchange_name.clone(),
                            category: "".to_string(),
                            symbol: "".to_string(),
                            msg: MarketMessage::Account(account.clone()),
                        });
                    }
                    _ => {
                        log::info!("User stream message: {:?}", message);
                    }
                }
            }
        }));

        Ok(())
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
    pub fn new(server_config: &BybitServerConfig, config: &MarketConfig, test_net: bool) -> Self {
        log::debug!("open market BybitMarket::new");
        BLOCK_ON(async { 
            Self::async_new(server_config, config, test_net).await.unwrap() 
        })
    }
    #[getter]
    fn get_config(&self) -> MarketConfig {
        MarketImpl::get_config(self)
    }
    #[getter]
    fn get_exchange_name(&self) -> String {
        MarketImpl::get_exchange_name(self)
    }
    #[getter]
    fn get_trade_category(&self) -> String {
        MarketImpl::get_trade_category(self)
    }

    #[getter]
    fn get_trade_symbol(&self) -> String {
        MarketImpl::get_trade_symbol(self)
    }

    fn drop_table(&mut self) -> anyhow::Result<()> {
        MarketImpl::drop_table(self)
    }

    #[getter]
    fn get_cache_duration(&self) -> MicroSec {
        MarketImpl::get_cache_duration(self)
    }

    fn reset_cache_duration(&mut self) {
        MarketImpl::reset_cache_duration(self)
    }

    /*
    fn stop_db_thread(&mut self) {
        MarketImpl::stop_db_thread(self)
    }
    */

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

    #[getter]
    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        MarketImpl::get_board(self)
    }

    #[getter]
    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        MarketImpl::get_board_vec(self)
    }

    #[getter]
    fn get_edge_price(&mut self) -> anyhow::Result<(Decimal, Decimal)> {
        MarketImpl::get_edge_price(self)
    }

    #[getter]
    fn get_file_name(&self) -> String {
        MarketImpl::get_file_name(self)
    }

    #[getter]
    fn get_running(&self) -> bool {
        MarketImpl::get_running(self)
    }

    fn vacuum(&self) {
        MarketImpl::vacuum(self)
    }

    fn _repr_html_(&self) -> String {
        MarketImpl::_repr_html_(self)
    }

    #[pyo3(signature = (ndays, force=false, verbose=false, low_priority=true))]    
    fn download_archive(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64> {
            MarketImpl::download_archives(self, ndays, force, verbose, low_priority)
    }

    #[pyo3(signature = (verbose=false))]    
    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        MarketImpl::download_latest(self, verbose)
    }

    #[pyo3(signature = (force=false))]
    fn expire_unfix_data(&mut self, force: bool) -> anyhow::Result<()> {
        BLOCK_ON(async {
            self.async_expire_unfix_data(force).await
        })
    }

    fn find_latest_gap(&self, force: bool) -> anyhow::Result<(MicroSec, MicroSec)> {
        MarketImpl::find_latest_gap(self, force)
    }

    #[pyo3(signature = (force=false, verbose=false))]    
    fn download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64> {
        MarketImpl::download_gap(self, force, verbose)
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        MarketImpl::start_market_stream(self)
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
    pub async fn async_new(
        server_config: &BybitServerConfig,
        config: &MarketConfig,
        test_mode: bool,
    ) -> anyhow::Result<Self> {
        let db_path = TradeTable::make_db_path(
            &config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            test_mode    
        );

        let db = TradeTable::open(&db_path)
            .with_context(|| format!("Error in TradeTable::open: {:?}", db_path))?;

        let public_ws = BybitPublicWsClient::new(&server_config, &config).await;

        let mut market = BybitMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: Arc::new(Mutex::new(db)),
            board: Arc::new(RwLock::new(OrderBook::new(&config))),
            public_handler: None,
        };

        Ok(market)
    }
}

impl MarketImpl<BybitRestApi, BybitServerConfig> for BybitMarket {
    fn get_config(&self) -> MarketConfig {
        self.config.clone()
    }

    fn get_server_config(&self) -> BybitServerConfig {
        self.server_config.clone()
    }

    fn get_exchange_name(&self) -> String {
        self.config.exchange_name.clone()
    }

    fn get_trade_category(&self) -> String {
        self.config.trade_category.clone()
    }

    fn get_trade_symbol(&self) -> String {
        self.config.trade_symbol.clone()
    }

    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async { self.async_download_latest(verbose).await })
    }

    fn download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async { self.async_download_gap(force, verbose).await })
    }


    fn get_db(&self) -> Arc<Mutex<TradeTable>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.history_web_base.clone()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    fn reflesh_order_book(&mut self) -> anyhow::Result<()>{
        BLOCK_ON(async {
            self.async_reflesh_order_book().await
        })
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async {
            self.async_start_market_stream().await
        })
    }
    
    fn download_archives(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64> {
        BLOCK_ON(async {
            self.async_download_archives(ndays, force, verbose, low_priority).await
        })
    }

}

impl BybitMarket {
    /*
    async fn async_download_lastest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        if verbose {
            print!("async_download_lastest");
            flush_log();
        }

        let trades = BybitRestApi::get_recent_trades(&self.server_config, &self.config).await?;
        let rec = trades.len() as i64;

        if verbose {
            println!("rec: {}", rec);
            flush_log();
        }

        {
            log::debug!("get db");
            flush_log();
            let tx = self.db.lock();

            if tx.is_err() {
                log::error!("Error in self.db.lock: {:?}", tx);
                return Err(anyhow::anyhow!("Error in self.db.lock: {:?}", tx));
            }
            let mut lock = tx.unwrap();
            let tx = lock.start_thread();

            log::debug!("start thread done");
            tx.send(trades)?;
        }

        Ok(rec)
    }
    */

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()> {
        if self.public_handler.is_some() {
            log::info!("market stream is already running.");
            return Ok(());
        }

        let db_channel = {
            let mut lock = self.db.lock().unwrap();
            lock.start_thread().await
        };

        let orderbook = self.board.clone();

        let server_config = self.server_config.clone();
        let config = self.config.clone();

        let hub_channel = MARKET_HUB.open_channel();

        self.public_handler = Some(tokio::task::spawn(async move {
            let mut public_ws = BybitPublicWsClient::new(&server_config, &config).await;

            let exchange_name = config.exchange_name.clone();
            let trade_category = config.trade_category.clone();
            let trade_symbol = config.trade_symbol.clone();

            public_ws.connect().await;
            let ws_stream = public_ws.open_stream().await;
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
                        let r = db_channel.send(trade.clone());

                        if r.is_err() {
                            log::error!("Error in db_channel.send: {:?}", r);
                        }

                        for message in trade {
                            let r = hub_channel.send(BroadcastMessage {
                                exchange: exchange_name.clone(),
                                category: trade_category.clone(),
                                symbol: trade_symbol.clone(),
                                msg: MarketMessage::Trade(message),
                            });
                            if r.is_err() {
                                log::error!("Error in hub_channel.send: {:?}", r);
                            }
                        }
                    }
                    MultiMarketMessage::Orderbook(board) => {
                            let mut b = orderbook.write().unwrap();
                            b.update(&board);
                    }
                    MultiMarketMessage::Control(control) => {
                        // TODO: alert or recovery.
                        if control.status == false {
                            log::error!("Control message: {:?}", control);
                        }
                    }
                    _ => {
                        log::info!("Market stream message: {:?}", messages);
                    } 
                }
            }
        }));

        Ok(())
    }

    async fn async_download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64> {
        log::debug!("[start] download_gap ");
        let gap_result = self.find_latest_gap(force);
        if gap_result.is_err() {
            log::warn!("no gap found: {:?}", gap_result);

            if verbose {
                println!("no gap found: {:?}", gap_result);
                flush_log();
            }

            return Ok(0);
        }

        let (unfix_start, unfix_end) = gap_result?;


        //let (unfix_start, unfix_end) = self.find_latest_gap();
        log::debug!("unfix_start: {:?}, unfix_end: {:?}", unfix_start, unfix_end);

        if verbose {
            println!("unfix_start: {:?}({:?}), unfix_end: {:?}({:?})"
                , time_string(unfix_start), unfix_start, time_string(unfix_end), unfix_end);
            flush_log();
        }

        if unfix_end != 0 && unfix_end - unfix_start <= HHMM(0, 1) {
            log::info!("no need to download");
            return Ok(0);
        }

        let klines = BybitRestApi::get_trade_klines(
            &self.server_config,
            &self.get_config(),
            unfix_start,
            unfix_end,
        ).await?;

        let trades: Vec<Trade> = convert_klines_to_trades(klines);
        let rec = trades.len() as i64;

        let expire_message = TradeTableDb::expire_control_message(unfix_start, unfix_end, false);

        let tx = {
            let mut lock = self.db.lock().unwrap();
            lock.start_thread().await
        };

        tx.send(expire_message)?;
        tx.send(trades)?;

        if verbose {
            println!("rec: {}", rec);
            flush_log();
        }

        Ok(rec)
    }

    async fn async_reflesh_order_book(&mut self) -> anyhow::Result<()> {
        let board = BybitRestApi::get_board_snapshot(&self.server_config, &self.config).await?;
        let mut b = self.board.write().unwrap();
        b.update(&board);

        Ok(())
    }

}

#[cfg(test)]
mod bybit_test {
    use rust_decimal_macros::dec;

    use crate::{Bybit, BybitConfig};

    #[test]
    fn test_create() {
        let bybit = Bybit::new(false);

        assert!(bybit.enable_order == false);
    }

    #[test]
    fn test_open_market() {
        let market = Bybit::new(false).open_market(&BybitConfig::BTCUSDT());
        assert!(market.get_config().trade_symbol == "BTCUSDT");
    }

    #[test]
    fn test_limit_order() {
        let mut bybit = Bybit::new(false);
        let config = BybitConfig::BTCUSDT();

        let rec = bybit.limit_order(&config, "Buy", dec![45000.0], dec![0.001], None);
        println!("{:?}", rec);
        assert!(rec.is_err()); // first enable flag.

        bybit.set_enable_order_with_my_own_risk(true);
        let rec = bybit.limit_order(&config, "Buy", dec![45000.0], dec![0.001], None);
        println!("{:?}", rec);
        assert!(rec.is_ok()); // first enable flag.
    }

    #[test]
    fn test_market_order() {
        let mut bybit = Bybit::new(false);
        let config = BybitConfig::BTCUSDT();

        let rec = bybit.market_order(&config, "Buy", dec![0.001], None);
        println!("{:?}", rec);
        assert!(rec.is_err()); // first enable flag.

        bybit.set_enable_order_with_my_own_risk(true);
        let rec = bybit.market_order(&config, "Buy", dec![0.001], None);
        println!("{:?}", rec);
        assert!(rec.is_ok()); // first enable flag.
    }

    #[test]
    fn test_cancel_order() -> anyhow::Result<()> {
        let mut bybit = Bybit::new(false);
        let config = BybitConfig::BTCUSDT();

        bybit.set_enable_order_with_my_own_risk(true);
        let rec = bybit.limit_order(&config, "Buy", dec![45000.0], dec![0.001], None)?;

        let order_id = rec[0].order_id.clone();

        let rec = bybit.cancel_order(&config, &order_id)?;
        println!("{:?}", rec);

        Ok(())
    }

    #[test]
    fn test_get_open_orders() -> anyhow::Result<()> {
        let mut bybit = Bybit::new(false);
        let config = BybitConfig::BTCUSDT();

        let rec = bybit.get_open_orders(&config)?;
        println!("{:?}", rec);

        Ok(())
    }

    #[test]
    fn test_get_account() {
        let mut bybit = Bybit::new(false);
        let config = BybitConfig::BTCUSDT();

        let rec = bybit.get_account();
        println!("{:?}", rec);
        assert!(rec.is_ok());
    }
}

#[cfg(test)]
mod market_test {
    use crate::BybitConfig;

    #[test]
    fn test_create() {
        use super::*;
        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        let market = BybitMarket::new(&server_config, &market_config, true);
    }

    #[ignore]
    #[tokio::test]
    async fn test_download_archive() {
        use super::*;
        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        let mut market = BybitMarket::new(&server_config, &market_config, true);

        let rec = market.async_download_archives(1, true, true, false).await;
        assert!(rec.is_ok());
    }

    #[ignore]
    #[test]
    fn test_download_latest() {
        use super::*;
        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        let mut market = BybitMarket::new(&server_config, &market_config, true);

        let rec = market.download_latest(true);
        assert!(rec.is_ok());
    }
}


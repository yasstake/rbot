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

use rbot_lib::db::{db_full_path, TradeArchive, TradeDataFrame, TradeDb, KEY};
use rbot_lib::net::{latest_archive_date, BroadcastMessage, RestApi, RestPage, UdpSender};

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

use anyhow::anyhow;
use tokio::task::JoinHandle;

pub const BYBIT: &str = "BYBIT";

#[pyclass]
pub struct Bybit {
    production: bool,
    enable_order: bool,
    server_config: BybitServerConfig,
    user_handler: Option<JoinHandle<()>>,
    api: BybitRestApi,
}

#[pymethods]
impl Bybit {
    #[new]
    #[pyo3(signature = (production=false))]
    pub fn new(production: bool) -> Self {
        let server_config = BybitServerConfig::new(production);
        let api = BybitRestApi::new(&server_config);

        return Bybit {
            production: production,
            enable_order: false,
            server_config: server_config,
            user_handler: None,
            api: api,
        };
    }

    #[getter]
    fn get_production(&self) -> bool {
        self.server_config.production
    }

    pub fn open_market(&self, config: &MarketConfig) -> BybitMarket {
        return BybitMarket::new(&self.server_config, config, self.production);
    }

    //--- OrderInterfaceImpl ----
    #[setter]
    pub fn set_enable_order_with_my_own_risk(&mut self, enable_order: bool) {
        self.set_enable_order_feature(enable_order);
    }

    #[getter]
    pub fn get_enable_order_with_my_own_risk(&self) -> bool {
        self.get_enable_order_feature()
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

impl OrderInterfaceImpl<BybitRestApi> for Bybit {
    fn get_restapi(&self) -> &BybitRestApi {
        &self.api
    }

    fn set_enable_order_feature(&mut self, enable_order: bool) {
        log::debug!("set enable order {}", enable_order);
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        log::debug!("get enable order {}", self.enable_order);
        self.enable_order
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

#[pyclass]
pub struct BybitMarket {
    pub server_config: BybitServerConfig,
    pub api: BybitRestApi,
    pub config: MarketConfig,
    pub db: Arc<Mutex<TradeDataFrame>>,
    pub board: Arc<RwLock<OrderBook>>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
}

#[pymethods]
impl BybitMarket {
    #[new]
    pub fn new(server_config: &BybitServerConfig, config: &MarketConfig, production: bool) -> Self {
        log::debug!("open market BybitMarket::new");
        BLOCK_ON(async {
            Self::async_new(server_config, config, production)
                .await
                .unwrap()
        })
    }
    #[getter]
    fn get_config(&self) -> MarketConfig {
        MarketImpl::get_config(self)
    }

    #[getter]
    fn get_start_time(&mut self) -> MicroSec {
        MarketImpl::start_time(self)
    }

    #[getter]
    fn get_end_time(&mut self) -> MicroSec {
        MarketImpl::end_time(self)
    }

    #[getter]
    fn get_archive_info(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        MarketImpl::get_archive_info(self)
    }

    #[getter]
    fn get_db_info(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        MarketImpl::get_db_info(self)
    }

    fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_trades(self, start_time, end_time)
    }

    fn _select_db_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_db_trades(self, start_time, end_time)
    }

    fn _select_archive_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_archive_trades(self, start_time, end_time)
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

    fn _repr_html_(&self) -> String {
        MarketImpl::_repr_html_(self)
    }

    #[pyo3(signature = (ndays, *, connect_ws=false, force=false, force_archive=false, force_recent=false, verbose=false))]
    fn download(&mut self, ndays: i64, connect_ws: bool, force: bool, force_archive: bool, force_recent: bool, verbose: bool) -> anyhow::Result<()>
    {
        BLOCK_ON(async {
            MarketImpl::async_download(self, ndays, connect_ws, force, force_archive, force_recent, verbose).await
        })
    }

    #[pyo3(signature = (ndays, force=false, verbose=false))]
    fn _download_archive(&mut self, ndays: i64, force: bool, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async { MarketImpl::async_download_archive(self, ndays, force, verbose).await })
    }

    fn _download_realtime(
        &mut self,
        force: bool,
        connect_ws: bool,
        verbose: bool,
    ) -> anyhow::Result<()> {
        BLOCK_ON(async {
            MarketImpl::async_download_realtime(self, connect_ws, force, verbose).await
        })
    }

    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<(MicroSec, MicroSec, MarketStream)> {
        MarketImpl::open_backtest_channel(self, time_from, time_to)
    }

    fn vaccum(&self) -> anyhow::Result<()> {
        let mut lock = self.db.lock().unwrap();

        lock.vacuum()
    }

    fn _cache_all_data(&mut self) -> anyhow::Result<()> {
        MarketImpl::cache_all_data(self)
    }

    #[pyo3(signature = (verbose=false))]
    fn _download_latest(&mut self, verbose: bool) -> anyhow::Result<(i64, i64)> {
        log::debug!("BybitMarket.download_latest(verbose={}", verbose);

        MarketImpl::download_latest(self, verbose)
    }

    fn _get_cache_duration(&self) -> MicroSec {
        MarketImpl::get_cache_duration(self)
    }

    fn _reset_cache_duration(&mut self) {
        MarketImpl::reset_cache_duration(self)
    }


    fn _latest_db_rec(&self, search_before: MicroSec) -> anyhow::Result<Trade> {
        let search_before = if 0 < search_before {
            search_before
        } else {
            NOW() + DAYS(1) // search from future
        };

        MarketImpl::latest_db_rec(self, search_before)
    }

    fn _last_db_sequence_start_rec(&self) -> PyResult<Py<PyAny>> {
        MarketImpl::db_start_up_rec(self)
    }

    fn _download_range(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        BLOCK_ON(async {
            MarketImpl::async_download_range(self, start_time, end_time, verbose).await
        })
    }
}

impl BybitMarket {
    pub async fn async_new(
        server_config: &BybitServerConfig,
        config: &MarketConfig,
        production: bool,
    ) -> anyhow::Result<Self> {
        let db = TradeDataFrame::get(config, server_config.production)
            .with_context(|| format!("Error in TradeTable::open: {:?}", config))?;

        let public_ws = BybitPublicWsClient::new(&server_config, &config).await;

        let mut market = BybitMarket {
            server_config: server_config.clone(),
            api: BybitRestApi::new(server_config),
            config: config.clone(),
            db: db,
            board: Arc::new(RwLock::new(OrderBook::new(&config))),
            public_handler: None,
        };

        Ok(market)
    }
}

impl MarketImpl<BybitRestApi> for BybitMarket {
    fn get_restapi(&self) -> &BybitRestApi {
        &self.api
    }

    fn get_config(&self) -> MarketConfig {
        self.config.clone()
    }

    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<(i64, i64)> {
        log::debug!("download latest(verbose={})", verbose);

        BLOCK_ON(async {
            log::debug!("download latest");
            self.async_download_latest(verbose).await
        })
    }

    fn get_db(&self) -> Arc<Mutex<TradeDataFrame>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.history_web_base.clone()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    fn refresh_order_book(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async { self.async_refresh_order_book().await })
    }

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()> {
        if self.public_handler.is_some() {
            log::info!("market stream is already running.");
            return Ok(());
        }

        let db_channel = {
            let mut lock = self.db.lock().unwrap();
            lock.open_channel()?
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

}

impl BybitMarket {

    async fn async_refresh_order_book(&mut self) -> anyhow::Result<()> {
        let api = self.get_restapi();
        let board = api.get_board_snapshot(&self.config).await?;
        let mut b = self.board.write().unwrap();
        b.update(&board);

        Ok(())
    }
}

#[cfg(test)]
mod bybit_test {
    use rbot_lib::common::init_debug_log;
    use rbot_market::OrderInterfaceImpl;
    use rust_decimal_macros::dec;

    use crate::{Bybit, BybitConfig};

    #[test]
    fn test_create() {
        init_debug_log();
        let mut bybit = Bybit::new(false);
        assert_eq!(bybit.get_enable_order_feature(), false);
        
        bybit.set_enable_order_feature(true);        
        assert_eq!(bybit.get_enable_order_feature(), true);
    }

    #[test]
    fn test_open_market() {
        let market = Bybit::new(false).open_market(&BybitConfig::BTCUSDT());
        assert!(market.get_config().trade_symbol == "BTCUSDT");
    }

    #[test]
    fn test_limit_order() {
        init_debug_log();
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

        init_debug_log();

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
    use rbot_lib::common::init_debug_log;

    use crate::BybitConfig;

    #[test]
    fn test_create() {
        use super::*;

        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        let market = BybitMarket::new(&server_config, &market_config, true);
    }

    #[ignore]
    #[test]
    fn test_download_archive() {
        use super::*;
        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        init_debug_log();
        let mut market = BybitMarket::new(&server_config, &market_config, true);

        let rec = market._download_archive(3, false, true);
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

    #[test]
    fn test_download_range() {
        use super::*;

        init_debug_log();

        let server_config = BybitServerConfig::new(false);
        let market_config = BybitConfig::BTCUSDT();

        let mut market = BybitMarket::new(&server_config, &market_config, true);

        market._download_range(0, 0, true);
    }
}

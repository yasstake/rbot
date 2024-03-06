// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::sync::{Arc, Mutex, RwLock};

use anyhow::Context;
use futures::StreamExt;
use pyo3_polars::PyDataFrame;
use rbot_blockon::BLOCK_ON;
use rbot_lib::common::flush_log;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::BoardItem;
use rbot_lib::common::MarketConfig;
use rbot_lib::common::MarketMessage;
use rbot_lib::common::MarketStream;
use rbot_lib::common::MultiMarketMessage;
use rbot_lib::common::Order;
use rbot_lib::common::OrderBook;
use rbot_lib::common::MARKET_HUB;
use rbot_lib::common::MicroSec;
use rbot_lib::db::TradeTable;
use rbot_lib::net::BroadcastMessage;
use rust_decimal::Decimal;
// Copyright(c) 2022-2024. yasstake. All rights reserved.
use tokio::task::JoinHandle;

// use rbot_market::OrderInterface;
use rbot_market::OrderInterfaceImpl;
use rbot_market::MarketImpl;
// use rbot_market::MarketInterface;

use crate::BinancePrivateWsClient;
use crate::BinancePublicWsClient;
use crate::BinanceRestApi;
use crate::BinanceServerConfig;

use pyo3::prelude::*;


#[pyclass]
pub struct Binance {
    enable_order: bool,
    server_config: BinanceServerConfig,
    user_handler: Option<JoinHandle<()>>,
}

#[pymethods]
impl 
// OrderInterface for
Binance {
    #[new]
    pub fn new(production: bool) -> Self {
        let server_config = BinanceServerConfig::new(production);

        Self {
            enable_order: false,
            server_config: server_config,
            user_handler: None,
        }
    }


    #[getter]
    pub fn get_exchange_name(&self) -> String {
        self.server_config.exchange_name.clone()
    }

    pub fn open_market(&self, config: &MarketConfig) -> BinanceMarket {
        return BinanceMarket::new(&self.server_config, config);
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


impl OrderInterfaceImpl<BinanceRestApi, BinanceServerConfig> for Binance {
    fn set_enable_order_feature(&mut self, enable_order: bool) {
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        self.enable_order
    }

    fn get_server_config(&self) -> &BinanceServerConfig {
        &self.server_config
    }

    async fn async_start_user_stream(&mut self) -> anyhow::Result<()> {

        let exchange_name = self.server_config.exchange_name.clone();
        let server_config = self.server_config.clone();

        self.user_handler = Some(tokio::task::spawn(async move {
            let mut ws = BinancePrivateWsClient::new(&server_config).await;
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
                            let _ =  market_channel.send(BroadcastMessage {
                                exchange: exchange_name.clone(),
                                category: o.category.clone(),
                                symbol: o.symbol.clone(),
                                msg: MarketMessage::Order(o.clone()),
                            });
                            log::debug!("Order: {:?}", o);
                        }
                    }
                    MultiMarketMessage::Account(account) => {
                        let _ = market_channel.send(BroadcastMessage {
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
pub struct BinanceMarket {
    server_config: BinanceServerConfig,
    config: MarketConfig,
    pub db: Arc<Mutex<TradeTable>>,
    pub board: Arc<RwLock<OrderBook>>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
}

#[pymethods]
impl BinanceMarket {
    #[new]
    pub fn new(server_config: &BinanceServerConfig, config: &MarketConfig) -> Self {
        log::debug!("open market BybitMarket::new");
        BLOCK_ON(async { Self::async_new(server_config, config).await.unwrap() })
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

    #[getter]
    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        MarketImpl::get_board(self)
    }

    #[getter]
    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        MarketImpl::get_board_vec(self)
    }

    #[getter]
    fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)> {
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

    #[pyo3(signature = (ndays, force=false, verbose=true, low_priority=true))]
    fn download_archive(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64> {
        BLOCK_ON(async {
            MarketImpl::async_download_archives(self, ndays, force, verbose, low_priority).await
        })
    }

    #[pyo3(signature = (verbose=true))]
    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async { self.async_download_lastest(verbose).await })
    }

    fn expire_unfix_data(&mut self) -> anyhow::Result<()> {
        MarketImpl::expire_unfix_data(self)
    }

    fn find_latest_gap(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        MarketImpl::find_latest_gap(self)
    }

    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64> {
        MarketImpl::download_gap(self, verbose)
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        MarketImpl::start_market_stream(self)
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


impl MarketImpl<BinanceRestApi, BinanceServerConfig> for BinanceMarket {
    fn get_server_config(&self) -> BinanceServerConfig {
        self.server_config.clone()
    }

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
        todo!()
    }

    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64> {
        todo!()
    }

    fn get_db(&self) -> Arc<Mutex<TradeTable>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.history_web_base.clone()
    }

    fn get_latest_archive_date(&self) -> anyhow::Result<rbot_lib::common::MicroSec> {
        todo!()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    fn reflesh_order_book(&mut self) {
        todo!()
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        todo!()
    }

    fn open_realtime_channel(&mut self) -> anyhow::Result<rbot_lib::common::MarketStream> {
        todo!()
    }

    fn open_backtest_channel(
        &mut self,
        time_from: rbot_lib::common::MicroSec,
        time_to: rbot_lib::common::MicroSec,
    ) -> anyhow::Result<rbot_lib::common::MarketStream> {
        todo!()
    }
}


impl BinanceMarket {
    async fn async_new(server_config: &BinanceServerConfig, config: &MarketConfig) -> anyhow::Result<Self> {
        let db_path = Self::make_db_path(
            &server_config.exchange_name,
            &config.trade_category,
            &config.trade_symbol,
            &server_config.db_base_dir,
        );

        let db = TradeTable::open(&db_path)
              .with_context(|| format!("Error in TradeTable::open: {:?}", db_path))?;

        let public_ws = BinancePublicWsClient::new(&server_config, &config).await;

        let mut market = BinanceMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: Arc::new(Mutex::new(db)),
            board: Arc::new(RwLock::new(OrderBook::new(server_config, &config))),            
            public_handler: None,
        };

        market.cache_all_data().with_context(|| format!("Error in cache_all_data"))?;

        Ok(market)
    }

    async fn async_download_lastest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        if verbose {
            println!("async_download_lastest");
            flush_log();
        }

        Ok(0)
    }

}

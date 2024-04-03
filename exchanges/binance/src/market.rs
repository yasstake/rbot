// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::sync::{Arc, Mutex, RwLock};
use std::thread::sleep;

use anyhow::Context;
use futures::StreamExt;
use pyo3_polars::PyDataFrame;
use rbot_blockon::BLOCK_ON;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::BoardItem;
use rbot_lib::common::BoardTransfer;
use rbot_lib::common::MarketConfig;
use rbot_lib::common::MarketMessage;
use rbot_lib::common::MarketStream;
use rbot_lib::common::MicroSec;
use rbot_lib::common::MultiMarketMessage;
use rbot_lib::common::Order;
use rbot_lib::common::OrderBook;
use rbot_lib::common::MARKET_HUB;
use rbot_lib::common::{flush_log, LogStatus};
use rbot_lib::common::{time_string, NOW};
use rbot_lib::db::TradeTable;
use rbot_lib::net::{BroadcastMessage, RestApi};
use rust_decimal::Decimal;
// Copyright(c) 2022-2024. yasstake. All rights reserved.
use tokio::task::JoinHandle;

// use rbot_market::OrderInterface;
use rbot_market::MarketImpl;
use rbot_market::OrderInterfaceImpl;
// use rbot_market::MarketInterface;

use crate::BinancePrivateWsClient;
use crate::BinancePublicWsClient;
use crate::BinanceRestApi;
use crate::BinanceServerConfig;

use pyo3::prelude::*;

pub const BINANCE:&str = "BINANCE";

#[pyclass]
pub struct Binance {
    enable_order: bool,
    server_config: BinanceServerConfig,
    user_handler: Option<JoinHandle<()>>,
}

#[pymethods]
impl Binance {
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
    fn get_production(&self) -> bool {
        self.server_config.production
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
        let exchange_name = BINANCE.to_string();
        let server_config = self.server_config.clone();

        self.user_handler = Some(tokio::task::spawn(async move {
            let mut ws = BinancePrivateWsClient::new(&server_config).await;
            ws.connect().await;

            let market_channel = MARKET_HUB.open_channel();
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
                            let _ = market_channel.send(BroadcastMessage {
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
        BLOCK_ON(async { 
            Self::async_new(server_config, config, ! server_config.production).await.unwrap() 
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
        if verbose {
            println!(
                "download_latest: {} {}",
                self.config.trade_category, self.config.trade_symbol
            );
            flush_log();
        }

        MarketImpl::download_latest(self, verbose)
    }

    #[pyo3(signature = (force=false))]
    fn expire_unfix_data(&mut self, force: bool) -> anyhow::Result<()> {
        BLOCK_ON(async { self.async_expire_unfix_data(force).await })
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

impl MarketImpl<BinanceRestApi, BinanceServerConfig> for BinanceMarket {
    fn get_server_config(&self) -> BinanceServerConfig {
        self.server_config.clone()
    }

    fn get_config(&self) -> MarketConfig {
        self.config.clone()
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

    fn get_db(&self) -> Arc<Mutex<TradeTable>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.history_web_base.clone()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    fn reflesh_order_book(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async { self.async_reflesh_order_book_self().await })
    }

    fn start_market_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async { self.async_start_market_stream().await })
    }

    fn download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64> {
        BLOCK_ON(async {
            log::debug!("download_gap");
            self.async_download_gap(force, verbose).await
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
            self.async_download_archives(ndays, force, verbose, low_priority)
                .await
        })
    }
}

impl BinanceMarket {
    async fn async_new(
        server_config: &BinanceServerConfig,
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

        // let public_ws = BinancePublicWsClient::new(&server_config, &config).await;

        let market = BinanceMarket {
            server_config: server_config.clone(),
            config: config.clone(),
            db: Arc::new(Mutex::new(db)),
            board: Arc::new(RwLock::new(OrderBook::new(&config))),
            public_handler: None,
        };

        Ok(market)
    }

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
            let mut public_ws = BinancePublicWsClient::new(&server_config, &config).await;

            let exchange_name = config.exchange_name.clone();
            let trade_category = config.trade_category.clone();
            let trade_symbol = config.trade_symbol.clone();

            public_ws.connect().await;

            let _ = Self::async_reflesh_order_book(&orderbook, &server_config, &config).await;

            sleep(std::time::Duration::from_secs(1));

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
                        Self::update_orderbook(&orderbook, &board);
                    }
                    MultiMarketMessage::Control(control) => {
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

    async fn async_reflesh_order_book_self(&mut self) -> anyhow::Result<()> {
        let transfer =
            BinanceRestApi::get_board_snapshot(&self.server_config, &self.config).await?;

        Self::update_orderbook(&self.board, &transfer);

        log::debug!("reflesh order book done");

        Ok(())
    }

    async fn async_reflesh_order_book(
        orderbook: &Arc<RwLock<OrderBook>>,
        server: &BinanceServerConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<()> {
        let transfer = BinanceRestApi::get_board_snapshot(&server, &config).await?;

        let orderbook = orderbook.clone();
        Self::update_orderbook(&orderbook, &transfer);

        log::debug!("reflesh order book done");

        Ok(())
    }

    fn update_orderbook(orderbook: &Arc<RwLock<OrderBook>>, transfer: &BoardTransfer) {
        let mut b = orderbook.write().unwrap();

        //        let first_update_id = b.get_first_update_id();
        let last_update_id = b.get_last_update_id();

        // 4. Drop any event where u is <= lastUpdateId in the snapshot.
        if last_update_id != 0 {
            if transfer.last_update_id <= last_update_id {
                log::warn!(
                    "transfer update_id is too small: {} <= {}",
                    transfer.last_update_id,
                    b.get_last_update_id()
                );
                return;
            }

            if last_update_id + 1 != transfer.first_update_id {
                log::warn!(
                    "last_update_id is not continuous: {} + 1 != {}",
                    last_update_id,
                    transfer.last_update_id
                );
            }
        }

        b.update(&transfer);
    }

    async fn async_download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64> {
        log::debug!("[start] download_gap ");

        let gap_result = self.find_latest_gap_trade(force);
        if gap_result.is_err() {
            log::warn!("no gap found: {:?}", gap_result);
            return Ok(0);
        }

        let start_time = NOW();
        let (unfix_start, unfix_end) = gap_result.unwrap();

        if verbose {
            if let Some(t) = unfix_start.clone() {
                println!("unfix_start: {:?}", t.__str__());
            }
            if let Some(t) = unfix_end.clone() {
                println!("unfix_end  : {:?}", t.__str__());
            }
            flush_log();
        }

        let unfix_start_id = if let Some(trade) = unfix_start {
            trade.id.parse::<i64>().unwrap()
        } else {
            log::warn!("no gap start found: {:?}", unfix_start);
            return Ok(0);
        };

        let unfix_end_id = if let Some(trade) = unfix_end {
            trade.id.parse::<i64>().unwrap_or_default()
        } else {
            0
        };

        let mut from_id = unfix_start_id + 1;
        let mut insert_len: i64 = 0;

        let tx = { self.db.lock().unwrap().start_thread().await };

        loop {
            if from_id == unfix_end_id {
                log::debug!("from_id == unfix_end_id: {:?}", from_id);
                break;
            }

            log::debug!("download recent from_id: {:?}", from_id);
            let mut trades = BinanceRestApi::get_historical_trades(
                &self.server_config,
                &self.config,
                from_id,
                0,
            )
            .await?;

            let l = trades.len();

            if l == 0 {
                if verbose {
                    println!("end of data");
                }
                break;
            }

            if verbose {
                println!(
                    "Downloaded: from:{}({})[{}]  to:{}({})[{}]",
                    time_string(trades[0].time),
                    trades[0].time,
                    trades[0].id,
                    time_string(trades[l - 1].time),
                    trades[l - 1].time,
                    trades[l - 1].id
                );
                flush_log();
            }

            let trade_id = trades[l - 1].id.parse::<i64>()?;
            from_id = trade_id + 1;

            if unfix_end_id == 0 {
                trades.retain(|t| t.time <= start_time);
            } else {
                trades.retain(|t| {
                    (t.id.parse::<i64>().unwrap() <= unfix_end_id) && (t.time <= start_time)
                });
            }

            let l = trades.len();
            log::debug!("Trim downloaded: {:?}", l);

            if l == 0 {
                break;
            }

            trades[0].status = LogStatus::FixRestApiStart;
            trades[l - 1].status = LogStatus::FixRestApiEnd;

            /*
            Binanceではklineから作ったデータは存在しないので削除不要
            let expire_message =
                TradeTableDb::expire_control_message(trades[0].time, trades[l - 1].time);

            tx.send(expire_message)?;
            */
            tx.send(trades)?;

            insert_len += l as i64;
            log::debug!("inserted: {}", l);
            std::thread::sleep(std::time::Duration::from_millis(200));
        }
        Ok(insert_len)
    }
}

#[cfg(test)]
mod binance_market_test {
    use rbot_lib::common::init_debug_log;

    use crate::BinanceConfig;

    #[test]
    fn test_dowlad_latest() {
        init_debug_log();
        use super::*;
        let server_config = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server_config, &config);

        log::debug!("market build complete");

        let r = market.download_latest(true);

        assert!(r.is_ok());
        log::debug!("download {:?}", r);

        sleep(std::time::Duration::from_secs(5));
    }

    #[test]
    fn test_dowlad_gap() {
        // init_debug_log();
        use super::*;
        let server_config = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server_config, &config);

        log::debug!("market build complete");

        let r = market.download_gap(true, true);

        assert!(r.is_ok());

        sleep(std::time::Duration::from_secs(60));
    }
}

#[cfg(test)]
mod test_market_impl {
    use rbot_lib::common::init_debug_log;

    use crate::BinanceConfig;

    #[tokio::test]
    async fn test_async_download_latest() -> anyhow::Result<()> {
        use super::*;
        init_debug_log();

        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        //let binance = Binance::new(true);

        let mut market = BinanceMarket::async_new(&server, &market_config, true).await?;
        //let mut market = binance.open_market(&market_config);

        let rec = market.async_download_latest(true).await.unwrap();

        assert!(rec > 0);

        Ok(())
    }

    #[test]
    fn test_download_latest() {
        init_debug_log();
        use super::*;
        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server, &market_config);

        let rec = market.download_latest(true).unwrap();
        assert!(rec > 0);
    }

    #[test]
    fn test_download_gap() {

        use super::*;
        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server, &market_config);

        let rec = market.download_gap(false, true).unwrap();
        assert!(rec > 0);
    }

    use rust_decimal_macros::dec;

    #[test]
    fn test_market_order() {
        init_debug_log();
        use super::*;
        // let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut binance = Binance::new(true);
        binance.enable_order = true;

        let rec = binance.market_order
            (&market_config, 
            "Buy", 
            dec![0.001],
            None);
        assert!(rec.is_ok());
    }
}

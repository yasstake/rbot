// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::sync::{Arc, Mutex, RwLock};

use anyhow::Context;
use futures::StreamExt;
use pyo3_polars::PyDataFrame;
use rbot_blockon::BLOCK_ON;
use rbot_lib::common::{AccountCoins, ExchangeConfig, Trade, DAYS, FLOOR_DAY};
use rbot_lib::common::BoardItem;
use rbot_lib::common::MarketConfig;
use rbot_lib::common::MarketMessage;
use rbot_lib::common::MarketStream;
use rbot_lib::common::MicroSec;
use rbot_lib::common::MultiMarketMessage;
use rbot_lib::common::Order;
use rbot_lib::common::OrderBook;
use rbot_lib::common::MARKET_HUB;
use rbot_lib::common::{time_string, NOW};
use rbot_lib::db::{TradeArchive, TradeDataFrame};
use rbot_lib::net::{BroadcastMessage, RestApi, WebSocketClient as _};
use rust_decimal::Decimal;
// Copyright(c) 2022-2024. yasstake. All rights reserved.
use tokio::task::JoinHandle;

// use rbot_market::OrderInterface;
use rbot_market::MarketImpl;
use rbot_market::OrderInterfaceImpl;
// use rbot_market::MarketInterface;

use crate::{BinancePrivateWsClient, BINANCE_BOARD_DEPTH};
use crate::BinancePublicWsClient;
use crate::BinanceRestApi;
use crate::BinanceServerConfig;

use pyo3::prelude::*;

use anyhow::anyhow;

pub const BINANCE:&str = "BINANCE";

#[pyclass]
pub struct Binance {
    production: bool,
    enable_order: bool,
    server_config: ExchangeConfig,
    user_handler: Option<JoinHandle<()>>,
    api: BinanceRestApi,
}

#[pymethods]
impl Binance {
    #[new]
    #[pyo3(signature = (production=false))]
    pub fn new(production: bool) -> Self {
        let server_config = BinanceServerConfig::new(production);

        let api = BinanceRestApi::new(&server_config);

        Self {
            production: production,
            enable_order: false,
            server_config: server_config,
            user_handler: None,
            api: api,
        }
    }

    #[getter]
    fn get_production(&self) -> bool {
        self.server_config.is_production()
    }

    pub fn open_market(&self, config: &MarketConfig) -> BinanceMarket {
        return BinanceMarket::new(&self.server_config, config);
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

    pub fn __str__(&self) -> String {
        format!(
            "{{production: {}, enable_order: {}, server_config: {:?} }}",
            self.production, self.enable_order, self.server_config
        )
    }
}

impl OrderInterfaceImpl<BinanceRestApi> for Binance {
    fn get_restapi(&self) -> &BinanceRestApi {
        &self.api
    }

    fn set_enable_order_feature(&mut self, enable_order: bool) {
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        self.enable_order
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
    server_config: ExchangeConfig,
    config: MarketConfig,
    api: BinanceRestApi,
    pub db: Arc<Mutex<TradeDataFrame>>,
    pub board: Arc<RwLock<OrderBook>>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
}

#[pymethods]
impl BinanceMarket {
    #[new]
    pub fn new(server_config: &ExchangeConfig, config: &MarketConfig) -> Self {
        log::debug!("open market BinanceMarket::new");
        BLOCK_ON(async { 
            Self::async_new(server_config, config).await.unwrap() 
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

    fn _select_cache_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_cache_df(self, start_time, end_time)
    }

    fn _select_cache_ohlcv_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        MarketImpl::select_cache_ohlcv_df(self, start_time, end_time)
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
        BLOCK_ON(async {
            MarketImpl::async_get_board(self).await
        })
    }

    #[getter]
    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        MarketImpl::get_board_vec(self)
    }

    #[getter]
    fn get_edge_price(&mut self) -> anyhow::Result<(Decimal, Decimal)> {
        BLOCK_ON(async {
            MarketImpl::async_get_edge_price(self).await
        })
    }

    fn _repr_html_(&self) -> String {
        MarketImpl::_repr_html_(self)
    }

    #[pyo3(signature = (ndays, *, connect_ws=false, force=false, force_archive=false, force_recent=false, verbose=false))]
    fn download(
        &mut self,
        ndays: i64,
        connect_ws: bool,
        force: bool,
        force_archive: bool,
        force_recent: bool,
        verbose: bool,
    ) -> anyhow::Result<()> {
        BLOCK_ON(async {
            MarketImpl::async_download::<BinancePublicWsClient>(
                self,
                ndays,
                connect_ws,
                force,
                force_archive,
                force_recent,
                verbose,
            )
            .await
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
            MarketImpl::async_download_realtime::<BinancePublicWsClient> (self, connect_ws, force, verbose).await
        })
    }

    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<(MicroSec, MicroSec, MarketStream)> {
        MarketImpl::open_backtest_channel(self, time_from, time_to)
    }

    fn open_market_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON (async {
            self.async_start_market_stream().await
        })
    }

    fn vaccum(&self) -> anyhow::Result<()> {
        let lock = self.db.lock().unwrap();

        lock.vacuum()
    }

    fn _cache_all_data(&mut self) -> anyhow::Result<()> {
        MarketImpl::cache_all_data(self)
    }

    #[pyo3(signature = (verbose=false))]
    fn _download_latest(&mut self, verbose: bool) -> anyhow::Result<(i64, i64)> {
        log::debug!("BinanceMarket._download_latest(verbose={}", verbose);

        BLOCK_ON(async {
            MarketImpl::async_download_latest(self, verbose).await
        })
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
            MarketImpl::_async_download_range(self, start_time, end_time, verbose).await
        })
    }
}

impl MarketImpl<BinanceRestApi> for BinanceMarket {
    fn get_restapi(&self) -> &BinanceRestApi {
        &self.api
    }

    fn get_config(&self) -> MarketConfig {
        self.config.clone()
    }

    fn get_db(&self) -> Arc<Mutex<TradeDataFrame>> {
        self.db.clone()
    }

    fn get_history_web_base_url(&self) -> String {
        self.server_config.get_historical_web_base()
    }

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()> 
    {
        if self.public_handler.is_some() {
            log::info!("market stream is already running.");
            return Ok(());
        }

        let db_channel = {
            let mut lock = self.db.lock().unwrap();
            lock.open_channel()
        }?;

        let orderbook = self.board.clone();

        let server_config = self.server_config.clone();
        let config = self.config.clone();

        let hub_channel = MARKET_HUB.open_channel();

        let mut public_ws = BinancePublicWsClient::new(&server_config, &config).await;

        let exchange_name = config.exchange_name.clone();
        let trade_category = config.trade_category.clone();
        let trade_symbol = config.trade_symbol.clone();

//         public_ws.connect().await;

        let _ = self.async_refresh_order_book().await;

        self.public_handler = Some(tokio::task::spawn(async move {
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
    
    fn get_order_book(&self) -> Arc<RwLock<OrderBook>> {
        self.board.clone()
    }

    async fn async_download_range(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        let time_from = if time_from == 0 || time_from < NOW() - DAYS(2) {
            FLOOR_DAY(NOW() - DAYS(1))
        }
        else {
            time_from
        };

        self._async_download_range(time_from, time_to, verbose).await
    }
}

impl BinanceMarket {
    async fn async_new(
        server_config: &ExchangeConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<Self> {
        let db = TradeDataFrame::get(config, server_config.is_production())
            .with_context(|| format!("Error in TradeTable::open: {:?}", config))?;


        let market = BinanceMarket {
            server_config: server_config.clone(),
            api: BinanceRestApi::new(server_config),
            config: config.clone(),
            db: db,
            board: Arc::new(RwLock::new(OrderBook::new(&config, BINANCE_BOARD_DEPTH))),
            public_handler: None,
        };

        Ok(market)
    }


/*
    async fn async_refresh_order_book(
        &self,
        orderbook: &Arc<RwLock<OrderBook>>,
        config: &MarketConfig,
    ) -> anyhow::Result<()> {
        let transfer = self.api.get_board_snapshot(&config).await?;

        let orderbook = orderbook.clone();
        Self::update_orderbook(&orderbook, &transfer);

        log::debug!("refresh order book done");

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
*/
}

#[cfg(test)]
mod binance_market_test {
    use std::thread::sleep;

    use rbot_lib::common::init_debug_log;

    use crate::BinanceConfig;

    #[test]
    fn test_down_load_latest() {
        init_debug_log();
        use super::*;
        let server_config = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server_config, &config);

        log::debug!("market build complete");

        let r = market._download_latest(true);

        assert!(r.is_ok());
        log::debug!("download {:?}", r);

        sleep(std::time::Duration::from_secs(5));
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

        let mut market = BinanceMarket::async_new(&server, &market_config).await?;
        //let mut market = binance.open_market(&market_config);

        let rec = market.async_download_latest(true).await.unwrap();

        assert!(rec.0 > 0);

        Ok(())
    }

    #[test]
    fn test_download_latest() {
        init_debug_log();
        use super::*;
        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server, &market_config);

        let rec = market._download_latest(true).unwrap();
        assert!(rec.0 > 0);
    }

    #[test]
    fn test_download() {
        init_debug_log();
        use super::*;
        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server, &market_config);

        market.download(3, false, false, false, false, true).unwrap();
    }

    #[test]
    fn test_download_archive() {
        init_debug_log();
        use super::*;
        let server = BinanceServerConfig::new(true);
        let market_config = BinanceConfig::BTCUSDT();

        let mut market = BinanceMarket::new(&server, &market_config);

        market._download_archive(3, false, true).unwrap();

        let trades = market._select_archive_trades(0, 0);

        println!("{:?}", trades);
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

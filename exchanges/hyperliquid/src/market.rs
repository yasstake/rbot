// Copyright(c) 2022-2024. yasstake. All rights reserved.
#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused)]

use chrono::Datelike;
use crossbeam_channel::Sender;
use csv::StringRecord;

use futures::StreamExt;
use polars::prelude::*;
use pyo3::ffi::getter;

use std::sync::{Arc, Mutex, RwLock};

use std::thread::sleep;
use std::time::Duration;

use rbot_lib::common::{
    convert_klines_to_trades, flush_log, time_string, to_naive_datetime, AccountCoins, AccountPair,
    BoardItem, BoardTransfer, LogStatus, MarketConfig, MarketMessage, MarketStream, MicroSec,
    MultiMarketMessage, Order, OrderBook, OrderBookRaw, OrderSide, OrderStatus, OrderType,
    ExchangeConfig, Trade, DAYS, FLOOR_DAY, HHMM, MARKET_HUB, NOW, SEC,
};

use rbot_lib::db::{db_full_path, TradeArchive, TradeDataFrame, TradeDb, KEY};
use rbot_lib::net::{latest_archive_date, BroadcastMessage, RestApi, RestPage, UdpSender, WebSocketClient};

use rbot_market::{generate_market_config, MarketImpl};
use rbot_market::{MarketInterface, OrderInterface, OrderInterfaceImpl};
use rbot_blockon::BLOCK_ON;

use crate::{HYPERLIQUID, HYPERLIQUID_BOARD_DEPTH};
use crate::message::HyperliquidUserWsMessage;

use crate::rest::HyperliquidRestApi;
use crate::ws::{HyperliquidPrivateWsClient, HyperliquidPublicWsClient, HyperliquidWsOpMessage};

use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use super::message::HyperliquidOrderStatus;
use super::message::{HyperliquidAccountInformation, HyperliquidPublicWsMessage};

use anyhow::Context;

#[pyclass]
pub struct HyperliquidExchange {
    pub server_config: ExchangeConfig,
    pub enable_order: bool,
    pub api: HyperliquidRestApi,
}

#[pymethods]
impl HyperliquidExchange {
    #[new]
    #[pyo3(signature = (production=false))]
    pub fn new(production: bool) -> Self {
        let public_api = if production {
            "https://api.hyperliquid.xyz"
        } else {
            "https://api.hyperliquid-testnet.xyz"
        };

        let private_api = public_api;

        let public_ws = if production {
            "wss://api.hyperliquid.xyz/ws"
        } else {
            "wss://api.hyperliquid-testnet.xyz/ws"
        };

        let private_ws = public_ws;

        let server_config = ExchangeConfig::new(
            HYPERLIQUID,
            production,
            public_api,
            private_api,
            public_ws,
            private_ws,
            "https://api.hyperliquid.xyz",
        );

        let api = HyperliquidRestApi::new(server_config.clone());

        HyperliquidExchange {
            server_config,
            enable_order: false,
            api,
        }
    }

    #[getter]
    fn get_production(&self) -> bool {
        self.server_config.is_production()
    }

    pub fn open_market(&self, symbol: &str) -> anyhow::Result<HyperliquidMarket> {
        let config = generate_market_config(&self.server_config.get_exchange_name(), symbol)?;
        Ok(HyperliquidMarket::new(&self.server_config, &config))
    }

    #[setter]
    pub fn set_enable_order_with_my_own_risk(&mut self, enable_order: bool) {
        OrderInterface::set_enable_order_feature(self, enable_order);
    }

    #[getter]
    pub fn get_enable_order_with_my_own_risk(&self) -> bool {
        OrderInterface::get_enable_order_feature(self)
    }

    pub fn __str__(&self) -> String {
        format!(
            "HyperliquidExchange(production={}, enable_order={})",
            self.server_config.is_production(), 
            self.enable_order
        )
    }
}

impl OrderInterface for HyperliquidExchange {
    fn set_enable_order_feature(&mut self, enable_order: bool) {
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        self.enable_order
    }

    fn limit_order(
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

    fn market_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async {
            OrderInterfaceImpl::market_order(self, market_config, side, size, client_order_id)
                .await
        })
    }

    fn dry_market_order(
        &self,
        market_config: &MarketConfig,
        create_time: MicroSec,
        order_id: &str,
        client_order_id: &str,
        side: OrderSide,
        size: Decimal,
        transaction_id: &str,
    ) -> Vec<Order> {
        // Default dry market order implementation
        vec![]
    }

    fn cancel_order(&self, market_config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        BLOCK_ON(async {
            OrderInterfaceImpl::cancel_order(self, market_config, order_id).await
        })
    }

    fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async {
            OrderInterfaceImpl::get_open_orders(self, market_config).await
        })
    }

    fn get_account(&self, market_config: &MarketConfig) -> anyhow::Result<AccountPair> {
        BLOCK_ON(async {
            self.api.get_account().await
        })
    }
}

#[pyclass]
pub struct HyperliquidMarket {
    pub server_config: ExchangeConfig,
    pub api: HyperliquidRestApi,
    pub config: MarketConfig,
    pub db: Arc<Mutex<TradeDataFrame>>,
    pub board: Arc<RwLock<OrderBook>>,
    pub public_handler: Option<tokio::task::JoinHandle<()>>,
}

#[pymethods]
impl HyperliquidMarket {
    #[new]
    pub fn new(server_config: &ExchangeConfig, config: &MarketConfig) -> Self {
        log::debug!("open market HyperliquidMarket::new");
        BLOCK_ON(async { Self::async_new(server_config, config).await.unwrap() })
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

    pub fn start_market_stream(&mut self) -> anyhow::Result<()> {
        BLOCK_ON(async {
            self.async_start_market_stream().await
        })
    }
}

impl HyperliquidMarket {
    pub async fn async_new(
        server_config: &ExchangeConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<Self> {
        let db = TradeDataFrame::get(config, server_config.is_production())
            .with_context(|| format!("Error in TradeTable::open: {:?}", config))?;

        let board = OrderBook::new(&config, HYPERLIQUID_BOARD_DEPTH);

        let mut market = HyperliquidMarket {
            server_config: server_config.clone(),
            api: HyperliquidRestApi::new(server_config.clone()),
            config: config.clone(),
            db: db.clone(),
            board: Arc::new(RwLock::new(board)),
            public_handler: None,
        };

        Ok(market)
    }
}

impl OrderInterfaceImpl<HyperliquidRestApi> for HyperliquidExchange {
    fn get_restapi(&self) -> &HyperliquidRestApi {
        &self.api
    }

    fn set_enable_order_feature(&mut self, enable_order: bool) {
        self.enable_order = enable_order;
    }

    fn get_enable_order_feature(&self) -> bool {
        self.enable_order
    }

    async fn async_start_user_stream(&mut self) -> anyhow::Result<()> {
        // Implementation for user stream
        Ok(())
    }
}

impl MarketImpl<HyperliquidRestApi> for HyperliquidMarket {
    fn get_restapi(&self) -> &HyperliquidRestApi {
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
        } else {
            time_from
        };
        Ok(0) // TODO: Implement actual download logic
    }

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()> {
        // Implementation for market stream
        Ok(())
    }
}
#![allow(unused)]
// Copyright(c) 2025. yasstake. All rights reserved.

use std::convert;
use std::fmt::format;

use chrono::Datelike as _;
use csv::StringRecord;
use polars::chunked_array::ops::ChunkCast as _;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::lazy::dsl::col;
use polars::lazy::dsl::lit;
use polars::lazy::frame::IntoLazy;
use polars::series::Series;
use rbot_lib::common::convert_klines_to_trades;
use rbot_lib::common::split_yyyymmdd;
use rbot_lib::common::time_string;
use rbot_lib::common::to_naive_datetime;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::AccountPair;
use rbot_lib::common::BoardTransfer;
use rbot_lib::common::Kline;
use rbot_lib::common::LogStatus;
use rbot_lib::common::FLOOR_SEC;
use rbot_lib::db::ohlcv_end;
use rbot_lib::db::ohlcv_start;
use rbot_lib::db::TradeDataFrame;
use rbot_lib::db::KEY;
use rbot_lib::net::check_exist;
use rbot_lib::net::RestPage;
use rust_decimal_macros::dec;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::from_str;
use serde_json::Value;

use rust_decimal::Decimal;

use anyhow::anyhow;
use anyhow::ensure;
#[allow(unused_imports)]
use anyhow::Context;
use anyhow::Result;

use rbot_lib::common::{
    hmac_sign, msec_to_microsec, MarketConfig, MicroSec, Order, OrderSide, OrderStatus, OrderType,
    ExchangeConfig, Trade, NOW,
};

use rbot_lib::net::{rest_get, rest_post, RestApi};

use crate::message::microsec_to_hyperliquid_timestamp;
use crate::HYPERLIQUID_BOARD_DEPTH;

use super::message::HyperliquidRestBoard;
use super::message::HyperliquidRestResponse;
use super::message::HyperliquidTrade;

pub struct HyperliquidRestApi {
    server_config: ExchangeConfig,
    api_key: Option<String>,
    api_secret: Option<String>,
}

impl HyperliquidRestApi {
    pub fn new(server_config: ExchangeConfig) -> Self {
        Self {
            server_config,
            api_key: None,
            api_secret: None,
        }
    }

    pub fn with_auth(mut self, api_key: String, api_secret: String) -> Self {
        self.api_key = Some(api_key);
        self.api_secret = Some(api_secret);
        self
    }

    fn sign_request(&self, payload: &str, timestamp: i64) -> Result<String> {
        if let (Some(secret), Some(_)) = (&self.api_secret, &self.api_key) {
            let message = format!("{}{}", timestamp, payload);
            Ok(hmac_sign(secret, &message))
        } else {
            Err(anyhow!("API credentials not set"))
        }
    }

    pub async fn get_board(&self, symbol: &str) -> Result<BoardTransfer> {
        let url = format!("{}/info", self.server_config.get_public_api());
        let payload = serde_json::json!({
            "type": "l2Book",
            "coin": symbol
        });

        let response: HyperliquidRestResponse = rest_post(&url, &payload.to_string()).await?;
        
        if response.is_success() {
            let board: HyperliquidRestBoard = serde_json::from_value(response.data)?;
            Ok(board.into())
        } else {
            Err(anyhow!("Failed to get board: {:?}", response.error))
        }
    }

    pub async fn get_trades(&self, symbol: &str, start_time: Option<i64>, end_time: Option<i64>) -> Result<Vec<Trade>> {
        let url = format!("{}/info", self.server_config.get_public_api());
        let mut payload = serde_json::json!({
            "type": "candleSnapshot",
            "req": {
                "coin": symbol,
                "interval": "1m"
            }
        });

        let response: HyperliquidRestResponse = rest_post(&url, &payload.to_string()).await?;
        
        if response.is_success() {
            let trades: Vec<HyperliquidTrade> = serde_json::from_value(response.data)?;
            Ok(trades.into_iter().map(|t| t.into()).collect())
        } else {
            Err(anyhow!("Failed to get trades: {:?}", response.error))
        }
    }
}

impl RestApi for HyperliquidRestApi {
    fn get_exchange(&self) -> ExchangeConfig {
        self.server_config.clone()
    }

    async fn get_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage,
    ) -> anyhow::Result<(Vec<Kline>, RestPage)> {
        let trades = self.get_trades(&config.trade_symbol, Some(start_time), Some(end_time)).await?;
        let klines = convert_klines_to_trades(&trades);
        Ok((klines, RestPage::Done))
    }

    fn klines_width(&self) -> i64 {
        60 // 1 minute in seconds
    }

    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        todo!("Implement new_order")
    }

    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        todo!("Implement cancel_order")
    }

    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        todo!("Implement open_orders")
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        todo!("Implement get_account")
    }

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String {
        format!("{}/history/{}", self.server_config.get_historical_web_base(), time_string(date))
    }

    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame> {
        Ok(df.clone())
    }
}
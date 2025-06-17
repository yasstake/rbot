// Copyright(c) 2022-2024. yasstake. All rights reserved.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(unused)]

use std::collections::HashMap;
use std::str::FromStr;

use pyo3::pyclass;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserializer;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;

use rbot_lib::common::{
    msec_to_microsec, string_to_decimal, string_to_i64, time_string, AccountCoins, AccountPair,
    Board, BoardTransfer, BoardItem, Coin, ControlMessage, Kline, LogStatus, MarketConfig, MarketMessage,
    MicroSec, MultiMarketMessage, Order, OrderBookRaw, OrderSide, OrderStatus, OrderType, Trade,
};

use crate::HYPERLIQUID;

pub type HyperliquidTimestamp = i64;

pub fn hyperliquid_timestamp_to_microsec(timestamp: HyperliquidTimestamp) -> MicroSec {
    timestamp * 1000
}

pub fn microsec_to_hyperliquid_timestamp(timestamp: MicroSec) -> HyperliquidTimestamp {
    timestamp / 1000
}

pub fn hyperliquid_order_status(status: &str) -> OrderStatus {
    match status {
        "open" => OrderStatus::New,
        "partiallyFilled" => OrderStatus::PartiallyFilled,
        "canceled" => OrderStatus::Canceled,
        "filled" => OrderStatus::Filled,
        "rejected" => OrderStatus::Rejected,
        _ => OrderStatus::Unknown,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HyperliquidRestResponse {
    pub success: bool,
    pub error: Option<String>,
    pub data: Value,
}

impl HyperliquidRestResponse {
    pub fn is_success(&self) -> bool {
        self.success
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HyperliquidRestBoard {
    pub coin: String,
    pub levels: Vec<Vec<String>>,
    pub time: HyperliquidTimestamp,
}

impl Into<BoardTransfer> for HyperliquidRestBoard {
    fn into(self) -> BoardTransfer {
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        for level in self.levels {
            if level.len() >= 3 {
                let price = Decimal::from_str(&level[0]).unwrap_or_default();
                let size = Decimal::from_str(&level[1]).unwrap_or_default();
                let is_bid = &level[2] == "1";

                let board_item = BoardItem {
                    price,
                    size,
                };

                if is_bid {
                    bids.push(board_item);
                } else {
                    asks.push(board_item);
                }
            }
        }

        BoardTransfer {
            last_update_time: hyperliquid_timestamp_to_microsec(self.time),
            first_update_id: 0,
            last_update_id: 0,
            bids,
            asks,
            snapshot: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HyperliquidTrade {
    pub coin: String,
    pub side: String,
    pub px: String,
    pub sz: String,
    pub time: HyperliquidTimestamp,
    pub tid: i64,
}

impl Into<Trade> for HyperliquidTrade {
    fn into(self) -> Trade {
        let side = match self.side.as_str() {
            "B" => OrderSide::Buy,
            "A" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

        Trade {
            time: hyperliquid_timestamp_to_microsec(self.time),
            order_side: side,
            price: Decimal::from_str(&self.px).unwrap_or_default(),
            size: Decimal::from_str(&self.sz).unwrap_or_default(),
            status: LogStatus::Unknown,
            id: self.tid.to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HyperliquidOrderStatus {
    Open,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HyperliquidOrder {
    pub coin: String,
    pub side: String,
    pub limitPx: String,
    pub sz: String,
    pub oid: u64,
    pub timestamp: HyperliquidTimestamp,
    pub origSz: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct HyperliquidAccountInformation {
    pub assetPositions: Vec<Value>,
    pub crossMarginSummary: Value,
    pub marginSummary: Value,
    pub withdrawable: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidWsMessage {
    pub channel: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidPublicWsMessage {
    pub channel: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidUserWsMessage {
    pub channel: String,
    pub data: Value,
}

pub type HyperliquidWsOpMessage = Value;
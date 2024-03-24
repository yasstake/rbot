// Copyright(c) 2022-2023. yasstake. All rights reserved.
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
    Board, BoardTransfer, Coin, ControlMessage, Kline, LogStatus, MarketConfig, MarketMessage,
    MicroSec, MultiMarketMessage, Order, OrderBookRaw, OrderSide, OrderStatus, OrderType, Trade,
};

use crate::Bybit;

pub type BybitTimestamp = i64;

pub fn bybit_timestamp_to_microsec(timestamp: BybitTimestamp) -> MicroSec {
    timestamp * 1000
}

pub fn microsec_to_bybit_timestamp(timestamp: MicroSec) -> BybitTimestamp {
    timestamp / 1000
}

pub fn bybit_order_status(status: &str) -> OrderStatus {
    match status {
        "New" => OrderStatus::New,
        "PartiallyFilled" => OrderStatus::PartiallyFilled,
        "Cancelled" | "PartiallyFilledCanceled" => OrderStatus::Canceled,
        "Filled" => OrderStatus::Filled,
        _ => OrderStatus::Unknown,
        /*
        "Created",
        "Untriggered"
        "Triggered"
        "Deactivated"
        "Rejected"
        */
    }
}

fn deserialize_ret_code<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let value = <Value as serde::Deserialize>::deserialize(deserializer)?;
    if let Some(code) = value.get("retCode").or_else(|| value.get("return_code")).and_then(Value::as_i64) {
        Ok(code as i64)
    } else {
        Err(serde::de::Error::custom("retCode/ret_code field not found"))
    }
}



#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitRestResponse {
    #[serde(rename="retCode")]
    pub return_code: i64,
    #[serde(rename = "retMsg")]
    pub return_message: String,
    #[serde(rename = "retExtInfo")]
    pub return_ext_info: Value,
    #[serde(rename = "time")]
    pub time: BybitTimestamp,
    #[serde(rename = "result")]
    pub body: Value,
}

impl BybitRestResponse {
    pub fn is_success(&self) -> bool {
        self.return_code == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitRestBoard {
    #[serde(rename = "ts")]
    pub timestamp: BybitTimestamp,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    #[serde(rename = "b")]
    pub bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    pub asks: Vec<(Decimal, Decimal)>,
}

impl Into<BoardTransfer> for BybitRestBoard {
    fn into(self) -> BoardTransfer {
        let mut bt = BoardTransfer::new();

        for bid in self.bids.iter() {
            bt.insert_bid(bid);
        }

        for ask in self.asks.iter() {
            bt.insert_ask(ask);
        }

        bt
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKline {
    pub timestamp: BybitTimestamp,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

impl BybitKline {
    pub fn __str__(&self) -> String {
        format! {"{:?}(): o:{}, h:{}, l:{}, c:{}, v:{}",
            time_string(msec_to_microsec(self.timestamp)),
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume
        }
    }

    /*
    pub fn extract_trade(&self) -> Vec<Trade> {
        let mut trades = Vec::new();

        let vol = self.volume / Decimal::from(4);
        let mut remain_vol = self.volume.clone();
        let tick = SEC(15);

        let t = Trade::new(
            msec_to_microsec(self.timestamp),
            OrderSide::Buy,
            self.open,
            vol,
            LogStatus::UnFix,
            &format!("KLINE{}-{}", self.timestamp, 0),
        );
        trades.push(t);

        remain_vol -= vol;

        let t = Trade::new(
            msec_to_microsec(self.timestamp) + tick,
            OrderSide::Buy,
            self.high,
            vol,
            LogStatus::UnFix,
            &format!("KLINE{}-{}", self.timestamp, 1),
        );
        trades.push(t);
        remain_vol -= vol;

        let t = Trade::new(
            msec_to_microsec(self.timestamp) + tick * 2,
            OrderSide::Sell,
            self.low,
            vol,
            LogStatus::UnFix,
            &format!("KLINE{}-{}", self.timestamp, 2),
        );
        trades.push(t);
        remain_vol -= vol;

        let t = Trade::new(
            msec_to_microsec(self.timestamp) + tick * 3,
            OrderSide::Buy,
            self.close,
            remain_vol,
            LogStatus::UnFix,
            &format!("KLINE{}-{}", self.timestamp, 3),
        );
        trades.push(t);

        trades
    }
    */
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKlines {
    category: String,
    symbol: String,
    pub klines: Vec<BybitKline>,
}

impl BybitKlines {
    pub fn new() -> BybitKlines {
        BybitKlines {
            category: "".to_string(),
            symbol: "".to_string(),
            klines: Vec::new(),
        }
    }

    pub fn append(&mut self, klines: &BybitKlines) {
        self.category = klines.category.clone();
        self.symbol = klines.symbol.clone();
        self.klines.append(&mut klines.klines.clone());
    }

    pub fn __str__(&self) -> String {
        let mut s = format! {"{:?}(): ", self.category};

        for kline in self.klines.iter() {
            s += &format!("{}\n", kline.__str__());
        }
        s
    }

    /*
    pub fn to_dataframe(&self) -> DataFrame {
        let mut time = Vec::<MicroSec>::new();
        let mut order_side = Vec::<f64>::new();
        let mut open = Vec::<f64>::new();
        let mut high = Vec::<f64>::new();
        let mut low = Vec::<f64>::new();
        let mut close = Vec::<f64>::new();
        let mut vol = Vec::<f64>::new();
        let mut count = Vec::<f64>::new();
        let mut start_time = Vec::<MicroSec>::new();
        let mut end_time = Vec::<MicroSec>::new();

        for kline in self.klines.iter() {
            time.push(msec_to_microsec(kline.timestamp));
            order_side.push(0.0);
            open.push(kline.open.to_f64().unwrap());
            high.push(kline.high.to_f64().unwrap());
            low.push(kline.low.to_f64().unwrap());
            close.push(kline.close.to_f64().unwrap());
            vol.push(kline.volume.to_f64().unwrap());
            count.push(1.0);
            start_time.push(msec_to_microsec(kline.timestamp));
            end_time.push(msec_to_microsec(kline.timestamp) + HHMM(0, 1) - 1);
        }

        let time = Series::new(KEY::time_stamp, &time);
        let order_side = Series::new(KEY::order_side, &order_side);
        let open = Series::new(KEY::open, &open);
        let high = Series::new(KEY::high, &high);
        let low = Series::new(KEY::low, &low);
        let close = Series::new(KEY::close, &close);
        let vol = Series::new(KEY::volume, &vol);
        let count = Series::new(KEY::count, &count);
        let start_time = Series::new(KEY::start_time, &start_time);
        let end_time = Series::new(KEY::end_time, &end_time);

        let df = DataFrame::new(vec![
            time, order_side, open, high, low, close, vol, count, start_time, end_time,
        ]);

        df.unwrap()
    }
    */
}

/*
impInto<Vec<Trade>> for BybitKlines {
    fn into(self) -> Vec<Trade> {
        let mut trades = Vec::new(
        for kline in self.klines.iter() {
            let mut t: Vec<Trade> = kline.extract_trade();
            trades.append(&mut t);
        }

        trades
    }
}
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKlinesResponse {
    #[serde(rename = "category")]
    category: String,
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "list")]
    //pub klines: Vec<(MicroSec, Decimal, Decimal, Decimal, Decimal, Decimal)>,
    pub klines: Vec<(String, String, String, String, String, String, String)>,
}

impl Into<Vec<Kline>> for BybitKlinesResponse {
    fn into(self) -> Vec<Kline> {
        let mut klines = Vec::new();
        for kline in self.klines {
            let timestamp: BybitTimestamp = kline.0.parse().unwrap();
            let open = Decimal::from_str(&kline.1).unwrap();
            let high = Decimal::from_str(&kline.2).unwrap();
            let low = Decimal::from_str(&kline.3).unwrap();
            let close = Decimal::from_str(&kline.4).unwrap();
            let volume = Decimal::from_str(&kline.5).unwrap();
            // let turnover = Decimal::from_str(&kline.6).unwrap();            // ignore turnover

            let kline = Kline {
                timestamp: bybit_timestamp_to_microsec(timestamp),
                open,
                high,
                low,
                close,
                volume,
            };

            klines.push(kline);
        }

        klines
    }
}

/*
impl Into<BybitKlines> for BybitKlinesResponse {
    fn into(self) -> BybitKlines {
        let mut klines = Vec::new();
        for kline in self.klines {
            let timestamp: BybitTimestamp = kline.0.parse().unwrap();
            let open = Decimal::from_str(&kline.1).unwrap();
            let high = Decimal::from_str(&kline.2).unwrap();
            let low = Decimal::from_str(&kline.3).unwrap();
            let close = Decimal::from_str(&kline.4).unwrap();
            let volume = Decimal::from_str(&kline.5).unwrap();
            // let turnover = Decimal::from_str(&kline.6).unwrap();            // ignore turnover

            let kline = BybitKline {
                timestamp: timestamp,
                open,
                high,
                low,
                close,
                volume,
            };

            klines.push(kline);
        }

        BybitKlines {
            category: self.category.clone(),
            symbol: self.symbol.clone(),
            klines,
        }
    }
}
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitTrade {
    #[serde(rename = "execId")]
    pub exec_id: String,
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "price", deserialize_with = "string_to_decimal")]
    //pub price: Decimal,
    pub price: Decimal,
    #[serde(rename = "size", deserialize_with = "string_to_decimal")]
    pub size: Decimal,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "time", deserialize_with = "string_to_i64")]
    pub time: BybitTimestamp,
    #[serde(rename = "isBlockTrade")]
    pub is_block_trade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitTradeResponse {
    pub category: String,
    #[serde(rename = "list")]
    pub trades: Vec<BybitTrade>,
}

impl Into<Vec<Trade>> for BybitTradeResponse {
    fn into(self) -> Vec<Trade> {
        let mut trades = Vec::new();

        for trade in self.trades.iter() {
            let t = Trade::new(
                msec_to_microsec(trade.time),
                OrderSide::from(&trade.side),
                trade.price,
                trade.size,
                LogStatus::UnFix,
                &trade.exec_id,
            );
            trades.push(t);
        }

        trades
    }
}
/*
        "list": [
            {
                "orderId": "fd4300ae-7847-404e-b947-b46980a4d140",
                "orderLinkId": "test-000005",
                "blockTradeId": "",
                "symbol": "ETHUSDT",
                "price": "1600.00",
                "qty": "0.10",
                "side": "Buy",
                "isLeverage": "",
                "positionIdx": 1,
                "orderStatus": "New",
                "cancelType": "UNKNOWN",
                "rejectReason": "EC_NoError",
                "avgPrice": "0",
                "leavesQty": "0.10",
                "leavesValue": "160",
                "cumExecQty": "0.00",
                "cumExecValue": "0",
                "cumExecFee": "0",
                "timeInForce": "GTC",
                "orderType": "Limit",
                "stopOrderType": "UNKNOWN",
                "orderIv": "",
                "triggerPrice": "0.00",
                "takeProfit": "2500.00",
                "stopLoss": "1500.00",
                "tpTriggerBy": "LastPrice",
                "slTriggerBy": "LastPrice",
                "triggerDirection": 0,
                "triggerBy": "UNKNOWN",
                "lastPriceOnCreated": "",
                "reduceOnly": false,
                "closeOnTrigger": false,
                "smpType": "None",
                "smpGroup": 0,
                "smpOrderId": "",
                "tpslMode": "Full",
                "tpLimitPrice": "",
                "slLimitPrice": "",
                "placeType": "",
                "createdTime": "1684738540559",
                "updatedTime": "1684738540561"
            }
        ],
        "nextPageCursor": "page_args%3Dfd4300ae-7847-404e-b947-b46980a4d140%26symbol%3D6%26",
        "category": "linear"
*/

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitOrderStatus {
    pub orderId: String,
    pub orderLinkId: String,
    pub blockTradeId: String,
    pub symbol: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub qty: Decimal,
    pub side: String,
    pub isLeverage: String,
    pub positionIdx: i64,
    pub orderStatus: String,
    pub cancelType: String,
    pub rejectReason: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub avgPrice: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub leavesQty: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub leavesValue: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub cumExecQty: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub cumExecValue: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub cumExecFee: Decimal,
    pub timeInForce: String,
    pub orderType: String,
    pub stopOrderType: String,
    pub orderIv: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub triggerPrice: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub takeProfit: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub stopLoss: Decimal,
    pub tpTriggerBy: String,
    pub slTriggerBy: String,
    pub triggerDirection: i64,
    pub triggerBy: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub lastPriceOnCreated: Decimal,
    pub reduceOnly: bool,
    pub closeOnTrigger: bool,
    pub smpType: String,
    pub smpGroup: i64,
    pub smpOrderId: String,
    pub tpslMode: String,
    pub tpLimitPrice: String,
    pub slLimitPrice: String,
    pub placeType: String,
    #[serde(deserialize_with = "string_to_i64")]
    pub createdTime: BybitTimestamp,
    #[serde(deserialize_with = "string_to_i64")]
    pub updatedTime: BybitTimestamp,
}

impl Into<Order> for &BybitOrderStatus {
    fn into(self) -> Order {
        let order_type = OrderType::from(&self.orderType);

        Order {
            category: "linear".to_string(),     // for default
            symbol: self.symbol.clone(),
            create_time: self.createdTime,
            status: bybit_order_status(&self.orderStatus),
            order_id: self.orderId.clone(),
            client_order_id: self.orderLinkId.clone(),
            order_side: OrderSide::from(&self.side),
            order_type: order_type.clone(),
            order_price: self.price,
            order_size: self.qty,
            remain_size: self.leavesQty,
            transaction_id: self.orderId.clone(),
            update_time: self.updatedTime,
            execute_price: self.avgPrice,
            execute_size: self.cumExecQty,
            quote_vol: self.price * self.qty,
            commission: self.cumExecFee,
            commission_asset: "".to_string(),
            is_maker: order_type.is_maker(),
            message: "".to_string(),    // DUMMY value
            commission_home: dec![0.0], // DUMMY value
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],
            log_id: 0,
            open_position: dec![0.0],
            close_position: dec![0.0],
            position: dec![0.0],
            profit: dec![0.0],
            fee: dec![0.0],
            total_profit: dec![0.0],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitMultiOrderStatus {
    pub list: Vec<BybitOrderStatus>,
    pub nextPageCursor: String,
    pub category: String,
}

impl Into<Vec<Order>> for BybitMultiOrderStatus {
    fn into(self) -> Vec<Order> {
        let category = self.category.clone();
        let mut orders: Vec<Order> = vec![];

        for order in self.list.iter() {
            let mut o: Order = order.into();
            o.category = category.clone();
            orders.push(o);
        }

        orders
    }
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitAccountInformation {}

/*------------- WS --------------------------- */

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BybitPublicWsMessage {
    Status(BybitWsStatus),
    Pong(BybitWsPongReply),
    Trade(BybitWsTradeMessage),
    Orderbook(BybitWsOrderbookMessage),
}

impl From<String> for BybitPublicWsMessage {
    fn from(message: String) -> Self {
        let result = serde_json::from_str::<BybitPublicWsMessage>(&message);
        return result.unwrap();
    }
}

impl Into<MultiMarketMessage> for BybitPublicWsMessage {
    fn into(self) -> MultiMarketMessage {
        match self {
            BybitPublicWsMessage::Trade(trade) => {
                let mut trades: Vec<Trade> = vec![];

                for trade in trade.data.iter() {
                    let t = Trade::new(
                        msec_to_microsec(trade.timestamp),
                        OrderSide::from(&trade.side),
                        trade.price,
                        trade.size,
                        LogStatus::UnFix,
                        &trade.trade_id,
                    );
                    trades.push(t);
                }
                return MultiMarketMessage::Trade(trades);
            }
            BybitPublicWsMessage::Orderbook(orderbook) => {
                let mut board: BoardTransfer  = orderbook.into();

                return MultiMarketMessage::Orderbook(board);
            }
            BybitPublicWsMessage::Status(status) => {
                return MultiMarketMessage::Control(ControlMessage {
                    status: status.success,
                    operation: status.op,
                    message: status.ret_msg,
                })
            }
            BybitPublicWsMessage::Pong(pong) => {
                return MultiMarketMessage::Control(ControlMessage {
                    status: true,
                    operation: pong.op,
                    message: pong.conn_id,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsData {
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(rename = "data")]
    pub data: Value,
    #[serde(rename = "ts")]
    pub timestamp: BybitTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsStatus {
    pub success: bool,
    pub ret_msg: String,
    pub op: String,
    pub conn_id: String,
    pub args: Option<Vec<String>>,
}

impl Into<ControlMessage> for BybitWsStatus {
    fn into(self) -> ControlMessage {
        ControlMessage {
            status: self.success,
            operation: self.op.clone(),
            message: format!("{:?} args:[{:?}]", self.ret_msg, self.args),
        }
    }
}

/*
"{\"op\":\"pong\",\"args\":[\"1707031861056\"],\"conn_id\":\"cmsuqo1qo29shn0o3qb0-44fy\"}"
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsPongReply {
    op: String,
    args: Vec<String>,
    conn_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsTradeMessage {
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(rename = "data")]
    pub data: Vec<BybitWsTrade>,
    #[serde(rename = "ts")]
    pub timestamp: BybitTimestamp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsTrade {
    #[serde(rename = "i")]
    pub trade_id: String,
    #[serde(rename = "T")]
    pub timestamp: BybitTimestamp,
    #[serde(rename = "p")]
    pub price: Decimal,
    #[serde(rename = "v")]
    pub size: Decimal,
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "BT")]
    pub is_block_trade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsOrderbook {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "b")]
    pub bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    pub asks: Vec<(Decimal, Decimal)>,
    #[serde(rename = "u")]
    pub update_id: i64,
    #[serde(rename = "seq")]
    pub sequence: i64,
    #[serde(rename = "cts")]
    pub create_timestamp: Option<BybitTimestamp>,
}

/*
    fn into(self) -> OrderBookRaw {
        let mut bids: HashMap<Decimal, Decimal> = HashMap::new();
        for item in self.bids.iter() {
            bids.insert(item.0, item.1);
        }

        let mut asks: HashMap<Decimal, Decimal> = HashMap::new();
        for item in self.asks.iter() {
            asks.insert(item.0, item.1);
        }

        let update_time = if let Some(t) = self.create_timestamp {
            bybit_timestamp_to_microsec(t)
        } else {
            0
        };

        OrderBookRaw {
            last_update_id: self.update_id as u64,
            last_update_time: update_time,
            asks: Board {
                asc: false,
                max_depth: 0,
                board: asks,
            },
            bids: Board {
                asc: true,
                max_depth: 0,
                board: bids,
            },
        }
    }
}

*/

impl Into<BoardTransfer> for BybitWsOrderbook {
    fn into(self) -> BoardTransfer {
        let mut bt = BoardTransfer::new();

        bt.last_update_id = self.update_id as u64;

        for bid in self.bids.iter() {
            bt.insert_bid(bid);
        }

        for ask in self.asks.iter() {
            bt.insert_ask(ask);
        }

        bt
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsOrderbookMessage {
    #[serde(rename = "topic")]
    pub topic: String,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(rename = "data")]
    pub data: BybitWsOrderbook,
    #[serde(rename = "ts")]
    pub timestamp: BybitTimestamp,
}

impl Into<BoardTransfer> for BybitWsOrderbookMessage {
    fn into(self) -> BoardTransfer {
        let update_id = self.data.update_id;
        let mut transfer: BoardTransfer = self.data.into();

        transfer.last_update_time = bybit_timestamp_to_microsec(self.timestamp);

        if self.message_type == "snapshot" || update_id == 1 {||
            transfer.snapshot = true;
        }
        else {
            transfer.snapshot = false;
        }

        transfer
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum BybitUserWsMessage {
    status(BybitWsStatus),
    pong(BybitWsPongReply),
    message(BybitUserMessage),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "topic")]
pub enum BybitUserMessage {
    order {
        id: String,
        //topic: String,
        creationTime: BybitTimestamp,
        data: Vec<BybitOrderStatus>,
    },
    wallet {
        id: String,
        //topic: String,
        creationTime: BybitTimestamp,
        data: Vec<BybitAccountStatus>,
    },
    execution {
        id: String,
        //topic: String,
        creationTime: BybitTimestamp,
        data: Vec<BybitExecution>,
    },
}

/*
            "category": "linear",
           "symbol": "XRPUSDT",
           "execFee": "0.005061",
           "execId": "7e2ae69c-4edf-5800-a352-893d52b446aa",
           "execPrice": "0.3374",
           "execQty": "25",
           "execType": "Trade",
           "execValue": "8.435",
           "isMaker": false,
           "feeRate": "0.0006",
           "tradeIv": "",
           "markIv": "",
           "blockTradeId": "",
           "markPrice": "0.3391",
           "indexPrice": "",
           "underlyingPrice": "",
           "leavesQty": "0",
           "orderId": "f6e324ff-99c2-4e89-9739-3086e47f9381",
           "orderLinkId": "",
           "orderPrice": "0.3207",
           "orderQty": "25",
           "orderType": "Market",
           "stopOrderType": "UNKNOWN",
           "side": "Sell",
           "execTime": "1672364174443",
           "isLeverage": "0",
           "closedSize": "",
           "seq": 4688002127
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitExecution {
    pub category: String,
    pub symbol: String,
    pub orderId: String,
    pub orderLinkId: String,
    pub side: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub orderPrice: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub orderQty: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub leavesQty: Decimal,
    pub orderType: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub execFee: Decimal,
    pub execId: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub execPrice: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub execQty: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub execValue: Decimal,
    #[serde(deserialize_with = "string_to_i64")]
    pub execTime: BybitTimestamp,
    pub isMaker: bool,
    #[serde(deserialize_with = "string_to_decimal")]
    pub feeRate: Decimal,
    pub seq: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitOrderUpdateMessage {
    pub id: String,
    pub topic: String,
    pub creationTime: BybitTimestamp,
    pub data: Vec<BybitOrderStatus>,
}

impl BybitOrderUpdateMessage {
    pub fn convert_to_market_message(&self, _config: &MarketConfig) -> Vec<MarketMessage> {
        let mut message: Vec<MarketMessage> = vec![];

        for order in self.data.iter() {
            let o: Order = order.into();

            let market_message = MarketMessage::Order(o);

            message.push(market_message);
        }

        message
    }
}

pub fn merge_order_and_execution(
    order: &Vec<BybitOrderStatus>,
    execution: &Vec<BybitExecution>,
) -> Vec<Order> {
    if order.len() != execution.len() {
        log::warn!("merge order and execution in different size");
    }

    let mut execution_map: HashMap<String, BybitExecution> = HashMap::new();

    for exec in execution.iter() {
        execution_map.insert(exec.orderId.clone(), exec.clone());
    }

    let mut m: Vec<Order> = vec![];

    for o in order.iter() {
        let exec = execution_map.get(&o.orderId).unwrap();

        let mut order: Order = o.into();

        order.is_maker = exec.isMaker;
        m.push(order);
    }

    m
}

/*
{"id":"100467532_wallet_1704705368721","topic":"wallet","creationTime":1704705368720,"data":[{"accountIMRate":"0.0312","accountMMRate":"0.0017","totalEquity":"10011.98943823","totalWalletBalance":"10003.19038373","totalMarginBalance":"10011.98943823","totalAvailableBalance":"9698.9208178","totalPerpUPL":"8.79905449","totalInitialMargin":"313.06862043","totalMaintenanceMargin":"17.32645111","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"10007.37603788","usdValue":"10011.98943823","walletBalance":"9998.58103788","availableToWithdraw":"9694.45167558","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"40.418","totalPositionIM":"272.5063623","totalPositionMM":"14.9004673","unrealisedPnl":"8.795","cumRealisedPnl":"-1.41896212","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}]}
*/

/*
 {"id":"100467532_wallet_1704710084610","topic":"wallet","creationTime":1704710084610,
 "data":[{"accountIMRate":"0.0774","accountMMRate":"0.0043","totalEquity":"10024.53809208","totalWalletBalance":"10002.88388718","totalMarginBalance":"10024.53809208","totalAvailableBalance":"9248.37416492","totalPerpUPL":"21.65420489","totalInitialMargin":"776.16392715","totalMaintenanceMargin":"43.89672512","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"10019.01761338","usdValue":"10024.53809208","walletBalance":"9997.37533338","availableToWithdraw":"9243.28111703","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"282.926","totalPositionIM":"492.81049635","totalPositionMM":"26.94655135","unrealisedPnl":"21.64228","cumRealisedPnl":"-2.62466662","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}]}
*/
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitAccountStatus {
    pub accountType: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub accountLTV: Decimal, // account total borrowed size / (account total equity + account total borrowed size). In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub accountIMRate: Decimal, // account initial margin rate. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub accountMMRate: Decimal, // account maintenance margin rate. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalEquity: Decimal, // account total equity. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalWalletBalance: Decimal, // account total wallet balance. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalMarginBalance: Decimal, // account total margin balance. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalAvailableBalance: Decimal, // account total available balance. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalPerpUPL: Decimal, // account total unrealized PnL. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalInitialMargin: Decimal, // account total initial margin. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    #[serde(deserialize_with = "string_to_decimal")]
    pub totalMaintenanceMargin: Decimal, // account total maintenance margin. In non-unified mode & unified (inverse) & unified (isolated_margin), the field will be returned as an empty string.
    pub coin: Vec<BybitAccountCoin>,
}

impl Into<AccountCoins> for BybitAccountStatus {
    fn into(self) -> AccountCoins {
        let mut coins: Vec<Coin> = vec![];
        for coin in self.coin.iter() {
            let c: Coin = coin.into();
            coins.push(c);
        }

        AccountCoins { coins: coins }
    }
}

pub fn convert_coin_to_account_status(
    config: &MarketConfig,
    coins: Vec<BybitAccountCoin>,
) -> AccountPair {
    let mut account = AccountPair::default();

    for coin in coins.iter() {
        if coin.coin == config.home_currency {
            account.home.volume = coin.equity;
            account.home.free = coin.availableToWithdraw;
            account.home.locked = coin.locked;
        } else if coin.coin == config.foreign_currency {
            account.foreign.volume = coin.equity;
            account.foreign.free = coin.availableToWithdraw;
            account.foreign.locked = coin.locked;
        }
    }

    account
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitAccountCoin {
    coin: String,
    #[serde(deserialize_with = "string_to_decimal")]
    equity: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    usdValue: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    walletBalance: Decimal,
    free: Option<String>,
    #[serde(deserialize_with = "string_to_decimal")]
    locked: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    spotHedgingQty: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    borrowAmount: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    availableToWithdraw: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    availableToBorrow: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    accruedInterest: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    totalOrderIM: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    totalPositionIM: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    unrealisedPnl: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    cumRealisedPnl: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    bonus: Decimal,
    collateralSwitch: bool,
    marginCollateral: bool,
}

impl Into<Coin> for &BybitAccountCoin {
    fn into(self) -> Coin {
        Coin {
            symbol: self.coin.clone(),
            volume: self.equity,
            free: self.availableToWithdraw,
            locked: self.locked,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitAccountResponse {
    list: Vec<BybitAccountResponseList>

 //   coin: Vec<BybitAccountCoin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitAccountResponseList {
    coin: Vec<BybitAccountCoin>,
}



// TODO: check if flatten is OK.
impl Into<AccountCoins> for BybitAccountResponse {
    fn into(self) -> AccountCoins {
        let mut coins: Vec<Coin> = vec![];
        for l in self.list {
            for coin in l.coin.iter() {
            let c: Coin = coin.into();
            coins.push(c);
            }
        }

        AccountCoins { coins: coins }
    }
}


#[cfg(test)]
#[allow(non_snake_case)]
#[allow(unused_imports)]
#[allow(unused_variables)]
mod bybit_message_test {
    use super::*;
    use rbot_lib::common::init_debug_log;

    use crate::message::{
        BybitAccountStatus, BybitExecution, BybitMultiOrderStatus, BybitRestResponse,
        BybitTradeResponse, BybitUserMessage, BybitUserWsMessage, BybitWsData, BybitWsOrderbook,
        BybitWsStatus, BybitWsTrade,
    };

    use super::{BybitPublicWsMessage, BybitRestBoard};

    #[test]
    fn test_bybit_rest_response() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"s":"BTCUSDT","a":[["29727.05","0.000069"],["29741.86","0.001922"],["29745","0.475333"],["29752.12","0.729903"],["29752.65","0.000553"]],"b":[["29714.9","0.0008"],["29636.2","0.001052"],["29636.1","0.001034"],["29635.9","0.001625"],["29620.9","0.002498"]],"ts":1703314210368,"u":1458765},"retExtInfo":{},"time":1703314210368}        
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    /// curl "https://api-testnet.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200"
    fn test_binance_board_message() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"s":"BTCUSDT","a":[["29727.05","0.000069"],["29741.86","0.001922"],["29745","0.475333"],["29752.12","0.729903"],["29752.65","0.000553"]],"b":[["29714.9","0.0008"],["29636.2","0.001052"],["29636.1","0.001034"],["29635.9","0.001625"],["29620.9","0.002498"]],"ts":1703314210368,"u":1458765},"retExtInfo":{},"time":1703314210368}        
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        assert!(result.is_ok());

        let result = result.unwrap();

        let message = result.body;

        let result = serde_json::from_value::<BybitRestBoard>(message);

        assert!(result.is_ok());

        println!("{:?}", result);
    }

    /// curl "https://api-testnet.bybit.com/v5/market/recent-trade?category=spot&symbol=BTCUSDT&limit=1000"
    #[test]
    fn test_binance_recent_trade_message() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"category":"spot","list":[{"execId":"2100000000055371969","symbol":"BTCUSDT","price":"29741.75","size":"0.002148","side":"Sell","time":"1703318475711","isBlockTrade":false},{"execId":"2100000000055371968","symbol":"BTCUSDT","price":"29741.76","size":"0.003827","side":"Sell","time":"1703318473308","isBlockTrade":false},{"execId":"2100000000055371967","symbol":"BTCUSDT","price":"29741.77","size":"0.001804","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371966","symbol":"BTCUSDT","price":"29741.77","size":"0.002185","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371965","symbol":"BTCUSDT","price":"29741.78","size":"0.00227","side":"Sell","time":"1703318471505","isBlockTrade":false}]},"retExtInfo":{},"time":1703318475875}
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        println!("{:?}", result);
        assert!(result.is_ok());

        let message = result.unwrap().body;

        let result = serde_json::from_value::<BybitTradeResponse>(message);
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_binance_recent_trade_only() {
        let message = r#"
        {"category":"spot","list":[{"execId":"2100000000055371969","symbol":"BTCUSDT","price":"29741.75","size":"0.002148","side":"Sell","time":"1703318475711","isBlockTrade":false},{"execId":"2100000000055371968","symbol":"BTCUSDT","price":"29741.76","size":"0.003827","side":"Sell","time":"1703318473308","isBlockTrade":false},{"execId":"2100000000055371967","symbol":"BTCUSDT","price":"29741.77","size":"0.001804","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371966","symbol":"BTCUSDT","price":"29741.77","size":"0.002185","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371965","symbol":"BTCUSDT","price":"29741.78","size":"0.00227","side":"Sell","time":"1703318471505","isBlockTrade":false}]}
        "#;

        let result = serde_json::from_str::<BybitTradeResponse>(&message);
        println!("{:?}", result);
    }
    /*
        const BYBIT_TRADE_MESSAGE: &str = r#"{"success":true,"ret_msg":"subscribe","conn_id":"6c642bd0-3fa2-408e-8617-3d62cb898d4c","op":"subscribe"}
    {"topic":"publicTrade.BTCUSDT","ts":1703430744103,"type":"snapshot","data":[{"i":"2290000000090712222","T":1703430744102,"p":"43774","v":"0.0026","S":"Sell","s":"BTCUSDT","BT":false}]}
    {"topic":"publicTrade.BTCUSDT","ts":1703430745372,"type":"snapshot","data":[{"i":"2290000000090712223","T":1703430745370,"p":"43774.01","v":"0.009516","S":"Buy","s":"BTCUSDT","BT":false}]}
    "#;
    */

    const BYBIT_TRADE_1: &str = r#"{"topic":"publicTrade.BTCUSDT","ts":1703430744103,"type":"snapshot","data":[{"i":"2290000000090712222","T":1703430744102,"p":"43774","v":"0.0026","S":"Sell","s":"BTCUSDT","BT":false}]}"#;

    /*
        const BYBIT_ORDERBOOK: &str = r#"{"success":true,"ret_msg":"subscribe","conn_id":"4492c76f-36ec-4e93-afa9-39d7d871d5d2","op":"subscribe"}
    {"topic":"orderbook.200.BTCUSDT","ts":1703430557696,"type":"snapshot","data":{"s":"BTCUSDT","b":[["43732.01","0.049391"],["43731.97","0.000609"],["43730","0.1101"],["43729.91","0.0015"],["43728.18","0.270649"],["43728.17","0.049996"],["43728","0.1101"],["43726.91","0.0015"],["43725.35","1.087502"],["43725.34","0.5"],["43725.32","0.5"],["43725.31","0.022866"],["43724.61","0.092303"],["43724.1","0.070238"],["43724.09","0.049"],["43724.08","0.004977"],["43724.07","0.003092"],["43723.92","0.153106"],["43723.91","0.0015"],["43723.63","0.1143"],["43722.82","0.011433"],["43720.93","0.171916"],["43720.92","0.079042"],["43720.91","0.0015"],["43720.53","0.000094"],["43720.25","0.027906"],["43718.3","0.221104"],["43717.91","0.0015"],["43717.88","0.009176"],["43717.64","1.725985"],["43717.63","0.525"],["43717.47","0.793556"],["43717","0.000281"],["43716.69","0.002326"],["43716.33","0.14254"],["43715.62","0.88"],["43715.22","0.511362"],["43715.2","0.027395"],["43714.91","0.0015"],["43714.35","1.935754"],["43714.34","0.89"],["43714.27","0.119008"],["43714.26","0.000236"],["43713.25","0.525"],["43712.82","0.454936"],["43711.92","0.113636"],["43711.91","0.0015"],["43711.26","0.5"],["43708.91","0.0015"],["43708.7","0.224061"],["43708.5","0.525"],["43708.04","0.840012"],["43707.89","3.125"],["43707.7","0.685953"],["43707.51","0.113636"],["43706.41","0.511362"],["43706.32","0.044552"],["43705.91","0.0015"],["43705.2","0.456799"],["43704.72","0.00005"],["43703.1","0.113636"],["43702.91","0.0015"],["43702.71","0.2"],["43702.61","0.2"],["43702.08","0.22919"],["43701.98","0.003092"],["43701.7","0.023854"],["43701.63","0.525"],["43700.09","0.038893"],["43700","0.131602"],["43699.91","0.0015"],["43699.08","0.373664"],["43699","0.000411"],["43698.69","0.113636"],["43697.61","3.03253"],["43697.6","0.511362"],["43697.09","1.458185"],["43696.91","0.0015"],["43695.99","0.001002"],["43694.7","0.525"],["43694.67","1.141999"],["43694.56","0.622616"],["43694.3","0.278599"],["43693.91","0.0015"],["43692.51","0.011866"],["43692.48","1.933829"],["43692.47","0.889116"],["43691.1","0.182719"],["43690.91","0.0015"],["43690.59","0.004589"],["43690.01","0.257097"],["43690","0.525"],["43688.79","0.511362"],["43688.17","0.000987"],["43687.91","0.0015"],["43687.5","0.000693"],["43685.95","2.23742"],["43685.85","0.011936"],["43685.63","0.525"],["43683.33","0.000825"],["43683.31","0.883286"],["43683.14","0.031359"],["43682.63","0.883509"],["43681.8","0.001987"],["43681.76","0.002031"],["43681.56","3.197596"],["43680.42","0.142757"],["43680.24","0.0031"],["43680","0.000558"],["43679.98","0.511362"],["43679.96","0.000591"],["43679.66","0.631129"],["43678.92","0.000048"],["43677.9","0.567432"],["43677.52","0.525"],["43676.69","0.178326"],["43676.48","0.038905"],["43675","0.000765"],["43674.71","0.003118"],["43674.36","0.000607"],["43673.99","0.000111"],["43673.57","0.178446"],["43672.52","0.525"],["43671.68","0.691331"],["43671.67","0.633662"],["43671.4","0.004094"],["43671.17","0.340908"],["43671.15","0.292488"],["43670.13","0.000527"],["43668.46","1.598798"],["43667.84","0.012126"],["43667.58","0.00005"],["43666.66","0.006544"],["43666.65","0.001384"],["43666.64","0.009062"],["43666.62","0.003046"],["43666.6","0.001085"],["43666.52","0.009102"],["43666.48","0.000636"],["43663.6","0.000996"],["43662.36","0.340908"],["43662.21","0.000987"],["43661.66","0.00157"],["43661.59","0.000445"],["43661.5","0.824216"],["43660.34","0.001071"],["43660","0.160919"],["43659.72","0.000089"],["43658.51","0.00314"],["43658.49","0.002291"],["43658.4","0.000552"],["43658.37","0.002658"],["43658","0.002003"],["43657.21","3.00464"],["43656.3","0.623071"],["43656","0.000775"],["43655.99","0.000086"],["43655.38","0.002025"],["43654.78","1.199158"],["43653.88","0.002003"],["43653.8","0.000442"],["43653.6","0.000458"],["43653.55","0.340908"],["43652.11","0.000229"],["43651.04","0.000979"],["43650.23","0.011936"],["43650","0.094281"],["43649.89","0.000919"],["43649.86","0.000432"],["43648.9","0.824216"],["43648.7","0.002087"],["43648.64","0.000048"],["43648.51","0.001678"],["43648.32","0.002725"],["43648.14","0.000221"],["43647.51","0.000712"],["43647.38","0.015993"],["43645.12","0.001429"],["43644.77","0.000069"],["43644.74","0.340908"],["43644.62","0.000995"],["43644.08","0.000645"],["43643.91","0.008024"],["43642.91","0.001851"],["43642.85","0.001552"],["43642.84","0.000513"],["43642.8","0.00132"],["43642.76","0.000811"],["43642.7","0.000869"],["43642.68","0.002062"],["43642.42","0.001513"],["43642.14","0.621927"],["43641.9","0.824216"],["43641.45","0.000381"],["43641.29","0.00087"],["43640.1","0.003091"],["43640","0.000394"],["43639.51","0.000961"],["43639.02","0.029316"],["43636.76","0.00308"]],"a":[["43732.02","7.191274"],["43732.03","1.538305"],["43732.04","0.005"],["43732.92","0.08"],["43732.97","0.19"],["43733.67","0.413254"],["43733.68","0.054941"],["43733.94","0.004948"],["43733.97","0.19"],["43734","0.1101"],["43734.01","0.1113"],["43734.18","0.19"],["43734.26","0.011433"],["43734.37","1.491977"],["43734.38","0.026744"],["43735.01","0.685966"],["43735.18","0.08"],["43735.65","0.091402"],["43735.66","0.091373"],["43735.9","0.033"],["43735.91","0.0015"],["43736","0.1101"],["43736.01","0.5251"],["43736.47","0.006"],["43736.77","0.006"],["43737.21","0.08"],["43737.79","0.011433"],["43737.84","0.247158"],["43737.85","0.022864"],["43738","0.1101"],["43738.23","0.011433"],["43738.38","0.113636"],["43738.91","0.0015"],["43738.98","0.072234"],["43739.17","0.027452"],["43739.26","0.066495"],["43739.99","0.488862"],["43740","0.1101"],["43740.01","0.033925"],["43740.64","0.224763"],["43741.65","0.511362"],["43741.87","0.224061"],["43741.91","0.0015"],["43741.97","0.011361"],["43741.99","1.495128"],["43742","0.1101"],["43742.32","0.701617"],["43742.79","0.113636"],["43743.55","1.599775"],["43743.89","0.801205"],["43743.9","0.036"],["43744","0.1101"],["43744.01","0.525"],["43744.91","0.0015"],["43745.19","0.468209"],["43745.36","0.031161"],["43745.53","0.071969"],["43746","0.1101"],["43746.59","0.247167"],["43746.6","0.022861"],["43747.06","0.034013"],["43747.2","0.113636"],["43747.54","1.491604"],["43747.59","0.050462"],["43747.9","0.077823"],["43747.91","0.0015"],["43747.92","0.456799"],["43748","0.1101"],["43748.39","0.525"],["43748.41","0.88"],["43749.5","0.278599"],["43749.99","0.1"],["43750.05","1.957508"],["43750.06","0.700795"],["43750.15","0.16649"],["43750.4","0.9"],["43750.46","0.511362"],["43750.91","0.0015"],["43751.61","0.113636"],["43751.7","3.125"],["43752","0.1101"],["43752.5","0.08"],["43753.91","0.0015"],["43753.99","0.055014"],["43754","0.1101"],["43754.01","0.525"],["43754.02","0.08"],["43754.39","0.823726"],["43754.44","0.002921"],["43754.5","0.567432"],["43754.76","0.47"],["43755.35","0.022855"],["43755.69","0.025385"],["43756.01","0.08"],["43756.02","0.113636"],["43756.91","0.0015"],["43756.92","0.08"],["43757.63","0.702775"],["43758.56","1.141999"],["43759.27","0.511362"],["43759.6","0.023203"],["43759.66","1.734988"],["43759.91","0.0015"],["43759.93","0.08"],["43760.24","3.197596"],["43760.35","1.141999"],["43760.43","0.113636"],["43760.48","0.525"],["43761.87","0.015"],["43762.87","1.141999"],["43762.91","0.0015"],["43763.41","1.497646"],["43764.5","0.11"],["43764.84","0.113636"],["43765.4","0.710682"],["43765.9","0.091432"],["43765.91","0.0015"],["43766.45","0.525"],["43767.49","0.002326"],["43767.75","0.182719"],["43768.08","0.511362"],["43768.17","0.525"],["43768.6","0.023856"],["43768.91","0.0015"],["43769.25","0.113636"],["43769.28","0.2"],["43769.38","0.2"],["43771.09","0.2"],["43771.77","0.031127"],["43771.91","0.0015"],["43772.12","1.604389"],["43772.64","0.71226"],["43773.36","1.598798"],["43773.66","0.113636"],["43773.8","0.824216"],["43774.01","0.525"],["43774.91","0.0015"],["43775.15","0.011936"],["43775.78","0.028345"],["43776.88","0.285659"],["43776.89","0.511362"],["43777.01","0.000301"],["43777.91","0.0015"],["43778.07","0.113636"],["43779.42","0.178446"],["43779.87","0.011863"],["43780.38","0.525"],["43781.22","0.292488"],["43781.3","0.031018"],["43781.67","0.227395"],["43782.48","0.113636"],["43783.74","0.025"],["43785.7","0.511362"],["43785.8","0.068042"],["43785.82","0.142757"],["43786.5","1.598798"],["43786.89","0.113636"],["43788.01","0.525"],["43788.07","0.030019"],["43789.21","0.002283"],["43789.61","0.178326"],["43791.01","0.006855"],["43791.3","0.113636"],["43794.12","0.525"],["43794.51","0.511362"],["43795.01","0.006855"],["43795.71","0.113636"],["43798.54","0.000157"],["43798.78","0.006855"],["43799","0.000283"],["43799.14","3.415467"],["43799.28","0.008568"],["43799.73","0.025016"],["43800","0.000675"],["43800.12","0.113636"],["43800.61","0.027424"],["43800.79","0.080008"],["43800.93","0.526765"],["43801.13","0.035021"],["43802","0.0022"],["43803.32","0.511362"],["43804","0.002398"],["43805.32","0.525"],["43805.99","0.000125"],["43810.39","0.525"],["43812.13","0.340908"],["43813.4","0.824216"],["43815.65","0.525"],["43816.62","0.00456"],["43816.95","1.217375"],["43818.47","0.030519"],["43820.43","0.525"],["43820.94","0.340908"],["43821.97","0.022601"],["43823.58","0.000048"],["43823.77","0.003064"],["43825.59","0.525"],["43825.72","0.00009"],["43829.75","0.340908"],["43829.92","0.003829"]],"u":5179079,"seq":19967460822},"cts":1703430557664}
    {"topic":"orderbook.200.BTCUSDT","ts":1703430557896,"type":"delta","data":{"s":"BTCUSDT","b":[["43728.19","0.5"],["43725.35","0"],["43725.34","0"],["43725.32","0"],["43724.11","0.333012"],["43724.1","0"],["43720.3","0.034717"],["43636.36","0.027705"]],"a":[["43736.01","0.525"],["43743.03","0.009152"],["43752.77","0.5"],["43829.75","0"],["43829.92","0"]],"u":5179080,"seq":19967461033},"cts":1703430557847}
    "#;
    */

    const BYBIT_STATUS: &str = r#"{"success":true,"ret_msg":"subscribe","conn_id":"4492c76f-36ec-4e93-afa9-39d7d871d5d2","op":"subscribe"}"#;

    const BYBIT_ORDERBOOK_1: &str = r#"{"topic":"orderbook.200.BTCUSDT","ts":1703430557896,"type":"delta","data":{"s":"BTCUSDT","b":[["43728.19","0.5"],["43725.35","0"],["43725.34","0"],["43725.32","0"],["43724.11","0.333012"],["43724.1","0"],["43720.3","0.034717"],["43636.36","0.027705"]],"a":[["43736.01","0.525"],["43743.03","0.009152"],["43752.77","0.5"],["43829.75","0"],["43829.92","0"]],"u":5179080,"seq":19967461033},"cts":1703430557847}"#;

    const BYBIT_ORDER_1: &str = r#"{"topic":"publicTrade.BTCUSDT","ts":1703430744103,"type":"snapshot","data":[{"i":"2290000000090712222","T":1703430744102,"p":"43774","v":"0.0026","S":"Sell","s":"BTCUSDT","BT":false}]}"#;

    #[test]
    fn test_bynance_trade_message() {
        let result = serde_json::from_str::<BybitWsStatus>(BYBIT_STATUS);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_str::<BybitWsData>(BYBIT_ORDERBOOK_1);
        assert!(result.is_ok());
        println!("{:?}", result);

        let value = result.unwrap().data;
        println!("{:?}", value);
        let result = serde_json::from_value::<BybitWsOrderbook>(value);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_str::<BybitWsData>(BYBIT_ORDER_1);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_value::<Vec<BybitWsTrade>>(result.unwrap().data);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_str::<BybitPublicWsMessage>(BYBIT_STATUS);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_str::<BybitPublicWsMessage>(BYBIT_ORDERBOOK_1);
        assert!(result.is_ok());
        println!("{:?}", result);

        let result = serde_json::from_str::<BybitPublicWsMessage>(BYBIT_TRADE_1);
        assert!(result.is_ok());
        println!("{:?}", result);
    }

    #[test]
    fn test_parse_multi_order_status() {
        let message = r#"
        {"retCode":0,
        "retMsg":"OK",
        "result":
        {"nextPageCursor":"36a2f9ec-700c-481e-bf05-5df9802bad86%3A1704539225091%2C550a465a-5a0d-4cac-b66e-733fa029166c%3A1704539202055",
        "category":"linear",
        "list":
        [{"symbol":"BTCUSDT","orderType":"Limit","orderLinkId":"",
        "slLimitPrice":"0","orderId":"36a2f9ec-700c-481e-bf05-5df9802bad86",
        "cancelType":"UNKNOWN","avgPrice":"","stopOrderType":"",
        "lastPriceOnCreated":"43634.8","orderStatus":"New",
        "createType":"CreateByUser","takeProfit":"",
        "cumExecValue":"0","tpslMode":"","smpType":"None",
        "triggerDirection":0,"blockTradeId":"",
        "isLeverage":"","rejectReason":"EC_NoError",
        "price":"40000","orderIv":"",
        "createdTime":"1704539225091",
        "tpTriggerBy":"","positionIdx":0,
        "timeInForce":"GTC",
        "leavesValue":"40",
        "updatedTime":"1704539225094",
        "side":"Buy",
        "smpGroup":0,"triggerPrice":"",
        "tpLimitPrice":"0","cumExecFee":"0",
        "leavesQty":"0.001","slTriggerBy":"",
        "closeOnTrigger":false,
        "placeType":"","cumExecQty":"0",
        "reduceOnly":false,
        "qty":"0.001","stopLoss":"",
        "marketUnit":"","smpOrderId":"",
        "triggerBy":""},
        {"symbol":"BTCUSDT","orderType":"Limit","orderLinkId":"","slLimitPrice":"0","orderId":"550a465a-5a0d-4cac-b66e-733fa029166c","cancelType":"UNKNOWN","avgPrice":"","stopOrderType":"","lastPriceOnCreated":"43634.9","orderStatus":"New","createType":"CreateByUser","takeProfit":"","cumExecValue":"0","tpslMode":"","smpType":"None","triggerDirection":0,"blockTradeId":"","isLeverage":"","rejectReason":"EC_NoError","price":"40000","orderIv":"","createdTime":"1704539202055","tpTriggerBy":"","positionIdx":0,"timeInForce":"GTC","leavesValue":"40","updatedTime":"1704539202058","side":"Buy","smpGroup":0,"triggerPrice":"","tpLimitPrice":"0","cumExecFee":"0","leavesQty":"0.001","slTriggerBy":"","closeOnTrigger":false,"placeType":"","cumExecQty":"0","reduceOnly":false,"qty":"0.001","stopLoss":"","marketUnit":"","smpOrderId":"","triggerBy":""}]},"retExtInfo":{},"time":1704541422547}
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        assert!(result.is_ok());

        let result = result.unwrap();

        let message = result.body;

        println!("{:?}", message);

        let result = serde_json::from_value::<BybitMultiOrderStatus>(message);

        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_user_stream_data() {
        // Control message
        let message: &str = r#"{"success":true,"ret_msg":"","op":"subscribe","conn_id":"cm6ickhqo29n65o1kpog-74ew"}"#;
        let result = serde_json::from_str::<BybitUserWsMessage>(message);
        println!("{:?}", result);
        assert!(result.is_ok());

        // Wallet message
        let message: &str = r#"
        {"accountIMRate":"0.0929","accountMMRate":"0.0052","totalEquity":"10140.2751031","totalWalletBalance":"10005.94544876","totalMarginBalance":"10140.2751031","totalAvailableBalance":"9197.25798225","totalPerpUPL":"134.32965434","totalInitialMargin":"943.01712085","totalMaintenanceMargin":"53.64509097","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"10131.33926188","usdValue":"10140.2751031","walletBalance":"9997.12798188","availableToWithdraw":"9189.15314918","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"404.18","totalPositionIM":"538.0061127","totalPositionMM":"29.4178177","unrealisedPnl":"134.21128","cumRealisedPnl":"-2.87201812","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}
        "#;
        let result = serde_json::from_str::<BybitAccountStatus>(message);
        println!("{:?}", result);
        assert!(result.is_ok());

        // wallet all
        let message: &str = r#"
        {"id":"100467532_wallet_1704721219498","topic":"wallet","creationTime":1704721219498,"data":[{"accountIMRate":"0.0929","accountMMRate":"0.0052","totalEquity":"10140.2751031","totalWalletBalance":"10005.94544876","totalMarginBalance":"10140.2751031","totalAvailableBalance":"9197.25798225","totalPerpUPL":"134.32965434","totalInitialMargin":"943.01712085","totalMaintenanceMargin":"53.64509097","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"10131.33926188","usdValue":"10140.2751031","walletBalance":"9997.12798188","availableToWithdraw":"9189.15314918","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"404.18","totalPositionIM":"538.0061127","totalPositionMM":"29.4178177","unrealisedPnl":"134.21128","cumRealisedPnl":"-2.87201812","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}]}
        "#;

        let result = serde_json::from_str::<BybitUserWsMessage>(message);
        println!("{:?}", result);
        assert!(result.is_ok());

        // Execution
        let message = r#"{
            "category":"linear",
            "symbol":"BTCUSDT",
            "closedSize":"0",
            "execFee":"0.02285465",
            "execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699",
            "execPrice":"41553.9",
            "execQty":"0.001",
            "execType":"Trade",
            "execValue":"41.5539",
            "feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}"#;
        let result = serde_json::from_str::<BybitExecution>(message);
        println!("{:?}", result);
        assert!(result.is_ok());

        let message = r#"{"topic":"execution",
        "id":"100467532_BTCUSDT_8883610598",
        "creationTime":1705761437507,
        "data":[
            {"category":"linear","symbol":"BTCUSDT","closedSize":"0","execFee":"0.02285465","execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699","execPrice":"41553.9","execQty":"0.001","execType":"Trade","execValue":"41.5539","feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}]}"#;
        let result = serde_json::from_str::<BybitUserWsMessage>(message);
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test__user_message() {
        let message = r#"{"id":"100467532_wallet_1705725452732","topic":"wallet","creationTime":1705725452731,"data":[{"accountIMRate":"0.0696","accountMMRate":"0.0038","totalEquity":"9593.08110909","totalWalletBalance":"10248.69855009","totalMarginBalance":"9593.08110909","totalAvailableBalance":"8925.14525897","totalPerpUPL":"-655.61744099","totalInitialMargin":"667.93585011","totalMaintenanceMargin":"36.52228963","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"9597.90885725","usdValue":"9593.08110909","walletBalance":"10253.85623978","availableToWithdraw":"8929.63686632","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"668.27199093","totalPositionMM":"36.54066959","unrealisedPnl":"-655.94738253","cumRealisedPnl":"253.85623978","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}]}"#;
        let result = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", result);

        let message = r#"{"topic":"order","id":"100467532_BTCUSDT_8883348664","creationTime":1705740966799,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"6e77763c-5589-41de-b52b-36358a577c6d","orderLinkId":"","blockTradeId":"","side":"Sell","positionIdx":0,"orderStatus":"Filled","cancelType":"UNKNOWN","rejectReason":"EC_NoError","timeInForce":"IOC","isLeverage":"","price":"39484.4","qty":"0.001","avgPrice":"41562","leavesQty":"0","leavesValue":"0","cumExecQty":"0.001","cumExecValue":"41.562","cumExecFee":"0.0228591","orderType":"Market","stopOrderType":"","orderIv":"","triggerPrice":"","takeProfit":"","stopLoss":"","triggerBy":"","tpTriggerBy":"","slTriggerBy":"","triggerDirection":0,"placeType":"","lastPriceOnCreated":"41562.5","closeOnTrigger":true,"reduceOnly":true,"smpGroup":0,"smpType":"None","smpOrderId":"","slLimitPrice":"0","tpLimitPrice":"0","tpslMode":"UNKNOWN","createType":"CreateByClosing","marketUnit":"","createdTime":"1705740966794","updatedTime":"1705740966797","feeCurrency":""}]}"#;
        let result = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", result);

        let message = r#"{"id":"100467532_wallet_1705740966800","topic":"wallet","creationTime":1705740966800,"data":[{"accountIMRate":"0.0692","accountMMRate":"0.0037","totalEquity":"9585.24656517","totalWalletBalance":"10244.82447339","totalMarginBalance":"9585.24656517","totalAvailableBalance":"8921.8608955","totalPerpUPL":"-659.57790822","totalInitialMargin":"663.38566967","totalMaintenanceMargin":"36.27348878","coin":[{"coin":"USDC","equity":"0","usdValue":"0","walletBalance":"0","availableToWithdraw":"0","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"0","totalPositionMM":"0","unrealisedPnl":"0","cumRealisedPnl":"0","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"},{"coin":"USDT","equity":"9588.79441911","usdValue":"9585.24656517","walletBalance":"10248.61646149","availableToWithdraw":"8925.16320589","availableToBorrow":"","borrowAmount":"0","accruedInterest":"0","totalOrderIM":"0","totalPositionIM":"663.63121322","totalPositionMM":"36.28691494","unrealisedPnl":"-659.82204238","cumRealisedPnl":"248.61646149","bonus":"0","collateralSwitch":true,"marginCollateral":true,"locked":"0","spotHedgingQty":"0"}],"accountLTV":"0","accountType":"UNIFIED"}]}"#;
        let result = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", result);

        let message = r#"{"topic":"execution","id":"100467532_BTCUSDT_8883610598","creationTime":1705761437507,"data":[{"category":"linear","symbol":"BTCUSDT","closedSize":"0","execFee":"0.02285465","execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699","execPrice":"41553.9","execQty":"0.001","execType":"Trade","execValue":"41.5539","feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}]}"#;
        let result = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", result);
    }

    #[test]
    fn test_bybit_execution() {
        let message = r#"{"category":"linear","symbol":"BTCUSDT","closedSize":"0","execFee":"0.02285465","execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699","execPrice":"41553.9","execQty":"0.001","execType":"Trade","execValue":"41.5539","feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}"#;
        let result = serde_json::from_str::<BybitExecution>(message);
        println!("{:?}", result);
    }

    #[test]
    fn test_bybit_order() {
        let message = r#"{"topic":"order","id":"100467532_BTCUSDT_8883348664","creationTime":1705740966799,"data":[{"category":"linear","symbol":"BTCUSDT","orderId":"6e77763c-5589-41de-b52b-36358a577c6d","orderLinkId":"","blockTradeId":"","side":"Sell","positionIdx":0,"orderStatus":"Filled","cancelType":"UNKNOWN","rejectReason":"EC_NoError","timeInForce":"IOC","isLeverage":"","price":"39484.4","qty":"0.001","avgPrice":"41562","leavesQty":"0","leavesValue":"0","cumExecQty":"0.001","cumExecValue":"41.562","cumExecFee":"0.0228591","orderType":"Market","stopOrderType":"","orderIv":"","triggerPrice":"","takeProfit":"","stopLoss":"","triggerBy":"","tpTriggerBy":"","slTriggerBy":"","triggerDirection":0,"placeType":"","lastPriceOnCreated":"41562.5","closeOnTrigger":true,"reduceOnly":true,"smpGroup":0,"smpType":"None","smpOrderId":"","slLimitPrice":"0","tpLimitPrice":"0","tpslMode":"UNKNOWN","createType":"CreateByClosing","marketUnit":"","createdTime":"1705740966794","updatedTime":"1705740966797","feeCurrency":""}]}"#;
        let order = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", order);

        let message = r#"{"topic":"execution","id":"100467532_BTCUSDT_8883610598","creationTime":1705761437507,"data":[{"category":"linear","symbol":"BTCUSDT","closedSize":"0","execFee":"0.02285465","execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699","execPrice":"41553.9","execQty":"0.001","execType":"Trade","execValue":"41.5539","feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}]}"#;
        let execution = serde_json::from_str::<BybitUserMessage>(message);
        println!("{:?}", execution);
    }

    #[test]
    fn test_bybit_order_and_execution() {
        let message = r#"
        [{"orderId":"43fac7fc-e2ae-4e80-bb50-0f2ff171fcc0","orderLinkId":"","blockTradeId":"","symbol":"BTCUSDT","price":"40643.7","qty":"0.001","side":"Sell","isLeverage":"","positionIdx":0,"orderStatus":"Filled","cancelType":"UNKNOWN","rejectReason":"EC_NoError","avgPrice":"42782","leavesQty":"0","leavesValue":"0","cumExecQty":"0.001","cumExecValue":"42.782","cumExecFee":"0.0235301","timeInForce":"IOC","orderType":"Market","stopOrderType":"","orderIv":"","triggerPrice":"0","takeProfit":"0","stopLoss":"0","tpTriggerBy":"","slTriggerBy":"","triggerDirection":0,"triggerBy":"","lastPriceOnCreated":"42782.8","reduceOnly":true,"closeOnTrigger":true,"smpType":"None","smpGroup":0,"smpOrderId":"","tpslMode":"UNKNOWN","tpLimitPrice":"0","slLimitPrice":"0","placeType":"","createdTime":1707036474698,"updatedTime":1707036474702}]
        "#;
        let order = serde_json::from_str::<Vec<BybitOrderStatus>>(message);
        println!("{:?}", order);

        let message = r#"        
        [{"category":"linear","symbol":"BTCUSDT","orderId":"43fac7fc-e2ae-4e80-bb50-0f2ff171fcc0","orderLinkId":"","side":"Sell","orderPrice":"40643.7","orderQty":"0.001","leavesQty":"0","orderType":"Market","execFee":"0.0235301","execId":"be10c344-e1d3-5949-8f56-d45dd192acca","execPrice":"42782","execQty":"0.001","execValue":"42.782","execTime":1707036474699,"isMaker":false,"feeRate":"0.00055","seq":8909836703}]
        "#;

        let execution = serde_json::from_str::<Vec<BybitExecution>>(message);
        println!("{:?}", execution);
    }

    #[test]
    fn test_account_status_message() -> anyhow::Result<()> {
        init_debug_log();

        let message: &str = r#"
        {"retCode":0,"retMsg":"OK","result":{"list":[{"totalEquity":"11671.04063119","accountIMRate":"0.0784","totalMarginBalance":"11671.04063119","totalInitialMargin":"915.40917399","accountType":"UNIFIED","totalAvailableBalance":"10755.6314572","accountMMRate":"0.0042","totalPerpUPL":"1471.35122086","totalWalletBalance":"10199.68941033","accountLTV":"0","totalMaintenanceMargin":"50.11637468","coin":[{"availableToBorrow":"","bonus":"0","accruedInterest":"0","availableToWithdraw":"10196.98720872","totalOrderIM":"12.1254","equity":"11667.94862481","totalPositionMM":"49.37769736","usdValue":"11671.04063119","unrealisedPnl":"1470.96141609","collateralSwitch":true,"spotHedgingQty":"0","borrowAmount":"0.000000000000000000","totalPositionIM":"903.04125483","walletBalance":"10196.98720872","cumRealisedPnl":"196.98720872","locked":"0","marginCollateral":true,"coin":"USDT"},{"availableToBorrow":"","bonus":"","accruedInterest":"","availableToWithdraw":"","totalOrderIM":"","equity":"","totalPositionMM":"","usdValue":"","unrealisedPnl":"","collateralSwitch":false,"spotHedgingQty":"0","borrowAmount":"","totalPositionIM":"","walletBalance":"","cumRealisedPnl":"","locked":"","marginCollateral":true,"coin":"BTC"}]}]},"retExtInfo":{},"time":1707918827165}
        "#;

        let response = serde_json::from_str::<BybitRestResponse>(message);

        println!("{:?}", response);

        let response = response.unwrap().body;

        println!("R>>>{:?}", response);
        let account_response = serde_json::from_value::<BybitAccountResponse>(response)?;
        println!("AR>>{:?}", account_response);

        //let ac = (account_response.list);

        //let ac: AccountCoins = account_response.into();

        Ok(())
    }

    #[test]
    fn test_parse_account_coin() {
        let MESSAGE: &str = r#"
        {
            "list":
            [
                {
                    "totalEquity":"11671.04063119","accountIMRate":"0.0784","totalMarginBalance":"11671.04063119","totalInitialMargin":"915.40917399","accountType":"UNIFIED","totalAvailableBalance":"10755.6314572","accountMMRate":"0.0042","totalPerpUPL":"1471.35122086","totalWalletBalance":"10199.68941033","accountLTV":"0","totalMaintenanceMargin":"50.11637468",
                    "coin":[
                    {"availableToBorrow":"","bonus":"0","accruedInterest":"0","availableToWithdraw":"10196.98720872","totalOrderIM":"12.1254","equity":"11667.94862481","totalPositionMM":"49.37769736","usdValue":"11671.04063119","unrealisedPnl":"1470.96141609","collateralSwitch":true,"spotHedgingQty":"0","borrowAmount":"0.000000000000000000","totalPositionIM":"903.04125483","walletBalance":"10196.98720872","cumRealisedPnl":"196.98720872","locked":"0","marginCollateral":true,"coin":"USDT"},
                    {"availableToBorrow":"","bonus":"","accruedInterest":"","availableToWithdraw":"","totalOrderIM":"","equity":"","totalPositionMM":"","usdValue":"","unrealisedPnl":"","collateralSwitch":false,"spotHedgingQty":"0","borrowAmount":"","totalPositionIM":"","walletBalance":"","cumRealisedPnl":"","locked":"","marginCollateral":true,"coin":"BTC"}
                    ]
                }
            ]
        }
        "#;

        let response = serde_json::from_str::<BybitAccountResponse>(MESSAGE);
        println!("{:?}", response);
        assert!(response.is_ok());
    }

    #[test]
    fn test_parse_coin() {
        const M: &str = r#"{"availableToBorrow":"","bonus":"0","accruedInterest":"0","availableToWithdraw":"10196.98720872","totalOrderIM":"12.1254","equity":"11667.94862481","totalPositionMM":"49.37769736","usdValue":"11671.04063119","unrealisedPnl":"1470.96141609","collateralSwitch":true,"spotHedgingQty":"0","borrowAmount":"0.000000000000000000","totalPositionIM":"903.04125483","walletBalance":"10196.98720872","cumRealisedPnl":"196.98720872","locked":"0","marginCollateral":true,"coin":"USDT"}"#;

        let coins = serde_json::from_str::<BybitAccountCoin>(M);

        println!("{:?}", coins);
        assert!(coins.is_ok());
    }

    fn test_parse() {
        let message: &str = r#"
        {"retCode":0,"retMsg":"OK","result":{"list":[{"totalEquity":"11745.04972951","accountIMRate":"0.0779","totalMarginBalance":"11745.04972951","totalInitialMargin":"916.00037165","accountType":"UNIFIED","totalAvailableBalance":"10829.04935785","accountMMRate":"0.0042","totalPerpUPL":"1543.8483218","totalWalletBalance":"10201.2014077","accountLTV":"0","totalMaintenanceMargin":"50.14874128","coin":[{"availableToBorrow":"","bonus":"0","accruedInterest":"0","availableToWithdraw":"10191.91657171","totalOrderIM":"12.1254","equity":"11734.3597278","totalPositionMM":"49.37769736","usdValue":"11745.04972951","unrealisedPnl":"1542.44315609","collateralSwitch":true,"spotHedgingQty":"0","borrowAmount":"0.000000000000000000","totalPositionIM":"903.04125483","walletBalance":"10191.91657171","cumRealisedPnl":"191.91657171","locked":"0","marginCollateral":true,"coin":"USDT"},{"availableToBorrow":"","bonus":"","accruedInterest":"","availableToWithdraw":"","totalOrderIM":"","equity":"","totalPositionMM":"","usdValue":"","unrealisedPnl":"","collateralSwitch":false,"spotHedgingQty":"0","borrowAmount":"","totalPositionIM":"","walletBalance":"","cumRealisedPnl":"","locked":"","marginCollateral":true,"coin":"BTC"}]}]},"retExtInfo":{},"time":1708051591009}        
        "#;
    }
}

/*
[2024-01-20T14:37:17Z DEBUG rbot::exchange::bybit::ws] raw msg: {"topic":"execution","id":"100467532_BTCUSDT_8883610598","creationTime":1705761437507,"data":[{"category":"linear","symbol":"BTCUSDT","closedSize":"0","execFee":"0.02285465","execId":"2800474f-1e3d-571e-9cc8-46e3bcb82699","execPrice":"41553.9","execQty":"0.001","execType":"Trade","execValue":"41.5539","feeRate":"0.00055","tradeIv":"","markIv":"","blockTradeId":"","markPrice":"41547.63","indexPrice":"","underlyingPrice":"","leavesQty":"0","orderId":"e4385ca4-59cf-4ef8-aa34-61b7ad99ae84","orderLinkId":"SkeltonAgentlp9qlB-0001","orderPrice":"43607.8","orderQty":"0.001","orderType":"Market","stopOrderType":"UNKNOWN","side":"Buy","execTime":"1705761437503","isLeverage":"0","isMaker":false,"seq":8883610598,"marketUnit":"","createType":"CreateByUser"}]}
[2024-01-20T14:37:17Z WARN  rbot::exchange::bybit::ws] Error in serde_json::from_str: Err(Error("data did not match any variant of untagged enum BybitUserStreamMessage", line: 0, column: 0))
*/

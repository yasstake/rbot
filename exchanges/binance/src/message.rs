// Copyright(c) 2022-2024. yasstake

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use pyo3::{pyclass, pymethods};
use rbot_lib::common::{
    msec_to_microsec, orderside_deserialize, orderstatus_deserialize, ordertype_deserialize,
    string_to_decimal, string_to_f64, AccountCoins, BoardItem, BoardTransfer, Coin, ControlMessage,
    LogStatus, MarketConfig, MultiMarketMessage, Order, OrderSide,
    OrderStatus, OrderType, Trade,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_derive::{Deserialize, Serialize};
use std::str::FromStr;


pub type BinanceMessageId = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BinanceWsRawMessage {
    message(BinancePublicWsMessage),
    reply(BinanceSubscriptionReply),
}

impl Into<BinancePublicWsMessage> for BinanceWsRawMessage {
    fn into(self) -> BinancePublicWsMessage {
        match self {
            BinanceWsRawMessage::message(m) => m,
            BinanceWsRawMessage::reply(r) => BinancePublicWsMessage::Control(
                if let Some(msg) = r.result {
                    msg
                }
                else {
                    "None".to_string()
                }
            )
        }
    }
}


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceSubscriptionReply {
    pub result: Option<String>,
    pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "e")]
pub enum BinancePublicWsMessage {
    #[serde(rename = "trade")]
    Trade(BinanceWsTradeMessage),
    #[serde(rename = "depthUpdate")]
    BoardUpdate(BinanceWsBoardUpdate),
    #[serde(rename = "control")]
    Control(String),
}

impl BinancePublicWsMessage {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

impl Into<MultiMarketMessage> for BinancePublicWsMessage {
    fn into(self) -> MultiMarketMessage {
        match self {
            BinancePublicWsMessage::Trade(trade) => {
                let mut trades: Vec<Trade> = vec![];

                let t = trade.to_trade();
                trades.push(t);
                MultiMarketMessage::Trade(trades)
            }
            BinancePublicWsMessage::BoardUpdate(board_update) => {
                let board: BoardTransfer = board_update.into();

                MultiMarketMessage::Orderbook(board)
            }
            BinancePublicWsMessage::Control(m) => MultiMarketMessage::Control(ControlMessage {
                status: true,
                operation: "".to_string(),
                message: m,
            }),
        }
    }
}

#[pyclass]
//  {"result":null,"id":1}
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceWsRespond {
    pub result: Option<String>,
    pub id: BinanceMessageId,
}

#[pyclass]
/// Represents a trade message received from the Binance exchange.
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceTradeMessage {
    pub id: BinanceMessageId,
    #[serde(deserialize_with = "string_to_decimal")]
    pub price: Decimal,
    #[serde(rename = "qty", deserialize_with = "string_to_decimal")]
    pub size: Decimal,
    #[serde(rename = "quoteQty", deserialize_with = "string_to_decimal")]
    pub volume_in_foreign: Decimal,
    pub time: i64,
    #[serde(rename = "isBuyerMaker")]
    pub is_buyer_maker: Option<bool>,
    #[serde(rename = "isBestMatch")]
    pub is_best_match: Option<bool>,
}

impl BinanceTradeMessage {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: msec_to_microsec(self.time),
            price: self.price,
            size: self.size,
            order_side: if self.is_buyer_maker.unwrap() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            status: LogStatus::UnFix,
            id: self.id.to_string(),
        };
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

// {"e":"trade","E":1693226465430,"s":"BTCUSDT","t":3200243634,"p":"26132.02000000","q":"0.00244000","b":22161265544,"a":22161265465,"T":1693226465429,"m":false,"M":true}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceWsTradeMessage {
    //#[serde(rename = "e")]
    //pub event_type: String, // "e":"trade"              Event type
    #[serde(rename = "E")]
    pub event_time: i64, // "E":1693226465430        Event time
    pub s: String,           // "s":"BTCUSDT"            Symbol
    pub t: BinanceMessageId, // "t":3200243634           Trade ID
    pub p: String,           // "p":"26132.02000000"     Price
    pub q: String,           // "q":"0.00244000"         Quantity
    pub b: BinanceMessageId, // "b":22161265544          Buyer order ID
    pub a: BinanceMessageId, // "a":22161265465          Seller order ID
    #[serde(rename = "T")]
    pub time: i64, // "T":1693226465429        Trade time
    pub m: bool,             // "m":false                Is the buyer the market maker?
    pub M: bool,             // "M":true                 Ignore
}

impl BinanceWsTradeMessage {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: msec_to_microsec(self.time),
            price: Decimal::from_str(&self.p).unwrap(), // self.p.parse::<f64>().unwrap(),
            size: Decimal::from_str(&self.q).unwrap(),  // parse::<f64>().unwrap(),
            order_side: if self.m {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            status: LogStatus::UnFix,
            id: self.t.to_string(),
        };
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

// hold the latest board(as sample message blow)
// {"lastUpdateId":18735297989,"bids":[["25993.48000000","0.15981000"],["25991.09000000","0.36750000"],["25991.08000000","0.03846000"]],"asks":[["25993.49000000","0.05770000"],["25993.50000000","0.00060000"],["25994.12000000","0.06100000"]]}
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceRestBoard {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<BoardItem>,
    pub asks: Vec<BoardItem>,
}

impl Into<BoardTransfer> for BinanceRestBoard {
    fn into(self) -> BoardTransfer {
        let mut board = BoardTransfer::new();
        board.bids = self.bids;
        board.asks = self.asks;
        board
    }
}

// {"e":"depthUpdate","E":1693266904308,"s":"BTCUSDT","U":38531387766,"u":38531387832,"b":[["26127.87000000","20.79393000"],["26126.82000000","0.02674000"],["26125.95000000","0.00000000"],["26125.78000000","0.38302000"],["26125.68000000","0.00000000"],["26125.10000000","0.00000000"],["26125.05000000","0.00000000"],["26124.76000000","0.00000000"],["26124.75000000","0.21458000"],["26114.84000000","1.14830000"],["26114.15000000","0.00000000"],["26090.85000000","0.00000000"],["26090.84000000","0.00000000"],["26090.32000000","2.29642000"],["26090.31000000","3.82738000"],["26087.99000000","0.03733000"],["26084.34000000","0.00000000"],["25553.07000000","0.13647000"],["25500.81000000","0.14160000"],["25496.85000000","0.00000000"],["25284.00000000","0.03996000"],["24827.83000000","0.00000000"],["24300.17000000","0.00000000"],["23772.50000000","0.00047000"],["23515.08000000","0.00000000"],["18289.50000000","0.00000000"],["13063.93000000","0.00091000"]],"a":[["26127.88000000","5.58099000"],["26128.39000000","0.20072000"],["26128.79000000","0.21483000"],["26129.26000000","0.38297000"],["26129.52000000","0.00000000"],["26129.53000000","0.00000000"],["26134.50000000","0.06000000"],["26134.99000000","1.07771000"],["26135.10000000","0.00700000"],["26155.27000000","0.00050000"],["26155.28000000","0.00000000"],["27027.87000000","0.00200000"],["27290.25000000","0.00000000"],["27817.92000000","0.00000000"],["28345.58000000","0.00000000"]]}
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceWsBoardUpdate {
    //#[serde(rename = "e")]
    //pub event_type: String, // "e":"depthUpdate"        Event type
    #[serde(rename = "E")]
    pub event_time: u64, // "E":1693266904308        Event time
    pub s: String,           // "s":"BTCUSDT"            Symbol
    pub U: BinanceMessageId, // "U":38531387766          First update ID in event
    pub u: BinanceMessageId, // "u":38531387832          Final update ID in event
    #[serde(rename = "b")]
    pub bids: Vec<BoardItem>, // "b":[["26127.87000000","20.79393000"],["26126.82000000","0.02674000"],["26125.95000000","0.00000000"],["26125.78000000","0.38302000"],["26125.68000000","0.00000000"],["26125.10000000","0.00000000"],["26125.05000000","0.00000000"],["26124.76000000","0.00000000"],["26124.75000000","0.21458000"],["26114.84000000","1.14830000"],["26114.15000000","0.00000000"],["26090.85000000","0.00000000"],["26090.84000000","0.00000000"],["26090.32000000","2.29642000"],["26090.31000000","3.82738000"],["26087.99000000","0.03733000"],["26084.34000000","0.00000000"],["25553.07000000","0.13647000"],["25500.81000000","0.14160000"],["25496.85000000","0.00000000"],["25284.00000000","0.03996000"],["24827.83000000","0.00000000"],["24300.17000000","0.00000000"],["23772.50000000","0.00047000"],["23515.08000000","0.00000000"],["18289.50000000","0.00000000"],["13063.93000000","0.00091000"]],"
    #[serde(rename = "a")]
    pub asks: Vec<BoardItem>, // "a":[]
}

impl BinanceWsBoardUpdate {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

impl Into<BoardTransfer> for BinanceWsBoardUpdate {
    fn into(self) -> BoardTransfer {
        let mut board = BoardTransfer::new();

        board.first_update_id = self.U;
        board.last_update_id = self.u;
        board.last_update_time = msec_to_microsec(self.event_time as i64);

        board.bids = self.bids;
        board.asks = self.asks;
        board
    }
}

#[derive(Debug, Serialize, Deserialize)]
// ["26127.87000000","20.79393000"]
#[pyclass]
pub struct BinanceWsBoardItem {
    #[serde(deserialize_with = "string_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub size: f64,
}

impl BinanceWsBoardItem {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

/*
struct BinanceOrderResponse will parse below json.

{
    "symbol": "BTCUSDT",
    "orderId": 28,
    "orderListId": -1, //Unless OCO, value will be -1
    "clientOrderId": "6gCrw2kRUAF9CvJDGP16IP",
    "transactTime": 1507725176595,
    "price": "0.00000000",
    "origQty": "10.00000000",
    "executedQty": "10.00000000",
    "cummulativeQuoteQty": "10.00000000",
    "status": "FILLED",
    "timeInForce": "GTC",
    "type": "MARKET",
    "side": "SELL",
    "workingTime": 1507725176595,
    "selfTradePreventionMode": "NONE",
    "fills": [
      {
        "price": "4000.00000000",
        "qty": "1.00000000",
        "commission": "4.00000000",
        "commissionAsset": "USDT",
        "tradeId": 56
      },
      {
        "price": "3999.00000000",
        "qty": "5.00000000",
        "commission": "19.99500000",
        "commissionAsset": "USDT",
        "tradeId": 57
      },
      {
        "price": "3998.00000000",
        "qty": "2.00000000",
        "commission": "7.99600000",
        "commissionAsset": "USDT",
        "tradeId": 58
      },
      {
        "price": "3997.00000000",
        "qty": "1.00000000",
        "commission": "3.99700000",
        "commissionAsset": "USDT",
        "tradeId": 59
      },
      {
        "price": "3995.00000000",
        "qty": "1.00000000",
        "commission": "3.99500000",
        "commissionAsset": "USDT",
        "tradeId": 60
      }
    ]
  }
*/
#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderFill {
    price: Decimal,
    qty: Decimal,
    commission: Decimal,
    commissionAsset: String,
    tradeId: i64,
}

#[pymethods]
impl BinanceOrderFill {
    #[new]
    pub fn new() -> Self {
        Self {
            price: dec![0.0],
            qty: dec![0.0],
            commission: dec![0.0],
            commissionAsset: "".to_string(),
            tradeId: 0, // only for SPOT
        }
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderResponse {
    symbol: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    transactTime: i64,
    price: Decimal,
    origQty: Decimal,
    executedQty: Decimal,
    cummulativeQuoteQty: Decimal,
    status: String,
    timeInForce: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    workingTime: i64, // only for SPOT
    selfTradePreventionMode: String,
    fills: Vec<BinanceOrderFill>,
}

// TODO: returns Vec<Order>
#[pymethods]
impl BinanceOrderResponse {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn to_order_vec(&self, config: &MarketConfig) -> Vec<Order> {
        let order_side: OrderSide = self.side.as_str().into();
        let order_type: OrderType = self.order_type.as_str().into();
        let order_status = OrderStatus::from_str(&self.status).unwrap();

        let order_head = Order::new(
            &config.trade_category,
            &self.symbol,
            msec_to_microsec(self.transactTime),
            &self.orderId.to_string(),
            &self.clientOrderId,
            order_side,
            order_type,
            order_status,
            self.price,
            self.origQty,
        );

        let mut orders: Vec<Order> = vec![];

        if 0 < self.fills.len() {
            let mut total_filled_size: Decimal = dec![0.0];

            for fill in self.fills.iter() {
                let mut order = order_head.clone();
                order.execute_price = fill.price;
                order.execute_size = fill.qty;

                total_filled_size = total_filled_size + fill.qty;
                order.remain_size = order.order_size - total_filled_size;

                if order.status == OrderStatus::Filled && order.remain_size != dec![0.0] {
                    order.status = OrderStatus::PartiallyFilled;
                }

                order.quote_vol = fill.price * fill.qty;
                order.commission = fill.commission;
                order.commission_asset = fill.commissionAsset.clone();
                order.is_maker = false; // immidately filled order is not maker.
                order.transaction_id = fill.tradeId.to_string();
                orders.push(order);
            }
        } else {
            orders.push(order_head);
        }

        orders
    }
}

pub fn binance_order_response_vec_to_orders(
    config: &MarketConfig,
    order_response: &Vec<BinanceOrderResponse>,
) -> Vec<Order> {
    let mut orders: Vec<Order> = vec![];

    for response in order_response {
        let r = response.to_order_vec(config);
        orders.extend(r);
    }

    orders
}

/*
BinaceCanceOrderResponse will parse below json.

{
  "symbol": "LTCBTC",
  "origClientOrderId": "myOrder1",
  "orderId": 4,
  "orderListId": -1, //Unless part of an OCO, the value will always be -1.
  "clientOrderId": "cancelMyOrder1",
  "transactTime": 1684804350068,
  "price": "2.00000000",
  "origQty": "1.00000000",
  "executedQty": "0.00000000",
  "cummulativeQuoteQty": "0.00000000",
  "status": "CANCELED",
  "timeInForce": "GTC",
  "type": "LIMIT",
  "side": "BUY",
  "selfTradePreventionMode": "NONE"
}
*/

#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceCancelOrderResponse {
    symbol: String,
    origClientOrderId: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    transactTime: i64,
    price: Decimal,
    origQty: Decimal,
    executedQty: Decimal,
    cummulativeQuoteQty: Decimal,
    status: String,
    timeInForce: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    selfTradePreventionMode: String,
}

#[pymethods]
impl BinanceCancelOrderResponse {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn to_order(&self, config: &MarketConfig) -> Order {
        let order_side: OrderSide = self.side.as_str().into();
        let order_type: OrderType = self.order_type.as_str().into();
        let order_status = OrderStatus::from_str(&self.status).unwrap();

        Order::new(
            &config.trade_category,
            &self.symbol,
            msec_to_microsec(self.transactTime),
            &self.orderId.to_string(),
            &self.clientOrderId,
            order_side,
            order_type,
            order_status,
            self.price,
            self.origQty,
        )
    }
}

/*
BiannceListOrderResponse will parse json below
[
  {
    "symbol": "BNBBTC",
    "id": 28457,
    "orderId": 100234,
    "orderListId": -1, //Unless OCO, the value will always be -1
    "price": "4.00000100",
    "qty": "12.00000000",
    "quoteQty": "48.000012",
    "commission": "10.10000000",
    "commissionAsset": "BNB",
    "time": 1499865549590,
    "isBuyer": true,
    "isMaker": false,
    "isBestMatch": true
  }
]

*/

/*
#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceListOrdersResponse {
    symbol: String,
    id: i64,
    orderId: i64,
    orderListId: i64,
    price: Decimal,
    qty: Decimal,
    quoteQty: Decimal,
    commission: Decimal,
    commissionAsset: String,
    time: i64,
    isBuyer: bool,
    isMaker: bool,
    isBestMatch: bool,
}

#[pymethods]
impl BinanceListOrdersResponse {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}
impl Into<Order> for BinanceListOrdersResponse {
    fn into(self) -> Order {
        let order_side: OrderSide = if self.isBuyer {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let order_type: OrderType = OrderType::Limit;
        let order_status = OrderStatus::Filled;

        Order {
            symbol: self.symbol.clone(),
            create_time: binance_to_microsec(self.time),
            order_id: self.orderId.to_string(),
            order_list_index: self.orderListId,
            client_order_id: "".to_string(),
            order_side: order_side,
            order_type: order_type,
            price: self.price,
            size: self.qty,
            remain_size: self.qty,
            status: order_status,
            account_change: AccountChange::new(),
            fills: OrderFill::new(),
            profit: None,
            message: "".to_string(),
        }
    }
}
*/

/*------------ Private WS ------------------------*/

/*
BinanceAccountUpdate is parse json as blow

{
  "e": "outboundAccountPosition", //Event type
  "E": 1564034571105,             //Event Time
  "u": 1564034571073,             //Time of last account update
  "B": [                          //Balances Array
    {
      "a": "ETH",                 //Asset
      "f": "10000.000000",        //Free
      "l": "0.000000"             //Locked
    }
  ]
}
*/
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceAccountUpdate {
    // e: String,
    E: i64,
    u: u64,
    B: Vec<BinanceBalance>,
}

impl BinanceAccountUpdate {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn to_coins(&self) -> AccountCoins {
        let mut coins: Vec<Coin> = vec![];

        for balance in self.B.iter() {
            let coin: Coin = balance.to_coin();
            coins.push(coin);
        }

        AccountCoins {
            coins: coins,
        }
    }
}


#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceBalance {
    #[serde(rename = "a")]
    a: String,
    #[serde(rename = "f")]
    f: Decimal,
    #[serde(rename = "l")]
    l: Decimal,
}

impl BinanceBalance {
    pub fn to_coin(&self) -> Coin {
        Coin {
            symbol: self.a.clone(),
            volume: self.f + self.l,
            free: self.f,
            locked: self.l,
        }
    }
}

/// BinanceBalancdUpdate
/// Represents a balance update message received from the Binance exchange.
/// sample as blow
///  {
///  "e": "balanceUpdate",         //Event Type
///  "E": 1573200697110,           //Event Time
///  "a": "BTC",                   //Asset
///  "d": "100.00000000",          //Balance Delta
///  "T": 1573200697068            //Clear Time
///  }
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceBalanceUpdate {
    // e: String,
    E: u64,
    a: String,
    d: Decimal,
    T: u64,
}

/*
{
  "e": "executionReport",        // Event type
  "E": 1499405658658,            // Event time
  "s": "ETHBTC",                 // Symbol
  "c": "mUvoqJxFIILMdfAW5iGSOW", // Client order ID
  "S": "BUY",                    // Side
  "o": "LIMIT",                  // Order type
  "f": "GTC",                    // Time in force
  "q": "1.00000000",             // Order quantity
  "p": "0.10264410",             // Order price
  "P": "0.00000000",             // Stop price
  "F": "0.00000000",             // Iceberg quantity
  "g": -1,                       // OrderListId
  "C": "",                       // Original client order ID; This is the ID of the order being canceled
  "x": "NEW",                    // Current execution type
  "X": "NEW",                    // Current order status
  "r": "NONE",                   // Order reject reason; will be an error code.
  "i": 4293153,                  // Order ID
  "l": "0.00000000",             // Last executed quantity
  "z": "0.00000000",             // Cumulative filled quantity
  "L": "0.00000000",             // Last executed price
  "n": "0",                      // Commission amount
  "N": null,                     // Commission asset
  "T": 1499405658657,            // Transaction time
  "t": -1,                       // Trade ID
  "I": 8641984,                  // Ignore
  "w": true,                     // Is the order on the book?
  "m": false,                    // Is this trade the maker side?
  "M": false,                    // Ignore
  "O": 1499405658657,            // Order creation time
  "Z": "0.00000000",             // Cumulative quote asset transacted quantity
  "Y": "0.00000000",             // Last quote asset transacted quantity (i.e. lastPrice * lastQty)
  "Q": "0.00000000",             // Quote Order Quantity
  "W": 1499405658657,            // Working Time; This is only visible if the order has been placed on the book.
  "V": "NONE"                    // selfTradePreventionMode
}

*/
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceExecutionReport {
    // e: String,
    #[serde(rename = "E")]
    time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "S")]
    #[serde(deserialize_with = "orderside_deserialize")]
    order_side: OrderSide,
    #[serde(rename = "o")]
    #[serde(deserialize_with = "ordertype_deserialize")]
    order_type: OrderType,
    #[serde(rename = "f")]
    time_in_force: String,
    #[serde(rename = "q")]
    order_quantity: Decimal,
    #[serde(rename = "p")]
    order_price: Decimal,
    #[serde(rename = "P")]
    stop_price: Decimal,
    #[serde(rename = "F")]
    ice_berg_quantity: Decimal,
    #[serde(rename = "g")]
    order_list_id: i64,
    #[serde(rename = "C")]
    original_client_order_id: String,
    #[serde(rename = "x")]
    current_execution_type: String,
    #[serde(rename = "X")]
    #[serde(deserialize_with = "orderstatus_deserialize")]
    current_order_status: OrderStatus,
    #[serde(rename = "r")]
    order_reject_reason: String,
    #[serde(rename = "i")]
    order_id: i64,
    #[serde(rename = "l")]
    last_executed_quantity: Decimal,
    #[serde(rename = "z")]
    cumulative_filled_quantity: Decimal,
    #[serde(rename = "L")]
    last_executed_price: Decimal,
    #[serde(rename = "n")]
    commission_amount: Decimal,
    #[serde(rename = "N")]
    commission_asset: Option<String>,
    #[serde(rename = "T")]
    transaction_time: i64,
    #[serde(rename = "t")]
    trade_id: i64,
    #[serde(rename = "I")]
    _ignore: i64,
    #[serde(rename = "w")]
    on_order_book: bool,
    #[serde(rename = "m")]
    is_maker: bool,
    #[serde(rename = "M")]
    _ignore2: bool,
    #[serde(rename = "O")]
    order_creation_time: u64,
    #[serde(rename = "Z")]
    cumulative_quote_quantity: Decimal,
    #[serde(rename = "Y")]
    last_quote_quantity: Decimal,
    #[serde(rename = "Q")]
    quoite_quantity: Decimal,
    #[serde(rename = "W")]
    working_time: i64,
    #[serde(rename = "V")]
    self_prevention_mode: String,
}

#[pymethods]
impl BinanceExecutionReport {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    fn to_order(&self, category: &str) -> Order {
        let mut order = Order::new(
            category,
            &self.symbol,
            msec_to_microsec(self.time),
            &self.order_id.to_string(),
            &self.client_order_id,
            self.order_side,
            self.order_type,
            self.current_order_status,
            self.order_price,
            self.order_quantity,
        );

        order.status = self.current_order_status;
        order.transaction_id = self.trade_id.to_string();
        order.update_time = msec_to_microsec(self.transaction_time);
        order.execute_price = self.last_executed_price;
        order.execute_size = self.last_executed_quantity;
        order.remain_size = self.order_quantity - self.cumulative_filled_quantity;
        order.quote_vol = self.last_quote_quantity;
        order.commission = self.commission_amount;
        order.commission_asset = self.commission_asset.clone().unwrap_or_default();
        order.is_maker = self.is_maker;

        if self.order_reject_reason != "NONE" {
            order.message = self.order_reject_reason.clone();
        }

        log::debug!("order: {:?}", order);

        order
    }
}


#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "e")]
pub enum BinanceUserWsMessage {
    outboundAccountPosition(BinanceAccountUpdate),
    balanceUpdate(BinanceBalanceUpdate),
    executionReport(BinanceExecutionReport),
}


impl BinanceUserWsMessage {
    pub fn convert_multimarketmessage(&self, category: &str) -> MultiMarketMessage {
        log::debug!("RAW user stream:\n{:?}\n", self);

        let message = match self {
            BinanceUserWsMessage::outboundAccountPosition(account) => {
                let coins: AccountCoins = account.to_coins();
                MultiMarketMessage::Account(coins)
            }
            BinanceUserWsMessage::balanceUpdate(_balance) => {
                log::error!("not implemented");
                MultiMarketMessage::Message("not implemented".to_string())
            }
            BinanceUserWsMessage::executionReport(report) => {
                let order: Order = report.to_order(category);
                MultiMarketMessage::Order(vec![order])
            }
        };

        message
    }
}


/*
https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data

BinanceAccountInformation is parse json as blow

{
  "makerCommission": 15,
  "takerCommission": 15,
  "buyerCommission": 0,
  "sellerCommission": 0,
  "commissionRates": {
    "maker": "0.00150000",
    "taker": "0.00150000",
    "buyer": "0.00000000",
    "seller": "0.00000000"
  },
  "canTrade": true,
  "canWithdraw": true,
  "canDeposit": true,
  "brokered": false,
  "requireSelfTradePrevention": false,
  "preventSor": false,
  "updateTime": 123456789,
  "accountType": "SPOT",
  "balances": [
    {
      "asset": "BTC",
      "free": "4723846.89208129",
      "locked": "0.00000000"
    },
    {
      "asset": "LTC",
      "free": "4763368.68006011",
      "locked": "0.00000000"
    }
  ],
  "permissions": [
    "SPOT"
  ],
  "uid": 354937868
}

*/
#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BinanceAccountInformation {
    #[pyo3(get)]
    pub makerCommission: i64,
    #[pyo3(get)]
    pub takerCommission: i64,
    #[pyo3(get)]
    pub buyerCommission: i64,
    #[pyo3(get)]
    pub sellerCommission: i64,
    #[pyo3(get)]
    pub commissionRates: BinanceCommissionRates,
    #[pyo3(get)]
    pub canTrade: bool,
    #[pyo3(get)]
    pub canWithdraw: bool,
    #[pyo3(get)]
    pub canDeposit: bool,
    #[pyo3(get)]
    pub brokered: bool,
    #[pyo3(get)]
    pub requireSelfTradePrevention: bool,
    #[pyo3(get)]
    pub preventSor: bool,
    #[pyo3(get)]
    pub updateTime: i64,
    #[pyo3(get)]
    pub accountType: String,
    #[pyo3(get)]
    pub balances: Vec<BinanceAccountBalance>,
    #[pyo3(get)]
    pub permissions: Vec<String>,
    #[pyo3(get)]
    pub uid: i64,
}

#[pymethods]
impl BinanceAccountInformation {
    pub fn into_coins(&self) -> AccountCoins {
        let mut coins = AccountCoins::new();

        for balance in &self.balances {
            let coin = Coin {
                symbol: balance.asset.clone(),
                volume: balance.free + balance.locked,
                free: balance.free,
                locked: balance.locked,
            };

            coins.push(coin);
        }

        coins
    }
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BinanceCommissionRates {
    #[pyo3(get)]
    maker: Decimal,
    #[pyo3(get)]
    taker: Decimal,
    #[pyo3(get)]
    buyer: Decimal,
    #[pyo3(get)]
    seller: Decimal,
}

#[pymethods]
impl BinanceCommissionRates {
    pub fn __repr__(&self) -> String {
        serde_json::to_string(self).unwrap()
        // format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BinanceAccountBalance {
    #[pyo3(get)]
    pub asset: String,
    #[pyo3(get)]
    pub free: Decimal,
    #[pyo3(get)]
    pub locked: Decimal,
}

#[pymethods]
impl BinanceAccountBalance {
    pub fn __repr__(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

/*
BinanceOrderStatus is parse json as blow

{
  "symbol": "LTCBTC",
  "orderId": 1,
  "orderListId": -1, //Unless OCO, value will be -1
  "clientOrderId": "myOrder1",
  "price": "0.1",
  "origQty": "1.0",
  "executedQty": "0.0",
  "cummulativeQuoteQty": "0.0",
  "status": "NEW",
  "timeInForce": "GTC",
  "type": "LIMIT",
  "side": "BUY",
  "stopPrice": "0.0",
  "icebergQty": "0.0",
  "time": 1499827319559,
  "updateTime": 1499827319559,
  "isWorking": true,
  "workingTime":1499827319559,
  "origQuoteOrderQty": "0.000000",
  "selfTradePreventionMode": "NONE"
}
*/
#[allow(non_snake_case)]
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderStatus {
    symbol: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    price: Decimal,
    origQty: Decimal,
    executedQty: Decimal,
    cummulativeQuoteQty: Decimal,
    #[serde(deserialize_with = "orderstatus_deserialize")]
    status: OrderStatus,
    timeInForce: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    stopPrice: Decimal,
    icebergQty: Decimal,
    time: i64,
    updateTime: i64,
    isWorking: bool,
    workingTime: i64,
    origQuoteOrderQty: Decimal,
    selfTradePreventionMode: String,
}

pub fn binance_order_status_vec_to_orders(
    config: &MarketConfig,
    order_response: &Vec<BinanceOrderStatus>,
) -> Vec<Order> {
    let mut orders: Vec<Order> = vec![];

    for response in order_response {
        let r = response.to_order(config);
        orders.push(r);
    }

    orders
}

#[pymethods]
impl BinanceOrderStatus {
    fn to_order(&self, config: &MarketConfig) -> Order {
        let order_side: OrderSide = self.side.as_str().into();
        let order_type: OrderType = self.order_type.as_str().into();
        let order_status = self.status;

        let mut order = Order::new(
            &config.trade_category,
            &self.symbol,
            msec_to_microsec(self.time),
            &self.orderId.to_string(),
            &self.clientOrderId,
            order_side,
            order_type,
            order_status,
            self.price,
            self.origQty,
        );

        order.remain_size = self.origQty - self.executedQty;
        //order.transaction_id: String,
        order.update_time = msec_to_microsec(self.updateTime);
        //order.execute_price
        order.execute_size = self.executedQty;
        // order.quote_vol: Decimal,
        //order.commission: Decimal,
        //order.commission_asset: String,
        order.is_maker = self.isWorking; // on board it's maker

        order
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(self).unwrap()
        // format!("{:?}", self)
    }
}




#[cfg(test)]
mod binance_message_test {
    use crate::config::BinanceConfig;
    // use crate::config::BinanceServerConfig;
    use super::*;

    #[test]
    fn test_binance_public_ws_message() {
        let message: BinancePublicWsMessage = serde_json::from_str(TRADE_WS).unwrap();
        println!("{:?}", message);

        let message: BinancePublicWsMessage = serde_json::from_str(BOARD_UPDATE).unwrap();
        println!("{:?}", message);
    }

    const TRADE_WS: &str = r#"{"e":"trade","E":1693226465430,"s":"BTCUSDT","t":3200243634,"p":"26132.02000000","q":"0.00244000","b":22161265544,"a":22161265465,"T":1693226465429,"m":false,"M":true}"#;        
    #[test]
    fn test_binance_trade_message() {

        let message: BinanceWsTradeMessage = serde_json::from_str(TRADE_WS).unwrap();

        println!("{:?}", message);
    }


    const BOARD_UPDATE: &str = r#"{"e":"depthUpdate","E":1693266904308,"s":"BTCUSDT","U":38531387766,"u":38531387832,"b":[["26127.87000000","20.79393000"],["26126.82000000","0.02674000"],["26125.95000000","0.00000000"],["26125.78000000","0.38302000"],["26125.68000000","0.00000000"],["26125.10000000","0.00000000"],["26125.05000000","0.00000000"],["26124.76000000","0.00000000"],["26124.75000000","0.21458000"],["26114.84000000","1.14830000"],["26114.15000000","0.00000000"],["26090.85000000","0.00000000"],["26090.84000000","0.00000000"],["26090.32000000","2.29642000"],["26090.31000000","3.82738000"],["26087.99000000","0.03733000"],["26084.34000000","0.00000000"],["25553.07000000","0.13647000"],["25500.81000000","0.14160000"],["25496.85000000","0.00000000"],["25284.00000000","0.03996000"],["24827.83000000","0.00000000"],["24300.17000000","0.00000000"],["23772.50000000","0.00047000"],["23515.08000000","0.00000000"],["18289.50000000","0.00000000"],["13063.93000000","0.00091000"]],"a":[["26127.88000000","5.58099000"],["26128.39000000","0.20072000"],["26128.79000000","0.21483000"],["26129.26000000","0.38297000"],["26129.52000000","0.00000000"],["26129.53000000","0.00000000"],["26134.50000000","0.06000000"],["26134.99000000","1.07771000"],["26135.10000000","0.00700000"],["26155.27000000","0.00050000"],["26155.28000000","0.00000000"],["27027.87000000","0.00200000"],["27290.25000000","0.00000000"],["27817.92000000","0.00000000"],["28345.58000000","0.00000000"]]}"#;
    #[test]
    fn test_binance_board_update() {
        let message: BinanceWsBoardUpdate = serde_json::from_str(BOARD_UPDATE).unwrap();

        println!("{:?}", message);
    }

    #[test]
    fn test_multi_market_message() {
        let trade: BinanceWsTradeMessage = serde_json::from_str(TRADE_WS).unwrap();
        let trade = BinancePublicWsMessage::Trade(trade);        
        let message: MultiMarketMessage = trade.into();
        println!("{:?}", message);

        let board: BinanceWsBoardUpdate = serde_json::from_str(BOARD_UPDATE).unwrap();
        let board = BinancePublicWsMessage::BoardUpdate(board);
        let message: MultiMarketMessage = board.into();
        println!("{:?}", message);
    }



    const HISTORY: &str = r#"[{"id":990877266,"price":"26092.63000000","qty":"0.00046000","quoteQty":"12.00260980","time":1692935644243,"isBuyerMaker":false,"isBestMatch":true},{"id":990877267,"price":"26093.10000000","qty":"0.00189000","quoteQty":"49.31595900","time":1692935644243,"isBuyerMaker":false,"isBestMatch":true},{"id":990877268,"price":"26093.75000000","qty":"0.00209000","quoteQty":"54.53593750","time":1692935644276,"isBuyerMaker":false,"isBestMatch":true},{"id":990877269,"price":"26094.00000000","qty":"0.00306000","quoteQty":"79.84764000","time":1692935647281,"isBuyerMaker":false,"isBestMatch":true},{"id":990877270,"price":"26094.00000000","qty":"0.00075000","quoteQty":"19.57050000","time":1692935647283,"isBuyerMaker":false,"isBestMatch":true}]"#;

    #[test]
    fn test_binance_trade_history_message() {
        let message: Vec<BinanceTradeMessage> = serde_json::from_str(HISTORY).unwrap();

        println!("{:?}", message);
    }

    const REST_BOARD: &str = r#"{
        "lastUpdateId": 1027024,
        "bids": [
          [
            "4.00000000",
            "431.00000000"
          ]
        ],
        "asks": [
          [
            "4.00000200",
            "12.00000000"
          ]
        ]
      }"#;

    
    #[test]
    fn test_binance_rest_board_update_message() {
        let message: BinanceRestBoard = serde_json::from_str(REST_BOARD).unwrap();

        println!("{:?}", message);
    }
    

    #[test]
    fn test_binance_ws_message() {
        let message: BinancePublicWsMessage = serde_json::from_str(TRADE_WS).unwrap();
        println!("{:?}", message);

        let message: BinancePublicWsMessage = serde_json::from_str(BOARD_UPDATE).unwrap();
        println!("{:?}", message);
    }

    #[test]
    fn test_binance_order_response() -> anyhow::Result<()>{
        let order: BinanceOrderResponse = serde_json::from_str(r#"{"symbol":"BTCUSDT","orderId":28,"orderListId":-1,"clientOrderId":"6gCrw2kRUAF9CvJDGP16IP","transactTime":1507725176595,"price":"0.00000000","origQty":"10.00000000","executedQty":"10.00000000","cummulativeQuoteQty":"10.00000000","status":"FILLED","timeInForce":"GTC","type":"MARKET","side":"SELL","workingTime":1507725176595,"selfTradePreventionMode":"NONE","fills":[{"price":"4000.00000000","qty":"1.00000000","commission":"4.00000000","commissionAsset":"USDT","tradeId":56},{"price":"3999.00000000","qty":"5.00000000","commission":"19.99500000","commissionAsset":"USDT","tradeId":57},{"price":"3998.00000000","qty":"2.00000000","commission":"7.99600000","commissionAsset":"USDT","tradeId":58},{"price":"3997.00000000","qty":"1.00000000","commission":"3.99700000","commissionAsset":"USDT","tradeId":59},{"price":"3995.00000000","qty":"1.00000000","commission":"3.99500000","commissionAsset":"USDT","tradeId":60}]}"#)?;
        println!("{:?}", order);

        let config = BinanceConfig::BTCUSDT();
        let order_vec = order.to_order_vec(&config);
        println!("{:?}", order_vec);

        let order_vec2 = binance_order_response_vec_to_orders(&config, &vec![order]);
        assert_eq!(order_vec, order_vec2);

        Ok(())
    }

    #[test]
    fn test_binance_cancel_order_response() {
        let order_response: BinanceCancelOrderResponse = serde_json::from_str(r#"{"symbol":"LTCBTC","origClientOrderId":"myOrder1","orderId":4,"orderListId":-1,"clientOrderId":"cancelMyOrder1","transactTime":1684804350068,"price":"2.00000000","origQty":"1.00000000","executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"CANCELED","timeInForce":"GTC","type":"LIMIT","side":"BUY","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);
    }

    // TODO: test cancel all orders response
    #[test]
    fn test_binance_cancel_multiple_orders() {
        let order_response: Vec<BinanceCancelOrderResponse> = serde_json::from_str(
            r#"
            [
                {
                  "symbol": "BTCUSDT",
                  "origClientOrderId": "E6APeyTJvkMvLMYMqu1KQ4",
                  "orderId": 11,
                  "orderListId": -1,
                  "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                  "transactTime": 1684804350068,
                  "price": "0.089853",
                  "origQty": "0.178622",
                  "executedQty": "0.000000",
                  "cummulativeQuoteQty": "0.000000",
                  "status": "CANCELED",
                  "timeInForce": "GTC",
                  "type": "LIMIT",
                  "side": "BUY",
                  "selfTradePreventionMode": "NONE"
                },
                {
                  "symbol": "BTCUSDT",
                  "origClientOrderId": "A3EF2HCwxgZPFMrfwbgrhv",
                  "orderId": 13,
                  "orderListId": -1,
                  "clientOrderId": "pXLV6Hz6mprAcVYpVMTGgx",
                  "transactTime": 1684804350069,
                  "price": "0.090430",
                  "origQty": "0.178622",
                  "executedQty": "0.000000",
                  "cummulativeQuoteQty": "0.000000",
                  "status": "CANCELED",
                  "timeInForce": "GTC",
                  "type": "LIMIT",
                  "side": "BUY",
                  "selfTradePreventionMode": "NONE"
                }
            ]
            "#,
        )
        .unwrap();

        println!("{:?}", order_response);
    }


    #[test]
    fn test_binance_account_inforamtion() {
        let order_response: BinanceAccountInformation = serde_json::from_str(
            r#"{"makerCommission":15,
            "takerCommission":15,
            "buyerCommission":0,
            "sellerCommission":0,
            "commissionRates":
            {"maker":"0.00150000","taker":"0.00150000","buyer":"0.00000000","seller":"0.00000000"},
            "canTrade":true,"canWithdraw":true,"canDeposit":true,"brokered":false,"requireSelfTradePrevention":false,
            "preventSor":false,
            "updateTime":123456789,
            "accountType":"SPOT",
            "balances":[{"asset":"BTC","free":"4723846.89208129","locked":"0.00000000"},{"asset":"LTC","free":"4763368.68006011","locked":"0.00000000"}],
            "permissions":["SPOT"],"uid":354937868}"#).unwrap();

        println!("{:?}", order_response);

        let coins = order_response.into_coins();
        println!("{:?}", coins);
    }

    
    #[test]
    fn test_binance_order_status() {
        let order_response: BinanceOrderStatus = serde_json::from_str(
            r#"{"symbol":"LTCBTC","orderId":1,"orderListId":-1,"clientOrderId":"myOrder1","price":"0.1","origQty":"1.0","executedQty":"0.0","cummulativeQuoteQty":"0.0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.0","icebergQty":"0.0","time":1499827319559,"updateTime":1499827319559,"isWorking":true,"workingTime":1499827319559,"origQuoteOrderQty":"0.000000","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);

        let config = BinanceConfig::BTCUSDT();
        let order = order_response.to_order(&config);
        println!("{:?}", order);
    }

    #[test]
    fn test_binance_order_status_cap_new() {
        let order_response: BinanceOrderStatus = serde_json::from_str(
            r#"{"symbol":"LTCBTC","orderId":1,"orderListId":-1,"clientOrderId":"myOrder1","price":"0.1","origQty":"1.0","executedQty":"0.0","cummulativeQuoteQty":"0.0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.0","icebergQty":"0.0","time":1499827319559,"updateTime":1499827319559,"isWorking":true,"workingTime":1499827319559,"origQuoteOrderQty":"0.000000","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);
    }


    /*------------ Private WS ------------------------*/
    #[test]
    fn test_account_update() {
        let account_update: BinanceAccountUpdate = serde_json::from_str(
            r#"
            {
                "e": "outboundAccountPosition",
                "E": 1564034571105,            
                "u": 1564034571073,            
                "B": [                         
                  {
                    "a": "ETH",                
                    "f": "10000.000000",       
                    "l": "0.000000"            
                  }
                ]
              }
            "#).unwrap();

        println!("{:?}", account_update);
    }

}


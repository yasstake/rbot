use std::str::FromStr;

use pyo3::{pyclass, pymethods};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::de::{self, Deserialize, Deserializer};
use serde_derive::{Deserialize, Serialize};
use strum_macros::Display;

use crate::{
    common::{
        AccountChange, MarketMessage, MicroSec,
        {Order, OrderFill, OrderSide, OrderStatus, OrderType, Trade}, AccountStatus, string_to_side, orderside_deserialize, ordertype_deserialize, orderstatus_deserialize, string_to_status,
    },
    exchange::{string_to_decimal, BoardItem, binance},
};

use super::{super::string_to_f64, binance_to_microsec, BinanceConfig, Market};

pub type BinanceMessageId = u64;

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
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
}

impl Into<MarketMessage> for BinancePublicWsMessage {
    fn into(self) -> MarketMessage {
        match self {
            BinancePublicWsMessage::Trade(trade) => MarketMessage {
                trade: Some(trade.to_trade()),
                order: None,
                account: None,
            },
            BinancePublicWsMessage::BoardUpdate(board_update) => {
                // TODO: implment
                log::warn!("BinancePublicWsMessage::BoardUpdate is not implemented yet");

                MarketMessage::new()
            }
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
    pub time: u64,
    #[serde(rename = "isBuyerMaker")]
    pub is_buyer_maker: Option<bool>,
    #[serde(rename = "isBestMatch")]
    pub is_best_match: Option<bool>,
}

impl BinanceTradeMessage {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: binance_to_microsec(self.time),
            price: self.price,
            size: self.size,
            order_side: if self.is_buyer_maker.unwrap() {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
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
    pub event_time: u64, // "E":1693226465430        Event time
    pub s: String,           // "s":"BTCUSDT"            Symbol
    pub t: BinanceMessageId, // "t":3200243634           Trade ID
    pub p: String,           // "p":"26132.02000000"     Price
    pub q: String,           // "q":"0.00244000"         Quantity
    pub b: BinanceMessageId, // "b":22161265544          Buyer order ID
    pub a: BinanceMessageId, // "a":22161265465          Seller order ID
    #[serde(rename = "T")]
    pub time: u64, // "T":1693226465429        Trade time
    pub m: bool,             // "m":false                Is the buyer the market maker?
    pub M: bool,             // "M":true                 Ignore
}

impl BinanceWsTradeMessage {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: binance_to_microsec(self.time),
            price: Decimal::from_str(&self.p).unwrap(), // self.p.parse::<f64>().unwrap(),
            size: Decimal::from_str(&self.q).unwrap(),  // parse::<f64>().unwrap(),
            order_side: if self.m {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
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
            tradeId: 0,                         // only for SPOT
        }
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}




impl From<BinanceOrderResponse> for Vec<Order> {
    fn from(order: BinanceOrderResponse) -> Self {
        let order_side: OrderSide = order.side.as_str().into();
        let order_type: OrderType = order.order_type.as_str().into();
        let order_status = OrderStatus::from_str(&order.status).unwrap();

        let order_head = Order::new(
            order.symbol,
            binance_to_microsec(order.transactTime),
            order.orderId.to_string(),
            order.clientOrderId,
            order_side,
            order_type,
            order_status,
            order.price,
            order.origQty,
        );

        let mut orders: Vec<Order> = vec![];

        if 0 < order.fills.len() {
            let mut total_filled_size: Decimal = dec![0.0];

            for fill in order.fills {
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
                order.commission_asset = fill.commissionAsset;
                order.is_maker = false; // immidately filled order is not maker.
                order.transaction_id = fill.tradeId.to_string();
                orders.push(order);
            }
        }
        else {
            orders.push(order_head);
        }

        orders
    }
}


#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderResponse {
    symbol: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    transactTime: u64,
    price: Decimal,
    origQty: Decimal,
    executedQty: Decimal,
    cummulativeQuoteQty: Decimal,
    status: String,
    timeInForce: String,
    #[serde(rename = "type")]
    order_type: String,
    side: String,
    workingTime: u64,                   // only for SPOT
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

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceCancelOrderResponse {
    symbol: String,
    origClientOrderId: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    transactTime: u64,
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
}


impl From<BinanceCancelOrderResponse> for Order {
    fn from(order: BinanceCancelOrderResponse) -> Self {
        let order_side: OrderSide = order.side.as_str().into();
        let order_type: OrderType = order.order_type.as_str().into();
        let order_status = OrderStatus::from_str(&order.status).unwrap();

        Order::new(
            order.symbol,
            binance_to_microsec(order.transactTime),
            order.orderId.to_string(),
            order.clientOrderId,
            order_side,
            order_type,
            OrderStatus::Canceled,
            order.price,
            order.origQty,
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
    time: u64,
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
/*
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
    E: u64,
    u: u64,
    B: Vec<BinanceBalance>,
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

pub fn binance_account_update_to_account_status(
    config: &BinanceConfig,
    account_update: &BinanceAccountUpdate,
) -> AccountStatus {

    let mut account_status = AccountStatus::default();

    let l = account_update.B.len();

    for i in 0..l {
        if account_update.B[i].a == config.foreign_currency {
            account_status.foreign_free = account_update.B[i].f;
            account_status.foreign_locked = account_update.B[i].l;                        
            account_status.foreign = account_status.foreign_free + account_status.foreign_locked;
        } else if account_update.B[i].a == config.home_currency {
            account_status.home_free = account_update.B[i].f;
            account_status.home_locked = account_update.B[i].l;
            account_status.home = account_status.home_free + account_status.home_locked;
        }
    }

    return account_status;
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
    time: u64,
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
    transaction_time: u64,
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
    working_time: u64,
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
}

impl From<&BinanceExecutionReport> for Order {
    fn from(value: &BinanceExecutionReport) -> Self {
        let mut order = Order::new(
            value.symbol.clone(),
            binance_to_microsec(value.time),
            value.order_id.to_string(),
            value.client_order_id.to_string(),
            value.order_side,
            value.order_type,
            value.current_order_status,
            value.order_price,
            value.order_quantity,
        );

        order.status = value.current_order_status;
        order.transaction_id = value.trade_id.to_string();
        order.update_time = binance_to_microsec(value.transaction_time);
        order.execute_price = value.last_executed_price;
        order.execute_size = value.last_executed_quantity;
        order.remain_size = value.order_quantity - value.cumulative_filled_quantity;
        order.quote_vol = value.last_quote_quantity;
        order.commission = value.commission_amount;
        order.commission_asset = value.commission_asset.clone().unwrap_or_default();
        order.is_maker= value.is_maker;

        if value.order_reject_reason != "NONE" {
            order.message  = value.order_reject_reason.clone();
        }

        log::debug!("order: {:?}", order);

        order
    }




    /*
    fn from(order: &BinanceExecutionReport) -> Self {
        let order_id = order.order_id.to_string();
        let client_order_id = order.client_order_id.clone();

        let order_side:OrderSide = order.order_side;
        let order_price: Decimal = order.order_price;
        let order_size:Decimal = order.order_quantity;
        let order_type: OrderType = order.order_type;
        let order_status = order.current_order_status;
        let order_quote = order_price * order_size;

        let trade_id = order.trade_id.to_string();
        //let create_time = binance_to_microsec(order.O);
        let update_time = binance_to_microsec(order.time);
        let execute_size = order.last_executed_quantity;
        let execute_price = order.last_executed_price;
        let execute_total = order.cumulative_filled_quantity;
        let commission_amount = order.commission_amount;
        let commition_asset = order.commission_asset.clone().unwrap_or_default();
        let ismaker = order.is_maker;

        let remain_size =
            if order_status == OrderStatus::Filled || order_status == OrderStatus::Canceled {
                Decimal::from(0)
            } else {
                order.order_quantity - execute_total
            };

        let fills = OrderFill {
            transaction_id: trade_id,
            update_time: update_time,
            price: execute_price,
            filled_size: execute_size,
            quote_vol: execute_price * execute_size,
            commission: order.commission_amount,
            commission_asset: commition_asset,
            maker: ismaker,
        };

        let mut account_change = AccountChange::new();

        match order_status {
            OrderStatus::New => {
                // in new order, asset is locked.
                if order_side == OrderSide::Buy {
                    let lock_size = order_size * order.order_price;
                    account_change.lock_home_change = lock_size;
                    account_change.free_home_change = - lock_size;
                    // TODO: may be need to add fee
                }
                else if order_side == OrderSide::Sell {
                    let lock_size = order_size;
                    account_change.lock_foreign_change = lock_size;
                    account_change.free_foreign_change = - lock_size;
                    // TODO: may be need to add fee
                }
            }
            
            OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                // lock is free, and foreing sizie is change.
                if order_side == OrderSide::Buy {
                    let unlock_size = execute_size * execute_price;
                    if order.is_maker {
                        account_change.lock_home_change = - unlock_size;
                    }
                    account_change.home_change = - unlock_size;

                    account_change.foreign_change = execute_size;
                }
                else if order_side == OrderSide::Sell {
                    let unlock_size = execute_size;

                    if order.is_maker {
                        account_change.lock_foreign_change = - unlock_size;
                    }
                    account_change.foreign_change = - unlock_size;

                    account_change.home_change = execute_size * execute_price;

                }
            }
            OrderStatus::Canceled => {
                // lock is free
                if order_side == OrderSide::Buy {
                    if order.on_order_book {
                        let unlock_size = order_size * order_price;
                        account_change.lock_home_change = - unlock_size;
                        account_change.free_home_change = unlock_size;
                    }
                }
                else if order_side == OrderSide::Sell {
                    if order.on_order_book {
                        let unlock_size = order_size;
                        account_change.lock_foreign_change = - unlock_size;
                        account_change.free_foreign_change = unlock_size;
                    }
                }
            }
            OrderStatus::Rejected => {log::error!("Rejected: not implemented {:?}", order);},
            OrderStatus::Expired => {log::error!("Expired: not implemented {:?}", order);},
            OrderStatus::Error => {log::error!("Error: not implemented {:?}", order);},
        }

        let order = Order::new(
            order.symbol,
            update_time,
            order_id,
            client_order_id,
            order_side,
            order_type,
            order_status,
            order_price,
            order_size,
        );
        )
        let r = Order {
            symbol: order.symbol.clone(),
            create_time: binance_to_microsec(order.transaction_time),
            order_id: order.order_id.to_string(),
            order_list_index: order.order_list_id,
            client_order_id: order.client_order_id.clone(),
            order_side: order_side,
            order_type: order_type,
            price: order.order_price,
            size: order.order_quantity,
            remain_size: remain_size,
            status: order_status,
            account_change: account_change,
            message: "".to_string(),
            fills: fills,
        };

        return r;
    }
    */
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "e")]
pub enum BinanceUserStreamMessage {
    outboundAccountPosition(BinanceAccountUpdate),
    balanceUpdate(BinanceBalanceUpdate),
    executionReport(BinanceExecutionReport),
}

impl BinanceUserStreamMessage {
    pub fn convert_to_market_message(&self, config: &BinanceConfig) -> MarketMessage {
        let mut message = MarketMessage::new();

        log::debug!("RAW user stream:\n{:?}\n", self);

        match self {
            BinanceUserStreamMessage::outboundAccountPosition(account) => {
                let status = binance_account_update_to_account_status(config, account);
                message.account = Some(status);
            }
            BinanceUserStreamMessage::balanceUpdate(balance) => {
                log::error!("not implemented");
            }
            BinanceUserStreamMessage::executionReport(order) => {
                let mut order: Order = order.into();
                order.update_balance(&config.market_config);
                message.order = Some(order);
            }
        };

        message
    }
}

/*
impl Into<Order> for BinanceUserStreamMessage {
    fn into(self) -> Order {
        match self {
            BinanceUserStreamMessage::executionReport(order) => order.into(),
            _ => {
                log::error!("not supported");
                Order {
                    symbol: "".to_string(),
                    create_time: 0,
                    order_id: "".to_string(),
                    order_list_index: 0,
                    client_order_id: "".to_string(),
                    order_side: OrderSide::Buy,
                    order_type: OrderType::Limit,
                    price: Decimal::from(0),
                    size: Decimal::from(0),
                    remain_size: Decimal::from(0),
                    status: OrderStatus::New,
                    account_change: AccountChange::new(),
                    message: "".to_string(),
                    fills: None,
                    profit: None,
                }
            }
        }
    }
}
*/

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
#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceAccountInformation {
    makerCommission: i64,
    takerCommission: i64,
    buyerCommission: i64,
    sellerCommission: i64,
    commissionRates: BinanceCommissionRates,
    canTrade: bool,
    canWithdraw: bool,
    canDeposit: bool,
    brokered: bool,
    requireSelfTradePrevention: bool,
    preventSor: bool,
    updateTime: u64,
    accountType: String,
    balances: Vec<BinanceAccountBalance>,
    permissions: Vec<String>,
    uid: i64,
}

#[pymethods]
impl BinanceAccountInformation {
    pub fn __repr__(&self) -> String {
        serde_json::to_string(self).unwrap()
        // format!("{:?}", self)
    }
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceCommissionRates {
    maker: Decimal,
    taker: Decimal,
    buyer: Decimal,
    seller: Decimal,
}

#[pyclass]
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceAccountBalance {
    asset: String,
    free: Decimal,
    locked: Decimal,
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
    time: u64,
    updateTime: u64,
    isWorking: bool,
    workingTime: u64,
    origQuoteOrderQty: Decimal,
    selfTradePreventionMode: String,
}


impl From<BinanceOrderStatus> for Order {
    fn from(border: BinanceOrderStatus) -> Order {
        let order_side: OrderSide = border.side.as_str().into();
        let order_type: OrderType = border.order_type.as_str().into();
        let order_status = border.status;

        let mut order = Order::new(
            border.symbol,
            binance_to_microsec(border.time),
            border.orderId.to_string(),
            border.clientOrderId,
            order_side,
            order_type,
            order_status,
            border.price,
            border.origQty,
        );

        order.remain_size =  border.origQty - border.executedQty;
        //order.transaction_id: String,
        order.update_time = binance_to_microsec(border.updateTime);
        //order.execute_price 
        order.execute_size = border.executedQty;
        // order.quote_vol: Decimal,
        //order.commission: Decimal,
        //order.commission_asset: String,
        order.is_maker = border.isWorking;  // on board it's maker
        // order.message: String,

        order
    }
}




#[pymethods]
impl BinanceOrderStatus {
    /*
        // TODO: implement to Order struct
        pub fn to_order(&self) -> Order {
            let order = Order::new(
            /*symbol*/
            self.symbol.clone(),
            /*size_in_price_currency*/
            Decimal::new(0, 0),
            /*create_time*/
            binance_to_microsec(self.time),
            //order_id
            self.orderId.to_string(),
            //client_order_id,
            self.clientOrderId.clone(),
            //order_side,
            OrderSide::from_str(self.side.as_ref().unwrap().as_str()).unwrap(),
            //order_type,
            OrderType::from_str(self.order_type.as_ref().unwrap().as_str()).unwrap(),
            //size,
            self.origQty,
            //filled_size,
            self.executedQty,
            None,
            //price,
            self.price,
            //home_change,
            None,
            //foreign_change,
            None,
            //profit,
            None,
            //maker,
            None,
            //fee,
            None,
            //message,
            None
            );

            return order;
        }
    */

    pub fn __repr__(&self) -> String {
        serde_json::to_string(self).unwrap()
        // format!("{:?}", self)
    }
}

#[cfg(test)]
mod binance_message_test {
    use super::*;
    use crate::exchange::binance::message::{
        BinanceCancelOrderResponse, BinanceExecutionReport, BinanceOrderResponse,
        BinancePublicWsMessage, BinanceTradeMessage, BinanceWsBoardUpdate, BinanceWsTradeMessage,
    };

    const HISTORY: &str = r#"[{"id":990877266,"price":"26092.63000000","qty":"0.00046000","quoteQty":"12.00260980","time":1692935644243,"isBuyerMaker":false,"isBestMatch":true},{"id":990877267,"price":"26093.10000000","qty":"0.00189000","quoteQty":"49.31595900","time":1692935644243,"isBuyerMaker":false,"isBestMatch":true},{"id":990877268,"price":"26093.75000000","qty":"0.00209000","quoteQty":"54.53593750","time":1692935644276,"isBuyerMaker":false,"isBestMatch":true},{"id":990877269,"price":"26094.00000000","qty":"0.00306000","quoteQty":"79.84764000","time":1692935647281,"isBuyerMaker":false,"isBestMatch":true},{"id":990877270,"price":"26094.00000000","qty":"0.00075000","quoteQty":"19.57050000","time":1692935647283,"isBuyerMaker":false,"isBestMatch":true}]"#;

    #[test]
    fn test_binance_message() {
        let message: Vec<BinanceTradeMessage> = serde_json::from_str(HISTORY).unwrap();

        println!("{:?}", message);
    }

    const BOARD_UPDATE: &str = r#"{"e":"depthUpdate","E":1693266904308,"s":"BTCUSDT","U":38531387766,"u":38531387832,"b":[["26127.87000000","20.79393000"],["26126.82000000","0.02674000"],["26125.95000000","0.00000000"],["26125.78000000","0.38302000"],["26125.68000000","0.00000000"],["26125.10000000","0.00000000"],["26125.05000000","0.00000000"],["26124.76000000","0.00000000"],["26124.75000000","0.21458000"],["26114.84000000","1.14830000"],["26114.15000000","0.00000000"],["26090.85000000","0.00000000"],["26090.84000000","0.00000000"],["26090.32000000","2.29642000"],["26090.31000000","3.82738000"],["26087.99000000","0.03733000"],["26084.34000000","0.00000000"],["25553.07000000","0.13647000"],["25500.81000000","0.14160000"],["25496.85000000","0.00000000"],["25284.00000000","0.03996000"],["24827.83000000","0.00000000"],["24300.17000000","0.00000000"],["23772.50000000","0.00047000"],["23515.08000000","0.00000000"],["18289.50000000","0.00000000"],["13063.93000000","0.00091000"]],"a":[["26127.88000000","5.58099000"],["26128.39000000","0.20072000"],["26128.79000000","0.21483000"],["26129.26000000","0.38297000"],["26129.52000000","0.00000000"],["26129.53000000","0.00000000"],["26134.50000000","0.06000000"],["26134.99000000","1.07771000"],["26135.10000000","0.00700000"],["26155.27000000","0.00050000"],["26155.28000000","0.00000000"],["27027.87000000","0.00200000"],["27290.25000000","0.00000000"],["27817.92000000","0.00000000"],["28345.58000000","0.00000000"]]}"#;

    #[test]
    fn test_binance_board_update() {
        let message: BinanceWsBoardUpdate = serde_json::from_str(BOARD_UPDATE).unwrap();

        println!("{:?}", message);
    }

    const TRADE_WS: &str = r#"{"e":"trade","E":1693226465430,"s":"BTCUSDT","t":3200243634,"p":"26132.02000000","q":"0.00244000","b":22161265544,"a":22161265465,"T":1693226465429,"m":false,"M":true}"#;

    #[test]
    fn test_binance_trade_message() {
        let message: BinanceWsTradeMessage = serde_json::from_str(TRADE_WS).unwrap();

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
    fn test_binance_order_response() {
        let order_response: BinanceOrderResponse = serde_json::from_str(r#"{"symbol":"BTCUSDT","orderId":28,"orderListId":-1,"clientOrderId":"6gCrw2kRUAF9CvJDGP16IP","transactTime":1507725176595,"price":"0.00000000","origQty":"10.00000000","executedQty":"10.00000000","cummulativeQuoteQty":"10.00000000","status":"FILLED","timeInForce":"GTC","type":"MARKET","side":"SELL","workingTime":1507725176595,"selfTradePreventionMode":"NONE","fills":[{"price":"4000.00000000","qty":"1.00000000","commission":"4.00000000","commissionAsset":"USDT","tradeId":56},{"price":"3999.00000000","qty":"5.00000000","commission":"19.99500000","commissionAsset":"USDT","tradeId":57},{"price":"3998.00000000","qty":"2.00000000","commission":"7.99600000","commissionAsset":"USDT","tradeId":58},{"price":"3997.00000000","qty":"1.00000000","commission":"3.99700000","commissionAsset":"USDT","tradeId":59},{"price":"3995.00000000","qty":"1.00000000","commission":"3.99500000","commissionAsset":"USDT","tradeId":60}]}"#).unwrap();

        println!("{:?}", order_response);
    }

    #[test]
    fn test_binance_cancel_order_response() {
        let order_response: BinanceCancelOrderResponse = serde_json::from_str(r#"{"symbol":"LTCBTC","origClientOrderId":"myOrder1","orderId":4,"orderListId":-1,"clientOrderId":"cancelMyOrder1","transactTime":1684804350068,"price":"2.00000000","origQty":"1.00000000","executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"CANCELED","timeInForce":"GTC","type":"LIMIT","side":"BUY","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);
    }
    
    // TODO: test cancel all orders response
    #[test]
    fn test_binance_cancel_all_orders() {
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
            "#).unwrap();

        println!("{:?}", order_response);
    }
    
    #[test]
    fn test_binance_exution_report() {
        let order_response: BinanceUserStreamMessage = serde_json::from_str(r#"{"e":"executionReport","E":1499405658658,"s":"ETHBTC","c":"mUvoqJxFIILMdfAW5iGSOW","S":"BUY","o":"LIMIT","f":"GTC","q":"1.00000000","p":"0.10264410","P":"0.00000000","F":"0.00000000","g":-1,"C":"","x":"NEW","X":"NEW","r":"NONE","i":4293153,"l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,"T":1499405658657,"t":-1,"I":8641984,"w":true,"m":false,"M":false,"O":1499405658657,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000","W":1499405658657,"V":"NONE"}"#).unwrap();

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
    }

    #[test]
    fn test_binance_order_status() {
        let order_response: BinanceOrderStatus = serde_json::from_str(
            r#"{"symbol":"LTCBTC","orderId":1,"orderListId":-1,"clientOrderId":"myOrder1","price":"0.1","origQty":"1.0","executedQty":"0.0","cummulativeQuoteQty":"0.0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.0","icebergQty":"0.0","time":1499827319559,"updateTime":1499827319559,"isWorking":true,"workingTime":1499827319559,"origQuoteOrderQty":"0.000000","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);
    }

    #[test]
    fn test_binance_order_status_cap_NEW() {
        let order_response: BinanceOrderStatus = serde_json::from_str(
            r#"{"symbol":"LTCBTC","orderId":1,"orderListId":-1,"clientOrderId":"myOrder1","price":"0.1","origQty":"1.0","executedQty":"0.0","cummulativeQuoteQty":"0.0","status":"NEW","timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.0","icebergQty":"0.0","time":1499827319559,"updateTime":1499827319559,"isWorking":true,"workingTime":1499827319559,"origQuoteOrderQty":"0.000000","selfTradePreventionMode":"NONE"}"#).unwrap();

        println!("{:?}", order_response);
    }


    #[test]
    fn test_binance_execution_report() {
        let execution: BinanceUserStreamMessage = serde_json::from_str(
            r#"
            {"e":"outboundAccountPosition","E":1694430911241,"u":1694430911240,"B":[{"a":"BTC","f":"1.16500000","l":"0.00000000"},{"a":"BUSD","f":"9599.07923000","l":"25.00000000"}]}
            "#).unwrap();
    }

    #[test]
    fn test_binance_list_orders_response() {
        let list = r#"[{"symbol":"BNBBTC","id":28457,"orderId":100234,"orderListId":-1,"price":"4.00000100","qty":"12.00000000","quoteQty":"48.000012","commission":"10.10000000","commissionAsset":"BNB","time":1499865549590,"isBuyer":true,"isMaker":false,"isBestMatch":true}]"#;

        let list: Vec<BinanceListOrdersResponse> = serde_json::from_str(list).unwrap();
    }


}

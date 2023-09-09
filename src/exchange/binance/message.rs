use rust_decimal::Decimal;
use serde::de::{self, Deserialize, Deserializer};
use serde_derive::{Deserialize, Serialize};
use strum_macros::Display;

use crate::{
    common::{
        order::{OrderSide, Trade},
        time::MicroSec,
    },
    exchange::BoardItem,
};

use super::super::string_to_f64;

pub type BinanceMessageId = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceSubscriptionReply {
    pub result: Option<String>,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "e")]
pub enum BinancePublicWsMessage {
    #[serde(rename = "trade")]
    Trade(BinanceWsTradeMessage),
    #[serde(rename = "depthUpdate")]
    BoardUpdate(BinanceWsBoardUpdate),
}

//  {"result":null,"id":1}
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceWsRespond {
    pub result: Option<String>,
    pub id: BinanceMessageId,
}

/// Represents a trade message received from the Binance exchange.
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceTradeMessage {
    pub id: BinanceMessageId,
    #[serde(deserialize_with = "string_to_f64")]
    pub price: f64,
    #[serde(rename = "qty", deserialize_with = "string_to_f64")]
    pub size: f64,
    #[serde(rename = "quoteQty", deserialize_with = "string_to_f64")]
    pub volume_in_foreign: f64,
    pub time: u64,
    #[serde(rename = "isBuyerMaker")]
    pub is_buyer_maker: bool,
    #[serde(rename = "isBestMatch")]
    pub is_best_match: bool,
}

impl BinanceTradeMessage {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: (self.time * 1_000) as MicroSec,
            price: self.price,
            size: self.size,
            order_side: if self.is_buyer_maker {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            id: self.id.to_string(),
        };
    }
}

// {"e":"trade","E":1693226465430,"s":"BTCUSDT","t":3200243634,"p":"26132.02000000","q":"0.00244000","b":22161265544,"a":22161265465,"T":1693226465429,"m":false,"M":true}
#[derive(Debug, Serialize, Deserialize)]
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
            time: (self.time * 1_000) as MicroSec,
            price: self.p.parse::<f64>().unwrap(),
            size: self.q.parse::<f64>().unwrap(),
            order_side: if self.m {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            id: self.t.to_string(),
        };
    }
}

// hold the latest board(as sample message blow)
// {"lastUpdateId":18735297989,"bids":[["25993.48000000","0.15981000"],["25991.09000000","0.36750000"],["25991.08000000","0.03846000"]],"asks":[["25993.49000000","0.05770000"],["25993.50000000","0.00060000"],["25994.12000000","0.06100000"]]}
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceRestBoard {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<BoardItem>,
    pub asks: Vec<BoardItem>,
}

// {"e":"depthUpdate","E":1693266904308,"s":"BTCUSDT","U":38531387766,"u":38531387832,"b":[["26127.87000000","20.79393000"],["26126.82000000","0.02674000"],["26125.95000000","0.00000000"],["26125.78000000","0.38302000"],["26125.68000000","0.00000000"],["26125.10000000","0.00000000"],["26125.05000000","0.00000000"],["26124.76000000","0.00000000"],["26124.75000000","0.21458000"],["26114.84000000","1.14830000"],["26114.15000000","0.00000000"],["26090.85000000","0.00000000"],["26090.84000000","0.00000000"],["26090.32000000","2.29642000"],["26090.31000000","3.82738000"],["26087.99000000","0.03733000"],["26084.34000000","0.00000000"],["25553.07000000","0.13647000"],["25500.81000000","0.14160000"],["25496.85000000","0.00000000"],["25284.00000000","0.03996000"],["24827.83000000","0.00000000"],["24300.17000000","0.00000000"],["23772.50000000","0.00047000"],["23515.08000000","0.00000000"],["18289.50000000","0.00000000"],["13063.93000000","0.00091000"]],"a":[["26127.88000000","5.58099000"],["26128.39000000","0.20072000"],["26128.79000000","0.21483000"],["26129.26000000","0.38297000"],["26129.52000000","0.00000000"],["26129.53000000","0.00000000"],["26134.50000000","0.06000000"],["26134.99000000","1.07771000"],["26135.10000000","0.00700000"],["26155.27000000","0.00050000"],["26155.28000000","0.00000000"],["27027.87000000","0.00200000"],["27290.25000000","0.00000000"],["27817.92000000","0.00000000"],["28345.58000000","0.00000000"]]}

#[derive(Debug, Serialize, Deserialize)]
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

#[derive(Debug, Serialize, Deserialize)]
// ["26127.87000000","20.79393000"]
pub struct BinanceWsBoardItem {
    #[serde(deserialize_with = "string_to_f64")]
    pub price: f64,
    #[serde(deserialize_with = "string_to_f64")]
    pub size: f64,
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
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderFill {
    price: Decimal,
    qty: Decimal,
    commission: Decimal,
    commissionAsset: String,
    tradeId: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceOrderResponse {
    symbol: String,
    orderId: i64,
    orderListId: i64,
    clientOrderId: String,
    transactTime: u64,
    price: Option<Decimal>,
    origQty: Option<Decimal>,
    executedQty: Option<Decimal>,
    cummulativeQuoteQty: Option<Decimal>,
    status: Option<String>,
    timeInForce: Option<String>,
    #[serde(rename = "type")]
    order_type: Option<String>,
    side: Option<String>,
    workingTime: Option<u64>,
    selfTradePreventionMode: Option<String>,
    fills: Option<Vec<BinanceOrderFill>>,
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
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceAccountUpdate {
    // e: String,
    E: u64,
    u: u64,
    B: Vec<BinanceBalance>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceBalance {
    a: String,
    f: Decimal,
    l: Decimal,
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
#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceExecutionReport {
    // e: String,
    E: u64,
    s: String,
    c: String,
    S: String,
    o: String,
    f: String,
    q: Decimal,
    p: Decimal,
    P: Decimal,
    F: Decimal,
    g: i64,
    C: String,
    x: String,
    X: String,
    r: String,
    i: i64,
    l: Decimal,
    z: Decimal,
    L: Decimal,
    n: Decimal,
    N: Option<String>,
    T: u64,
    t: i64,
    I: i64,
    w: bool,
    m: bool,
    M: bool,
    O: u64,
    Z: Decimal,
    Y: Decimal,
    Q: Decimal,
    W: u64,
    V: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "e")]
pub enum BinanceUserStreamMessage {
    outboundAccountPosition(BinanceAccountUpdate),
    balanceUpdate(BinanceBalanceUpdate),
    executionReport(BinanceExecutionReport),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct BinanceCommissionRates {
    maker: Decimal,
    taker: Decimal,
    buyer: Decimal,
    seller: Decimal,
}

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
    status: String,
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

}

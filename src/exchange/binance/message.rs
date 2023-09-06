use serde::de::{self, Deserialize, Deserializer};
use serde_derive::{Deserialize, Serialize};
use strum_macros::Display;

use crate::{common::{
    order::{OrderSide, Trade},
    time::MicroSec,
}, exchange::BoardItem};

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
    pub s: String, // "s":"BTCUSDT"            Symbol
    pub t: BinanceMessageId,    // "t":3200243634           Trade ID
    pub p: String, // "p":"26132.02000000"     Price
    pub q: String, // "q":"0.00244000"         Quantity
    pub b: BinanceMessageId,    // "b":22161265544          Buyer order ID
    pub a: BinanceMessageId,    // "a":22161265465          Seller order ID
    #[serde(rename = "T")]
    pub time: u64, // "T":1693226465429        Trade time
    pub m: bool,   // "m":false                Is the buyer the market maker?
    pub M: bool,   // "M":true                 Ignore
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
    pub s: String, // "s":"BTCUSDT"            Symbol
    pub U: BinanceMessageId,    // "U":38531387766          First update ID in event
    pub u: BinanceMessageId,    // "u":38531387832          Final update ID in event
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

#[cfg(test)]
mod binance_message_test {
    use crate::exchange::binance::message::{BinanceTradeMessage, BinanceWsBoardUpdate, BinanceWsTradeMessage, BinancePublicWsMessage};

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

}

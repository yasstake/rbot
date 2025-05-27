use std::str::FromStr as _;

use rbot_lib::common::{
    string_to_decimal, BoardItem, BoardTransfer, Kline, LogStatus, MicroSec, MultiMarketMessage,
    Order, OrderSide, OrderStatus, OrderType, Trade,
};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{self, Deserialize, Serialize};
use serde_json::{self, Value};
use json5;

// {"transaction_id":1173302044,"side":"sell","price":"9097038","amount":"0.1000","executed_at":1724716801484}

pub fn bitbank_timestamp_to_microsec(timestamp: i64) -> MicroSec {
    timestamp * 1_000
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitbankRestResponse {
    pub success: i64,
    pub data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BitbankOrder {
    pub order_id: i64,
    pub pair: String,
    pub side: String,
    pub position_side: Option<String>,
    #[serde(rename = "type")]
    pub order_type: String,
    #[serde(deserialize_with = "string_to_decimal")]
    pub start_amount: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub remaining_amount: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub executed_amount: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub price: Decimal,
    pub post_only: bool,
    pub user_cancelable: bool,
    #[serde(deserialize_with = "string_to_decimal")]
    pub average_price: Decimal,
    pub ordered_at: i64,
    pub expire_at: Option<i64>,
    //#[serde(deserialize_with = "string_to_decimal")]
    // pub trigger_price: Decimal,
    pub status: String,
}

pub fn bitbank_order_status(status: &str) -> OrderStatus {
    match status {
        //注文ステータス: INACTIVE 非アクティブ, UNFILLED 注文中, PARTIALLY_FILLED 注文中(一部約定), FULLY_FILLED 約定済み, CANCELED_UNFILLED 取消済, CANCELED_PARTIALLY_FILLED 取消済(一部約定)
        "INACTIVE" => OrderStatus::New,
        "UNFILLED" => OrderStatus::New,
        "PARTIALLY_FILLED" => OrderStatus::PartiallyFilled,
        "FULLY_FILLED" => OrderStatus::Filled,
        "CANCELED_UNFILLED" => OrderStatus::Canceled,
        "CANCELED_PARTIALLY_FILLED" => OrderStatus::Canceled,
        _ => {
            log::error!("unknown order status: {:?}", status);
            OrderStatus::Unknown
        }
    }
}

impl Into<Order> for BitbankOrder {
    fn into(self) -> Order {
        let order_type = OrderType::from(&self.order_type);
        let order_side = OrderSide::from(&self.side);

        Order {
            category: "spot".to_string(),
            symbol: self.pair,
            create_time: bitbank_timestamp_to_microsec(self.ordered_at),
            status: bitbank_order_status(&self.status),
            order_id: self.order_id.to_string(),
            client_order_id: "".to_string(),
            order_side,
            order_type,
            order_price: self.price,
            order_size: self.start_amount,
            remain_size: self.remaining_amount,
            transaction_id: self.order_id.to_string(),
            update_time: self.ordered_at * 1000,
            execute_price: self.average_price,
            execute_size: self.executed_amount,
            quote_vol: self.price * self.start_amount,
            commission: dec![0.0],
            commission_asset: "".to_string(),
            is_maker: order_type.is_maker(),
            message: "".to_string(),
            commission_home: dec![0.0],
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

#[derive(Serialize, Deserialize, Debug)]
pub struct BitbankCancelOrderData {
    pub success: i64,
    pub data: Value,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BitbankDepth {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
    #[serde(deserialize_with = "string_to_decimal")]
    pub asks_over: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub bids_under: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub asks_under: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub bids_over: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub ask_market: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub bid_market: Decimal,
    pub timestamp: i64,
    #[serde(rename = "sequenceId")]
    pub sequence_id: String,
}

impl Into<BoardTransfer> for BitbankDepth {
    fn into(self) -> BoardTransfer {
        let mut board = BoardTransfer::new();
        board.asks = self
            .asks
            .into_iter()
            .map(|a| BoardItem {
                price: Decimal::from_str(&a[0]).unwrap(),
                size: Decimal::from_str(&a[1]).unwrap(),
            })
            .collect();
        board.bids = self
            .bids
            .into_iter()
            .map(|b| BoardItem {
                price: Decimal::from_str(&b[0]).unwrap(),
                size: Decimal::from_str(&b[1]).unwrap(),
            })
            .collect();
        board
    }
}

impl Into<BoardTransfer> for BitbankRestResponse {
    fn into(self) -> BoardTransfer {
        let depth = serde_json::from_value::<BitbankDepth>(self.data.clone()).unwrap();
        depth.into()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitbankTransaction {
    transaction_id: i64,
    side: String,
    #[serde(deserialize_with = "string_to_decimal")]
    price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    #[serde(rename = "amount")]
    size: Decimal,
    #[serde(rename = "executed_at")]
    timestamp: i64,
}

impl Into<Vec<Trade>> for BitbankRestResponse {
    fn into(self) -> Vec<Trade> {
        let transactions =
            serde_json::from_value::<Vec<BitbankTransaction>>(self.data["transactions"].clone())
                .unwrap();
        transactions
            .into_iter()
            .map(|t| t.into())
            .collect::<Vec<Trade>>()
    }
}

impl Into<Trade> for BitbankTransaction {
    fn into(self) -> Trade {
        let timestamp = bitbank_timestamp_to_microsec(self.timestamp);
        let order_side = OrderSide::from(&self.side);
        let id = format!("{:?}", self.transaction_id);

        Trade {
            time: timestamp,
            order_side,
            price: self.price,
            size: self.size,
            status: LogStatus::FixArchiveBlock,
            id,
        }
    }
}

impl Into<Vec<Kline>> for BitbankRestResponse {
    fn into(self) -> Vec<Kline> {
        let candlestick = &self.data["candlestick"][0]["ohlcv"];
        let mut klines: Vec<Kline> = Vec::new();

        for kline in candlestick.as_array().unwrap() {
            let open = Decimal::from_str(&kline[0].as_str().unwrap()).unwrap();
            let high = Decimal::from_str(&kline[1].as_str().unwrap()).unwrap();
            let low = Decimal::from_str(&kline[2].as_str().unwrap()).unwrap();
            let close = Decimal::from_str(&kline[3].as_str().unwrap()).unwrap();
            let volume = Decimal::from_str(&kline[4].as_str().unwrap()).unwrap();
            let timestamp = bitbank_timestamp_to_microsec(kline[5].as_i64().unwrap());

            klines.push(Kline {
                open,
                high,
                low,
                close,
                volume,
                timestamp,
            });
        }

        klines
    }
}


#[derive(Debug, Serialize, Deserialize)]
pub struct BitbankPrivateStreamKey {
    pub pubnub_channel: String,
    pub pubnub_token: String,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct BitbankPublicWsMessage {
    pub room_name: String,
    pub data: BitbankPublicWsMessageData,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum BitbankPublicWsMessageData {
    Board(BitbankDepth),
    Snapshot(BitbankSnapshot),
    Transactions(BitbankTransactions),
}

impl BitbankPublicWsMessage {
    pub fn from_str(message: &str) -> anyhow::Result<Self> {
        let message: Vec<Value> = json5::from_str(message)?;

        if let Some(event_type) = message.get(0).and_then(|v| v.as_str()) {
            println!("Event type: {}", event_type);
        }

        let message = message.get(1).unwrap().clone();
        let room_name = message.get("room_name").unwrap().as_str().unwrap();

        if room_name.starts_with("depth_diff_") {
            let depth_diff = serde_json::from_value::<BitbankSnapshot>(message.get("message").unwrap().get("data").unwrap().clone())?;
            return Ok(
                BitbankPublicWsMessage {
                    room_name: room_name.to_string(),
                    data: BitbankPublicWsMessageData::Snapshot(depth_diff),
                }
            );
        }
        else if room_name.starts_with("depth_whole_") {
            let depth_whole = serde_json::from_value::<BitbankDepth>(message.get("message").unwrap().get("data").unwrap().clone())?;
            return Ok(
                BitbankPublicWsMessage {
                    room_name: room_name.to_string(),
                    data: BitbankPublicWsMessageData::Board(depth_whole),
                }
            );
        }
        else if room_name.starts_with("transactions_") {
            let transactions = serde_json::from_value::<BitbankTransactions>(message.get("message").unwrap().get("data").unwrap().clone())?;
            return Ok(
                BitbankPublicWsMessage {
                    room_name: room_name.to_string(),
                    data: BitbankPublicWsMessageData::Transactions(transactions),
                }
            );
        }
        else {
            return Err(anyhow::anyhow!("Invalid room name: {}", room_name));
        }
    }
}




#[derive(Debug, Serialize, Deserialize)]
pub struct BitbankTransactions{
    pub transactions: Vec<BitbankTransaction>,
}


#[derive(Debug, Serialize, Deserialize)]

pub struct BitbankSnapshot {
    #[serde(rename = "b")]
    pub bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    pub asks: Vec<[String; 2]>,
    #[serde(rename = "ao")]
    pub asks_over: Option<String>,
    #[serde(rename = "bu")]
    pub bids_under: Option<String>,
    #[serde(rename = "am")]
    pub ask_market: Option<String>,
    #[serde(rename = "bm")]
    pub bid_market: Option<String>,
    #[serde(rename = "t")]
    pub timestamp: i64,
    #[serde(rename = "s")]
    pub sequence_id: String,
}


pub struct BitbankPrivateWsMessage {
    pub message: Value,
}


#[cfg(test)]
mod test_bitbank_message {
    use anyhow::anyhow;
    use rbot_lib::common::{BoardTransfer, Kline, Trade};

    use crate::{
        BitbankPublicWsMessage, BitbankRestResponse
    };

    const MESSAGE: &str = r#"
    {"success":1,"data":{"transactions":[{"transaction_id":1173386862,"side":"buy","price":"8613303","amount":"0.0001","executed_at":1724803202489},{"transaction_id":1173386863,"side":"buy","price":"8613303","amount":"0.0006","executed_at":1724803203116}]}}
"#;

    #[test]
    fn test_parse_response() -> anyhow::Result<()> {
        let message = serde_json::from_str::<BitbankRestResponse>(MESSAGE)?;

        let trades: Vec<Trade> = message.into();
        println!("{:?}", trades);

        Ok(())
    }

    #[test]

    fn test_parse_board_snapshot() -> anyhow::Result<()> {
        const MESSAGE: &str = r#"
        {
            "success": 1,
            "data": {
                "asks": [["8613303", "0.0001"], ["8613302", "0.0002"]],
                "bids": [["8613304", "0.0003"], ["8613305", "0.0004"]]
            }
        }
        "#;
        let message = serde_json::from_str::<BitbankRestResponse>(MESSAGE)?;

        let board: BoardTransfer = message.into();

        println!("{:?}", board);

        Ok(())
    }

    #[tokio::test]
    async fn test_parse_klines() -> anyhow::Result<()> {
        const MESSAGE: &str = r#"
{"success":1,"data":{"candlestick":
    [{"type":"1min","ohlcv":[
        ["15479864","15502282","15479864","15501110","0.0910",1747612800000],
        ["15501109","15501109","15468579","15468579","0.1250",1747612860000],
        ["15311163","15311163","15311163","15311163","0.0000",1747699140000]]}],"timestamp":1747699143282}}
"#;

        let message = serde_json::from_str::<BitbankRestResponse>(MESSAGE)?;

        if message.success == 0 {
            return Err(anyhow!("get_klines error: {:?}", message.data));
        }

        let klines: Vec<Kline> = message.into();
        println!("{:?}", klines);

        Ok(())
    }

    const EVENT_MESSAGE: &str = r#"
        "["message",{"room_name":"depth_diff_xrp_jpy","message":{"data":{"a":[["328.342","200"],["328.437","0"],["328.442","0"],["328.340","560.4881"],["328.374","78.5429"],["328.459","9253.7732"],["328.454","371.4093"]],"b":[["328.245","200"],["328.253","0"],["328.251","1000"],["328.216","0"],["317.001","0"],["328.202","0"]],"t":1748176124581,"s":"25555832386","ao":"20249010.0488","bu":"56926540.4967"}}}]"
    "#;

    #[test]
    fn test_parse_ws_depth() -> anyhow::Result<()> {
        let message = serde_json::from_str::<Vec<serde_json::Value>>(EVENT_MESSAGE)?;
        println!("{:?}", message);
        Ok(())
    }

    #[test]
    fn test_parse_ws_room_message() -> anyhow::Result<()> {
        const EVENT_MESSAGE: &str = r#"
        ["message",{"room_name":"transactions_xrp_jpy","message":{"pid":851205254,"data":{"transactions":[{"transaction_id":34745047,"side":"sell","price":"26.930","amount":"4703.5671","executed_at":1570080162855},{"transaction_id":34745046,"side":"sell","price":"26.930","amount":"500.0000","executed_at":1570080162829},{"transaction_id":34745045,"side":"sell","price":"26.930","amount":"378.0000","executed_at":1570080162802},{"transaction_id":34745044,"side":"sell","price":"26.930","amount":"12.0000","executed_at":1570080162758},{"transaction_id":34745043,"side":"sell","price":"26.930","amount":"301.4874","executed_at":1570080162725}]}}}]
    "#;

        let message = BitbankPublicWsMessage::from_str(EVENT_MESSAGE)?;
        println!("{:?}", message);
        Ok(())
    }

    #[test]
    fn test_parse_ws_board_whole() -> anyhow::Result<()> {
        const EVENT_MESSAGE: &str = r#"
[
    "message",
    {
        "room_name": "depth_whole_xrp_jpy",
        "message": {
            "data": {
                "bids": [
                    [
                        "27.537",
                        "6211.6210"
                    ],
                    [
                        "27.523",
                        "875.3413"
                    ],
                ],
                "asks": [
                    [
                        "27.538",
                        "7233.6837"
                    ],
                    [
                        "27.540",
                        "19.4551"
                    ],
                ],
                "asks_over": "0.123",
                "bids_under": "0.123",
                "asks_under": "0",
                "bids_over": "0",
                "ask_market": "0",
                "bid_market": "0",
                "timestamp": 1568344476514,
                "sequenceId": "1234567890"
            }
        }
    }
]        
"#;

        let message = BitbankPublicWsMessage::from_str(EVENT_MESSAGE)?;
        println!("{:?}", message);
        Ok(())
    }

    #[test]
    fn test_parse_ws_snapshot() -> anyhow::Result<()> {
        const EVENT_MESSAGE: &str = r#"
["message",{"room_name":"depth_diff_xrp_jpy","message":{"data":{"a":[],"b":[["26.212","1000.0000"],["26.815","0"]],"t":1570080270100,"s":"1234567893"}}}]
        "#;
        let message = BitbankPublicWsMessage::from_str(EVENT_MESSAGE)?;
        println!("{:?}", message);
        Ok(())
    }
}

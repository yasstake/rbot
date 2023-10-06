


use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde_derive::Serialize;
use serde_derive::Deserialize;
use serde_json::Value;

use crate::common::OrderSide;
use crate::common::Trade;
use crate::exchange::string_to_i64;

use super::super::string_to_f64;


#[derive(Debug, Serialize, Deserialize)]
pub struct BybitTradeMessage {
    #[serde(rename="retCode")]
    ret_code: i32,  // "retCode":0
    #[serde(rename="retMsg")]
    ret_msg: String, // "retMsg":"OK"
    result: BybitTradeMessageResult, //"result":{xxxx}
    #[serde(rename="retExtInfo")]
    ret_ext_info: Value, // "retExtInfo":{}
    time: i64, // "time":1693025204738
}

impl BybitTradeMessage {
    pub fn to_trades(&self) -> Vec<Trade>{
        let mut trade: Vec<Trade> = vec![];

        for t in &self.result.list {
            trade.push(t.to_trade());
        }

        trade
    }
}




#[derive(Debug, Serialize, Deserialize)]
pub struct BybitTradeMessageResult {
    category: String,     // "category":""
    list: Vec<BybitTrade>, // "list": [xxxx]
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BybitTrade {
    #[serde(rename="execId")]
    exec_id: String,   // "execId":"a68aac05-d414-5bab-a3f9-bee60dfe0c34"
    symbol: String,    // "symbol":"BTCUSD"
    #[serde(deserialize_with = "string_to_f64")]    
    price: f64,   // "price":"26043.00"
    #[serde(deserialize_with = "string_to_f64")]    
    size: f64,  // "size":"504"
    side: String,  // "side":"Buy"
    #[serde(deserialize_with = "string_to_i64")]        
    time: i64, // "time":"1693025204735"
    #[serde(rename="isBlockTrade")]
    is_block_trade: bool, // "isBlockTrade":false
}

impl BybitTrade {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: self.time * 1_000,
            price: Decimal::from_f64(self.price).unwrap(),
            size: Decimal::from_f64(self.size).unwrap(),
            order_side: if self.side == "Buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            },
            id: self.exec_id.to_string(),
        };
    }
}

#[cfg(test)]
mod bybit_message_test {
    const RECENT_TRADE:&str = r#"{"retCode":0,"retMsg":"OK","result":{"category":"","list":[{"execId":"c75042a9-41b2-5305-b79d-2925e0f55246","symbol":"BTCUSD","price":"26062.50","size":"913","side":"Buy","time":"1693022317155","isBlockTrade":false},{"execId":"24acf946-298a-5eca-89cb-514f3844f02f","symbol":"BTCUSD","price":"26062.50","size":"1","side":"Buy","time":"1693022304532","isBlockTrade":false},{"execId":"c7ce4fbc-a44b-5643-bda1-5d215c8e8fb4","symbol":"BTCUSD","price":"26062.00","size":"5000","side":"Sell","time":"1693022304511","isBlockTrade":false}]},"retExtInfo":{},"time":1693022397040}"#;

    #[test]
    fn test_parse_recent_trade() {
        use super::BybitTradeMessage;

        let result: BybitTradeMessage = serde_json::from_str(RECENT_TRADE).unwrap();

        println!("result: {:?}", result);

        println!("result: {:?}", result.to_trades());
    }
}
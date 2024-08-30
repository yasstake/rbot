
use rbot_lib::common::{string_to_decimal, LogStatus, MicroSec, OrderSide, Trade};
use rust_decimal::Decimal;
use serde::{self, Deserialize, Serialize};
use serde_derive;
use serde_json::{self, Value};
use tokio::time;

// {"transaction_id":1173302044,"side":"sell","price":"9097038","amount":"0.1000","executed_at":1724716801484}

pub fn bitbank_timestamp_to_microsec(timestamp: i64) -> MicroSec {
    timestamp * 1_000
}


#[derive(Serialize, Deserialize, Debug)]
pub struct BitbankTransactions {
    transaction_id: i64,
    side: String,
    #[serde(deserialize_with = "string_to_decimal")]
    price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    #[serde(rename="amount")]
    size: Decimal,
    #[serde(rename="executed_at")]
    timestamp: i64
}

impl Into<Trade> for BitbankTransactions {
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

#[derive(Serialize, Deserialize, Debug)]
pub enum BitbankRestData {
    #[serde{rename="transactions"}]
    Transactions(Vec<BitbankTransactions>)
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BitbankRestResponse {
    pub success: i64,
    pub data: BitbankRestData
}



#[cfg(test)]
mod test_bitbank_message {
    use crate::BitbankRestResponse;

const MESSAGE: &str = r#"
    {"success":1,"data":{"transactions":[{"transaction_id":1173386862,"side":"buy","price":"8613303","amount":"0.0001","executed_at":1724803202489},{"transaction_id":1173386863,"side":"buy","price":"8613303","amount":"0.0006","executed_at":1724803203116}]}}
"#;

    #[test]
    fn test_parse_response() {
        let message = serde_json::from_str::<BitbankRestResponse>(MESSAGE);

        println!("{:?}", message);

    }


}
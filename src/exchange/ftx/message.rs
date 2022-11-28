

// https://docs.rs/serde_json/latest/serde_json/
// serde_json = {version = "1.0.87"}
// serde_derive = {version = "1.0.147}


use serde_derive::{Deserialize, Serialize};
use crate::common::order::Trade;
use crate::OrderSide;
use serde_json::Error;

//use log::Log;
use crate::common::time::parse_time;

#[derive(Debug, Serialize, Deserialize)]
pub struct FtxTradeMessage {
    pub success: bool,
    pub(crate) result: Vec<FtxTrade>
}

impl FtxTradeMessage {
    pub fn get_trades(&mut self) -> Vec<Trade>{
        let mut trade: Vec<Trade> = vec![];

        for t in &self.result {
            trade.push(t.to_trade());
        }

        trade
    }

    pub fn from_str(message: &str) -> Result<Self, Error> {
        let result = serde_json::from_str::<FtxTradeMessage>(message);

        match result {
            Ok(m) => {
                if ! m.success {
                    println!("REST ERROR {:?}", m);
                }

                Ok(m)
            },
            Err(e) => {
                log::debug!("Parse error {:?}", e);
                Err(e)
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FtxTrade {
    pub id: i64,   // "id":5196537114
    pub price: f64,   // "price":19226.0
    pub size: f64,    // "size":0.0147
    pub side: String, // "side":"sell"
    pub liquidation: bool, // "liquidation":false
    pub time: String       // "time":"2022-10-22T14:22:43.407735+00:00"
}

impl FtxTrade {
    pub fn to_trade(&self) -> Trade {
        return Trade {
            time: parse_time(self.time.as_str()),
            price: self.price,
            size: self.size,
            order_side: OrderSide::from_str(&self.side),
            id: self.id.to_string()
        }
    }
}

#[cfg(test)]
mod test_ftx_message {
    use crate::common::time::parse_time;
    use crate::exchange::ftx::message::FtxTradeMessage;

    const MESSAGE: &str = r#"
     {"success":true,
            "result":[
               {"id":5196537114,"price":19226.0,"size":0.0147,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:43.407735+00:00"},
               {"id":5196537109,"price":19226.0,"size":0.0823,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:43.325945+00:00"},
               {"id":5196537075,"price":19226.0,"size":0.0992,"side":"sell","liquidation":false,"time":"2022-10-22T14:22:42.465804+00:00"}
               ]
     }
    "#;


    #[test]
    fn test_ftx_trade_message() {
        let message: FtxTradeMessage = serde_json::from_str(MESSAGE).unwrap();

        println!("{:?}", message);
        assert_eq!(message.success, true);
        assert_eq!(message.result.len(), 3);
        println!("{:?}", parse_time(message.result[0].time.as_str() ));
    }

    #[test]
    fn test_ftx_trade_message_to_trade () {
        let mut message: FtxTradeMessage = serde_json::from_str(MESSAGE).unwrap();

        let trades = message.get_trades();
        println!("{:?}", trades);
    }
}







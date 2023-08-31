use serde_json::Value;

use crate::{exchange::{rest_get, bb::message::BybitTradeMessage}, common::order::Trade};


const SERVER: &str = "https://api.bybit.com";

fn get_recent_trades(symbol: &str) -> Result<Vec<Trade>, String> 
{
    let path = format!("/v5/market/recent-trade?symbol={}&limit=1000", symbol);

    let result = rest_get(SERVER, path.as_str());

//    print!("result: {:?}", result.unwrap());

    match result {
        Ok(message)=> {
            //let t: Vec<BybitTradeMessage> = serde_json::from_str(message.as_str()).unwrap();
            let m: BybitTradeMessage = serde_json::from_str(message.as_str()).unwrap();            
    
            Ok(m.to_trades())
        },
        Err(e) => {
            Err(e)
        }
    }
}

#[cfg(test)]
mod bybit_rest_test {
    #[test]
    fn test_get_recent_trades() {
        use super::get_recent_trades;

        let result = get_recent_trades("BTCUSD");

        println!("result: {:?}", result);
    }
}



use crate::{exchange::{rest_get, bb::message::BybitTradeMessage}, common::Trade};



fn get_recent_trades(symbol: &str) -> Result<Vec<Trade>, String> 
{
    let path = format!("/v5/market/recent-trade?symbol={}&limit=1000", symbol);

    pub const SERVER: &str = "https://api.bybit.com";        
    let result = rest_get(SERVER, path.as_str(), vec![], None, None);

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
    use super::*;


    pub const SERVER: &str = "https://api.bybit.com";    
    use crate::exchange::rest_get;

    #[test]
    fn test_get_recent_trades() {
        use super::get_recent_trades;

        let result = get_recent_trades("BTCUSD");

        println!("result: {:?}", result);
    }


    #[test]
    fn test_get_recent_trades2() {
        let path = format!("/v5/market/recent-trade?symbol={}&limit=3&start=1694015208464", "BTCUSD");

        let result = rest_get(SERVER, path.as_str(), vec![], None, None);

        println!("result: {:?}", result.unwrap());
    }

    #[test]
    fn test_get_recent_trades3() {
        let path = format!("/v5/market/historical-trade?symbol={}&limit=3&since=1694015208464", "BTCUSD");

        let result = rest_get(SERVER, path.as_str(), vec![], None, None);

        println!("result: {:?}", result.unwrap());
    }

}



use crate::{exchange::rest_get, common::{order::Trade, time::MicroSec}};
use super::message::BinanceTradeMessage;

const SERVER: &str ="https://api.binance.com";

fn get_recent_trades(symbol: &str) -> Result<Vec<Trade>, String> {
    let path = format!("/api/v3/trades?symbol={}&limit=1000", symbol);

    let result = rest_get(SERVER, path.as_str());

    match result {
        Ok(message)=> {
            let t: Vec<BinanceTradeMessage> = serde_json::from_str(message.as_str()).unwrap();
    
            let mut trades: Vec<Trade> = vec![];
        
            for m in t {
                trades.push(m.to_trade());
            }
            
            Ok(trades)        
        },
        Err(e) => {
            Err(e)
        }
    }
}

fn get_old_trade(symbol: &str, from_id: u64) -> Result<Vec<Trade>, String> {
    let mut path: String;

    if from_id == 0 {
        path = format!("/api/v3/historicalTrades?symbol={}&limit=1000", symbol);
    }
    else {
        path = format!("/api/v3/historicalTrades?symbol={}&fromId={}&limit=1000", symbol, from_id);
    }

    let result = rest_get(SERVER, path.as_str());

    match result {
        Ok(message)=> {
            let t: Vec<BinanceTradeMessage> = serde_json::from_str(message.as_str()).unwrap();
    
            let mut trades: Vec<Trade> = vec![];
        
            for m in t {
                trades.push(m.to_trade());
            }
            
            Ok(trades)        
        },
        Err(e) => {
            Err(e)
        }
    }
}

// get old trades until specified time
fn get_old_trade_until(symbol: &str, until: MicroSec) {
    let mut from_id = 0;

    let mut count = 0;

    loop {
        println!("from_id: {}", from_id);

        let result = get_old_trade(symbol, from_id);

        match result {
            Ok(trades) => {
                if trades.len() == 0 {
                    break;
                }

                println!("{} trades", trades.len());
                println!("{:?}", trades.last().unwrap().__str__());
                println!("{:?}", trades.first().unwrap().__str__());

                let last_trade = trades.first().unwrap();

                if last_trade.time < until {
                    break;
                }

                from_id = last_trade.id.parse::<u64>().unwrap();
                from_id -= 999;
            },
            Err(e) => {
                println!("Error: {:?}", e);
                break;
            }
        }

        count += 1;
        print!("{} ", count);

        if 1000 <= count {
            break;
        }
    }
}

#[cfg(test)]
mod binance_rest_tests {
    use crate::common::time::{NOW, HHMM};

    use super::*;

    #[test]
    fn test_get_recent_trades_success() {
        // Act
        let result = get_recent_trades("BTCUSDT");

        println!("{:?}", result);
    }

    #[test]
    fn test_get_recent_trades_error() {
        // Act
        let result = get_recent_trades("INVALID_SYMBOL");

        // Assert
//        assert!(result.is_err());
    }

    #[test]
    fn test_get_old_trade_until_success() {
        let until = NOW() - HHMM(24, 5);
        let result = get_old_trade_until("BTCUSDT", until);
    }    
}

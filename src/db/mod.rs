// use crate::exchange::ftx::FtxMarket;

use crate::exchange::binance::BinanceMarket;
use self::sqlite::TradeTable;

pub mod sqlite;
pub mod df;

pub fn open_db(exchange_name: &str, market_name: &str) -> TradeTable {
    match exchange_name.to_uppercase().as_str() {
        /*
        "FTX" => {
            let ftx = FtxMarket::new(market_name, true);

            return ftx.db;
        }
        */
        "BN" => {
            let binance = BinanceMarket::new(market_name, true);

            return binance.db;
        }
        _ => {
            panic!("Unknown exchange {}", exchange_name);
        }
    }
}
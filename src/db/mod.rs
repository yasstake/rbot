// use crate::exchange::ftx::FtxMarket;

use crate::exchange::binance::BinanceMarket;
use self::sqlite::TradeTable;

pub mod sqlite;
pub mod df;
pub mod hdf;

pub fn open_db(exchange_name: &str, market_name: &str) -> TradeTable {
    match exchange_name.to_uppercase().as_str() {
        /*
        "FTX" => {
            let ftx = FtxMarket::new(market_name, true);

            return ftx.db;
        }
        */
        "BN" => {
            // 参照用のみのため、order_in_homeは事実上ダミー。
            let binance = BinanceMarket::new(market_name, true);

            return binance.db;
        }
        _ => {
            panic!("Unknown exchange {}", exchange_name);
        }
    }
}
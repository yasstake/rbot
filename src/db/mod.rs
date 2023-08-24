// use crate::exchange::ftx::FtxMarket;

use crate::exchange::{binance::BinanceMarket, bb::BBMarket};
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
            log::debug!("open_db: Binance / {}", market_name);
            let binance = BinanceMarket::new(market_name, true);

            return binance.db;
        },
        "BB" => {
            log::debug!("open_db: ByBit / {}", market_name);            
            let bb = BBMarket::new(market_name, true);

            return bb.db;
        },
        _ => {
            panic!("Unknown exchange {}", exchange_name);
        }
    }
}
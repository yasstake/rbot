// use crate::exchange::ftx::FtxMarket;

use crate::exchange::{binance::BinanceMarket, bb::BBMarket};
use self::sqlite::TradeTable;

pub mod sqlite;
pub mod df;
pub mod hdf;


// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.

#[cfg(test)]
mod test;

use binance::{Binance, BinanceConfig};
use pyo3::{pymodule, types::PyModule, wrap_pyfunction, PyResult, Python};
use rbot_lib::common::{
    get_orderbook_list,
    get_orderbook,
    init_debug_log, init_log, time_string, 
    AccountPair, BoardItem, MarketConfig, Order, OrderSide, OrderStatus, OrderType, Trade, DAYS, DAYS_BEFORE, FLOOR_SEC, HHMM, MIN, NOW, SEC
};


use rbot_session::{Logger, Session, Runner, ExecuteMode};

use bybit::{Bybit, BybitConfig};




/*
use exchange::BoardItem;
use exchange::bybit::{BybitMarket, Bybit};
use exchange::bybit::config::{BybitConfig, BybitServerConfig};
use pyo3::prelude::*;
// use exchange::ftx::FtxMarket;
use exchange::binance::{BinanceMarket, BinanceConfig, Binance};
// use exchange::bb::BBMarket;

use common::*;
use session::{Session, ExecuteMode, Logger};
use session::Runner;
*/


// use net::{Broadcast, BroadcastMessage};
//use sim::back::BackTester;
//use sim::session::DummySession;

/// A Python module implemented in Rust.

use tracing_subscriber;


// use console_subscriber;
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    // console_subscriber::init();  // for tokio thread debug
    tracing_subscriber::fmt::init();

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(init_log, m)?)?;
    m.add_function(wrap_pyfunction!(init_debug_log, m)?)?;

    m.add_function(wrap_pyfunction!(get_orderbook_list, m)?)?;
    m.add_function(wrap_pyfunction!(get_orderbook, m)?)?;

    // time util
    m.add_function(wrap_pyfunction!(time_string, m)?)?;
    m.add_function(wrap_pyfunction!(NOW, m)?)?;
    m.add_function(wrap_pyfunction!(DAYS_BEFORE, m)?)?;
    m.add_function(wrap_pyfunction!(DAYS, m)?)?;
    m.add_function(wrap_pyfunction!(HHMM, m)?)?;
    m.add_function(wrap_pyfunction!(MIN, m)?)?;
    m.add_function(wrap_pyfunction!(SEC, m)?)?;

    m.add_function(wrap_pyfunction!(FLOOR_SEC, m)?)?;

    // classes
    m.add_class::<MarketConfig>()?;
    m.add_class::<OrderStatus>()?;
    m.add_class::<AccountPair>()?;
    
    m.add_class::<Logger>()?;

    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<OrderType>()?;
    m.add_class::<Trade>()?;
    m.add_class::<BoardItem>()?;

    m.add_class::<Session>()?;
    m.add_class::<Runner>()?;
    m.add_class::<ExecuteMode>()?;

    //m.add_class::<Broadcast>()?;
    //m.add_class::<BroadcastMessage>()?;
    
    // Binance
    m.add_class::<Binance>()?;
    m.add_class::<BinanceConfig>()?;
    

    // ByBit
    m.add_class::<Bybit>()?;
    m.add_class::<BybitConfig>()?;    
    /* 
    m.add_class::<BybitMarket>()?;

    m.add_class::<BybitServerConfig>()?;
    */

    Ok(())
}

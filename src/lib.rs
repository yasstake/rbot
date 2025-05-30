// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.
use pyo3::{pymodule, types::{PyModule, PyModuleMethods}, wrap_pyfunction, Bound, PyResult};
use rbot_lib::{common::{
    get_orderbook, get_orderbook_list, init_debug_log, init_log, time_string, AccountCoins, AccountPair,
    BoardItem, FeeType, MarketConfig, Order, OrderSide, OrderStatus, OrderType,
    ExchangeConfig, Trade, DAYS, DAYS_BEFORE, FLOOR_SEC, HHMM, MIN, NOW, SEC
}, db::{__delete_data_root, get_data_root, set_data_root}};

use rbot_session::{Logger, Session, Runner, ExecuteMode};
use bybit::{Bybit, BybitConfig};
use binance::{Binance, BinanceConfig};

// use binance::{Binance, BinanceConfig};


// use console_subscriber;
#[pymodule]
fn rbot(m: &Bound<'_, PyModule>) -> PyResult<()> {
// fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    // console_subscriber::init();  // for tokio thread debug
    //tracing_subscriber::fmt::init();

    m.add("__version__", env!("CARGO_PKG_VERSION"))?;

    m.add_function(wrap_pyfunction!(get_data_root, m)?)?;
    m.add_function(wrap_pyfunction!(set_data_root, m)?)?;

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

    m.add_function(wrap_pyfunction!(__delete_data_root, m)?)?;


    // classes
    m.add_class::<ExchangeConfig>()?;
    m.add_class::<MarketConfig>()?;
    m.add_class::<OrderStatus>()?;
    m.add_class::<AccountPair>()?;
    m.add_class::<AccountCoins>()?;    
    
    m.add_class::<Logger>()?;

    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<OrderType>()?;
    m.add_class::<Trade>()?;
    m.add_class::<BoardItem>()?;

    m.add_class::<Session>()?;
    m.add_class::<Runner>()?;
    m.add_class::<ExecuteMode>()?;

    m.add_class::<FeeType>()?;

    // Binance
    m.add_class::<Binance>()?;
    m.add_class::<BinanceConfig>()?;
    
    // ByBit
    m.add_class::<Bybit>()?;
    m.add_class::<BybitConfig>()?;    


    Ok(())
}

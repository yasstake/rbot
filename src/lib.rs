// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.

pub mod common;
pub mod db;
pub mod exchange;
pub mod fs;
pub mod sim;

use common::{
    init_debug_log, init_log,
    order::{Order, OrderSide},
    time::time_string,
};
use pyo3::prelude::*;
// use exchange::ftx::FtxMarket;
use exchange::binance::{BinanceMarket, BinanceConfig};
use exchange::bb::BBMarket;

use common::time::*;
use sim::back::BackTester;
use sim::session::DummySession;


/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_log, m)?)?;
    m.add_function(wrap_pyfunction!(init_debug_log, m)?)?;

    // time util
    m.add_function(wrap_pyfunction!(time_string, m)?)?;
    m.add_function(wrap_pyfunction!(NOW, m)?)?;
    m.add_function(wrap_pyfunction!(DAYS_BEFORE, m)?)?;
    m.add_function(wrap_pyfunction!(DAYS, m)?)?;
    m.add_function(wrap_pyfunction!(HHMM, m)?)?;
    m.add_function(wrap_pyfunction!(MIN, m)?)?;
    m.add_function(wrap_pyfunction!(SEC, m)?)?;

    m.add_function(wrap_pyfunction!(FLOOR, m)?)?;

    // classes
    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    m.add_class::<DummySession>()?;
    m.add_class::<BackTester>()?;

    // Binance
    m.add_class::<BinanceMarket>()?;
    m.add_class::<BinanceConfig>()?;

    // ByBit
    m.add_class::<BBMarket>()?;    

    Ok(())
}

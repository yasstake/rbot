// Copyright (C) @yasstake
// All rights reserved. Absolutely NO warranty.

pub mod common;
pub mod sim;
mod db;
mod exchange;
mod fs;

use pyo3::prelude::*;
use common::{
    order::{Order, OrderSide},
    time::time_string,
    init_log,
    init_debug_log,
};
// use exchange::ftx::FtxMarket;
use exchange::binance::BinanceMarket;

use common::time::*;
use sim::session::DummySession;
use sim::back::BackTester;



/// A Python module implemented in Rust.
#[pymodule]
fn rbot(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(init_log, m)?)?;
    m.add_function(wrap_pyfunction!(init_debug_log, m)?)?;

    // time util
    m.add_function(wrap_pyfunction!(time_string, m)?)?;
    m.add_function(wrap_pyfunction!(NOW, m)?)?;    
    m.add_function(wrap_pyfunction!(DAYS, m)?)?;
    m.add_function(wrap_pyfunction!(HHMM, m)?)?;

    // classes
    m.add_class::<Order>()?;
    m.add_class::<OrderSide>()?;
    //m.add_class::<FtxMarket>()?;
    m.add_class::<BinanceMarket>()?;
    m.add_class::<DummySession>()?; 
    m.add_class::<BackTester>()?; 

    Ok(())
}


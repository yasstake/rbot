use std::f32::consts::E;

use hmac::digest::typenum::Or;
use numpy::PyArray2;
use pyo3::{pyclass, pymethods, PyObject, PyAny, Python, types::PyTuple};

use crate::{exchange::binance::Market, common::{OrderSide, MarketStream, MicroSec}};

use super::{OrderList, has_method};
use pyo3::prelude::*;

use crate::common::MarketMessage;
use crate::common::Trade;
use crate::common::Order;


#[pyclass(name = "Session")]
#[derive(Debug)]
pub struct Session {
    buy_orders: OrderList,
    sell_orders: OrderList,
    market: PyObject,
    current_time: MicroSec,
    dummy: bool
}

#[pymethods]
impl Session {
    #[new]
    pub fn new(market: PyObject, dummy: bool) -> Self {
        Self {
            buy_orders: OrderList::new(OrderSide::Buy),
            sell_orders: OrderList::new(OrderSide::Sell),
            market,            
            current_time: 0,
            dummy,
        }
    }

    pub fn on_tick(&mut self, tick: &Trade) {
        self.current_time = tick.time;
        println!("set currenttime to {}", self.current_time);

        if self.dummy == false {
            return; 
        }

        if tick.order_side == OrderSide::Buy {
            self.sell_orders.consume_trade(tick);
        }
        else if tick.order_side == OrderSide::Sell {
            self.buy_orders.consume_trade(tick);            
        }            
        else {
            log::error!("Unknown order side: {:?}", tick.order_side)
        }
    }

    pub fn on_message(&mut self, message: &MarketMessage) {
        if let Some(trade) = &message.trade {
            log::debug!("on_message: trade={:?}", trade);
            self.on_tick(trade);
        }
    }

    #[getter]
    pub fn get_current_time(&self) -> MicroSec {
        self.current_time
    }

    #[getter]
    pub fn get_bids(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| {
            self.market.getattr(py, "bids")
        })
    }

    #[getter]
    pub fn get_asks(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| {
            self.market.getattr(py, "asks")
        })
    }

}


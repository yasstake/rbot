use pyo3::{pyclass, pymethods};

use crate::{exchange::binance::Market, common::OrderSide};

use super::OrderList;
use pyo3::prelude::*;


trait Session {
    fn run(&mut self, market: &PyAny);
}


#[pyclass(name = "_LiveSession")]
#[derive(Debug, Clone)]
struct LiveSession {
    buy_orders: OrderList,
    sell_orders: OrderList,

}

#[pymethods]
impl LiveSession {
    #[new]
    pub fn new() -> Self {
        Self {
            buy_orders: OrderList::new(OrderSide::Buy),
            sell_orders: OrderList::new(OrderSide::Sell),
        }
    }

    /*
    pub fn run(&mut self, market: &PyAny) {
        let market: Market = market.extract().unwrap();
        println!("market={:?}", market);
    }
    */
        
}


#[pyclass(name = "_DummySession")]
#[derive(Debug, Clone)]
struct DummySession {
    buy_orders: OrderList,
    sell_orders: OrderList,

}

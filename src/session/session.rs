use std::f32::consts::E;

use hmac::digest::typenum::Or;
use numpy::PyArray2;
use pyo3::{pyclass, pymethods, PyObject, PyAny, Python, types::PyTuple};

use crate::{exchange::binance::Market, common::{OrderSide, MarketStream, MicroSec, AccountStatus, OrderStatus}};

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
    account: AccountStatus,
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
            account: AccountStatus::default(),
            market,            
            current_time: 0,
            dummy,
        }
    }

    pub fn on_account_update(&mut self, account: &AccountStatus) {
        self.account = account.clone();
    }

    pub fn on_order_update(&mut self, order: &Order) {
        if order.order_side == OrderSide::Buy {
            if order.status == OrderStatus::Filled ||
                order.status == OrderStatus::Canceled {
                    self.buy_orders.remove(order);
            }
            else {
                self.buy_orders.update_or_insert(order);
            }
        }
        else if order.order_side == OrderSide::Sell {
            if order.status == OrderStatus::Filled ||
                order.status == OrderStatus::Canceled {
                    self.sell_orders.remove(order);
            }
            else {
                self.sell_orders.update_or_insert(order);
            }
        }            
        else {
            log::error!("Unknown order side: {:?}", order.order_side)
        }
    }

    #[getter]
    pub fn get_account(&self) -> AccountStatus {
        self.account.clone()
    }

    #[getter]
    pub fn get_buy_orders(&self) -> OrderList {
        self.buy_orders.clone()
    }

    #[getter]
    pub fn get_sell_orders(&self) -> OrderList {
        self.sell_orders.clone()
    }


    pub fn calc_account(&mut self, order: &Order) {


        /*
        if order.order_side == OrderSide::Buy {
            self.account.free -= order.quantity * order.price;
            self.account.locked += order.quantity * order.price;
        }
        else if order.order_side == OrderSide::Sell {
            self.account.free += order.quantity * order.price;
            self.account.locked -= order.quantity * order.price;
        }            
        else {
            log::error!("Unknown order side: {:?}", order.order_side)
        }
        */
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

        if let Some(order) = &message.order {
            log::debug!("on_message: order={:?}", order);
            self.on_order_update(order);
        }

        if let Some(account) = &message.account {
            log::debug!("on_message: account={:?}", account);
            self.on_account_update(account);
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


use std::f32::consts::E;

use hmac::digest::typenum::Or;
use numpy::PyArray2;
use pyo3::{pyclass, pymethods, types::PyTuple, PyAny, PyObject, Python};
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;

use crate::{
    common::{AccountStatus, MarketStream, MicroSec, OrderSide, OrderStatus},
    exchange::binance::Market,
};

use super::{has_method, OrderList};
use pyo3::prelude::*;

use crate::common::MarketMessage;
use crate::common::Order;
use crate::common::Trade;

#[pyclass(name = "Session")]
#[derive(Debug)]
pub struct Session {
    buy_orders: OrderList,
    sell_orders: OrderList,
    account: AccountStatus,
    market: PyObject,
    current_time: MicroSec,
    dummy: bool,
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

    #[getter]
    pub fn get_current_time(&self) -> MicroSec {
        self.current_time
    }

    #[getter]
    pub fn get_bids_a(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "bids_a"))
    }

    #[getter]
    pub fn get_bids(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "bids"))
    }

    #[getter]
    pub fn get_asks_a(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "asks_a"))
    }

    #[getter]
    pub fn get_asks(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "asks"))
    }

    // account information
    #[getter]
    pub fn get_account(&self) -> AccountStatus {
        self.account.clone()
    }

    // order information
    #[getter]
    pub fn get_buy_orders(&self) -> OrderList {
        self.buy_orders.clone()
    }

    #[getter]
    pub fn get_buy_order_amount(&self) -> Decimal {
        self.buy_orders.remain_size()
    }

    #[getter]
    pub fn get_sell_orders(&self) -> OrderList {
        self.sell_orders.clone()
    }

    #[getter]
    pub fn get_sell_order_amount(&self) -> Decimal {
        self.sell_orders.remain_size()
    }

    pub fn market_order(&self, side: OrderSide, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.call_method1(py, "market_order", (side, size)))
    }

    pub fn limit_order(
        &mut self,
        side: OrderSide,
        size: Decimal,
        price: Decimal,
    ) -> Result<Order, PyErr> {
        // first push order to order list

        // then call market.limit_order
        let r = Python::with_gil(|py| {
            let result = self.market
                .call_method1(py, "limit_order", (side, size, price));

                match result {
                    // if success update order list
                    Ok(order) => {
                        let o: Order = order.extract(py).unwrap();

                        if o.order_side == OrderSide::Buy {
                            self.buy_orders.update_or_insert(&o);
                        } else if o.order_side == OrderSide::Sell {
                            self.sell_orders.update_or_insert(&o);
                        } else {
                            log::error!("Unknown order side: {:?}", o.order_side)
                        }

                        return Ok(o)
                    }
                    Err(e) => {
                        log::error!("limit_order error: {:?}", e);
                        return Err(e)
                    }
                }
        });

        return r;        
    }

    /*
    /// fecth order list from exchange
    /// update order list
    pub fn sync_orderlist(&mut self) {
        let r = Python::with_gil(|py| {
            let result = self.market
                .call_method0(py, "sync_orderlist");

                match result {
                    // if success update order list
                    Ok(orderlist) => {
                        let o: OrderList = orderlist.extract(py).unwrap();

                        self.buy_orders = o.buy_orders;
                        self.sell_orders = o.sell_orders;

                        return Ok(())
                    }
                    Err(e) => {
                        log::error!("sync_orderlist error: {:?}", e);
                        return Err(e)
                    }
                }
        });

        return r;        
    }
    */

    // Message handling
    pub fn on_tick(&mut self, tick: &Trade) {
        self.current_time = tick.time;
        println!("set currenttime to {}", self.current_time);

        if self.dummy == false {
            return;
        }

        if tick.order_side == OrderSide::Buy {
            self.sell_orders.consume_trade(tick);
        } else if tick.order_side == OrderSide::Sell {
            self.buy_orders.consume_trade(tick);
        } else {
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

    pub fn on_account_update(&mut self, account: &AccountStatus) {
        self.account = account.clone();
    }

    pub fn on_order_update(&mut self, order: &Order) {
        if order.order_side == OrderSide::Buy {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::Canceled {
                self.buy_orders.remove(order);
            } else {
                self.buy_orders.update_or_insert(order);
            }
        } else if order.order_side == OrderSide::Sell {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::Canceled {
                self.sell_orders.remove(order);
            } else {
                self.sell_orders.update_or_insert(order);
            }
        } else {
            log::error!("Unknown order side: {:?}", order.order_side)
        }
    }
}

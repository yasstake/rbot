use std::{fs::OpenOptions, path::Path};
use std::io::Write;

use pyo3::{pyclass, pymethods, types::PyTuple, PyAny, PyObject, Python};
use pyo3_polars::PyDataFrame;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;

use crate::{
    common::{
        date_string, hour_string, min_string, AccountStatus, MarketStream, MicroSec, OrderSide,
        OrderStatus, NOW, MarketConfig,
    },
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
    pub session_name: String,
    order_number: i64,

    commission_home: Decimal,
    commission_foreign: Decimal,
    home_change: Decimal,
    foreign_change: Decimal,
    free_home_change: Decimal,
    free_foreign_change: Decimal,
    lock_home_change: Decimal,
    lock_foreign_change: Decimal,

    market_config: MarketConfig,

    log_file: Option<std::fs::File>,
}

#[pymethods]
impl Session {
    #[new]
    pub fn new(market: PyObject, dummy: bool, market_config: &MarketConfig, session_name: Option<&str>) -> Self {
        let session_name = match session_name {
            Some(name) => name.to_string(),
            None => {
                let now = NOW();
                format!(
                    "{}T{}{}",
                    date_string(now),
                    hour_string(now),
                    min_string(now)
                )
                .to_string()
            }
        };

        let mut session = Self {
            buy_orders: OrderList::new(OrderSide::Buy),
            sell_orders: OrderList::new(OrderSide::Sell),
            account: AccountStatus::default(),
            market,
            current_time: 0,
            dummy,
            session_name,
            order_number: 0,

            commission_home: dec![0.0],
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],

            market_config: market_config.clone(),

            log_file:None,
        };

        session.load_order_list().unwrap();

        return session;
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
    pub fn get_buy_orders(&self) -> Vec<Order> {
        self.buy_orders.get()
    }

    #[getter]
    pub fn get_buy_order_amount(&self) -> f64 {
        self.buy_orders.remain_size().to_f64().unwrap()
    }

    #[getter]
    pub fn get_sell_orders(&self) -> Vec<Order> {
        self.sell_orders.get()
    }

    #[getter]
    pub fn get_sell_order_amount(&self) -> f64 {
        self.sell_orders.remain_size().to_f64().unwrap()
    }

    #[getter]
    pub fn get_commission_home(&self) -> f64 {
        self.commission_home.to_f64().unwrap()
    }

    #[getter]
    pub fn commission_foreign(&self) -> f64 {
        self.commission_foreign.to_f64().unwrap()
    }
    #[getter]
    pub fn home_change(&self) -> f64 {
        self.home_change.to_f64().unwrap()
    }

    #[getter]
    pub fn foreign_change(&self) -> f64 {
        self.foreign_change.to_f64().unwrap()
    }

    #[getter]
    pub fn free_home_change(&self) -> f64 {
        self.free_home_change.to_f64().unwrap()
    }

    #[getter]
    pub fn free_foreign_change(&self) -> f64 {
        self.free_foreign_change.to_f64().unwrap()
    }

    #[getter]
    pub fn lock_home_change(&self) -> f64 {
        self.lock_home_change.to_f64().unwrap()
    }

    #[getter]
    pub fn lock_foreign_change(&self) -> f64 {
        self.lock_foreign_change.to_f64().unwrap()
    }

    /// cancel order
    /// if success return order id
    /// if fail return None
    pub fn cancel_order(&mut self, order_id: &str) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| {
            let r = self.market.call_method1(py, "cancel_order", (order_id,));

            if r.is_err() {
                let none = Python::None(py);
                return Ok(none);
            }
            r
        })
    }


    pub fn market_order(&mut self, side: OrderSide, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);

        log::debug!("market_order: side={:?}, size={}", side, size);

        Python::with_gil(|py| {
            self.market
                .call_method1(py, "market_order", (side, size, local_id))
        })
    }


    pub fn limit_order(
        &mut self,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
    ) -> Result<Vec<Order>, PyErr> {
        let price_scale = self.market_config.price_scale;
        let pricedp = price.round_dp(price_scale);

        let size_scale = self.market_config.size_scale;        
        let sizedp = size.round_dp(size_scale);

        // first push order to order list
        let local_id = self.new_order_id(&side);

        log::debug!(
            "limit_order: side={:?}, size={}, price={}",
            side,
            sizedp,
            pricedp
        );

        // then call market.limit_order
        let r = Python::with_gil(|py| {
            let result = self
                .market
                .call_method1(py, "limit_order", (side, pricedp, sizedp, local_id));

            match result {
                // if success update order list
                Ok(order) => {
                    let orders: Vec<Order> = order.extract(py).unwrap();

                    for o in &orders {
                        if o.order_side == OrderSide::Buy {
                            self.buy_orders.update_or_insert(&o);
                        } else if o.order_side == OrderSide::Sell {
                            self.sell_orders.update_or_insert(&o);
                        } else {
                            log::error!("Unknown order side: {:?}", o.order_side);
                        }
                    }

                    return Ok(orders);
                }
                Err(e) => {
                    log::error!("limit_order error: {:?}  / priceorg={:?}/sizeorg={:?}", e, price, size);
                    return Err(e);
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

        self.update_balance(order);

        self.log(&order.__str__());
    }

    fn new_order_id(&mut self, side: &OrderSide) -> String {
        self.order_number += 1;

        match side {
            OrderSide::Buy => format!("{}-{:04}BUY", self.session_name, self.order_number),
            OrderSide::Sell =>format!("{}-{:04}SEL", self.session_name, self.order_number),
            OrderSide::Unknown => format!("{}-{:04}UNK", self.session_name, self.order_number)
        }
    }

    fn load_order_list(&mut self) -> Result<(), PyErr> {
        // when dummy mode, order list is start with empty.
        if self.dummy == true {
            return Ok(());
        }

        let r = Python::with_gil(|py| {
            let result = self.market.getattr(py, "open_orders");

            match result {
                // if success update order list
                Ok(orderlist) => {
                    let orders: Vec<Order> = orderlist.extract(py).unwrap();
                    log::debug!("OpenOrders {:?}", orderlist);

                    for order in orders {
                        log::debug!("OpenOrder {:?}", order);
                        if order.order_side == OrderSide::Buy {
                            self.buy_orders.update_or_insert(&order);
                        } else if order.order_side == OrderSide::Sell {
                            self.sell_orders.update_or_insert(&order);
                        } else {
                            log::error!("Unknown order side: {:?}", order.order_side)
                        }
                    }

                    return Ok(());
                }
                Err(e) => {
                    log::error!("sync_orderlist error: {:?}", e);
                    return Err(e);
                }
            }
        });

        return r;
    }

    pub fn update_balance(&mut self, order: &Order) {
        self.commission_foreign += order.commission_foreign;
        self.commission_home += order.commission_home;

        self.home_change += order.home_change;
        self.free_home_change += order.free_home_change;
        self.lock_home_change += order.lock_home_change;

        self.foreign_change += order.foreign_change;
        self.free_foreign_change += order.free_foreign_change;
        self.lock_foreign_change += order.lock_foreign_change;
    }

    pub fn open_log(&mut self, path: &str) -> Result<(), std::io::Error> {
        let log_file = Path::new(path).with_extension("log");

        self.log_file = Some(
            OpenOptions::new()
                .append(true)
                .create(true)
                .open(log_file)?
        );
        Ok(())
    }

    pub fn log(&mut self, message: &str) -> Result<(), std::io::Error> {
        if let Some(file) = &mut self.log_file {
            writeln!(file, "{}", message)?;
            //file.write_all(format!("{}", message)?.as_bytes())?;
        }

        Ok(())
    }

}

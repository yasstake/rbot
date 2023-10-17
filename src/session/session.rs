use std::collections::VecDeque;
use std::io::Write;
use std::sync::Mutex;
use std::{fs::OpenOptions, path::Path};

use polars_core::utils::arrow::array::ord;
use pyo3::{pyclass, pymethods, types::PyTuple, PyAny, PyObject, Python};
use pyo3_polars::PyDataFrame;
use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;

use crate::exchange::BoardItem;
use crate::{
    common::{
        date_string, hour_string, min_string, AccountStatus, MarketConfig, MarketStream, MicroSec,
        OrderSide, OrderStatus, NOW,
    },
    exchange::binance::Market,
};

use super::{has_method, OrderList};
use pyo3::prelude::*;

use crate::common::Trade;
use crate::common::{MarketMessage, SEC};
use crate::common::{Order, OrderType};

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
    transaction_number: i64,

    commission_home: Decimal,
    commission_foreign: Decimal,
    home_change: Decimal,
    foreign_change: Decimal,
    free_home_change: Decimal,
    free_foreign_change: Decimal,
    lock_home_change: Decimal,
    lock_foreign_change: Decimal,

    market_config: MarketConfig,

    dummy_q: Mutex<VecDeque<Vec<Order>>>,

    log_file: Option<std::fs::File>,
}

#[pymethods]
impl Session {
    #[new]
    pub fn new(
        market: PyObject,
        dummy: bool,
        market_config: &MarketConfig,
        session_name: Option<&str>,
    ) -> Self {
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
            transaction_number: 0,

            commission_home: dec![0.0],
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],

            market_config: market_config.clone(),

            dummy_q: Mutex::new(VecDeque::new()),

            log_file: None,
        };

        session.load_order_list().unwrap();

        return session;
    }

    pub fn ohlcv(&self, interval: i64, count: i64) -> Result<Py<PyAny>, PyErr> {
        let window_sec = interval * count;
        let time_from = self.current_time - SEC(window_sec);
        let time_to = self.current_time;

        Python::with_gil(|py| {
            let result = self
                .market
                .call_method1(py, "ohlcv", (time_from, time_to, interval));

            match result {
                Ok(df) => {
                    return Ok(df);
                }
                Err(e) => {
                    log::error!("ohlcv error: {:?}", e);
                    return Err(e);
                }
            }
        })
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
    pub fn get_bids_vec(&self) -> Result<Vec<BoardItem>, PyErr> {
        Python::with_gil(|py| {
            let asks = self.market.getattr(py, "bids_vec")?;
            let asks: Vec<BoardItem> = asks.extract(py)?;
            Ok(asks)
        })
    }

    #[getter]
    pub fn get_asks(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "asks"))
    }

    #[getter]
    pub fn get_asks_a(&self) -> Result<Py<PyAny>, PyErr> {
        Python::with_gil(|py| self.market.getattr(py, "asks_a"))
    }

    #[getter]
    pub fn get_asks_vec(&self) -> Result<Vec<BoardItem>, PyErr> {
        Python::with_gil(|py| {
            let asks = self.market.getattr(py, "asks_vec")?;
            let asks: Vec<BoardItem> = asks.extract(py)?;
            Ok(asks)
        })
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

    pub fn cancel_order(&mut self, order_id: &str) -> PyResult<Py<PyAny>> {
        if self.dummy {
            self.dummy_cancel_order(order_id)
        } else {
            self.real_cancel_order(order_id)
        }
    }

    /// cancel order
    /// if success return order id
    /// if fail return None
    pub fn real_cancel_order(&mut self, order_id: &str) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            let r = self.market.call_method1(py, "cancel_order", (order_id,));

            if r.is_err() {
                let none = Python::None(py);
                return Ok(none);
            }
            r.into()
        })
    }

    pub fn dummy_cancel_order(&mut self, order_id: &str) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            let mut order: Order = if let Some(order) = self.buy_orders.get_item_by_id(order_id) {
                order
            } else if let Some(order) = self.sell_orders.get_item_by_id(&order_id) {
                order
            } else {
                log::error!("dummy_cancel_order: order not found: {}", order_id);
                return Ok(Python::None(py));
            };

            order.status = OrderStatus::Canceled;
            self.update_balance(&order);
            order.update_time = self.current_time;

            self.push_dummy_q(&vec![order.clone()]);

            return Ok(order.into_py(py));
        })
    }

    pub fn market_order(&mut self, side: OrderSide, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        if self.dummy {
            return self.dummy_market_order(side, size);
        } else {
            return self.real_market_order(side, size);
        }
    }

    pub fn real_market_order(&mut self, side: OrderSide, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);

        log::debug!("market_order: side={:?}, size={}", side, size);

        Python::with_gil(|py| {
            self.market
                .call_method1(py, "market_order", (side, size, local_id))
        })
    }

    /// TODO: implement
    pub fn dummy_market_order(
        &mut self,
        side: OrderSide,
        size: Decimal,
    ) -> Result<Py<PyAny>, PyErr> {
        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);

        let board = if side == OrderSide::Buy {
            self.get_asks_vec()
        } else {
            self.get_bids_vec()
        };

        if board.is_err() {
            // TODO: バックテストのときのアルゴリズムを考える。
            log::error!("dummy_market_order: cannot get board.");
            return Err(board.unwrap_err());
        }

        let board = board.unwrap();

        if board.len() == 0 {
            // TODO: バックテストのときのアルゴリズムを考える。
            log::error!("dummy_market_order: board is empty");
        }


        let mut filled_orders = vec![];
        let mut remain_size = size;

        log::debug!("board: {:?}", board);

        for item in board {
            if remain_size <= dec![0.0] {
                break;
            }

            let mut order = Order::new(
                self.market_config.symbol(),
                self.current_time,
                local_id.clone(),
                local_id.clone(),
                side,
                OrderType::Market,
                OrderStatus::Filled,
                item.price,
                size,
            );

            order.transaction_id = self.dummy_transaction_id();
            order.update_time = self.current_time;

            if remain_size < item.size {
                order.status = OrderStatus::Filled;
                order.execute_size = remain_size;
                order.remain_size = dec![0.0];
                order.execute_price = item.price;
                order.quote_vol = order.execute_price * order.execute_size;
            }
            else {
                order.status = OrderStatus::PartiallyFilled;
                order.execute_size = item.size;
                order.remain_size = remain_size - item.size;
                order.execute_price = item.price;
                order.quote_vol = order.execute_price * order.execute_size;
            }

            order.update_balance(&self.market_config);
            filled_orders.push(order.clone());
            remain_size -= item.size;
        }

        Python::with_gil(|py| {
            if filled_orders.len() == 0 {
                let none = Python::None(py);
                return Ok(none);
            } else {
                self.push_dummy_q(&filled_orders);
                return Ok(filled_orders.into_py(py));
            }
        })
    }

    pub fn limit_order(
        &mut self,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
    ) -> Result<Vec<Order>, PyErr> {
        if self.dummy {
            return self.dummy_limit_order(side, price, size);
        } else {
            return self.real_limit_order(side, price, size);
        }
    }

    pub fn real_limit_order(
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
            let result =
                self.market
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
                    log::error!(
                        "limit_order error: {:?}  / priceorg={:?}/sizeorg={:?}",
                        e,
                        price,
                        size
                    );
                    return Err(e);
                }
            }
        });

        return r;
    }

    pub fn dummy_limit_order(
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
            "dummuy_limit_order: side={:?}, size={}, price={}",
            side,
            sizedp,
            pricedp
        );

        let mut order = Order::new(
            self.market_config.symbol(),
            self.current_time,
            local_id.clone(),
            local_id.clone(),
            side,
            OrderType::Limit,
            OrderStatus::New,
            pricedp,
            sizedp,
        );

        order.update_balance(&self.market_config);

        if side == OrderSide::Buy {
            self.buy_orders.update_or_insert(&order);
        } else if side == OrderSide::Sell {
            self.sell_orders.update_or_insert(&order);
        } else {
            log::error!("Unknown order side: {:?}", side);
        }

        self.push_dummy_q(&vec![order.clone()]);

        return Ok(vec![order]);
    }

    pub fn on_message(&mut self, message: &MarketMessage) -> Vec<Order> {
        let mut result = vec![];

        if let Some(trade) = &message.trade {
            log::debug!("on_message: trade={:?}", trade);
            result = self.on_tick(trade);

            if 0 < result.len() {
                for order in result.iter() {
                    self.on_order_update(&order);
                }
            }
        }

        if let Some(order) = &message.order {
            log::debug!("on_message: order={:?}", order);
            self.on_order_update(order);
        }

        if let Some(account) = &message.account {
            log::debug!("on_message: account={:?}", account);
            self.on_account_update(account);
        }

        return result;
    }

    // Message handling
    pub fn on_tick(&mut self, tick: &Trade) -> Vec<Order> {
        self.current_time = tick.time;

        if self.dummy == false {
            return vec![];
        }

        return self.execute_dummuy_tick(tick);
    }

    pub fn on_account_update(&mut self, account: &AccountStatus) {
        self.account = account.clone();
    }

    pub fn on_order_update(&mut self, order: &Order) {
        if order.order_side == OrderSide::Buy {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::Canceled {
                self.buy_orders.remove(&order.order_id);
            } else {
                self.buy_orders.update_or_insert(order);
            }
        } else if order.order_side == OrderSide::Sell {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::Canceled {
                self.sell_orders.remove(&order.order_id);
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
            OrderSide::Sell => format!("{}-{:04}SEL", self.session_name, self.order_number),
            OrderSide::Unknown => format!("{}-{:04}UNK", self.session_name, self.order_number),
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

    // TODO: check if this is correct
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
                .open(log_file)?,
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

impl Session {
    fn push_dummy_q(&mut self, message: &Vec<Order>) {
        let mut q = self.dummy_q.lock().unwrap();
        q.push_back(message.clone());
    }

    fn pop_dummy_q(&mut self) -> Option<Vec<Order>> {
        let mut q = self.dummy_q.lock().unwrap();
        q.pop_front()
    }

    fn dummy_transaction_id(&mut self) -> String {
        self.transaction_number += 1;
        format!("{}-{:04}", self.session_name, self.transaction_number)
    }

    fn update_dummy_orders(&mut self, orders: &mut Vec<Order>, is_maker: bool) {
        for o in orders {
            o.update_balance(&self.market_config);
            o.is_maker = is_maker;

            if o.status == OrderStatus::Filled || o.status == OrderStatus::PartiallyFilled {
                if o.transaction_id == "" {
                    o.transaction_id = self.dummy_transaction_id();
                }
            }
        }
    }

    fn execute_dummuy_tick(&mut self, tick: &Trade) -> Vec<Order> {
        let orders = self.pop_dummy_q();

        if let Some(mut order_vec) = orders {
            log::debug!("pop dummy order: {:?}", order_vec);
            self.update_dummy_orders(&mut order_vec, false);

            return order_vec;
        }

        // consume order
        if tick.order_side == OrderSide::Buy {
            let mut os = self.sell_orders.consume_trade(tick);
            self.update_dummy_orders(&mut os, true);
            log::debug!("consume_trade Buy: {:?}, {:?}", tick, os);

            return os;
        } else if tick.order_side == OrderSide::Sell {
            let mut os = self.buy_orders.consume_trade(tick);
            self.update_dummy_orders(&mut os, true);
            log::debug!("consume_trade Sell: {:?}, {:?}", tick, os);

            return os;
        } else {
            log::error!("Unknown order side: {:?}", tick.order_side);
            return vec![];
        }
    }
}

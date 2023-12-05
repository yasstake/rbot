// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::collections::VecDeque;
use std::sync::Mutex;

use pyo3::{pyclass, pymethods, PyAny, PyObject, Python};
use pyo3_polars::PyDataFrame;

use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;

use super::{OrderList, Logger};
use crate::common::{
    date_string, hour_string, min_string, AccountStatus, MarketConfig, MicroSec, OrderSide,
    OrderStatus, NOW, time_string,
};
use pyo3::prelude::*;

use crate::common::{ordervec_to_dataframe, Trade};
use crate::common::{MarketMessage, SEC};
use crate::common::{Order, OrderType};



#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub enum ExecuteMode {
    Real,
    BackTest,
    Dry,
}

#[pymethods]
impl ExecuteMode {
    #[new]
    pub fn new(mode: &str) -> Self {
        let mode = mode.to_uppercase();

        match mode.as_str() {
            "REAL" => ExecuteMode::Real,
            "DUMMY" => ExecuteMode::BackTest,
            "DRY" => ExecuteMode::Dry,
            _ => ExecuteMode::BackTest,
        }
    }

    pub fn __str__(&self) -> String {
        match self {
            ExecuteMode::Real => "Real",
            ExecuteMode::BackTest => "Dummy",
            ExecuteMode::Dry => "Dry",
        }
        .to_string()
    }
}



#[pyclass(name = "Session")]
#[derive(Debug)]
pub struct Session {
    execute_mode: ExecuteMode,
    buy_orders: OrderList,
    sell_orders: OrderList,
    real_account: AccountStatus,
    psudo_account: AccountStatus,
    market: PyObject,
    current_timestamp: MicroSec,
    pub session_name: String,
    order_number: i64,
    transaction_number: i64,

    position: Decimal,

    commission_home_sum: Decimal,
    commission_foreign_sum: Decimal,
    home_sum: Decimal,
    foreign_sum: Decimal,
    free_home_sum: Decimal,
    free_foreign_sum: Decimal,
    lock_home_sum: Decimal,
    lock_foreign_sum: Decimal,

    clock_interval: i64,

    asks_edge: Decimal,
    bids_edge: Decimal,

    market_config: MarketConfig,

    dummy_q: Mutex<VecDeque<Vec<Order>>>,

    log: Logger,
}

#[pymethods]
impl Session {
    #[new]
    #[pyo3(signature = (market, execute_mode, session_name=None, log_memory=true))]
    pub fn new(
        market: PyObject,
        execute_mode: ExecuteMode,
        session_name: Option<&str>,
        log_memory: bool,
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

        let config = Python::with_gil(|py| {
            let config = market.getattr(py, "market_config").unwrap();
            let config: MarketConfig = config.extract(py).unwrap();

            config
        });

        let mut session = Self {
            execute_mode: execute_mode,
            buy_orders: OrderList::new(OrderSide::Buy),
            sell_orders: OrderList::new(OrderSide::Sell),
            real_account: AccountStatus::default(),
            psudo_account: AccountStatus::default(),
            market,
            current_timestamp: 0,
            session_name,
            order_number: 0,
            transaction_number: 0,

            position: dec![0.0],

            commission_home_sum: dec![0.0],
            commission_foreign_sum: dec![0.0],
            home_sum: dec![0.0],
            foreign_sum: dec![0.0],
            free_home_sum: dec![0.0],
            free_foreign_sum: dec![0.0],
            lock_home_sum: dec![0.0],
            lock_foreign_sum: dec![0.0],

            clock_interval: 0,

            asks_edge: dec![0.0],
            bids_edge: dec![0.0],

            market_config: config,

            dummy_q: Mutex::new(VecDeque::new()),

            log: Logger::new(log_memory),
        };

        session.load_order_list().unwrap();

        return session;
    }

    // -----   market information -----
    // call market with current_timestamp
    pub fn ohlcv(&self, interval: i64, count: i64) -> Result<Py<PyAny>, PyErr> {
        let window_sec = interval * count;
        let time_from = self.current_timestamp - SEC(window_sec);
        let time_to = self.current_timestamp;

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
    pub fn get_timestamp(&self) -> MicroSec {
        self.current_timestamp
    }

    #[getter]
    pub fn get_board(&self) -> Result<Py<PyAny>, PyErr> {
        if self.execute_mode == ExecuteMode::BackTest {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "get_board is not supported in backtest mode",
            ));
        }
        Python::with_gil(|py| self.market.getattr(py, "board"))
    }

    // account information
    #[getter]
    pub fn get_account(&self) -> AccountStatus {
        self.real_account.clone()
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
        self.commission_home_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn commission_foreign(&self) -> f64 {
        self.commission_foreign_sum.to_f64().unwrap()
    }
    #[getter]
    pub fn home_change(&self) -> f64 {
        self.home_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn foreign_change(&self) -> f64 {
        self.foreign_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn free_home_change(&self) -> f64 {
        self.free_home_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn free_foreign_change(&self) -> f64 {
        self.free_foreign_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn lock_home_change(&self) -> f64 {
        self.lock_home_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn lock_foreign_change(&self) -> f64 {
        self.lock_foreign_sum.to_f64().unwrap()
    }

    #[getter]
    pub fn get_last_price(&self) -> (f64, f64) {
        (
            self.bids_edge.to_f64().unwrap(),
            self.asks_edge.to_f64().unwrap(),
        )
    }

    #[getter]
    pub fn get_psudo_position(&self) -> f64 {
        self.position.to_f64().unwrap()
    }

    #[getter]
    pub fn get_psudo_account(&self) -> AccountStatus {
        self.psudo_account.clone()
    }

    #[getter]
    pub fn get_log(&self) -> Vec<Order> {
        self.log.get()
    }

    #[getter]
    pub fn get_log_df(&self) -> PyResult<PyDataFrame> {
        let df = ordervec_to_dataframe(self.get_log());

        Ok(PyDataFrame(df))
    }

    pub fn cancel_order(&mut self, order_id: &str) -> PyResult<Py<PyAny>> {
        if self.execute_mode == ExecuteMode::BackTest || self.execute_mode == ExecuteMode::Dry {
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
            let mut order_to_cancel: Order =
                if let Some(order) = self.buy_orders.get_item_by_id(order_id) {
                    order
                } else if let Some(order) = self.sell_orders.get_item_by_id(&order_id) {
                    order
                } else {
                    log::error!("dummy_cancel_order: order not found: {}", order_id);
                    return Ok(Python::None(py));
                };

            order_to_cancel.status = OrderStatus::Canceled;
            self.update_psudo_position(&order_to_cancel);
            order_to_cancel.update_time = self.current_timestamp;

            self.push_dummy_q(&vec![order_to_cancel.clone()]);

            return Ok(order_to_cancel.into_py(py));
        })
    }

    pub fn market_order(&mut self, side: String, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        match self.execute_mode {
            ExecuteMode::Real => self.real_market_order(side, size),
            ExecuteMode::BackTest => self.dummy_market_order(side, size),
            ExecuteMode::Dry => self.dry_market_order(side, size),
        }
    }

    pub fn real_market_order(&mut self, side: String, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        log::debug!("market_order: side={:}, size={}", &side, size);

        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);

        Python::with_gil(|py| {
            self.market
                .call_method1(py, "market_order", (side, size, local_id))
        })
    }

    pub fn calc_dummy_execute_price_by_slip(&mut self, side: OrderSide) -> Decimal {
        // 板がないので、最後の約定価格＋スリッページで約定したことにする（オーダーは分割されないと想定）
        if self.execute_mode != ExecuteMode::BackTest {
            log::error!(
                "calc_dummy_execute_price: dummy_execute_price should be used in BackTest mode, current mode= {:?}",
                self.execute_mode
            );
            return dec![0.0];
        }

        let execute_price = if side == OrderSide::Buy {
            self.asks_edge + self.market_config.market_order_price_slip
        } else {
            self.bids_edge - self.market_config.market_order_price_slip
        };

        return execute_price;
    }

    pub fn dry_market_order(&mut self, side: String, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);
        let order_side = OrderSide::from(&side);

        let transaction_id = self.dummy_transaction_id();

        Python::with_gil(|py| {
            self.market.call_method1(
                py,
                "dry_market_order",
                (
                    self.current_timestamp,
                    local_id.clone(),
                    local_id.clone(),
                    order_side,
                    size,
                    transaction_id,
                ),
            )
        })
    }

    pub fn dummy_market_order(&mut self, side: String, size: Decimal) -> Result<Py<PyAny>, PyErr> {
        let size_scale = self.market_config.size_scale;
        let size = size.round_dp(size_scale);

        let local_id = self.new_order_id(&side);
        let order_side = OrderSide::from(&side);

        let execute_price = self.calc_dummy_execute_price_by_slip(order_side);

        let mut order = Order::new(
            self.market_config.symbol(),
            self.current_timestamp,
            local_id.clone(),
            local_id.clone(),
            order_side,
            OrderType::Market,
            OrderStatus::Filled,
            dec![0.0],
            size,
        );

        order.transaction_id = self.dummy_transaction_id();
        order.update_time = self.current_timestamp;
        order.is_maker = false;

        order.execute_size = size;
        order.remain_size = dec![0.0];
        order.execute_price = execute_price;
        order.quote_vol = order.execute_price * order.execute_size;

        let orders = vec![order];
        self.push_dummy_q(&orders);

        Python::with_gil(|py| {
            return Ok(orders.into_py(py));
        })
    }

    pub fn limit_order(
        &mut self,
        side: String,
        price: Decimal,
        size: Decimal,
    ) -> Result<Vec<Order>, PyErr> {
        if self.execute_mode == ExecuteMode::BackTest || self.execute_mode == ExecuteMode::Dry {
            return self.dummy_limit_order(side, price, size);
        } else {
            return self.real_limit_order(side, price, size);
        }
    }

    pub fn real_limit_order(
        &mut self,
        side: String,
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
        side: String,
        price: Decimal,
        size: Decimal,
    ) -> Result<Vec<Order>, PyErr> {
        let price_scale = self.market_config.price_scale;
        let pricedp = price.round_dp(price_scale);

        let size_scale = self.market_config.size_scale;
        let sizedp = size.round_dp(size_scale);

        // first push order to order list
        let local_id = self.new_order_id(&side);

        let order_side = OrderSide::from(&side);

        log::debug!(
            "dummuy_limit_order: side={:?}, size={}, price={}",
            side,
            sizedp,
            pricedp
        );

        let mut order = Order::new(
            self.market_config.symbol(),
            self.current_timestamp,
            local_id.clone(),
            local_id.clone(),
            order_side,
            OrderType::Limit,
            OrderStatus::New,
            pricedp,
            sizedp,
        );

        order.is_maker = true;

        self.push_dummy_q(&vec![order.clone()]);

        return Ok(vec![order]);
    }

    pub fn on_message(&mut self, message: &MarketMessage) -> Vec<Order> {
        let mut result = vec![];

        if let Some(trade) = &message.trade {
            log::debug!("on_message: trade={:?}", trade);
            result = self.on_tick(trade);

            // ダミーモードの場合は約定キューからの処理が発生する。
            if 0 < result.len() {
                for order in result.iter_mut() {
                    order.update_balance(&self.market_config);
                    self.on_order_update(order);
                }
            }
        }

        if let Some(order) = &message.order {
            let mut order = order.clone();
            log::debug!("on_message: order={:?}", order);
            self.on_order_update(&mut order);
        }

        if let Some(account) = &message.account {
            log::debug!("on_message: account={:?}", account);
            self.on_account_update(account);
        }

        return result;
    }

    pub fn update_psudo_account_by_order(&mut self, order: &Order) -> bool {
        self.psudo_account.apply_order(order);

        if order.status == OrderStatus::Filled || order.status == OrderStatus::PartiallyFilled {
            return true;
        } else {
            return false;
        }
    }

    #[getter]
    pub fn get_clock_interval(&self) -> i64 {
        self.clock_interval
    }

    #[setter]
    pub fn set_clock_interval(&mut self, interval: i64) {
        self.clock_interval = interval;
    }

    #[getter]
    pub fn get_dummy_q(&self) -> Vec<Vec<Order>> {
        let q = self.dummy_q.lock().unwrap();
        q.iter().map(|x| x.clone()).collect()
    }


    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        let mut json = "{".to_string();

        json += &format!("\"timestamp\":{},", self.current_timestamp);
        json += &format!("\"timestamp_str\": {},\n", time_string(self.current_timestamp));

        // let (last_sell_price, last_buy_price) = self.get_last_price();

        json += "\"orders\":";
        // order list
        json += "{\"buy\":";
        json += self.buy_orders.__repr__().as_str();
        json += ", \"sell\":";
        json += self.sell_orders.__repr__().as_str();
        json += "}, \n";

        // account information
        json += &format!("\"account\":{}, ", self.real_account.__repr__());

        json += &format!("\"psudo_account\":{},",self.psudo_account.__repr__());

        json += &format!("\"psudo_position\":{}", self.position);
        json += "}";
        json
    }
}

impl Session {
    pub fn log(&mut self, order: &Order) -> Result<(), std::io::Error> {
        self.log.log_order(self.current_timestamp, order)
    }

    pub fn open_log(&mut self, path: &str) -> Result<(), std::io::Error> {
        self.log.open_log(path)
    }
}

impl Session {
    /// 約定情報の処理
    fn on_tick(&mut self, tick: &Trade) -> Vec<Order> {
        self.current_timestamp = tick.time;

        if tick.order_side == OrderSide::Buy {
            self.asks_edge = tick.price;
            if self.asks_edge <= self.bids_edge {
                self.bids_edge = self.asks_edge - self.market_config.price_unit;
            }
        } else if tick.order_side == OrderSide::Sell {
            self.bids_edge = tick.price;
            if self.asks_edge <= self.bids_edge {
                self.asks_edge = self.bids_edge + self.market_config.price_unit;
            }
        }

        if self.execute_mode == ExecuteMode::BackTest || self.execute_mode == ExecuteMode::Dry {
            return self.execute_dummuy_tick(tick);
        } else {
            return vec![];
        }
    }

    pub fn on_account_update(&mut self, account: &AccountStatus) {
        self.real_account = account.clone();
    }

    pub fn on_order_update(&mut self, order: &mut Order) {
        order.update_balance(&self.market_config);

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

        self.update_psudo_position(order);

        let _ = self.log(&order);
    }

    fn new_order_id(&mut self, side: &str) -> String {
        self.order_number += 1;

        format!("{}-{:04}{:}", self.session_name, self.order_number, side)
    }

    fn load_order_list(&mut self) -> Result<(), PyErr> {
        // when dummy mode, order list is start with empty.
        if self.execute_mode == ExecuteMode::BackTest || self.execute_mode == ExecuteMode::Dry {
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
    pub fn update_psudo_position(&mut self, order: &Order) {
        if order.order_side == OrderSide::Buy {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::PartiallyFilled {
                self.position += order.execute_size;
            }
        } else if order.order_side == OrderSide::Sell {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::PartiallyFilled {
                self.position -= order.execute_size;
            }
        } else {
            log::error!("Unknown order side: {:?}", order.order_side)
        }
    }

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

    /// update order balance information accroding to market config
    fn update_dummy_orders(&mut self, orders: &mut Vec<Order>) {
        for o in orders {
            o.update_time = self.current_timestamp;
            o.update_balance(&self.market_config);

            if o.status == OrderStatus::Filled || o.status == OrderStatus::PartiallyFilled {
                if o.transaction_id == "" {
                    o.transaction_id = self.dummy_transaction_id();
                }
            }
        }
    }

    fn execute_dummuy_tick(&mut self, tick: &Trade) -> Vec<Order> {
        // process dummy order queue
        let orders = self.pop_dummy_q();
        if let Some(mut order_vec) = orders {
            log::debug!("pop dummy order: {:?}", order_vec);
            self.update_dummy_orders(&mut order_vec);

            return order_vec;
        }

        // consume order
        if tick.order_side == OrderSide::Buy {
            let mut os = self.sell_orders.consume_trade(tick);
            self.update_dummy_orders(&mut os);
            log::debug!("consume_trade Buy: {:?}, {:?}", tick, os);

            return os;
        } else if tick.order_side == OrderSide::Sell {
            let mut os = self.buy_orders.consume_trade(tick);
            self.update_dummy_orders(&mut os);
            log::debug!("consume_trade Sell: {:?}, {:?}", tick, os);

            return os;
        } else {
            log::error!("Unknown order side: {:?}", tick.order_side);
            return vec![];
        }
    }

    pub fn set_real_account(&mut self, account: &AccountStatus) {
        self.real_account = account.clone();
    }
}

// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::collections::VecDeque;
use std::sync::Mutex;

use pyo3::{pyclass, pymethods, PyAny, PyObject, Python};

use rust_decimal::{prelude::ToPrimitive, Decimal};
use rust_decimal_macros::dec;

use super::{Logger, OrderList};
use crate::common::{
    date_string, hour_string, min_string, time_string, AccountStatus, MarketConfig, MicroSec,
    OrderSide, OrderStatus, NOW,
};
use pyo3::prelude::*;

use crate::common::Trade;
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
    current_clock_time: MicroSec,
    pub session_name: String,
    order_number: i64,
    transaction_number: i64,

    position: Decimal,
    average_price: Decimal,
    profit: Decimal,

    commission_home_sum: Decimal,
    commission_foreign_sum: Decimal,
    home_sum: Decimal,
    foreign_sum: Decimal,
    free_home_sum: Decimal,
    free_foreign_sum: Decimal,
    lock_home_sum: Decimal,
    lock_foreign_sum: Decimal,

    log_id: i64,

    clock_interval_sec: i64,

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
            current_clock_time: 0,
            session_name,
            order_number: 0,
            transaction_number: 0,

            position: dec![0.0],
            average_price: dec![0.0],
            profit: dec![0.0],

            commission_home_sum: dec![0.0],
            commission_foreign_sum: dec![0.0],
            home_sum: dec![0.0],
            foreign_sum: dec![0.0],
            free_home_sum: dec![0.0],
            free_foreign_sum: dec![0.0],
            lock_home_sum: dec![0.0],
            lock_foreign_sum: dec![0.0],

            log_id: 0,

            clock_interval_sec: 0,

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

    #[setter]
    pub fn set_current_clock(&mut self, timestamp: MicroSec) {
        self.current_clock_time = timestamp;
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

    #[getter]
    pub fn get_execute_mode(&self) -> String {
        self.execute_mode.__str__()
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
    pub fn get_position(&self) -> f64 {
        self.position.to_f64().unwrap()
    }

    #[getter]
    pub fn get_psudo_account(&self) -> AccountStatus {
        self.psudo_account.clone()
    }

    #[getter]
    pub fn get_real_account(&self) -> AccountStatus {
        self.real_account.clone()
    }

    #[getter]
    pub fn get_account(&self) -> AccountStatus {
        match self.execute_mode {
            ExecuteMode::Real => self.real_account.clone(),
            ExecuteMode::BackTest => self.psudo_account.clone(),
            ExecuteMode::Dry => self.psudo_account.clone(),
        }
    }

    #[getter]
    pub fn get_log(&self) -> Logger {
        self.log.clone()
    }

    pub fn log_indicator(&mut self, name: String, value: f64) {
        let timestamp = self.calc_log_timestamp();

        let r = self
            .log
            .log_indicator(timestamp, &name, value, None, None, None, None);
        if r.is_err() {
            log::error!("log_indicator error: {:?}", r);
        }
    }

    pub fn expire_order(&mut self, ttl_sec: i64) -> bool {
        let mut has_expire = false;
        
        let expire_time = self.current_timestamp - SEC(ttl_sec);

        for order in self.buy_orders.get_old_orders(expire_time) {
            if self.cancel_order(&order.order_id).is_ok() {
                has_expire = true;
                log::debug!("expire_orders: cancel order: {:?}", order);
            }
            else {
                log::warn!("expire_orders: cancel order error: {:?}", order);
            }

        }

        for order in self.sell_orders.get_old_orders(expire_time) {
            if self.cancel_order(&order.order_id).is_ok() {
                has_expire = true;
                log::debug!("expire_orders: cancel order: {:?}", order);
            }
            else {
                log::warn!("expire_orders: cancel order error: {:?}", order);
            }
        }

        has_expire
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
            self.calc_log_timestamp(),
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
            self.calc_log_timestamp(),
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
    pub fn get_clock_interval_sec(&self) -> i64 {
        self.clock_interval_sec
    }

    #[setter]
    pub fn set_clock_interval_sec(&mut self, interval: i64) {
        self.clock_interval_sec = interval;
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
        json += &format!(
            "\"timestamp_str\": {},\n",
            time_string(self.current_timestamp)
        );

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

        json += &format!("\"psudo_account\":{},", self.psudo_account.__repr__());

        json += &format!("\"psudo_position\":{}", self.position);
        json += "}";
        json
    }
}

impl Session {
    pub fn log(&mut self, order: &Order) -> Result<(), std::io::Error> {
        let time = self.calc_log_timestamp();

        self.log.log_order(time, order)
    }

    pub fn open_log(&mut self, path: &str) -> Result<(), std::io::Error> {
        self.log.open_log(path)
    }

    pub fn log_profit(
        &mut self,
        log_id: i64,
        open_position: Decimal,
        close_position: Decimal,
        position: Decimal,
        profit: Decimal,
        fee: Decimal,
        total_profit: Decimal,
    ) -> Result<(), std::io::Error> {
        let time = self.calc_log_timestamp();

        self.log.log_profit(
            time,
            log_id,
            open_position.to_f64().unwrap(),
            close_position.to_f64().unwrap(),
            position.to_f64().unwrap(),
            profit.to_f64().unwrap(),
            fee.to_f64().unwrap(),
            total_profit.to_f64().unwrap(),
        )
    }

    pub fn log_account(&mut self, account: &AccountStatus) -> Result<(), std::io::Error> {
        let time = self.calc_log_timestamp();

        self.log.log_account(time, account)
    }

    pub fn calc_log_timestamp(&self) -> MicroSec {
        if self.current_timestamp < self.current_clock_time {
            self.current_clock_time
        } else {
            self.current_timestamp
        }
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

        if self.log_account(account).is_err() {
            log::error!("log_account_status error");
        };
    }

    pub fn on_order_update(&mut self, order: &mut Order) {
        self.log_id += 1;
        order.log_id = self.log_id;
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

        if self.log(&order).is_err() {
            log::error!("log order error{:?}", order);
        };

        self.update_psudo_position(order);
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

    // ポジションが変化したときは平均購入単価と仮想Profitを計算する。
    pub fn update_psudo_position(&mut self, order: &Order) {
        let mut open_position = dec![0.0];
        let mut close_position = dec![0.0];
        let mut profit = dec![0.0];

        if order.order_side == OrderSide::Buy {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::PartiallyFilled {
                if dec![0.0] <= self.position {
                    self.open_position(order.execute_price, order.execute_size);
                    open_position = order.execute_size;
                } else {
                    (close_position, open_position, profit) =
                        self.close_position(order.execute_price, order.execute_size);
                }
            }
        } else if order.order_side == OrderSide::Sell {
            if order.status == OrderStatus::Filled || order.status == OrderStatus::PartiallyFilled {
                if dec![0.0] <= self.position {
                    (close_position, open_position, profit) =
                        self.close_position(order.execute_price, -order.execute_size);
                } else {
                    self.open_position(order.execute_price, -order.execute_size);
                    open_position = -order.execute_size;
                }
            }
        } else {
            log::error!("Unknown order side: {:?}", order.order_side)
        }

        let fee = if order.is_maker {
            order.execute_price * order.execute_size * self.market_config.maker_fee
        } else {
            order.execute_price * order.execute_size * self.market_config.taker_fee
        };

        let total_profit = profit - fee;

        if self
            .log_profit(
                order.log_id,
                open_position,
                close_position,
                self.position,
                profit,
                fee,
                total_profit,
            )
            .is_err()
        {
            log::error!("log_profit error");
        };
    }

    /// returns position change
    pub fn open_position(&mut self, price: Decimal, position: Decimal) {
        let total_cost = (self.average_price * self.position) + (price * position);
        let total_size = self.position + position;

        self.average_price = total_cost / total_size;
        self.position += position;
    }

    /// retruns position change, and profit change
    pub fn close_position(
        &mut self,
        price: Decimal,
        position: Decimal,
    ) -> (Decimal, Decimal, Decimal) {
        // close_position, open_position, profit_change
        let close_position: Decimal;
        let mut open_position: Decimal = dec![0.0];

        let profit = if position.abs() <= self.position.abs() {
            close_position = -position;
            self.position -= close_position;

            let profit = (price * close_position) - (self.average_price * close_position);
            self.profit += profit;

            profit
        } else {
            close_position = self.position;
            let new_position = close_position + position;

            // self.position += close_position;
            let profit = (price * close_position) - (self.average_price * close_position);
            self.profit += profit;

            log::debug!(
                "close_position: close_pos={} / closed_pos={}",
                position,
                close_position
            );
            log::debug!(
                "close_position: old_pos={} / new_position={}",
                self.position,
                new_position
            );

            self.position = dec![0.0];
            self.average_price = dec![0.0];
            open_position = new_position;
            self.open_position(price, new_position);

            profit
        };

        (close_position, open_position, profit)
    }

    /*
    pub fn change_psudo_position(&mut self, price: Decimal, position_change: Decimal, home_change: Decimal) {
        // position and position_change have same sign, Open position
        if dec![0.0] <= self.position * position_change {
            self.open_position(price, position_change, home_change);
        }
        else {
            self.close_position(price, position_change, home_change);
        }
    }
    */

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
                o.transaction_id = self.dummy_transaction_id();
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

#[cfg(test)]
mod session_tests {
    use crate::{
        common::init_debug_log,
        exchange::binance::{BinanceConfig, BinanceMarket},
    };

    use super::*;

    fn new_session() -> Session {
        pyo3::prepare_freethreaded_python();

        let config = BinanceConfig::BTCUSDT();
        let binance = BinanceMarket::new(&config);

        Python::with_gil(|py| {
            let session = Session::new(binance.into_py(py), ExecuteMode::BackTest, None, true);

            session
        })
    }

    #[test]
    fn test_open_plus_position() {
        let mut session = new_session();
        session.open_position(dec![100.0], dec![10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![10.0]);
        assert_eq!(session.profit, dec![0.0]);

        session.open_position(dec![200.0], dec![10.0]);
        assert_eq!(session.average_price, dec![150.0]);
        assert_eq!(session.position, dec![20.0]);
    }

    #[test]
    fn test_open_minus_position() {
        let mut session = new_session();
        session.open_position(dec![100.0], dec![-10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-10.0]);

        session.open_position(dec![200.0], dec![-10.0]);
        assert_eq!(session.average_price, dec![150.0]);
        assert_eq!(session.position, dec![-20.0]);
    }

    #[test]
    fn test_close_position_less_than_position() {
        //init_debug_log();
        let mut session = new_session();
        session.open_position(dec![100.0], dec![10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![10.0]);

        session.close_position(dec![150.0], dec![-5.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![5.0]);

        // 150 * 5 - 100 * 5 = 250
        assert_eq!(session.profit, dec![250.0]);
    }

    #[test]
    fn test_close_position_less_than_position_minus() {
        //init_debug_log();
        let mut session = new_session();
        session.open_position(dec![100.0], dec![-10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-10.0]);

        session.close_position(dec![150.0], dec![5.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-5.0]);

        // 150 * 5 - 100 * 5 = 250
        assert_eq!(session.profit, dec![-250.0]);
    }

    #[test]
    fn test_close_position_greater_than_position() {
        // init_debug_log();
        let mut session = new_session();
        session.open_position(dec![100.0], dec![10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![10.0]);

        // TODO: Fic profit calculation
        session.close_position(dec![150.0], dec![-11.0]);
        assert_eq!(session.profit, dec![500.0]);
        assert_eq!(session.position, dec![-1.0]);
        assert_eq!(session.average_price, dec![150.0]);
    }

    #[test]
    fn test_close_position_greater_than_position_minus() {
        init_debug_log();
        let mut session = new_session();
        session.open_position(dec![100.0], dec![-10.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-10.0]);

        // TODO: Fic profit calculation
        session.close_position(dec![150.0], dec![11.0]);
        assert_eq!(session.profit, dec![-500.0]);
        assert_eq!(session.position, dec![1.0]);
        assert_eq!(session.average_price, dec![150.0]);
    }

    #[test]
    fn test_close_position_break_outsample() {
        //init_debug_log();
        let mut session = new_session();
        session.open_position(dec![100.0], dec![-0.00095]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-0.00095]);

        session.open_position(dec![100.0], dec![-0.00905]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-0.01]);

        session.close_position(dec![100.0], dec![0.00101]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![-0.00899]);

        session.close_position(dec![100.0], dec![0.01899]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![0.01]);
    }

    /*
    #[test]
    fn test_change_position() {
        let mut session = new_session();

        // buy 10
        session.change_psudo_position(dec![100.0], dec![10.0], dec![1000.0]);
        assert_eq!(session.average_price, dec![100.0]);
        assert_eq!(session.position, dec![10.0]);

        // buy more 10, with 200 price
        session.change_psudo_position(dec![200.0], dec![10.0], dec![2000.0]);
        assert_eq!(session.average_price, dec![150.0]);
        assert_eq!(session.position, dec![20.0]);
        assert_eq!(session.profit, dec![0.0]);

        // sell 10
        session.change_psudo_position(dec![200.0], dec![-10.0], dec![1500.0]);
        assert_eq!(session.average_price, dec![150.0]);
        assert_eq!(session.position, dec![10.0]);
        assert_eq!(session.profit, dec![500.0]);
    }
    */
}

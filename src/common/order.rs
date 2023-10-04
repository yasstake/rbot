// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::time::time_string;

use super::time::MicroSec;
use super::MarketConfig;
use super::MarketMessage;
use pyo3::pyclass;
use pyo3::pymethods;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::de;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use strum::EnumString;
use strum_macros::Display;

#[derive(Debug, Clone)]
pub struct TimeChunk {
    pub start: MicroSec,
    pub end: MicroSec,
}

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, Serialize, Deserialize)]
pub enum OrderStatus {
    #[strum(ascii_case_insensitive)]
    New, // 処理中
    #[strum(
        ascii_case_insensitive,
        serialize = "PartiallyFilled",
        serialize = "PARTIALLY_FILLED"
    )]
    PartiallyFilled, // 一部約定
    #[strum(ascii_case_insensitive)]
    Filled,
    #[strum(ascii_case_insensitive)]
    Canceled, // ユーザによるキャンセル
    #[strum(ascii_case_insensitive)]
    Rejected, // システムからの拒否（指値範囲外、数量不足など）
    #[strum(ascii_case_insensitive)]
    Error, // その他エラー
}

pub fn orderstatus_deserialize<'de, D>(deserializer: D) -> Result<OrderStatus, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(string_to_status(&s))
}

pub fn string_to_status(s: &str) -> OrderStatus {
    let order_status: OrderStatus = s.parse().unwrap_or(OrderStatus::Error);

    return order_status;
}

#[pymethods]
impl OrderStatus {
    pub fn __str__(&self) -> String {
        return self.to_string();
    }

    pub fn __repr__(&self) -> String {
        return self.to_string();
    }

    pub fn _html_repr_(&self) -> String {
        return self.to_string();
    }
}

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Display, Serialize, Deserialize)]
/// Enum representing the side of an order, either Buy or Sell.
/// Buy is represented by the value "Buy", "BUY", "buy", "B",
/// Sell is represented by the value "Sell", "SELL", "sell", "b"
pub enum OrderSide {
    #[strum(ascii_case_insensitive)]
    Buy,
    #[strum(ascii_case_insensitive)]
    Sell,
    /// Represents an unknown order side.
    Unknown,
}

impl OrderSide {
    pub fn from_buy_side(buy_side: bool) -> Self {
        match buy_side {
            true => OrderSide::Buy,
            _ => OrderSide::Sell,
        }
    }

    pub fn is_buy_side(&self) -> bool {
        match &self {
            OrderSide::Buy => true,
            _ => false,
        }
    }
}

pub fn orderside_deserialize<'de, D>(deserializer: D) -> Result<OrderSide, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(string_to_side(&s))
}

pub fn string_to_side(side: &str) -> OrderSide {
    match side.to_uppercase().as_str() {
        "BUY" | "B" => OrderSide::Buy,
        "SELL" | "S" => OrderSide::Sell,
        _ => {
            log::error!("Unknown order side: {:?}", side);
            OrderSide::Unknown
        }
    }
}

impl From<&String> for OrderSide {
    fn from(side: &String) -> Self {
        string_to_side(side)
    }
}

impl From<&str> for OrderSide {
    fn from(side: &str) -> Self {
        string_to_side(side)
    }
}

/*
impl Into<String> for OrderSide {
    fn into(self) -> String {
        self.to_string()
    }
}
*/

#[pymethods]
impl OrderSide {
    pub fn __repr__(&self) -> String {
        return self.to_string();
    }
    pub fn _html_repr_(&self) -> String {
        return self.to_string();
    }
}

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Display, Serialize, Deserialize)]
/// enum order type
pub enum OrderType {
    #[strum(ascii_case_insensitive, serialize = "LIMIT")]
    Limit,
    #[strum(ascii_case_insensitive, serialize = "MARKET")]
    Market,
}
#[pymethods]
impl OrderType {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

pub fn ordertype_deserialize<'de, D>(deserializer: D) -> Result<OrderType, D::Error>
where
    D: de::Deserializer<'de>,
{
    let s: String = de::Deserialize::deserialize(deserializer)?;
    Ok(str_to_order_type(&s))
}

fn str_to_order_type(order_type: &str) -> OrderType {
    match order_type.to_uppercase().as_str() {
        "LIMIT" => OrderType::Limit,
        "MARKET" => OrderType::Market,
        _ => {
            log::error!("Unknown order type: {:?}", order_type);
            OrderType::Limit
        }
    }
}

impl From<&str> for OrderType {
    fn from(order_type: &str) -> Self {
        str_to_order_type(order_type)
    }
}

impl From<&String> for OrderType {
    fn from(order_type: &String) -> Self {
        str_to_order_type(order_type)
    }
}

// Represent one Trade execution.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Represents a trade made on an exchange.
pub struct Trade {
    /// The time the trade was executed, in microseconds since the epoch.
    pub time: MicroSec,
    /// The side of the order that was filled.
    pub order_side: OrderSide,
    /// The price at which the trade was executed.
    pub price: Decimal,
    /// The size of the trade.
    pub size: Decimal,
    /// The unique identifier for the trade.
    pub id: String,
}

#[pymethods]
impl Trade {
    #[new]
    pub fn new(
        time_microsecond: MicroSec,
        order_side: OrderSide,
        price: Decimal,
        size: Decimal,
        id: String,
    ) -> Self {
        return Trade {
            time: time_microsecond,
            order_side,
            price,
            size,
            id,
        };
    }

    pub fn to_csv(&self) -> String {
        format!(
            "{:?}, {:?}, {:?}, {:?}, {:?}\n",
            self.time, self.order_side, self.price, self.size, self.id
        )
    }

    pub fn __str__(&self) -> String {
        format!(
            "{{timestamp:{}({:?}), order_side:{:?}, price:{:?}, size:{:?}, id:{:?}}}",
            time_string(self.time),
            self.time,
            self.order_side,
            self.price,
            self.size,
            self.id
        )
    }

    pub fn __repr__(&self) -> String {
        format!(
            "{{timestamp:{:?}, order_side:{:?}, price:{:?}, size:{:?}, id:{:?}}}",
            self.time, self.order_side, self.price, self.size, self.id
        )
    }
}

impl Into<MarketMessage> for Trade {
    fn into(self) -> MarketMessage {
        MarketMessage {
            trade: Some(self.clone()),
            order: None,
            account: None,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OrderFill {
    // 約定時に確定するデータ
    pub transaction_id: String,
    pub update_time: MicroSec, // in us
    pub price: Decimal,
    pub filled_size: Decimal,     // 約定数
    pub quote_vol: Decimal,       // in opposite currency
    pub commission: Decimal,      //
    pub commission_asset: String, //
    pub maker: bool,
}

#[pymethods]
impl OrderFill {
    #[new]
    pub fn new() -> Self {
        return OrderFill {
            transaction_id: "".to_string(),
            update_time: 0,
            price: dec![0.0],
            filled_size: dec![0.0],
            quote_vol: dec![0.0],
            commission: dec![0.0],
            commission_asset: "".to_string(),
            maker: false,
        };
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountChange {
    pub commission_home: Decimal,    // in home currency
    pub commission_foreign: Decimal, // in foreign currency
    pub home_change: Decimal,
    pub foreign_change: Decimal,
    pub free_home_change: Decimal,
    pub free_foreign_change: Decimal,
    pub lock_home_change: Decimal,
    pub lock_foreign_change: Decimal,
}

impl AccountChange {
    pub fn new() -> Self {
        return AccountChange {
            commission_home: Decimal::new(0, 0),
            commission_foreign: Decimal::new(0, 0),
            home_change: Decimal::new(0, 0),
            foreign_change: Decimal::new(0, 0),
            free_home_change: Decimal::new(0, 0),
            free_foreign_change: Decimal::new(0, 0),
            lock_home_change: Decimal::new(0, 0),
            lock_foreign_change: Decimal::new(0, 0),
        };
    }
}

impl Default for AccountChange {
    fn default() -> Self {
        return AccountChange::new();
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AccountStatus {
    pub home: Decimal,
    pub home_free: Decimal,
    pub home_locked: Decimal,

    pub foreign: Decimal,
    pub foreign_free: Decimal,
    pub foreign_locked: Decimal,
}

impl Default for AccountStatus {
    fn default() -> Self {
        return AccountStatus {
            home: Decimal::new(0, 0),
            home_free: Decimal::new(0, 0),
            home_locked: Decimal::new(0, 0),

            foreign: Decimal::new(0, 0),
            foreign_free: Decimal::new(0, 0),
            foreign_locked: Decimal::new(0, 0),
        };
    }
}

#[pymethods]
impl AccountStatus {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
    #[getter]
    pub fn get_home(&self) -> f64 {
        return self.home.to_f64().unwrap();
    }
    #[getter]
    pub fn get_home_free(&self) -> f64 {
        return self.home_free.to_f64().unwrap();
    }
    #[getter]
    pub fn get_home_locked(&self) -> f64 {
        return self.home_locked.to_f64().unwrap();
    }
    #[getter]
    pub fn get_foreign(&self) -> f64 {
        return self.foreign.to_f64().unwrap();
    }
    #[getter]
    pub fn get_foreign_free(&self) -> f64 {
        return self.foreign_free.to_f64().unwrap();
    }
    #[getter]
    pub fn get_foreign_locked(&self) -> f64 {
        return self.foreign_locked.to_f64().unwrap();
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Order {
    // オーダー作成時に必須のデータ。以後変化しない。
    #[pyo3(get)]
    pub symbol: String,
    #[pyo3(get)]
    pub create_time: MicroSec, // in us
    #[pyo3(get)]
    pub order_id: String, // YYYY-MM-DD-SEQ
    #[pyo3(get)]
    pub client_order_id: String,
    #[pyo3(get)]
    pub order_side: OrderSide,
    #[pyo3(get)]
    pub order_type: OrderType,
    // #[pyo3(get)]
    pub order_price: Decimal, // in Market order, price is 0.0
    //#[pyo3(get)]
    pub order_size: Decimal, // in foreign

    // 以後オーダーの状況に応じてUpdateされる。
    //#[pyo3(get)]
    pub remain_size: Decimal, // 残数
    #[pyo3(get)]
    pub status: OrderStatus,
    #[pyo3(get)]
    pub transaction_id: String,
    #[pyo3(get)]
    pub update_time: MicroSec,
    //#[pyo3(get)]
    pub execute_price: Decimal,
    //#[pyo3(get)]
    pub execute_size: Decimal,
    //#[pyo3(get)]
    pub quote_vol: Decimal,
    //#[pyo3(get)]
    pub commission: Decimal,
    #[pyo3(get)]
    pub commission_asset: String,
    #[pyo3(get)]
    pub is_maker: bool,
    #[pyo3(get)]
    pub message: String,
    pub commission_home: Decimal,    // in home currency
    pub commission_foreign: Decimal, // in foreign currency
    pub home_change: Decimal,
    pub foreign_change: Decimal,
    pub free_home_change: Decimal,
    pub free_foreign_change: Decimal,
    pub lock_home_change: Decimal,
    pub lock_foreign_change: Decimal,
}

impl Order {
    pub fn new(
        symbol: String,
        create_time: MicroSec,
        order_id: String,
        client_order_id: String,
        order_side: OrderSide,
        order_type: OrderType,
        order_status: OrderStatus,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        return Order {
            symbol,
            create_time,
            order_id,
            client_order_id,
            order_side,
            order_type,
            order_price: price,
            order_size: size,
            remain_size: size,
            status: order_status,
            transaction_id: "".to_string(),
            update_time: 0,
            execute_price: dec![0.0],
            execute_size: dec![0.0],
            quote_vol: dec![0.0],
            commission: dec![0.0],
            commission_asset: "".to_string(),
            is_maker: false,
            message: "".to_string(),
            commission_home: dec![0.0],
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],
        };
    }
}

#[pymethods]
impl Order {
    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn update(&mut self, order: &Order) {
        /* unchange feild
        order_id,
        client_order_id,
        order_side,
        order_type,
        order_price: price,
        order_size: size,
        */

        self.remain_size = order.remain_size;
        self.status = order.status;
        self.transaction_id = order.transaction_id.clone();
        self.update_time = order.update_time;
        self.execute_price = order.execute_price;
        self.execute_size = order.execute_size;
        self.quote_vol = order.quote_vol;
        self.commission = order.commission;
        self.commission_asset = order.commission_asset.clone();
        self.is_maker = order.is_maker;

        if order.message.len() > 0 {
            self.message = order.message.clone();
        }
    }

    #[getter]
    pub fn get_order_price(&self) -> f64 {
        return self.order_price.to_f64().unwrap();
    }
    //#[pyo3(get)]
    #[getter]
    pub fn get_order_size(&self) -> f64 {
        return self.order_size.to_f64().unwrap();
    }

    #[getter]
    pub fn get_remain_size(&self) -> f64 {
        return self.remain_size.to_f64().unwrap();
    }

    #[getter]
    pub fn get_execute_price(&self) -> f64 {
        return self.execute_price.to_f64().unwrap();
    }

    #[getter]
    pub fn get_execute_size(&self) -> f64 {
        return self.execute_size.to_f64().unwrap();
    }

    #[getter]
    pub fn get_quote_vol(&self) -> f64 {
        return self.quote_vol.to_f64().unwrap();
    }

    #[getter]
    pub fn get_commission(&self) -> f64 {
        return self.commission.to_f64().unwrap();
    }
    #[getter]
    pub fn get_commission_home(&self) -> f64 {
        return self.commission_home.to_f64().unwrap();
    }

    #[getter]
    pub fn get_commission_foreign(&self)->f64 {
        return self.commission_foreign.to_f64().unwrap();
    }
    #[getter]
    pub fn home_change(&self) -> f64 {
        return self.home_change.to_f64().unwrap();
    }
    #[getter]
    pub fn get_foreign_change(&self) -> f64 {
        return self.foreign_change.to_f64().unwrap();
    }
    #[getter]
    pub fn get_free_home_change(&self) -> f64 {
        return self.free_home_change.to_f64().unwrap();
    }

    #[getter]
    pub fn get_free_foreign_change(&self) -> f64 {
        return self.free_foreign_change.to_f64().unwrap();
    }

    #[getter]
    pub fn get_lock_home_change(&self) -> f64 {
        return self.lock_home_change.to_f64().unwrap();
    }
    #[getter]
    pub fn get_lock_foreign_change(&self) -> f64 {
        return self.lock_foreign_change.to_f64().unwrap();
    }


}

impl Into<MarketMessage> for Order {
    fn into(self) -> MarketMessage {
        MarketMessage {
            trade: None,
            order: Some(self.clone()),
            account: None,
        }
    }
}

impl Order {
    pub fn update_balance(&mut self, config: &MarketConfig) {
        match self.status {
            OrderStatus::New => {
                self.update_balance_new(config);
            }
            OrderStatus::PartiallyFilled | OrderStatus::Filled => {
                self.update_balance_filled(config);
            }
            OrderStatus::Canceled | OrderStatus::Rejected | OrderStatus::Error => {
                self.update_balance_canceled(config);
            }
        }
    }

    /// in order book, accout locked the size of order
    fn update_balance_new(&mut self, _config: &MarketConfig) {
        let order_size= self.order_size;
        let order_quote_vol = self.order_size * self.order_price;

        if self.order_side == OrderSide::Buy {
            // move home to foregin
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = - order_quote_vol;
            self.free_foreign_change = dec![0.0];
            self.lock_home_change = order_quote_vol;
            self.lock_foreign_change = dec![0.0];
        } else { // Sell
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = dec![0.0];
            self.free_foreign_change = - order_size;
            self.lock_home_change = dec![0.0];
            self.lock_foreign_change = order_size;
        }
    }

    /// The lock is freed and shift the balance.
    fn update_balance_filled(&mut self, config: &MarketConfig) {
        let filled_size = self.execute_size;
        let filled_quote_vol = self.execute_size * self.execute_price;
        let commission = self.commission;
        let commission_asset = self.commission_asset.clone();

        if commission_asset == config.home_currency {
            self.commission_home = commission;
            self.commission_foreign = dec![0.0];
        } else {
            self.commission_home = dec![0.0];
            self.commission_foreign = commission;
        }

        if self.order_side == OrderSide::Buy {
            // move home to foregin
            self.home_change = - filled_quote_vol;
            self.foreign_change = filled_size;
            self.free_home_change = - filled_quote_vol;
            self.free_foreign_change = filled_size;
            self.lock_home_change = - filled_quote_vol;
            self.lock_foreign_change = dec![0.0];
        } else { // Sell
            self.home_change = filled_quote_vol;
            self.foreign_change = - filled_size;
            self.free_home_change = filled_quote_vol;
            self.free_foreign_change = - filled_size;
            self.lock_home_change = dec![0.0];
            self.lock_foreign_change = - filled_size;
        }
    }

    //
    fn update_balance_canceled(&mut self, _config: &MarketConfig) {
        let order_size= self.order_size;
        let order_quote_vol = self.order_size * self.order_price;

        if self.order_side == OrderSide::Buy {
            // move home to foregin
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = order_quote_vol;
            self.free_foreign_change = dec![0.0];
            self.lock_home_change = - order_quote_vol;
            self.lock_foreign_change = dec![0.0];
        } else { // Sell
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = dec![0.0];
            self.free_foreign_change = order_size;
            self.lock_home_change = dec![0.0];
            self.lock_foreign_change = - order_size;
        }
    }
}

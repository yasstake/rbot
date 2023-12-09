// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::time::time_string;

use super::time::MicroSec;
use super::MarketConfig;
use super::MarketMessage;
use polars_core::prelude::DataFrame;
use polars_core::prelude::NamedFrom;
use polars_core::prelude::TimeUnit;
use polars_core::series::Series;
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
    #[strum(ascii_case_insensitive)]
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
    #[strum(ascii_case_insensitive, serialize = "Limit")]
    Limit,
    #[strum(ascii_case_insensitive, serialize = "Market")]
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
            // OrderType::Limit
            // TODO: for debuging purpose
            panic!("Unknown order type: {:?}", order_type);
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

#[derive(Debug, Clone, Copy, PartialEq, Display, Serialize, Deserialize)]
#[pyclass]
pub enum LogStatus {
    UnFix,           // データはWebSocketなどから取得されたが、まだ確定していない
    FixBlockStart,   // データが確定(アーカイブ）し、ブロックの開始を表す
    FixArchiveBlock, // データが確定(アーカイブ）し、ブロックの中間を表す（アーカイブファイル）
    FixBlockEnd,     // データが確定(アーカイブ）し、ブロックの終了を表す
    FixRestApiStart,
    FixRestApiBlock, // データが確定(アーカイブ）し、ブロックの中間を表す（REST API）
    FixRestApiEnd,
    Unknown, // 未知のステータス / 未確定のステータス
}

impl Default for LogStatus {
    fn default() -> Self {
        LogStatus::UnFix
    }
}

impl From<&str> for LogStatus {
    fn from(status: &str) -> Self {
        match status.to_uppercase().as_str() {
            "U" => LogStatus::UnFix,
            "S" => LogStatus::FixBlockStart,
            "A" => LogStatus::FixArchiveBlock,
            "E" => LogStatus::FixBlockEnd,
            "s" => LogStatus::FixRestApiStart,
            "a" => LogStatus::FixRestApiBlock,
            "e" => LogStatus::FixRestApiEnd,
            _ => {
                log::error!("Unknown log status: {:?}", status);
                LogStatus::Unknown
            }
        }
    }
}

impl LogStatus {
    pub fn to_string(&self) -> String {
        match self {
            LogStatus::UnFix => "U".to_string(),
            LogStatus::FixBlockStart => "S".to_string(),
            LogStatus::FixArchiveBlock => "A".to_string(),
            LogStatus::FixBlockEnd => "E".to_string(),
            LogStatus::FixRestApiStart => "s".to_string(),
            LogStatus::FixRestApiBlock => "a".to_string(),
            LogStatus::FixRestApiEnd => "e".to_string(),
            LogStatus::Unknown => "X".to_string(),
        }
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
    /// trade status
    pub status: LogStatus,
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
        status: LogStatus,
        id: String,
    ) -> Self {
        return Trade {
            time: time_microsecond,
            order_side,
            price,
            size,
            status,
            id,
        };
    }

    pub fn to_csv(&self) -> String {
        format!(
            "{:?}, {:?}, {:?}, {:?}, {:?}, {:?}\n",
            self.time, self.order_side, self.price, self.size, self.status, self.id
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

impl Into<MarketMessage> for &Trade {
    fn into(self) -> MarketMessage {
        MarketMessage {
            trade: Some(self.clone()),
            order: None,
            account: None,
        }
    }
}

/*
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
*/

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
            home: dec![0.0],
            home_free: dec![0.0],
            home_locked: dec![0.0],

            foreign: dec![0.0],
            foreign_free: dec![0.0],
            foreign_locked: dec![0.0]
        };
    }
}

#[pymethods]
impl AccountStatus {
    pub fn apply_order(&mut self, order: &Order) {
        self.foreign += order.foreign_change;
        self.foreign_free += order.free_foreign_change;
        self.foreign_locked += order.lock_foreign_change;

        self.home += order.home_change;
        self.home_free += order.free_home_change;
        self.home_locked += order.lock_home_change;
    }

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
    pub status: OrderStatus,
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
    #[pyo3(get)]
    pub remain_size: Decimal, // 残数
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
    pub log_id: i64,
}

#[pymethods]
impl Order {
    #[new]
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
            status: order_status,
            order_id,
            client_order_id,
            order_side,
            order_type,
            order_price: price,
            order_size: size,
            remain_size: size,
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
            log_id: 0,
        };
    }

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
    pub fn get_commission_foreign(&self) -> f64 {
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

impl Into<MarketMessage> for &Order {
    fn into(self) -> MarketMessage {
        MarketMessage {
            trade: None,
            order: Some(self.clone()),
            account: None,
        }
    }
}

pub fn ordervec_to_dataframe(orders: Vec<Order>) -> DataFrame {
    let mut log_id = Vec::<i64>::new();
    let mut symbol = Vec::<String>::new();
    let mut create_time = Vec::<MicroSec>::new();
    let mut status = Vec::<String>::new();
    let mut order_id = Vec::<String>::new();
    let mut client_order_id = Vec::<String>::new();
    let mut order_side = Vec::<String>::new();
    let mut order_type = Vec::<String>::new();
    let mut order_price = Vec::<f64>::new();
    let mut order_size = Vec::<f64>::new();
    let mut remain_size = Vec::<f64>::new();
    let mut transaction_id = Vec::<String>::new();
    let mut update_time = Vec::<MicroSec>::new();
    let mut execute_price = Vec::<f64>::new();
    let mut execute_size = Vec::<f64>::new();
    let mut quote_vol = Vec::<f64>::new();
    let mut commission = Vec::<f64>::new();
    let mut commission_asset = Vec::<String>::new();
    let mut is_maker = Vec::<bool>::new();
    let mut message = Vec::<String>::new();
    let mut commission_home = Vec::<f64>::new();
    let mut commission_foreign = Vec::<f64>::new();
    let mut home_change = Vec::<f64>::new();
    let mut foreign_change = Vec::<f64>::new();
    let mut free_home_change = Vec::<f64>::new();
    let mut free_foreign_change = Vec::<f64>::new();
    let mut lock_home_change = Vec::<f64>::new();
    let mut lock_foreign_change = Vec::<f64>::new();

    for order in orders {
        log_id.push(order.log_id);
        symbol.push(order.symbol.clone());
        create_time.push(order.create_time);
        status.push(order.status.to_string());
        order_id.push(order.order_id.clone());
        client_order_id.push(order.client_order_id.clone());
        order_side.push(order.order_side.to_string());
        order_type.push(order.order_type.to_string());
        order_price.push(order.order_price.to_f64().unwrap());
        order_size.push(order.order_size.to_f64().unwrap());
        remain_size.push(order.remain_size.to_f64().unwrap());
        transaction_id.push(order.transaction_id.clone());
        update_time.push(order.update_time);
        execute_price.push(order.execute_price.to_f64().unwrap());
        execute_size.push(order.execute_size.to_f64().unwrap());
        quote_vol.push(order.quote_vol.to_f64().unwrap());
        commission.push(order.commission.to_f64().unwrap());
        commission_asset.push(order.commission_asset.clone());
        is_maker.push(order.is_maker);
        message.push(order.message.clone());

        commission_home.push(order.commission_home.to_f64().unwrap());
        commission_foreign.push(order.commission_foreign.to_f64().unwrap());
        home_change.push(order.home_change.to_f64().unwrap());
        foreign_change.push(order.foreign_change.to_f64().unwrap());
        free_home_change.push(order.free_home_change.to_f64().unwrap());
        free_foreign_change.push(order.free_foreign_change.to_f64().unwrap());
        lock_home_change.push(order.lock_home_change.to_f64().unwrap());
        lock_foreign_change.push(order.lock_foreign_change.to_f64().unwrap());
    }

    let log_id = Series::new("log_id", log_id);
    let symbol = Series::new("symbol", symbol);
    let create_time = Series::new("create_time", create_time);
    let status = Series::new("status", status);
    let order_id = Series::new("order_id", order_id);
    let client_order_id = Series::new("client_order_id", client_order_id);
    let order_side = Series::new("order_side", order_side);
    let order_type = Series::new("order_type", order_type);
    let order_price = Series::new("order_price", order_price);
    let order_size = Series::new("order_size", order_size);
    let remain_size = Series::new("remain_size", remain_size);
    let transaction_id = Series::new("transaction_id", transaction_id);
    let update_time = Series::new("update_time", update_time);
    let execute_price = Series::new("execute_price", execute_price);
    let execute_size = Series::new("execute_size", execute_size);
    let quote_vol = Series::new("quote_vol", quote_vol);
    let commission = Series::new("commission", commission);
    let commission_asset = Series::new("commission_asset", commission_asset);
    let is_maker = Series::new("is_maker", is_maker);
    let message = Series::new("message", message);

    let commission_home = Series::new("commission_home", commission_home);
    let commission_foreign = Series::new("commission_foreign", commission_foreign);
    let home_change = Series::new("home_change", home_change);
    let foreign_change = Series::new("foreign_change", foreign_change);
    let free_home_change = Series::new("free_home_change", free_home_change);
    let free_foreign_change = Series::new("free_foreign_change", free_foreign_change);
    let lock_home_change = Series::new("lock_home_change", lock_home_change);
    let lock_foreign_change = Series::new("lock_foreign_change", lock_foreign_change);

    let mut df = DataFrame::new(vec![
        log_id,
        symbol,
        update_time,
        create_time,
        status,
        order_id,
        client_order_id,
        order_side,
        order_type,
        order_price,
        order_size,
        remain_size,
        transaction_id,
        execute_price,
        execute_size,
        quote_vol,
        commission,
        commission_asset,
        is_maker,
        message,
        commission_home,
        commission_foreign,
        home_change,
        foreign_change,
        free_home_change,
        free_foreign_change,
        lock_home_change,
        lock_foreign_change,
    ])
    .unwrap();

    let time = df.column("create_time").unwrap().i64().unwrap().clone();
    let date_time = time.into_datetime(TimeUnit::Microseconds, None);
    let df = df.with_column(date_time).unwrap();

    let time = df.column("update_time").unwrap().i64().unwrap().clone();
    let date_time = time.into_datetime(TimeUnit::Microseconds, None);
    let df = df.with_column(date_time).unwrap();

    return df.clone();
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

    fn calc_quote_vol(&self) -> Decimal {
        return self.order_price * self.order_size;
    }

    fn calc_home_lock_size_new(&self) -> Decimal {
        match self.is_maker {
            true => self.calc_quote_vol() + self.commission_home,
            false => dec![0.0],
        }
    }

    fn calc_foreign_lock_size_new(&self) -> Decimal {
        match self.is_maker {
            true => self.order_size + self.commission_foreign,
            false => dec![0.0],
        }
    }

    /*
    fn calc_execute_vol(&self) -> Decimal {
        return self.execute_price * self.execute_size;
    }
    */

    /*
    fn calc_home_lock_change(&self) -> Decimal {
        match self.is_maker {
            true => {
                match self.order_side {
                    OrderSide::Buy => {
                        return -self.calc_quote_vol();
                    }
                    OrderSide::Sell => {
                        dec![0,0]
                    }
                    _ => {
                        log::warn!("Order.calc_home_lock_change: Unknown order side. order={:?}", self);
                        return dec![0.0];
                    }

                }
            }
            false => {
                return dec![0.0];
            }
        }
    }

    fn calc_home_change(&self) -> Decimal {
        let change = self.calc_execute_vol() - self.commission_home;
        match self.order_side {
            OrderSide::Buy => {
                -change
            }
            OrderSide::Sell => {
                change
            }
            _ => {
                log::warn!("Order.calc_home_change: Unknown order side. order={:?}", self);
                dec![0.0]
            }
        }
    }

    fn calc_foreign_change(&self) -> Decimal {
        let change = self.execute_size - self.commission_foreign;

        match self.order_side {
            OrderSide::Buy => {
                change
            }
            OrderSide::Sell => {
                -change
            }
            _ => {
                log::warn!("Order.calc_foreign_change: Unknown order side. order={:?}", self);
                return dec![0.0]
            }
        }
    }
    */

    // TODO: BackTest時の計算を追加する。
    // 通常は、約定時にサーバからのデータで確定するが、
    // BackTest時は、ローカルで計算する必要がある。
    fn update_commision(&mut self, config: &MarketConfig) {
        let commission = self.commission;
        let commission_asset = self.commission_asset.clone();

        if commission_asset == config.home_currency {
            self.commission_home = commission;
            self.commission_foreign = dec![0.0];
        } else {
            self.commission_home = dec![0.0];
            self.commission_foreign = commission;
        }
    }

    /// in order book, accout locked the size of order
    /// order bookでは、オーダーのサイズがロックされる。
    /// このロックサイズは、約定時に解除される。
    /// home, foreignは変化しない。
    fn update_balance_new(&mut self, config: &MarketConfig) {
        self.update_commision(config);

        let home_lock_size = self.calc_home_lock_size_new();
        let foreign_lock_size = self.calc_foreign_lock_size_new();

        //let home_change = self.calc_quote_vol();
        //let foreign_change = self.execute_size;

        if self.order_side == OrderSide::Buy {
            //self.commission_home = dec![0.0];
            //self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.free_home_change = -home_lock_size;
            self.lock_home_change =  home_lock_size;            

            self.foreign_change = dec![0.0];
            self.free_foreign_change = dec![0.0];
            self.lock_foreign_change = dec![0.0];
        }
        else {
            //self.commission_home = dec![0.0];
            //self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.free_home_change = dec![0.0];
            self.lock_home_change = dec![0.0];

            self.foreign_change = dec![0.0];
            self.free_foreign_change = -foreign_lock_size;
            self.lock_foreign_change = foreign_lock_size;
        }
    }

    /// The lock is freed and shift the balance.
    /// Lockが解除され、残高が移動する。
    fn update_balance_filled(&mut self, config: &MarketConfig) {
        self.update_commision(config);

        let home_lock = self.calc_home_lock_size_new();
        let foreign_lock = self.calc_foreign_lock_size_new();

        let filled_ratio = self.execute_size / self.order_size;

        let home_lock_size_change = home_lock * filled_ratio;
        let foreign_lock_size_change = foreign_lock * filled_ratio;

        let home_change = self.execute_price * self.execute_size;
        let foreign_change = self.execute_size;

        if self.order_side == OrderSide::Buy {
            //self.commission_home = dec![0.0];
            //self.commission_foreign = dec![0.0];
            self.home_change = -home_change;
            self.free_home_change = dec![0.0];            
            self.lock_home_change = -home_lock_size_change;            

            self.foreign_change = foreign_change;
            self.free_foreign_change = foreign_change;
            self.lock_foreign_change = dec![0.0];
        }
        else {
            //self.commission_home = dec![0.0];
            //self.commission_foreign = dec![0.0];
            self.home_change = home_change;
            self.free_home_change = home_change;
            self.lock_home_change = dec![0.0];

            self.foreign_change = -foreign_change;
            self.free_foreign_change = -foreign_change;
            self.lock_foreign_change = -foreign_lock_size_change;
        }
    }

    //
    fn update_balance_canceled(&mut self, config: &MarketConfig) {
        self.update_commision(config);
        let remain_size = self.remain_size;
        let order_quote_vol = self.order_price * remain_size;

        if self.order_side == OrderSide::Buy {
            // move home to foregin
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = order_quote_vol;
            self.free_foreign_change = dec![0.0];
            self.lock_home_change = -order_quote_vol;
            self.lock_foreign_change = dec![0.0];
        } else {
            // Sell
            self.commission_home = dec![0.0];
            self.commission_foreign = dec![0.0];
            self.home_change = dec![0.0];
            self.foreign_change = dec![0.0];
            self.free_home_change = dec![0.0];
            self.free_foreign_change = remain_size;
            self.lock_home_change = dec![0.0];
            self.lock_foreign_change = -remain_size;
        }
    }
}


#[cfg(test)]
mod order_tests {
    use crate::exchange::binance::BinanceConfig;

    use super::*;

    fn create_order() -> Order {
        let mut order = Order::new(
            "".to_string(),
            0,
            "".to_string(),
            "".to_string(),
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![1234.5],
            dec![0.0001],
        );
        order.update_time = 0;
        order
    }

    #[test]
    fn test_update_balance_filled() {
        let mut order = create_order();
        let config = BinanceConfig::BTCUSDT();
        let market_config = config.market_config.clone();

        //// ＊＊＊＊Limit Orderの場合＊＊＊＊
        // Newの場合は、Lockが増える。
        order.order_side = OrderSide::Buy;
        order.status = OrderStatus::New;
        order.is_maker = true;
        order.update_balance(&market_config);

        println!("order={:?}", order);
        assert_eq!(order.home_change, dec![0.0], "Incorrect home_lock value");
        assert_eq!(order.lock_home_change,
            dec![0.12345],
            "Incorrect home_lock value"
        );
        assert_eq!(
            order.foreign_change,
            dec![0.0],
            "Incorrect home_lock value"
        );
        assert_eq!(
            order.lock_foreign_change,
            dec![0.0],
            "Incorrect lock-home_lock value"
        );

        order.order_side = OrderSide::Sell;
        order.status = OrderStatus::New;
        order.is_maker = true;
        order.update_balance(&market_config);

        println!("order={:?}", order);
        assert_eq!(order.home_change, dec![0.0], "Incorrect home_lock value");
        assert_eq!(order.lock_home_change,
            dec![0.0],
            "Incorrect home_lock value"
        );
        assert_eq!(
            order.foreign_change,
            dec![0.0],
            "Incorrect home_lock value"
        );
        assert_eq!(
            order.lock_foreign_change,
            dec![0.0001],
            "Incorrect lock-home_lock value"
        );
    }

    #[test]
    fn test_order_fill_update_balance() {
        let mut order = create_order();
        let config = BinanceConfig::BTCUSDT();
        let market_config = config.market_config.clone();

        //------------------FILLED-----------------
        // Filledの場合は、残高が移動する。
        // &Makerの場合は、Lockが減り、Freeが増える。

        // buy side
        order.status = OrderStatus::Filled;
        order.order_side = OrderSide::Buy;
        order.execute_price = dec![1234.5];
        order.execute_size = dec![0.0001];
        order.is_maker = true;
        order.update_balance(&market_config);

        println!("order={:?}", order);
        assert_eq!(order.home_change, dec![-0.12345]);
        assert_eq!(order.foreign_change, dec![0.0001]);

        assert_eq!(order.lock_home_change, dec![-0.12345]);
        assert_eq!(order.lock_foreign_change, dec![0.0]);

        // sell side
        order.status = OrderStatus::Filled;
        order.order_side = OrderSide::Sell;
        order.execute_price = dec![1234.5];
        order.execute_size = dec![0.0001];
        order.is_maker = true;
        order.update_balance(&market_config);

        println!("order={:?}", order);
        assert_eq!(order.home_change, dec![0.12345]);
        assert_eq!(order.foreign_change, dec![-0.0001]);

        assert_eq!(order.lock_home_change, dec![0.0]);
        assert_eq!(order.lock_foreign_change, dec![-0.0001]);

        // maker ではない場合はLockしないのでLockの数は変化しない。
        order.is_maker = false;
        order.update_balance(&market_config);        
        assert_eq!(order.lock_home_change, dec![0.0]);
        assert_eq!(order.lock_foreign_change, dec![0.0]);
    }

    #[test]
    fn test_order_calncel_update_balance() {
        let mut order = create_order();
        let config = BinanceConfig::BTCUSDT();
        let market_config = config.market_config.clone();

        order.status = OrderStatus::Canceled;
        order.order_side = OrderSide::Buy;
        order.order_price = dec![1234.5];
        order.order_size = dec![0.0001];
        order.is_maker = true;
        order.update_balance(&market_config);

        assert_eq!(order.home_change, dec![0.0]);
        assert_eq!(order.foreign_change, dec![0.0]);        

        assert_eq!(order.free_home_change, dec![0.12345]);
        assert_eq!(order.free_foreign_change, dec![0.0]);

        assert_eq!(order.lock_home_change, dec![-0.12345]);
        assert_eq!(order.lock_foreign_change, dec![0.0]);


        order.order_side = OrderSide::Sell;
        order.update_balance(&market_config);        
        assert_eq!(order.free_home_change, dec![0.0]);
        assert_eq!(order.free_foreign_change, dec![0.0001]);

        assert_eq!(order.lock_home_change, dec![0.0]);
        assert_eq!(order.lock_foreign_change, dec![-0.0001]);

    }
}

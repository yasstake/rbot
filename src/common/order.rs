// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::time::time_string;
use crate::json_struct;

use super::time::MicroSec;
use pyo3::pyclass;
use pyo3::pyfunction;
use pyo3::pymethods;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use std::str::FromStr;
use strum::EnumString;
use strum_macros::Display;

#[derive(Debug, Clone)]
pub struct TimeChunk {
    pub start: MicroSec,
    pub end: MicroSec,
}

#[pyclass]
#[derive(Debug, PartialEq, Clone, Copy, Display, strum_macros::EnumString)]
pub enum OrderStatus {
    New,          // 処理中
    PartiallyFilled, // 一部約定
    Filled, 
    OrderComplete, // temporary status.
    Canceled,     // キャンセル
    Rejected,     // 拒否
    Expired,      // 期限切れ
    Error,        // その他エラー
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
#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, Serialize, Deserialize)]
/// Enum representing the side of an order, either Buy or Sell.
/// Buy is represented by the value "Buy", "BUY", "buy", "B",
/// Sell is represented by the value "Sell", "SELL", "sell", "b"
pub enum OrderSide {
    #[strum(ascii_case_insensitive, serialize = "Buy", serialize = "B")]
    Buy,
    #[strum(ascii_case_insensitive, serialize = "Sell", serialize = "S")]
    Sell,
    /// Represents an unknown order side.
    Unknown,
}

impl OrderSide {
    pub fn from_str_default(side: &str) -> Self {
        match OrderSide::from_str(side) {
            Ok(side) => {
                side
            }
            Err(_) => {
                OrderSide::Unknown
            }
        }
    }

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
#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString, Serialize, Deserialize)]
/// enum order type
pub enum OrderType {
    LIMIT,
    MARKET,
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

// Represent one Trade execution.
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
/// Represents a trade made on an exchange.
pub struct Trade {
    /// The time the trade was executed, in microseconds since the epoch.
    pub time: MicroSec,
    /// The side of the order that was filled.
    pub order_side: OrderSide,
    /// The price at which the trade was executed.
    pub price: f64,
    /// The size of the trade.
    pub size: f64,
    /// The unique identifier for the trade.
    pub id: String,
}

#[pymethods]
impl Trade {
    #[new]
    pub fn new(
        time_microsecond: MicroSec,
        order_side: OrderSide,
        price: f64,
        size: f64,
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
            time_string(self.time), self.time, self.order_side, self.price, self.size, self.id
        )
    }

    pub fn __repr__(&self) -> String {
        format!(
            "{{timestamp:{:?}, order_side:{:?}, price:{:?}, size:{:?}, id:{:?}}}",
            self.time, self.order_side, self.price, self.size, self.id
        )
    }
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderFill {
    pub transaction_id: Option<String>,
    pub update_time: Option<MicroSec>,      // in us
    pub filled_size: Option<Decimal>,      // 約定数
    pub remain_size: Option<Decimal>,      // 残数
    pub home_change: Option<Decimal>,      // in home
    pub foreign_change: Option<Decimal>,   // in foreign
    pub profit: Option<Decimal>,
    pub maker: Option<bool>,           
    pub fee: Option<Decimal>,              
    pub message: Option<String>,
}


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    // オーダー作成時に必須のデータ。以後変化しない。
    pub symbol: String,
    pub size_in_foreign: bool,        
    pub create_time: MicroSec, // in us
    pub order_id: String,      // YYYY-MM-DD-SEQ
    pub order_list_index: i64,    
    pub client_order_id: String, 
    pub order_side: OrderSide,
    pub order_type: OrderType,
    pub price: Decimal,            // in Market order, price is 0.0
    pub size: Decimal,             // in foreign
    pub fills: Option<OrderFill>
}

#[pymethods]
impl Order {
    #[new]
    pub fn new(
        symbol: String,
        size_in_foreign: bool,
        create_time: MicroSec,
        order_id: String,
        order_list_index: i64,
        client_order_id: String,
        order_side: OrderSide,
        order_type: OrderType,
        price: Decimal,
        size: Decimal,
    ) -> Self {
        return Order {
            symbol,
            size_in_foreign,
            create_time,
            order_id,
            order_list_index,
            client_order_id,
            order_side,
            order_type,
            price,
            size,
            fills: None,
        };
    }

    pub fn __str__(&self) -> String {
        self.__repr__()
    }

    pub fn __repr__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}



/*
// 約定結果
#[pyclass]
#[derive(Debug, Clone)]
pub struct OrderResult {
    #[pyo3(get)]
    pub update_time: MicroSec,
    #[pyo3(get)]
    pub order_id: String,
    #[pyo3(get)]
    pub order_sub_id: i32, // 分割された場合に利用
    #[pyo3(get)]
    pub order_side: OrderSide,
    #[pyo3(get)]
    pub post_only: bool,
    #[pyo3(get)]
    pub create_time: MicroSec,
    #[pyo3(get)]
    pub status: OrderStatus,
    #[pyo3(get)]
    pub open_price: f64,
    #[pyo3(get)]
    pub open_home_size: f64,
    #[pyo3(get)]
    pub open_foreign_size: f64,
    #[pyo3(get)]
    pub close_price: f64,
    #[pyo3(get)]
    pub close_home_size: f64,
    #[pyo3(get)]
    pub close_foreign_size: f64,
    #[pyo3(get)]
    pub order_price: f64,
    #[pyo3(get)]
    pub order_home_size: f64,
    #[pyo3(get)]
    pub order_foreign_size: f64,
    #[pyo3(get)]
    pub profit: f64,
    #[pyo3(get)]
    pub fee: f64,
    #[pyo3(get)]
    pub total_profit: f64,
    #[pyo3(get)]
    pub position_change: f64,
    #[pyo3(get)]    
    pub message: String,
    #[pyo3(get)]
    pub size_in_price_currency: bool,
}

impl OrderResult {
    pub fn from_order(timestamp: MicroSec, order: &Order, status: OrderStatus) -> Self {
        if order.size == 0.0 {
            log::error!("{}  /order size=0", timestamp);
        }

        let result = OrderResult {
            update_time: timestamp,
            order_id: order.order_id.clone(),
            order_sub_id: 0,
            order_side: order.order_side,
            post_only: order.post_only,
            create_time: order.create_time,
            status,
            open_price: 0.0,
            open_home_size: 0.0,
            open_foreign_size: 0.0,
            close_price: 0.0,
            close_home_size: 0.0,
            close_foreign_size: 0.0,
            order_price: order.price,
            order_home_size: order.size,
            order_foreign_size: OrderResult::calc_foreign_size(
                order.price,
                order.size,
                order.size_in_price_currency,
            ),
            profit: 0.0,
            fee: 0.0,
            total_profit: 0.0,
            position_change: 0.0,
            message: order.message.clone(),
            size_in_price_currency: order.size_in_price_currency,
        };

        return result;
    }

    // TODO: calc size in currency.
    pub fn calc_foreign_size(price: f64, home_size: f64, size_in_price_currency: bool) -> f64 {
        if price == 0.0 {
            print!("Div 0 in calc_foreign size {}/{}", price, home_size);
            panic!("Div 0");
        }

        if size_in_price_currency {
            return home_size / price;
        } else {
            return home_size * price;
        }
    }

    /// オーダーを指定された大きさで２つに分ける。
    /// 一つはSelf, もう一つはCloneされたChild Order
    /// 子供のオーダについては、sub_idが１インクリメントする。
    /// 分けられない場合(境界が大きすぎる） NoActionが返る。
    pub fn split_child(&mut self, size: f64) -> Result<OrderResult, OrderStatus> {
        if self.order_home_size <= size {
            // do nothing.
            return Err(OrderStatus::NoAction);
        }

        let mut child = self.clone();

        child.order_sub_id = self.order_sub_id + 1;
        child.order_home_size = self.order_home_size - size;
        if child.order_price == 0.0 {
            log::error!("Div 0 by child {:?}", child);
        }
        child.order_foreign_size = OrderResult::calc_foreign_size(
            child.order_price,
            child.order_home_size,
            self.size_in_price_currency,
        );

        self.order_home_size = size;

        if self.order_price == 0.0 {
            log::error!("Div 0 by parent {:?}", self);
        }
        self.order_foreign_size = OrderResult::calc_foreign_size(
            self.order_price,
            self.order_home_size,
            self.size_in_price_currency,
        );

        return Ok(child);
    }
}
*/
/*
#[pymethods]
impl OrderResult {
    pub fn __str__(&self) -> String {
        return format!("update_time: {:?}, order_id: {:?}, order_sub_id: {:?}, order_side: {:?}, post_only: {:?}, create_time: {:?}, status: {:?}, open_price: {:?}, open_home_size: {:?}, open_foreign_size: {:?}, close_price: {:?}, close_home_size: {:?}, close_foreign_size: {:?}, order_price: {:?}, order_home_size: {:?}, order_foreign_size: {:?}, profit: {:?}, fee: {:?}, total_profit: {:?}, position_change: {:?}, message: {:?}",
                       self.update_time,
                       self.order_id,
                       self.order_sub_id,
                       self.order_side,
                       self.post_only,
                       self.create_time,
                       self.status,
                       self.open_price,
                       self.open_home_size,
                       self.open_foreign_size,
                       self.close_price,
                       self.close_home_size,
                       self.close_foreign_size,
                       self.order_price,
                       self.order_home_size,
                       self.order_foreign_size,
                       self.profit,
                       self.fee,
                       self.total_profit,
                       self.position_change,
                       self.message);
    }

    pub fn __repr__(&self) -> String {
        return self.__str__();
    }
}

/// on memory log archive for OrderResult
pub type LogBuffer = Vec<OrderResult>;

/// make log buffer for log_order_result
pub fn make_log_buffer() -> LogBuffer {
    vec![]
}

pub fn log_order_result(log_buffer: &mut LogBuffer, order_result: OrderResult) {
    log_buffer.push(order_result);
}

pub fn print_order_results(log_buffer: &LogBuffer) {
    for log in log_buffer {
        println!("{:?}", log);
    }
}

///////////////////////////////////////////////////////////////////////////////
//     Unit TEST
///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod order_side_test {
    use std::str::FromStr;

    use super::*;
    #[test]
    fn test_from_str() {
        assert_eq!(OrderSide::from_str("B").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("Buy").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("BUY").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("S").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("Sell").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("SELL").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str_default("BS"), OrderSide::Unknown);
    }

    #[test]
    fn test_to_string() {
        assert_eq!(OrderSide::Buy.to_string(), "Buy");
        assert_eq!(OrderSide::Sell.to_string(), "Sell");
    }

    #[test]
    fn test_from_buy_side() {
        assert_eq!(OrderSide::from_buy_side(true), OrderSide::Buy);
        assert_eq!(OrderSide::from_buy_side(false), OrderSide::Sell);
    }

    #[test]
    fn test_is_buy_side() {
        assert_eq!(OrderSide::Buy.is_buy_side(), true);
        assert_eq!(OrderSide::Sell.is_buy_side(), false);
    }
}


*/

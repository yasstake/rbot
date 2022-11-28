use super::time::MicroSec;
use pyo3::pyclass;
use pyo3::pymethods;

use strum_macros::Display;

#[pyclass]
#[derive(Debug, Clone, Copy, PartialEq, Display)]
pub enum OrderSide {
    Buy,
    Sell,
    Unknown,
}

impl OrderSide {
    pub fn from_str(order_type: &str) -> Self {
        match order_type.to_uppercase().as_str() {
            "B" | "BUY" => OrderSide::Buy,
            "S" | "SELL" | "SEL" => OrderSide::Sell,
            _ => OrderSide::Unknown,
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

// Represent one Trade execution.
#[pyclass]
#[derive(Debug, Clone)]
pub struct Trade {
    pub time: MicroSec,
    pub order_side: OrderSide,
    pub price: f64,
    pub size: f64,
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
            "{{timestamp:{:?}, order_side:{:?}, price:{:?}, size:{:?}, id:{:?}}}",
            self.time, self.order_side, self.price, self.size, self.id
        )
    }

    pub fn __repr__(&self) -> String {
        return self.__str__();
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct Order {
    _order_index: i64,
    pub create_time: MicroSec, // in ns
    pub order_id: String,      // YYYY-MM-DD-SEQ
    pub order_side: OrderSide,
    pub post_only: bool,
    pub valid_until: MicroSec, // in ns
    pub price: f64,            // in
    pub size: f64,             // in foreign
    pub message: String,
    pub remain_size: f64, // ログから想定した未約定数。０になったら全部約定。
}

#[pymethods]
impl Order {
    #[new]
    pub fn new(
        create_time: MicroSec, // in ns
        order_id: String,      // YYYY-MM-DD-SEQ
        order_side: OrderSide,
        post_only: bool,
        valid_until: MicroSec, // in ns
        price: f64,
        size: f64, // in foreign currency.
        message: String,
    ) -> Self {
        return Order {
            _order_index: 0,
            create_time,
            order_id,
            order_side,
            post_only,
            valid_until,
            price,
            size,
            message,
            remain_size: size,
        };
    }

    pub fn __str__(&self) -> String {
        return format!("{{order_index:{}, create_time:{}, order_id:{}, order_side:{:?}, post_only:{}, valid_until:{}, price:{}, size:{}, message:{}, remain_size:{}}}",
        self._order_index,
        self.create_time,
        self.order_id,
        self.order_side,
        self.post_only,
        self.valid_until,
        self.price,
        self.size,
        self.message,
        self.remain_size,
    );
    }

    pub fn __repr__(&self) -> String {
        return self.__str__();
    }
}

#[pyclass]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OrderStatus {
    NoAction,
    Wait,          // 処理中
    InOrder,       // オーダー中
    OrderComplete, // temporary status.
    OpenPosition,  // ポジションオープン
    ClosePosition, // ポジションクローズ（このときだけ、損益計算する）
    OverPosition,  // ポジション以上の反対売買。別途分割して処理する。
    ExpireOrder,   // 期限切れ
    Liquidation,   // 精算
    PostOnlyError, // 指値不成立。
    NoMoney,       //　証拠金不足（オーダできず）
    Cancel,        //  ユーザによるオーダーキャンセル
    Error,         // その他エラー（基本的には発生させない）
}

#[test]
fn test_print_order_status() {
    let s = OrderStatus::Wait;

    println!("{:#?}", s);

    println!("{}", s.to_string());
}

impl OrderStatus {
    pub fn to_string(&self) -> String {
        return format!("{:#?}", self);
    }
}

// 約定結果
#[pyclass]
#[derive(Debug, Clone)]
pub struct OrderResult {
    pub update_time: MicroSec,
    pub order_id: String,
    pub order_sub_id: i32, // 分割された場合に利用
    pub order_side: OrderSide,
    pub post_only: bool,
    pub create_time: MicroSec,
    pub status: OrderStatus,
    pub open_price: f64,
    pub open_home_size: f64,
    pub open_foreign_size: f64,
    pub close_price: f64,
    pub close_home_size: f64,
    pub close_foreign_size: f64,
    pub order_price: f64,
    pub order_home_size: f64,
    pub order_foreign_size: f64,
    pub profit: f64,
    pub fee: f64,
    pub total_profit: f64,
    pub message: String,
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
            order_foreign_size: OrderResult::calc_foreign_size(order.price, order.size),
            profit: 0.0,
            fee: 0.0,
            total_profit: 0.0,
            message: order.message.clone(),
        };

        return result;
    }

    fn calc_foreign_size(price: f64, home_size: f64) -> f64 {
        if price == 0.0 {
            print!("Div 0 in calc_foreign size {}/{}", price, home_size);            
            panic!("Div 0");
        }
        return home_size / price;
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
        child.order_foreign_size =
            OrderResult::calc_foreign_size(child.order_price, child.order_home_size);

        self.order_home_size = size;
        if self.order_price == 0.0 {
            log::error!("Div 0 by parent {:?}", self);
        }
        self.order_foreign_size =
            OrderResult::calc_foreign_size(self.order_price, self.order_home_size);

        return Ok(child);
    }
}

#[pymethods]
impl OrderResult {
    pub fn __str__(&self) -> String {
        return format!("update_time: {:?}, order_id: {:?}, order_sub_id: {:?}, order_side: {:?}, post_only: {:?}, create_time: {:?}, status: {:?}, open_price: {:?}, open_home_size: {:?}, open_foreign_size: {:?}, close_price: {:?}, close_home_size: {:?}, close_foreign_size: {:?}, order_price: {:?}, order_home_size: {:?}, order_foreign_size: {:?}, profit: {:?}, fee: {:?}, total_profit: {:?}, message: {:?}",
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
    use super::*;
    #[test]
    fn test_from_str() {
        assert_eq!(OrderSide::from_str("B"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("Buy"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("BUY"), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("S"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("Sell"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("SELL"), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("BS"), OrderSide::Unknown);
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

#[cfg(test)]
mod order_test {
    use super::*;
    #[test]
    fn test_new() {
        let _order_index = 0;
        let create_time = 1;
        let order_id = "id".to_string();
        let order_side = OrderSide::Buy;
        let post_only = false;
        let valid_until = 10;
        let price = 10.0;
        let size = 12.0;
        let message = "message".to_string();

        let order = Order::new(
            create_time,
            order_id.clone(),
            order_side,
            post_only,
            valid_until,
            price,
            size,
            message.clone(),
        );

        println!("{:?}", order);
        assert_eq!(0, order._order_index);
        assert_eq!(create_time, order.create_time);
        assert_eq!(order_id, order.order_id);
        assert_eq!(order_side, order.order_side);
        assert_eq!(post_only, order.post_only);
        assert_eq!(valid_until, order.valid_until);
        assert_eq!(price, order.price);
        assert_eq!(message, order.message);
        assert_eq!(size, order.remain_size);
    }
}

#[cfg(test)]
#[allow(unused_results)]
#[allow(unused_variables)]
mod order_result_test {
    use super::*;

    #[test]
    fn test_from_order() {
        let _order_index = 0;
        let create_time = 1;
        let order_id = "id".to_string();
        let order_side = OrderSide::Buy;
        let post_only = false;
        let valid_until = 10;
        let price = 10.0;
        let size = 12.0;
        let message = "message".to_string();

        let order = Order::new(
            create_time,
            order_id.clone(),
            order_side,
            post_only,
            valid_until,
            price,
            size,
            message.clone(),
        );

        let current_time: MicroSec = 100;
        let order_result = OrderResult::from_order(current_time, &order, OrderStatus::OpenPosition);

        assert_eq!(order_result.update_time, current_time);
    }

    #[test]
    fn test_split_order_small() {
        let order = Order::new(
            1,
            "close".to_string(),
            OrderSide::Buy,
            true,
            100,
            10.0,
            100.0,
            "msg".to_string(),
        );

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);
        assert_eq!(closed_order.order_home_size, 100.0);
        assert_eq!(closed_order.order_foreign_size, 100.0 / 10.0);

        let result = &closed_order.split_child(10.0);

        match result {
            Ok(child) => {
                assert_eq!(closed_order.order_price, 10.0);
                assert_eq!(closed_order.order_home_size, 10.0);
                assert_eq!(closed_order.order_foreign_size, 10.0 / 10.0);
                assert_eq!(child.order_price, 10.0);
                assert_eq!(child.order_home_size, 90.0);
                assert_eq!(child.order_foreign_size, 90.0 / 10.0);

                println!("{:?}", child);
            }
            Err(_) => {
                assert!(false);
            }
        }
    }

    #[test]
    /// オーダは０と１００には分割できない（きっちりのサイズの場合はエラー）
    fn test_split_order_eq() {
        let order = Order::new(
            1,
            "close".to_string(),
            OrderSide::Buy,
            true,
            100,
            10.0,
            100.0,
            "msg".to_string(),
        );

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);

        let result = &closed_order.split_child(100.0);

        match result {
            Ok(child) => {
                /*
                println!("{:?}", closed_order);
                assert_eq!(closed_order.order_price, 10.0);
                assert_eq!(closed_order.order_home_size, 100.0);
                assert_eq!(closed_order.order_foreign_size, 100.0 / 10.0);
                assert_eq!(child.order_price, 10.0);
                assert_eq!(child.order_home_size, 0.0);
                assert_eq!(child.order_foreign_size, 0.0);

                println!("{:?}", child);
                */
                assert!(false);
            }
            Err(_) => {
                assert!(true);
            }
        }
    }

    #[test]
    fn test_split_order_big() {
        let order = Order::new(
            1,
            "close".to_string(),
            OrderSide::Buy,
            true,
            100,
            10.0,
            100.0,
            "msg".to_string(),
        );

        let mut closed_order = OrderResult::from_order(2, &order, OrderStatus::OrderComplete);

        let result = &closed_order.split_child(100.1);

        match result {
            Ok(_) => {
                assert!(false);
            }
            Err(_) => {
                assert!(true);
            }
        }
    }
}

#[derive(Debug)]
pub struct TimeChunk {
    pub start: MicroSec,
    pub end: MicroSec,
}

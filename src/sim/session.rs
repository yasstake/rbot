// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::order::LogBuffer;

use crate::common::order::Order;
use crate::common::order::OrderResult;
use crate::common::order::OrderSide;
use crate::common::order::OrderStatus;

use crate::common::order::log_order_result;
use crate::common::order::Trade;
use crate::common::time::MicroSec;
use crate::sim::market::OrderQueue;

use crate::sim::market::Positions;

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::pyclass;
use pyo3::prelude::pymethods;

use crate::SEC;
use pyo3::*;

///
/// オーダー処理の基本
///     make_order(buy_or_sell, price, size)
///     ＜初期化状態の確認＞
///         last_sell_price, last_buy_priceともに０ではない。
///         current_timeが０ではない。
///
///         NG->エラー
///
///     ＜残高の確認＞
///         available_balance よりsizeが小さい（余裕がある）
///             oK -> available_balanceからorder_marginへへSize分を移動させてオーダー処理実行
///             not-> エラー
///
///     ＜価格の確認＞
///         価格が0: ->  最終約定価格にセット
///         価格が板の反対側：→ Takerとして、オーダーリストへ登録
///         価格が板の内側：　→　makerとしてオーダーリストへ登録
///
///     ＜戻り値＞
///     オーダーIDをつくり、返却
///

#[pyclass(name = "_DummySession")]
#[derive(Clone, Debug)]
pub struct DummySession {
    _order_index: i64,
    #[pyo3(get)]
    pub current_timestamp: i64,
    #[pyo3(get)]
    pub sell_board_edge_price: f64, // best ask price　買う時の価格
    #[pyo3(get)]
    pub buy_board_edge_price: f64, // best bit price 　売る時の価格
    #[pyo3(get)]
    pub exchange_name: String,
    #[pyo3(get)]
    pub market_name: String,
    #[pyo3(get)]
    pub server_delay: MicroSec,
    #[pyo3(get)]
    pub maker_fee: f64,
    pub long_orders: OrderQueue,
    pub short_orders: OrderQueue,
    pub positions: Positions,
    pub wallet_balance: f64, // 入金額
    pub size_in_price_currency: bool,
}

/// implement for Python export
#[pymethods]
impl DummySession {
    #[new]
    pub fn new(exchange_name: &str, market_name: &str, size_in_price_currency: bool) -> Self {
        return DummySession {
            _order_index: 0,
            current_timestamp: 0,
            sell_board_edge_price: 0.0,
            buy_board_edge_price: 0.0,
            exchange_name: exchange_name.to_string().to_ascii_uppercase(),
            market_name: market_name.to_string().to_ascii_uppercase(),
            server_delay: 100_000,  // 0.1 sec
            maker_fee: 0.01 * 0.01, // 0.01%
            long_orders: OrderQueue::new(true),
            short_orders: OrderQueue::new(false),
            positions: Positions::new(),
            wallet_balance: 0.0,
            size_in_price_currency,
            //        indicators: HashMap::new()
        };
    }

    #[getter]
    pub fn get_center_price(&self) -> f64 {
        if self.buy_board_edge_price == 0.0 || self.sell_board_edge_price == 0.0 {
            return 0.0;
        }

        return (self.buy_board_edge_price + self.sell_board_edge_price) / 2.0;
    }

    /*
    #[getter]
    // TODO: 計算する。
    pub fn get_available_balance(&self) -> f64 {
        assert!(false, "not implemented");
        return 0.0;
    }
    */

    /// 未約定でキューに入っているlong orderのサイズ（合計）
    #[getter]
    pub fn get_long_order_size(&self) -> f64 {
        return self.long_orders.get_size();
    }

    /// 未約定でキューに入っているlong orderの個数
    #[getter]
    pub fn get_long_order_len(&self) -> i64 {
        return self.long_orders.len();
    }

    ///　未約定のlong order一覧
    #[getter]
    pub fn get_long_orders(&self) -> Vec<Order> {
        return self.long_orders.get_q();
    }

    /// 未約定でキューに入っているshort orderのサイズ（合計）
    #[getter]
    pub fn get_short_order_size(&self) -> f64 {
        return self.short_orders.get_size();
    }

    /// 未約定でキューに入っているshort orderの個数（合計）
    #[getter]
    pub fn get_short_order_len(&self) -> i64 {
        return self.short_orders.len();
    }

    ///　未約定のshort order一覧
    #[getter]
    pub fn get_short_orders(&self) -> Vec<Order> {
        return self.short_orders.get_q();
    }

    /// longポジションのサイズ（合計）
    #[getter]
    pub fn get_long_position_size(&self) -> f64 {
        return self.positions.get_long_position_size();
    }

    #[getter]
    pub fn get_long_position_price(&self) -> f64 {
        return self.positions.get_long_position_price();
    }

    /// shortポジションのサイズ（合計）
    #[getter]
    pub fn get_short_position_size(&self) -> f64 {
        return self.positions.get_short_position_size();
    }

    #[getter]
    pub fn get_short_position_price(&self) -> f64 {
        return self.positions.get_short_position_price();
    }

    /// オーダー作りオーダーリストへ追加する。
    /// 最初にオーダー可能かどうか確認する（余力の有無）
    pub fn place_order(
        &mut self,
        side: &str,
        price: f64,
        size: f64,
        duration_sec: i64,
        message: String,
    ) -> PyResult<OrderStatus> {
        match self._place_order(
            OrderSide::from_str_default(side),
            price,
            size,
            duration_sec,
            message,
        ) {
            Ok(result) => Ok(result),
            Err(e) => Err(PyTypeError::new_err(e.to_string())),
        }
    }

    /// デバック／マニュアル内説明用。約定ログを入力して動きをPythonから確認する。
    pub fn trade(
        &mut self,
        time: MicroSec,
        order_side: String,
        price: f64,
        size: f64,
        id: String,
    ) -> Vec<OrderResult> {
        let trade: Trade = Trade{
            time,
            order_side: OrderSide::from_str_default(order_side.as_str()),
            price,
            size,
            id,
        };

        let mut tick_result: Vec<OrderResult> = vec![];

        self.process_trade(&trade, &mut tick_result);

        return tick_result;
    }
}

/// Implement for Rust interface
impl DummySession {
    /// マージンの計算式
    /// price x sizeのオーダを発行できるか確認する。
    ///   if unrealised_pnl > 0:
    ///      available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin)
    ///      if unrealised_pnl < 0:
    ///          available_balance = wallet_balance - (position_margin + occ_closing_fee + occ_funding_fee + order_margin) + unrealised_pnl

    ///　ログイベントを処理してセッション情報を更新する。
    ///  0. AgentへTick更新イベントを発生させる。
    ///  1. 時刻のUpdate
    ///  ２。マーク価格の更新
    /// 　2. オーダ中のオーダーを更新する。
    /// 　　　　　期限切れオーダーを削除する。
    /// 　　　　　現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ完了（一部約定はしない想定）
    ///
    ///   3. 処理結果を履歴へ投入する。
    /// データがそろうまではFalseをかえす。ウォーミングアップ完了後Trueへ更新する。
    ///
    //ログに実行結果を追加
    ///
    fn update_trade_time(&mut self, trade: &Trade) {
        self.current_timestamp = trade.time;
    }

    fn update_edge_price(&mut self, trade: &Trade) {
        //  ２。マーク価格の更新。ログとはエージェント側からみるとエッジが逆になる。
        match trade.order_side {
            OrderSide::Buy => {
                self.sell_board_edge_price = trade.price;
            }
            OrderSide::Sell => {
                self.buy_board_edge_price = trade.price;
            }
            _ => {}
        }

        // 逆転したら補正。　(ほとんど呼ばれない想定)
        // 数値が初期化されていない場合２つの値は0になっているが、マイナスにはならないのでこれでOK.
        if self.sell_board_edge_price < self.buy_board_edge_price {
            self.sell_board_edge_price = self.buy_board_edge_price;
        }
    }

    /// オーダの期限切れ処理を行う。
    /// エラーは返すが、エラーが通常のため処理する必要はない。
    /// 逆にOKの場合のみ、上位でログ処理する。
    pub fn update_expire_order(
        &mut self,
        current_time_ms: i64,
    ) -> Result<OrderResult, OrderStatus> {
        // ロングの処理
        match self.long_orders.expire(current_time_ms) {
            Ok(result) => {
                return Ok(result);
            }
            _ => {
                // do nothing
            }
        }
        // ショートの処理
        match self.short_orders.expire(current_time_ms) {
            Ok(result) => {
                return Ok(result);
            }
            _ => {
                // do nothing
            }
        }
        return Err(OrderStatus::NoAction);
    }

    //　売りのログのときは、買いオーダーを処理
    // 　買いのログの時は、売りオーダーを処理。
    fn update_order_queue(&mut self, trade: &Trade) -> Result<OrderResult, OrderStatus> {
        return match trade.order_side {
            OrderSide::Buy => self.short_orders.consume(trade, self.server_delay),
            OrderSide::Sell => self.long_orders.consume(trade, self.server_delay),
            _ => Err(OrderStatus::Error),
        };
    }

    fn update_position(
        &mut self,
        tick_result: &mut LogBuffer,
        order_result: &mut OrderResult,
    ) -> Result<(), OrderStatus> {
        //ポジションに追加しする。
        //　結果がOpen,Closeポジションが行われるのでログに実行結果を追加

        match self.positions.update_small_position(order_result) {
            Ok(()) => {
                self.log_order_result(tick_result, order_result.clone());
                Ok(())
            }
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.positions.split_order(order_result) {
                        Ok(mut child_order) => {
                            let _r = self.positions.update_small_position(order_result);
                            self.log_order_result(tick_result, order_result.clone());

                            let _r = self.positions.update_small_position(&mut child_order);
                            self.log_order_result(tick_result, child_order);

                            Ok(())
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Err(e)
                }
            }
        }
    }

    fn generate_id(&mut self) -> String {
        self._order_index += 1;
        let index = self._order_index;

        let upper = index / 10000;
        let lower: i64 = index % 10000;

        let id = format! {"{:04}-{:04}", upper, lower};

        return id.to_string();
    }

    /// order_resultのログを蓄積する（オンメモリ）
    /// ログオブジェクトは配列にいれるため移動してしまう。
    /// 必要に応じて呼び出し側でCloneする。
    fn log_order_result(&mut self, tick_log: &mut LogBuffer, mut order_result: OrderResult) {
        order_result.update_time = self.current_timestamp;

        self.calc_profit(&mut order_result);
        log_order_result(tick_log, order_result);
    }

    // TODO: fix in size_in_price_currency
    fn calc_profit(&self, order: &mut OrderResult) {
        if order.status == OrderStatus::OpenPosition {
            order.fee = order.open_foreign_size * self.maker_fee;
        } else if order.status == OrderStatus::ClosePosition {
            order.fee = order.close_foreign_size * self.maker_fee;
        }
        order.total_profit = order.profit - order.fee;
    }

    /* TODO: マージンの計算とFundingRate計算はあとまわし */
    pub fn process_trade(&mut self, trade: &Trade, tick_result: &mut Vec<OrderResult>) {

        self.update_trade_time(trade);
        self.update_edge_price(trade);
        // 初期化未のためリターン。次のTickで処理。
        if self.buy_board_edge_price == 0.0 || self.buy_board_edge_price == 0.0 {
            return;
        }

        // 　2. オーダ中のオーダーを更新する。
        // 　　　　　期限切れオーダーを削除する。
        //          毎秒１回実施する（イベントを間引く）
        //      処理継続
        match self.update_expire_order(self.current_timestamp) {
            Ok(result) => {
                self.log_order_result(tick_result, result.clone());
            }
            _ => {
                // Do nothing
            }
        }

        //現在のオーダーから執行可能な量を _partial_workから引き算し０になったらオーダ成立（一部約定はしない想定）
        match self.update_order_queue(trade) {
            Ok(mut order_result) => {
                //ポジションに追加する。
                let _r = self.update_position(tick_result, &mut order_result);
            }
            Err(_e) => {
                // no
            }
        }
    }

    /// make order with OrderSide (instead of string like, "BUY" and "SELL")
    fn _place_order(
        &mut self,
        side: OrderSide,
        price: f64,
        size: f64,
        duration_sec: i64,
        message: String,
    ) -> Result<OrderStatus, String> {
        // TODO: 発注可能かチェックする
        /*
        if 証拠金不足
            return Err(OrderStatus::NoMoney);
        */

        let timestamp = self.current_timestamp;

        if size == 0.0 {
            return Err("Order size cannot be 0".to_string());
        }

        if price == 0.0 {
            return Err("Order price cannot be 0".to_string());
        }

        let order_id = self.generate_id();
        let order = Order::new(
            timestamp,
            order_id,
            side,
            true, // TODO: post only 以外のオーダを検討する。
            self.current_timestamp + SEC(duration_sec),
            price,
            size,
            message,
            self.size_in_price_currency,
        );

        // TODO: enqueue の段階でログに出力する。
        match side {
            OrderSide::Buy => {
                // TODO: Takerになるかどうか確認
                self.long_orders.queue_order(&order);

                Ok(OrderStatus::InOrder)
            }
            OrderSide::Sell => {
                self.short_orders.queue_order(&order);

                Ok(OrderStatus::InOrder)
            }
            _ => {
                let message = format!("Unknown order type {:?} / use B or S", side);
                log::warn!("{}", message);

                Err(message)
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////
// TEST Suite
///////////////////////////////////////////////////////////////////////

// TODO: EXPIRE時にFreeを計算してしまう（キャンセルオーダー扱い）
// TODO: EXPIRE時にポジションか、オーダーキューに残っている。
// TODO:　キャンセルオーダーの実装。

#[cfg(test)]
fn generate_trades_vec1(start_time: i64) -> Vec<Trade> {
    let mut trades: Vec<Trade> = vec![];

    for i in 1..100 {
        if i % 2 == 0 {
            trades.push(Trade {
                time: SEC(start_time + i),
                order_side: OrderSide::Buy,
                price: (i as f64),
                size: (i * 2) as f64,
                id: i.to_string(),
            });
        } else {
            trades.push(Trade {
                time: SEC(start_time + i),
                order_side: OrderSide::Sell,
                price: (i as f64),
                size: (i * 2) as f64,
                id: i.to_string(),
            });
        }
    }

    trades
}

#[allow(unused_results)]
#[cfg(test)]
mod test_session {
    use crate::common::order::make_log_buffer;
    use crate::common::order::print_order_results;

    use super::*;
    #[test]
    fn test_new() {
        let _session = DummySession::new("FTX", "BTC-PERP", false);
        assert_eq!(_session.exchange_name, "FTX");
        assert_eq!(_session.market_name, "BTC-PERP");
    }

    #[test]
    fn test_session_generate_id() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);

        // IDの生成テスト
        // self.generate_id
        let id1 = session.generate_id();
        let id2 = session.generate_id();
        println!("{}/{}", id1, id2);
        assert_ne!(id1, id2); // check unique
    }

    #[test]
    /// CenterPriceは刻みより小さい値を許容する。
    fn test_session_calc_center_price() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        // test center price
        session.buy_board_edge_price = 100.0;
        session.sell_board_edge_price = 100.5;
        assert_eq!(session.get_center_price(), 100.25);

        // test center price
        session.buy_board_edge_price = 200.0;
        session.sell_board_edge_price = 200.0;
        assert_eq!(session.get_center_price(), 200.0);
    }

    #[test]
    fn test_100_orders_open_close() {
        let mut session = DummySession::new("FTX", "BTC-PERP", true);
        let mut result_log = make_log_buffer();

        let _r = session._place_order(OrderSide::Buy, 100.0, 100.0, 100, "".to_string());
        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Sell, 60.0, 100.0, 100, "".to_string());
        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!("{:?}", result_log);
    }

    #[test]
    fn test_100_orders_open_expire() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        let mut result_log = make_log_buffer();

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 0.9, 100.0, 10, "".to_string());
        let _r = session._place_order(OrderSide::Sell, 100.1, 100.0, 10, "".to_string());

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!("{:?}", result_log);
    }

    #[test]
    fn test_100_orders_open_small_order() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        let mut result_log = make_log_buffer();

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 50.0, 0.1, 100, "".to_string());

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }
        let _r = session._place_order(OrderSide::Sell, 60.0, 0.1, 100, "".to_string());
        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!("{:?}", result_log);
    }

    #[test]
    fn test_100_orders_open_big_order() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        let mut result_log = make_log_buffer();

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 50.0, 500.0, 300, "".to_string());

        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!(
            "price={:?} size={:?}",
            session.get_long_position_size(),
            session.get_long_position_price()
        );

        let _r = session._place_order(OrderSide::Sell, 55.0, 500.0, 300, "".to_string());
        for t in generate_trades_vec1(200) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(300) {
            session.process_trade(&t, &mut result_log);
        }

        print_order_results(&result_log);
    }

    #[test]
    fn test_100_orders_open_big_close_small_position() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        let mut result_log = make_log_buffer();

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 50.0, 500.0, 300, "".to_string());

        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!(
            "price={:?} size={:?}",
            session.get_long_position_size(),
            session.get_long_position_price()
        );

        let _r = session._place_order(OrderSide::Sell, 55.0, 400.0, 300, "".to_string());
        for t in generate_trades_vec1(200) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(300) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Sell, 55.0, 100.0, 300, "".to_string());
        for t in generate_trades_vec1(400) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(500) {
            session.process_trade(&t, &mut result_log);
        }
        print_order_results(&result_log);
    }

    #[test]
    fn test_100_orders_open_small_close_big_position() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        let mut result_log = make_log_buffer();

        for t in generate_trades_vec1(0) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 50.0, 500.0, 300, "".to_string());

        for t in generate_trades_vec1(100) {
            session.process_trade(&t, &mut result_log);
        }

        println!(
            "price={:?} size={:?}",
            session.get_long_position_size(),
            session.get_long_position_price()
        );

        let _r = session._place_order(OrderSide::Sell, 55.0, 600.0, 300, "".to_string());
        for t in generate_trades_vec1(200) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(300) {
            session.process_trade(&t, &mut result_log);
        }

        let _r = session._place_order(OrderSide::Buy, 56.0, 100.0, 300, "".to_string());
        for t in generate_trades_vec1(400) {
            session.process_trade(&t, &mut result_log);
        }
        for t in generate_trades_vec1(500) {
            session.process_trade(&t, &mut result_log);
        }

        print_order_results(&result_log);
    }

    #[test]
    fn test_exec_event_execute_order0() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);

        let mut result_log = make_log_buffer();

        let _r = session._place_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);
        assert_eq!(session.get_long_order_size(), 10.0);
        assert_eq!(session.get_short_order_size(), 0.0);

        let trade = Trade {
            time: 1,
            order_side: OrderSide::Sell,
            price: 50.0,
            size: 5.0,
            id: "".to_string(),
        };

        session.process_trade(&trade, &mut result_log);
        println!("{:?}", session.long_orders);

        let r = session.update_order_queue(&Trade {
            time: 3,
            order_side: OrderSide::Sell,
            price: 49.0,
            size: 5.0,
            id: "".to_string(),
        });
        println!("{:?}", session.long_orders);
        println!("{:?}", r);
    }

    #[test]
    fn test_exec_event_execute_order() {
        let mut session = DummySession::new("FTX", "BTC-PERP", false);
        assert_eq!(session.current_timestamp, 0); // 最初は０

        let mut tick_result: Vec<OrderResult> = vec![];

        // Warm Up
        session.process_trade(
            &Trade {
                time: 1,
                order_side: OrderSide::Buy,
                price: 150.0,
                size: 150.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );

        session.process_trade(
            &Trade {
                time: 2,
                order_side: OrderSide::Buy,
                price: 151.0,
                size: 151.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );

        println!("--make long order--");

        // TODO: 書庫金不足を確認する必要がある.
        let _r = session._place_order(OrderSide::Buy, 50.0, 10.0, 100, "".to_string());
        println!("{:?}", session.long_orders);

        // 売りよりも高い金額のオファーにはなにもしない。
        session.process_trade(
            &Trade {
                time: 3,
                order_side: OrderSide::Sell,
                price: 50.0,
                size: 150.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定
        session.process_trade(
            &Trade {
                time: 4,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 5.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 売りよりもやすい金額があると約定。Sizeが小さいので一部約定.２回目で約定。ポジションに登録
        session.process_trade(
            &Trade {
                time: 5,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 5.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(
            &Trade {
                time: 5,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 5.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        println!("--make short order--");

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.

        let _r = session._place_order(OrderSide::Sell, 40.0, 12.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        let _r = session._place_order(OrderSide::Sell, 41.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        session.process_trade(
            &Trade {
                time: 5,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 11.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(
            &Trade {
                time: 5,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 20.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        session.process_trade(
            &Trade {
                time: 5,
                order_side: OrderSide::Sell,
                price: 49.5,
                size: 100.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);

        // 決裁オーダーTODO: 書庫金不足を確認する必要がある.
        let _r = session._place_order(OrderSide::Buy, 80.0, 10.0, 100, "".to_string());
        // println!("{:?}", session.order_history);
        println!("{:?}", session.short_orders);
        println!("{:?}", session.positions);

        // 約定
        session.process_trade(
            &Trade {
                time: 8,
                order_side: OrderSide::Sell,
                price: 79.5,
                size: 200.0,
                id: "".to_string(),
            },
            &mut tick_result,
        );
        println!("{:?}", session.long_orders);
        println!("{:?}", session.positions);
        println!("{:?}", tick_result);
    }
}

// Copyright(c) 2022. yasstake. All rights reserved.

use crate::common::{
    order::Order, order::OrderResult, order::OrderSide, order::OrderStatus, time::MicroSec,
};

use std::cmp::Ordering;
use std::iter::Iterator;
use crate::common::order::Trade;

/// 未実現オーダーリストを整理する。
/// ・　オーダーの追加
/// ・　オーダの削除（あとで実装）
/// ・　オーダー中のマージン計算
/// ・　オーダーのExpire
/// ・　オーダーの約定

#[derive(Debug, Clone)]
pub struct OrderQueue {
    buy_queue: bool,
    q: Vec<Order>,
}

impl OrderQueue {
    pub fn new(buy_order: bool) -> Self {
        return OrderQueue {
            buy_queue: buy_order,
            q: vec![],
        };
    }

    pub fn get_q(&self) -> Vec<Order> {
        return self.q.clone();
    }

    pub fn get_size(&self) -> f64 {
        let sum: f64 = self.q.iter().fold(0.0, |sum, item| sum + item.size);
        return sum;
    }

    /// オーダーをキューに入れる。
    pub fn queue_order(&mut self, order: &Order) {
        self.q.push(order.clone());
        self.sort();
    }

    /*
    // TODO: キャンセルオーダーの実装
    // TOD: ログ出力の実装（上位で実装？）
    pub fn cancel_order(&mut self, order_id: String) -> bool {
        let index = 0;

        for order in self.q {
            if order.order_id == order_id {
                order
                return true;
            }
            index += 1;
        }
        return false;
    }
    */
    /*
    pub fn margin(&mut self) -> f64 {
        let mut m: f64 = 0.0;

        for order in &self.q {
            m += order.size;
        }

        return m;
    }
    */

    // Sellオーダーを約定しやすい順番に整列させる
    //   *やすい順番
    //   早い順番
    fn sell_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Less;
        } else if a.price > b.price {
            return Ordering::Greater;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    // buyオーダーを約定しやすい順番に整列させる
    //   *高い順番
    //   早い順番
    fn buy_comp(a: &Order, b: &Order) -> Ordering {
        if a.price < b.price {
            return Ordering::Greater;
        } else if a.price > b.price {
            return Ordering::Less;
        } else if a.create_time < b.create_time {
            return Ordering::Less;
        }
        return Ordering::Equal;
    }

    fn sort(&mut self) {
        if self.buy_queue {
            // 高い方・古い方から並べる
            self.q.sort_by(OrderQueue::buy_comp);
        } else {
            // 安い方・古い方から並べる
            self.q.sort_by(OrderQueue::sell_comp);
        }
    }

    /// Queueの中にオーダがはいっているかを確認する。
    pub fn has_q(&self) -> bool {
        return self.q.is_empty() == false;
    }

    /// Queueの数
    pub fn len(&self) -> i64 {
        return self.q.len() as i64;
    }

    ///　全件なめる処理になるので数秒ごとに１回でOKとする。
    /// 先頭の１つしかExpireしないが、何回も呼ばれるのでOKとする（多少の誤差を許容）
    pub fn expire(&mut self, current_time: MicroSec) -> Result<OrderResult, OrderStatus> {
        let l = self.q.len();

        if l == 0 {
            return Err(OrderStatus::NoAction);
        }

        for i in 0..l {
            if self.q[i].valid_until < current_time {
                let order = self.q.remove(i);

                if order.price == 0.0 {
                    println!("Div 0 in expire / order={:?}", order);                    
                    log::debug!("Div 0 in expire / order={:?}", order);
                }

                let close_order =
                    OrderResult::from_order(current_time, &order, OrderStatus::ExpireOrder);

                return Ok(close_order);
            }
        }

        return Err(OrderStatus::NoAction);
    }

    /// 約定履歴からオーダーを処理する。
    /// 優先度の高いほうから１つづつ処理することとし、先頭のオーダ一つが約定したらリターンする。
    /// うまくいった場合はClosedOrderを返す（ほとんどの場合はErrを返す(前回から変化が小さいのでなにもしていない）
    /// 約定は、一つ下の刻みのログが発生したらカウントする。
    /// 超巨大オーダがきた場合でも複数約定はさせず、次回に回す。
    pub fn consume(
        &mut self,
        trade: &Trade,
        server_delay: MicroSec
    ) -> Result<OrderResult, OrderStatus> {
        if self.has_q() == false {
            return Err(OrderStatus::NoAction);
        }

        if self.execute_remain_size(trade, server_delay) {
            return self.pop_closed_order(trade.time);
        }

        return Err(OrderStatus::NoAction);
    }

    /// キューの中に処理できるオーダーがあれば、size_remainをへらしていく。
    /// size_remainが０になったらオーダ完了の印。
    /// 実際の取り出しは pop_close_orderで実施する。
    fn execute_remain_size(&mut self, trade: &Trade, server_delay: MicroSec) -> bool {
        if self.has_q() == false {
            return false;
        }

        let l = self.q.len();
        let mut size_remain = trade.size;
        let mut complete_order = false;

        // 順番に価格条件をみたしたものから約定したこととし、remain_sizeをへらしていく。
        for i in 0..l {
            if trade.time + server_delay <= self.q[i].create_time {
                continue;
            }

            if ((self.buy_queue == true) && (trade.price  < self.q[i].price))          // Buy Case
                || ((self.buy_queue == false) && (self.q[i].price < trade.price))      // Sell case
            {
                if self.q[i].remain_size <= size_remain {
                    complete_order = true;
                    size_remain -= self.q[i].remain_size;
                    self.q[i].remain_size = 0.0;
                } else {
                    self.q[i].remain_size -= size_remain;
                    // size_remain = 0.0;

                    break;
                }
            } else {
                // ソートされているので全件検索は不要。
                break;
            }
        }

        return complete_order;
    }

    ///　全額処理されたオーダをキューから取り出し ClosedOrderオブジェクトを作る。
    fn pop_closed_order(&mut self, current_time: MicroSec) -> Result<OrderResult, OrderStatus> {
        let l = self.q.len();

        for i in 0..l {
            // 約定完了のオーダーバックログを発見。処理は１度に１回のみ。本来は巨大オーダで複数処理されることがあるけど実装しない。
            if self.q[i].remain_size <= 0.0 {
                let order = &self.q.remove(i);

                if order.size == 0.0 {
                    log::error!("{} / Div 0 order size", order.create_time);
                }

                let close_order =
                    OrderResult::from_order(current_time, &order, OrderStatus::OrderComplete);

                return Ok(close_order);
            }
        }

        return Err(OrderStatus::NoAction);
    }

    #[allow(unused)]
    /// ID で指定されたオーダをキャンセルする。
    fn cancel_order(&mut self, current_time: MicroSec, order_id: String) -> Result<OrderResult, OrderStatus> {
        let l = self.q.len();

        for i in 0..l {
            if self.q[i].order_id == order_id {
                let order = &self.q.remove(i);

                let cancel_order =
                    OrderResult::from_order(current_time, &order, OrderStatus::Cancel);

                return Ok(cancel_order);
            }
        }

        return Err(OrderStatus::NoAction);
    }
}

#[derive(Debug, Clone, Copy)]
///　ポジションの１項目
/// 　Positionsでポジションリストを扱う。
pub struct Position {
    price: f64,
    home_size: f64, // ポジションは証拠金通貨単位(home_size)
}

impl Position {
    fn new() -> Self {
        return Position {
            price: 0.0,
            home_size: 0.0,
        };
    }

    /// ポジションをオープンする。
    /// すでに約定は済んでいるはずなので、エラーは出ない。
    /// 新規にポジションの平均取得単価を計算する。
    pub fn open_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {

        if self.home_size == 0.0 {
            // 最初のポジションだった場合
            self.price = order.order_price;
            self.home_size = order.order_home_size;
        } else {
            // 追加ポジションだった場合。既存ポジション＋新規ポジションの平均を計算する。
            let new_size = self.home_size + order.order_home_size;

            // TODO: 計算方法シンプルに変更する（できるはず）
            // ポジションの平均価格の計算（加重平均）
            // 価格　＝　（単価old＊数量old) + (新規new*数量new) / 　(新規合計数量）
            self.price = (self.price * self.home_size + order.order_price * order.order_home_size) / new_size;
            self.home_size = new_size;
        }

        order.status = OrderStatus::OpenPosition;
        order.open_price = order.order_price;
        order.open_home_size = order.order_home_size;
        order.open_foreign_size = order.order_foreign_size;

        log::debug!("Open Position {:?}", order);

        return Ok(());
    }

    /// ポジションを閉じる。
    /// 閉じるポジションがない場合：　          なにもしない
    /// オーダーがポジションを越える場合：      エラーを返す（呼び出し側でオーダーを分割し、ポジションのクローズとオープンを行う）
    /// オーダーよりポジションのほうが大きい場合：オーダ分のポジションを解消する。
    pub fn close_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        if self.home_size == 0.0 {
            // ポジションがない場合なにもしない。
            self.price = 0.0; // （誤差蓄積解消のためポジション０のときはリセット）
            return Err(OrderStatus::NoAction);
        } else if self.home_size < order.order_home_size {
            // ポジション以上にクローズしようとした場合なにもしない（別途、クローズとオープンに分割して処理する）
            return Err(OrderStatus::OverPosition);
        }
        // オーダの全部クローズ（ポジションは残る）
        order.status = OrderStatus::ClosePosition;

        order.open_price = self.price;
        order.open_home_size = self.home_size;
        order.open_foreign_size = 
        OrderResult::calc_foreign_size(order.open_price, order.open_home_size, order.size_in_price_currency);

        // TODO: calc foreing size
        //order.open_foreign_size = 

        // order.open_price = self.price;
        order.close_price = order.order_price;
        order.close_home_size = order.order_home_size;
        order.close_foreign_size = order.order_foreign_size;
        log::debug!("Close Position {:?}", order);

        match order.order_side {
            OrderSide::Buy => {
                // order.profit = (order.open_price - order.close_price)/order.open_price * order.order_foreign_size;
                               // ex) Sell Price  80              Buy Price 100  = -20
                order.profit = order.open_foreign_size - order.close_foreign_size;                
            }
            OrderSide::Sell => {
                // order.profit = (order.close_price - order.open_price)/order.open_price * order.order_foreign_size;
                                // ex) Sell Price 100            Buy Price 80 =  20
                order.profit = order.close_foreign_size - order.open_foreign_size;
            }
            OrderSide::Unknown => {
                log::error!("Unknown side");
            }
        }
        // ポジションの整理
        self.home_size -= order.order_home_size;

        if self.home_size == 0.0 {
            self.price = 0.0;
        }

        return Ok(());
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Positions {
    pub long_position: Position,
    pub short_position: Position,
}

impl Positions {
    pub fn new() -> Self {
        return Positions {
            long_position: Position::new(),
            short_position: Position::new(),
        };
    }

    pub fn get_long_position_price(&self) -> f64 {
        return self.long_position.price;
    }

    pub fn get_long_position_size(&self) -> f64 {
        return self.long_position.home_size;
    }

    pub fn get_short_position_price(&self) -> f64 {
        return self.short_position.price;
    }

    pub fn get_short_position_size(&self) -> f64 {
        return self.short_position.home_size;
    }

    /*
    fn long_volume(&self) -> f64 {
        return self.long_position.calc_volume();
    }

    fn short_volume(&self) -> f64 {
        return self.short_position.calc_volume();
    }
    */

    /*
    /// ポジションからできるマージンを計算する。
    /// 本来は手数料も込みだが、あとまわし　TODO: 手数料計算
    pub fn get_margin(&self, center_price: f64) -> f64 {
        let long_margin = (center_price - self.long_position.price)     // 購入単価 - 現在単価
             * self.long_volume();

        let short_margin = (self.short_position.price - center_price)    // 購入単価 - 現在単価
            * self.short_volume();

        return long_margin + short_margin;
    }
     */

    pub fn update_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        match self.update_small_position(order) {
            Ok(()) => return Ok(()),
            Err(e) => {
                if e == OrderStatus::OverPosition {
                    match self.split_order(order) {
                        Ok(mut child_order) => {
                            // すでにサイズはチェック済みなので戻り値は無視する。
                            let _r = self.update_small_position(order);
                            let _r = self.update_small_position(&mut child_order);

                            return Ok(());
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                } else {
                    return Err(e);
                }
            }
        }
    }

    /// ClosedOrderによりポジションを更新する。
    ///     1) 逆側のLong/Short ポジションがある分は精算(open_priceの書き換え)
    ///     2) Long/Short ポジションがない（０の場合は、新たにポジションを作る。
    ///     3  Long/Shortポジションが不足した場合はエラーOverPositionを戻すので小さく分割してやり直しする。
    pub fn update_small_position(&mut self, order: &mut OrderResult) -> Result<(), OrderStatus> {
        match order.order_side {
            OrderSide::Buy => match self.short_position.close_position(order) {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    if e == OrderStatus::NoAction {
                        return self.long_position.open_position(order);
                    } else {
                        return Err(e);
                    }
                }
            },
            OrderSide::Sell => match self.long_position.close_position(order) {
                Ok(()) => {
                    return Ok(());
                }
                Err(e) => {
                    if e == OrderStatus::NoAction {
                        return self.short_position.open_position(order);
                    } else {
                        return Err(e);
                    }
                }
            },
            _ => {
                return Err(OrderStatus::Error);
            }
        }
    }

    // ポジションクローズできるサイズにオーダーを修正。
    // 残りのオーダを新たなオーダとして返却
    // クローズするためには、BuyのときにはShortの大きさが必要（逆になる）
    pub fn split_order(&self, order: &mut OrderResult) -> Result<OrderResult, OrderStatus> {
        let size: f64;

        match order.order_side {
            OrderSide::Buy => {
                size = self.short_position.home_size;
            }
            OrderSide::Sell => {
                size = self.long_position.home_size;
            }
            _ => {
                // size = 0.0;
                return Err(OrderStatus::Error);
            }
        }

        return order.split_child(size);
    }
}

///-------------------------------------------------------------------------------------
/// TEST SECTION
/// ------------------------------------------------------------------------------------
///

#[cfg(test)]
fn test_build_orders() -> Vec<OrderResult> {
    let sell_order01 = Order::new(
        1,
        "neworder".to_string(),
        OrderSide::Sell,
        true,
        100,
        200.0,
        200.0,
        "".to_string(),
        false,
    );

    let sell_open00 = OrderResult::from_order(2, &sell_order01, OrderStatus::InOrder);

    let mut sell_open01 = sell_open00.clone();
    sell_open01.order_id = "aa".to_string();
    let sell_open02 = sell_open00.clone();
    let sell_open03 = sell_open00.clone();
    let sell_open04 = sell_open00.clone();

    let buy_order = Order::new(
        1,
        "buyorder".to_string(),
        OrderSide::Buy,
        true,
        100,
        50.0,
        100.0,
        "".to_string(),
        false,
    );

    let buy_close00 = OrderResult::from_order(2, &buy_order, OrderStatus::InOrder);
    let buy_close01 = buy_close00.clone();
    let buy_close02 = buy_close00.clone();
    let buy_close03 = buy_close00.clone();
    let buy_close04 = buy_close00.clone();

    return vec![
        sell_open00,
        sell_open01,
        sell_open02,
        sell_open03,
        sell_open04,
        buy_close00,
        buy_close01,
        buy_close02,
        buy_close03,
        buy_close04,
    ];
}






#[cfg(test)]
mod test_order_positon {
    use super::*;
    #[cfg(test)]

    #[test]
    pub fn test_update_position() {
        let mut orders = test_build_orders();

        let mut position = Position::new();
        // ポジションがないときはなにもしないテスト
        let result = position.close_position(&mut orders[0]);
        assert_eq!(result.err(), Some(OrderStatus::NoAction));
        assert_eq!(position.home_size, 0.0);
        assert_eq!(position.price, 0.0);

        // ポジションを作る。
        assert_eq!(orders[1].order_price, 200.0);
        assert_eq!(orders[1].order_home_size, 200.0);
        assert_eq!(orders[1].order_foreign_size, 200.0*200.0);
        let _r = position.open_position(&mut orders[1]); // price 200.0, size 200.0
        println!("{:?}  {:?}", position, _r);
        assert_eq!(position.price, 200.0);
        assert_eq!(position.home_size, 200.0);

        // 購入平均単価へposition が更新されることの確認
        let _r = position.open_position(&mut orders[2]); // price 200.0, size 200.0
        println!("{:?} {:?}", position, orders[2]);
        assert_eq!(position.price, 200.0);
        assert_eq!(position.home_size, 200.0 + 200.0);

        let _r = position.open_position(&mut orders[3]);
        println!("{:?} {:?}", position, orders[3]);
        assert_eq!(position.price, 200.0);
        assert_eq!(position.home_size, 200.0 + 200.0 + 200.0);

        let _r = position.open_position(&mut orders[4]);
        println!("{:?} {:?}", position, orders[4]);
        assert_eq!(position.price, 200.0);
        assert_eq!(position.home_size, 200.0 + 200.0 + 200.0 + 200.0);

        let _r = position.open_position(&mut orders[5]); // price 50.0, size 100.0
        println!("{:?} {:?}", position, orders[5]);
        assert_eq!(orders[5].order_price, 50.0);
        assert_eq!(orders[5].order_home_size, 100.0);
        assert_eq!(
            position.price,
            ((200.0 * 800.0) + (50.0 * 100.0)) / (800.0 + 100.0)
        );
        assert_eq!(position.home_size, 200.0 + 200.0 + 200.0 + 200.0 + 100.0); // 900.0

        // ポジションのクローズのテスト（小さいオーダーのクローズ
        println!("-- CLOSE ---");
        let _r = position.close_position(&mut orders[6]); // price 50.0, size 100.0
        println!("{:?} {:?}", position, orders[6]);
        assert_eq!(position.home_size, 200.0 * 4.0 + 100.0 - 100.0); // 数は減る 800.0
        let last_price = ((200.0 * 800.0) + (50.0 * 100.0)) / (800.0 + 100.0);
        assert_eq!(position.price, last_price); // 単価は同じ
        assert_eq!(orders[6].open_price, last_price);
        assert_eq!(orders[6].close_price, 50.0);


        // 収益の計算はforeinサイズの差で求められる。
        assert_eq!(
            orders[6].profit,
            (orders[6].open_foreign_size - orders[6].close_foreign_size)
        );

        //ポジションクローズのテスト（大きいオーダーのクローズではエラーがかえってくる）
        println!("-- CLOSE BIG ---");
        orders[0].order_home_size = 10000.0;
        println!("{:?} {:?}", position, orders[0]);
        let r = position.close_position(&mut orders[0]);

        println!("{:?} {:?}", position, orders[0]);
        assert_eq!(r.err(), Some(OrderStatus::OverPosition));

        //オーダーの分割テスト（大きいオーダを分割して処理する。１つがPositionを全クリアする大きさにして、残りを新規ポジションの大きさにする）
        let small_order = &mut orders[0];
        println!("{:?}", small_order);

        let remain_order = &mut small_order.split_child(position.home_size).unwrap();
        println!("{:?}", small_order);
        println!("{:?}", remain_order);
        println!("{:?}", position);

        let _r = position.close_position(small_order);
        println!("{:?}", small_order);
        println!("{:?}", position);
        let _r = position.open_position(remain_order);
        println!("{:?}", remain_order);
        println!("{:?}", position);
    }
}

#[cfg(test)]
#[allow(unused_variables)]
#[allow(unused_results)]
mod test_positions {
    use super::*;

    #[test]
    fn test_update_position() {
        // 新規だった場合はOpenOrderを返す。
        // クローズだった場合はCLoseOrderを返す。
        // クローズしきれなかった場合は、OpenとCloseを返す。
        // LongとShortをオーダーの中身を見て判断する。

        let mut data = test_build_orders();

        let mut session = Positions::new();

        let _r = session.update_position(&mut data[0]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[1]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[2]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[3]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[4]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[5]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[6]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
        let _r = session.update_position(&mut data[7]);
        println!("{:?}", session);
    }
}

/*
[OrderResult { timestamp: 5, order_id: "0000-000000000000-001", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 2, status: OpenPosition, open_price: 50.0, close_price: 0.0, size: 10.0, volume: 0.2, profit: 0.0, fee: 0.005999999999999999, total_profit: -0.005999999999999999 },
OrderResult { timestamp: 5, order_id: "0000-000000000000-002", order_sub_id: 0, order_type: Sell, post_only: true, create_time: 5, status: ClosePosition, open_price: 50.0, close_price: 40.0, size: 10.0, volume: 0.25, profit: 2.5, fee: 0.005999999999999999, total_profit: 2.494 },
OrderResult { timestamp: 5, order_id: "0000-000000000000-002", order_sub_id: 1, order_type: Sell, post_only: true, create_time: 5, status: OpenPosition, open_price: 40.0, close_price: 0.0, size: 2.0, volume: 0.05, profit: 0.0, fee: 0.0012, total_profit: -0.0012 },
OrderResult { timestamp: 8, order_id: "0000-000000000000-004", order_sub_id: 0, order_type: Buy, post_only: true, create_time: 5, status: ClosePosition, open_price: 40.0, close_price: 80.0, size: 2.0, volume: 0.025, profit: 1.0, fee: 0.0012, total_profit: 0.9988 },
OrderResult { timestamp: 8, order_id: "0000-000000000000-004", order_sub_id: 1, order_type: Buy, post_only: true, create_time: 5, status: OpenPosition, open_price: 80.0, close_price: 0.0, size: 8.0, volume: 0.1, profit: 0.0, fee: 0.0048, total_profit: -0.0048 }]
*/

/////////////////////////////////////////////////////////////////////////////////
// TEST
///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod order_side_test {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_from_str() {
        assert_eq!(OrderSide::from_str("buy").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("Buy").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("B").unwrap(), OrderSide::Buy);
        assert_eq!(OrderSide::from_str("BUY").unwrap(), OrderSide::Buy);

        assert_eq!(OrderSide::Buy.to_string(), "Buy");

        assert_eq!(OrderSide::from_str("Sell").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("S").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("SELL").unwrap(), OrderSide::Sell);
        assert_eq!(OrderSide::from_str("sell").unwrap(), OrderSide::Sell);

        assert_eq!(OrderSide::Sell.to_string(), "Sell");
    }
}

#[cfg(test)]
mod closed_order_test {
    use super::*;

    #[test]
    fn test_closed_order_usd() {
        // 101を51(指定サイズ:Self側）と51（新規）に分割するテスト
        let order = Order::new(
            1,
            "close".to_string(),
            OrderSide::Buy,
            true,
            100,
            100.1,
            101.0,
            "".to_string(),
            true,
        );

        let mut close_order = OrderResult::from_order(100, &order, OrderStatus::OrderComplete);
        assert_eq!(close_order.order_home_size, 101.0);
        assert_eq!(close_order.order_foreign_size, 101.0 / 100.1);

        let result = &close_order.split_child(50.0).unwrap();
        assert_eq!(close_order.order_home_size, 50.0);
        assert_eq!(result.order_home_size, 51.0);

        assert_eq!(
            101.0 / 100.1,
            result.order_foreign_size + close_order.order_foreign_size
        );

        println!("{:?}", order);
        println!("{:?}", close_order);        
        println!("{:?}", result);                
    }
}

#[cfg(test)]
fn make_orders(buy_order: bool) -> OrderQueue {
    let mut orders = OrderQueue::new(buy_order);
    assert_eq!(orders.has_q(), false);

    let o1 = Order::new(
        1,
        "low price".to_string(),
        OrderSide::Buy,
        false,
        100,
        100.0,
        100.0,
        "".to_string(),
        false
    );
    let o2 = Order::new(
        3,
        "low price but later".to_string(),
        OrderSide::Buy,
        false,
        200,
        100.0,
        50.0,
        "".to_string(),
        false,
    );
    let o3 = Order::new(
        2,
        "high price".to_string(),
        OrderSide::Buy,
        false,
        300,
        200.0,
        200.0,
        "".to_string(),
        false,
    );
    let o4 = Order::new(
        1,
        "high price but first".to_string(),
        OrderSide::Buy,
        false,
        400,
        200.0,
        50.0,
        "".to_string(),
        false,
    );

    orders.queue_order(&o1);
    assert_eq!(orders.has_q(), true);
    orders.queue_order(&o2);
    orders.queue_order(&o3);
    orders.queue_order(&o4);

    return orders;
}

#[cfg(test)]
mod test_orders {

    use super::*;
    #[test]
    fn test_orders() {
        test_buy_orders();
        test_sell_orders();
    }

    #[test]
    fn test_buy_orders() {
        // Buy orderキューの作成
        let mut orders = make_orders(true);

        assert_eq!(orders.q[0].price, 200.0);
        assert_eq!(orders.q[0].size, 50.0);
        assert_eq!(orders.q[1].price, 200.0);
        assert_eq!(orders.q[1].size, 200.0);
        assert_eq!(orders.q[2].price, 100.0);
        assert_eq!(orders.q[2].size, 100.0);
        assert_eq!(orders.q[3].price, 100.0);
        assert_eq!(orders.q[3].size, 50.0);


        assert_eq!(orders.q[0].remain_size, 50.0);
        assert_eq!(orders.q[1].remain_size, 200.0);

        // 書いオーダに対し、買いのログがきてもなにもしない。
        assert_eq!(orders.execute_remain_size(&Trade{ time: 1000, order_side: OrderSide::Buy, price: 1000.0, size:125.0, id: "".to_string() }, 0), false);
        assert_eq!(orders.q[0].remain_size, 50.0);
        assert_eq!(orders.q[1].remain_size, 200.0);

        // 買いオーダーに対し、売りオーダーがきたら消費するが価格が同じ場合は消費しない。
        assert_eq!(orders.execute_remain_size(&Trade{ time: 1000, order_side: OrderSide::Sell, price: 200.0, size:125.0, id: "".to_string() }, 0), false);
        assert_eq!(orders.q[0].remain_size, 50.0);
        assert_eq!(orders.q[1].remain_size, 200.0);

        // 買いオーダーに対し、売りオーダーがきたら消費する。200にたいし199なので消費するが、オーダー時間よりサーバディレイ分Delayしていないので消費しない。
        assert_eq!(orders.execute_remain_size(&Trade{ time: 0, order_side: OrderSide::Sell, price: 199.0, size:125.0, id: "".to_string() }, 0), false);
        assert_eq!(orders.q[0].remain_size, 50.0);
        assert_eq!(orders.q[1].remain_size, 200.0);

        // 買いオーダーに対し、売りオーダーがきたら消費する。200にたいし199なので消費する。
        assert_eq!(orders.execute_remain_size(&Trade{ time: 1000, order_side: OrderSide::Sell, price: 199.0, size:125.0, id: "".to_string() }, 0), true);
        assert_eq!(orders.q[0].remain_size, 0.0);
        assert_eq!(orders.q[1].remain_size, 125.0);

    }

    #[test]
    fn test_sell_orders() {

        let mut orders = make_orders(false);

        // 売りオーダーにQが並ぶ。
        assert_eq!(orders.q[0].price, 100.0);
        assert_eq!(orders.q[0].size, 100.0);
        assert_eq!(orders.q[1].price, 100.0);
        assert_eq!(orders.q[1].size, 50.0);
        assert_eq!(orders.q[2].price, 200.0);
        assert_eq!(orders.q[2].size, 50.0);
        assert_eq!(orders.q[3].price, 200.0);
        assert_eq!(orders.q[3].size, 200.0);

        assert_eq!(orders.q[0].remain_size, 100.0);
        assert_eq!(orders.q[1].remain_size, 50.0);
        // 安い値段で買われても約定しない。OrderSideはQorderQueueではチェックしない
        assert_eq!(orders.execute_remain_size(&Trade{ time: 200, order_side: OrderSide::Buy, price: 99.9, size:125.0, id: "".to_string() }, 0), false);
        assert_eq!(orders.execute_remain_size(&Trade{ time: 200, order_side: OrderSide::Buy, price: 100.0, size:125.0, id: "".to_string() }, 0), false);

        // まだ約定していない。
        match orders.pop_closed_order(1000) {
            Ok(order) => {
                println!("err {:?}", order);
                assert!(false)
            }
            Err(e) => {
                assert_eq!(e, OrderStatus::NoAction);
            }
        }

        assert_eq!(orders.execute_remain_size(&Trade{ time: 200, order_side: OrderSide::Buy, price: 100.1, size:125.0, id: "".to_string() }, 0), true);
        println!("--after--");
        assert_eq!(orders.q[0].remain_size, 0.0);
        assert_eq!(orders.q[1].remain_size, 25.0);

        // １件約定した。
        match orders.pop_closed_order(1000) { // 時間はみていない
            Ok(order) => {
                assert_eq!(order.order_id, "low price");
                assert_eq!(orders.q.len(), 3);
                println!("OK {:?}", order);
            }
            Err(_e) => {
                assert!(false);
            }
        }

        // もおういちどとりだしてもでてこない。
        match orders.pop_closed_order(1001) {
            Ok(_order) => {
                assert!(false);
            }
            Err(e) => {
                assert_eq!(e, OrderStatus::NoAction);
            }
        }

        // ログをおくったら約定する。
        assert_eq!(orders.execute_remain_size(&Trade{ time: 201, order_side: OrderSide::Buy, price: 100.1, size:125.0, id: "".to_string() }, 0), true);
        match orders.pop_closed_order(1001) {
            Ok(order) => {
                assert_eq!(order.order_id, "low price but later");
                assert_eq!(orders.q.len(), 2);
                println!("OK {:?}", order);
            }
            Err(_e) => {
                assert!(false);
            }
        }
    }

    #[test]
    fn test_expire_order() {
        let mut orders = make_orders(true);

        // ValidUnitl時刻と同じ場合（または未満）は、Expireしない。
        let r = orders.expire(100);
        match r {
            Ok(_r) => {
                println!("ERROR error ");
                assert!(false);
            }
            Err(r) => {
                assert_eq!(r, OrderStatus::NoAction);
            }
        }

        // 途中であっても、１つしかExpireしない。
        let r = orders.expire(250);
        match r {
            Ok(order) => {
                assert_eq!(order.status, OrderStatus::ExpireOrder);
                assert_eq!(order.order_id, "low price");
            }
            Err(_r) => {
                assert!(false);
                println!("ERROR error ");
            }
        }

        // もういちどやると次のqつがExpireする。
        let r = orders.expire(250);
        match r {
            Ok(order) => {
                assert_eq!(order.status, OrderStatus::ExpireOrder);
                assert_eq!(order.order_id, "low price but later");
                assert_eq!(orders.q.len(), 2);
                println!("{:?}", order);
            }
            Err(_r) => {
                println!("ERROR error ");
                assert!(false);
            }
        }
    }
}

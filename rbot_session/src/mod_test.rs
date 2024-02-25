

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use rbot_lib::common::{OrderSide, Order, NOW, OrderType, OrderStatus, Trade, init_debug_log, LogStatus};

    use crate::orderlist::OrderList;

    #[test]
    fn test_order_list() {
        let mut order_list = OrderList::new(OrderSide::Sell);

        let now = NOW();

        let order1 = Order::new(
            "linear",
            "BTCUSDT",
            now,
            "MYORDER-1",
            "MYORDER-1",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![100.0], 
            dec![10.0]
        );

        let mut order2 = order1.clone();
        order2.order_id =  "2".to_string();
        order2.order_price= dec![200.0];
        order2.order_size = dec![20.0];
        order2.remain_size = dec![20.0];
        order2.order_side = OrderSide::Sell;

        let mut order3 = order1.clone();
        order3.order_id =  "3".to_string();
        order3.order_price= dec![150.0];
        order3.order_size = dec![15.0];
        order3.remain_size = dec![15.0];
        order3.order_side = OrderSide::Buy;

        // Test append
        order_list.append(order1.clone());
        assert_eq!(order_list.len(), 1);
        assert_eq!(order_list.remain_size(), dec![10.0]);

        order_list.append(order2.clone());
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.remain_size(), dec![30.0]);

        order_list.append(order3.clone());
        assert_eq!(order_list.len(), 3);
        assert_eq!(order_list.remain_size(), dec![45.0]);


        // Test sort
        order_list.sort();
        assert_eq!(order_list.list[0].order_id, order1.order_id);
        assert_eq!(order_list.list[1].order_id, order3.order_id);
        assert_eq!(order_list.list[2].order_id, order2.order_id);

        // Test update
        let mut updated_order1 = order1.clone();
            
        updated_order1.order_price = dec![150.1];
        updated_order1.order_size = dec![10.0];
        updated_order1.remain_size = dec![5.0];
        updated_order1.order_side = OrderSide::Buy;


        assert_eq!(order_list.list[0].order_id, order1.order_id);

        assert_eq!(order_list.update(updated_order1.clone()), true);
        assert_eq!(order_list.list[1].order_id, updated_order1.order_id);


        let mut updated_order4 = order1.clone();
        updated_order4.order_id = "4".to_string();
        assert_eq!(order_list.update(updated_order4.clone()), false);


        assert_eq!(order_list.remain_size(), dec![40.0]);
        // Test remove
        order_list.remove(&updated_order1.order_id);
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.remain_size(), dec![35.0]);

        let result = order_list.remove(&updated_order4.order_id);
        assert_eq!(result.is_some(), true);
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.remain_size(), dec![35.0]);

        // Test clear
        order_list.clear();
        assert_eq!(order_list.len(), 0);
        assert_eq!(order_list.remain_size(), dec![0.0]);

    }


    #[test]
    fn test_consume_trade() {
        // Test consume_trade
        // asc means buy order list
        let mut order_list = OrderList::new(OrderSide::Buy);

        let now = NOW();

        let order1 = Order::new(
            "linear",
            "BTCUSDT",
            now,
            "MYORDER-1",
            "MYORDER-1",
            OrderSide::Buy,
            OrderType::Limit,
            OrderStatus::New,
            dec![100.0], 
            dec![10.0]
        );

        order_list.append(order1.clone());

        let mut order2 = order1.clone();
        order2.order_id =  "2".to_string();
        order2.order_price= dec![200.0];
        order2.order_size = dec![20.0];
        order2.remain_size = dec![20.0];
        order_list.append(order2);

        let mut order3 = order1.clone();
        order3.order_id =  "3".to_string();
        order3.order_price= dec![150.0];
        order3.order_size = dec![15.0];
        order3.remain_size = dec![15.0];
        order_list.append(order3);        

        let mut order4 = order1.clone();
        order4.order_id =  "4".to_string();
        order4.order_price= dec![250.0];
        order4.order_size = dec![25.0];
        order4.remain_size = dec![25.0];
        order_list.append(order4);

        init_debug_log();

        // if buy trades comes in the buy trades list, its araises error log, and returns empty list.
        let trade = Trade::new(
            NOW(), OrderSide::Buy,
            dec![150.0], dec![10.0], 
            LogStatus::UnFix,            
            "ordr1");

            let filled_orders = order_list.consume_trade(&trade);

            assert_eq!(filled_orders.len(), 0);

        // if sell trades and equal to the highest buy order price, it will do nothing.
        let trade2 = Trade::new(
            NOW(), OrderSide::Sell,
            dec![250.0], dec![250.0], 
            LogStatus::UnFix,            
            "ordr2");


        let filled_orders = order_list.consume_trade(&trade2);
        assert_eq!(filled_orders.len(), 0);

        // Partially filled.
        let trade3 = Trade::new(
            NOW(), OrderSide::Sell,
            dec![249.9], dec![10.0],
            LogStatus::UnFix,
            "ordr3");

        let filled_orders = order_list.consume_trade(&trade3);
        assert_eq!(filled_orders.len(), 1);
        println!("filled_orders={:?}", filled_orders);
        assert_eq!(filled_orders[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(filled_orders[0].remain_size, dec![15.0]);
        assert_eq!(order_list.remain_size(), dec![60.0]);

        // Filled filled.
        let trade3 = Trade::new(
            NOW(), OrderSide::Sell,
            dec![199.0], dec![100.0], 
            LogStatus::UnFix,
            "ordr3");

        let filled_orders = order_list.consume_trade(&trade3);
        assert_eq!(filled_orders.len(), 2);
        println!("filled_orders={:?}", filled_orders);
        assert_eq!(filled_orders[0].status, OrderStatus::Filled);
        assert_eq!(filled_orders[0].remain_size, dec![0.0]);

        assert_eq!(filled_orders[1].status, OrderStatus::Filled);
        assert_eq!(filled_orders[1].remain_size, dec![0.0]);

        assert_eq!(order_list.remain_size(), dec![25.0]);

/*

        assert_eq!(filled_orders.len(), 2);
        assert_eq!(filled_orders[0].remain_size, dec!(0));
        assert_eq!(filled_orders[1].remain_size, dec!(1));
        assert_eq!(order_list.remain_size(), dec!(2));
        */
    }

}
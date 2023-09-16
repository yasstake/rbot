

#[cfg(test)]
mod tests {
    use super::*;
    use orderlist::OrderList;
    use rust_decimal_macros::dec;

    use crate::{session::orderlist, common::{OrderSide, Order, NOW, OrderType, OrderStatus, AccountChange}};

    use super::*;

    #[test]
    fn test_order_list() {
        let mut order_list = OrderList::new(true);

        let now = NOW();

        let order1 = Order {
            order_id: "1".to_string(),
            price: dec![100.0],
            size: dec![10.0],
            remain_size: dec![10.0],
            symbol: "BTCUSDT".to_string(),
            create_time: now,
            order_list_index: -1,
            client_order_id: "MYORDER-1".to_string(),
            order_side: OrderSide::Buy,
            order_type: OrderType::Limit,
            status: OrderStatus::New,
            account_change: AccountChange::new(),
            message: "".to_string(),
            fills: None,
            profit: None,
        };

        let mut order2 = order1.clone();
        order2.order_id =  "2".to_string();
        order2.price= dec![200.0];
        order2.size = dec![20.0];
        order2.remain_size = dec![20.0];
        order2.order_side = OrderSide::Sell;

        let mut order3 = order1.clone();
        order3.order_id =  "3".to_string();
        order3.price= dec![150.0];
        order3.size = dec![15.0];
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
            
        updated_order1.price = dec![150.1];
        updated_order1.size = dec![10.0];
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
        order_list.remove(updated_order1.clone());
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.remain_size(), dec![35.0]);

        let result = order_list.remove(updated_order4.clone());
        assert_eq!(result, false);
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.remain_size(), dec![35.0]);

        // Test clear
        order_list.clear();
        assert_eq!(order_list.len(), 0);
        assert_eq!(order_list.remain_size(), dec![0.0]);

    }
}
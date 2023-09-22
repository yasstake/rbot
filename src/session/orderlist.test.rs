#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_clear() {
        let mut order_list = OrderList::new();
        order_list.append(Order::new(OrderSide::Buy, dec!(100), dec!(1)));
        order_list.append(Order::new(OrderSide::Sell, dec!(200), dec!(2)));
        order_list.clear();
        assert_eq!(order_list.len(), 0);
    }

    #[test]
    fn test_update() {
        let mut order_list = OrderList::new();
        let order1 = Order::new(OrderSide::Buy, dec!(100), dec!(1));
        let order2 = Order::new(OrderSide::Sell, dec!(200), dec!(2));
        order_list.append(order1.clone());
        order_list.append(order2.clone());
        let updated_order = Order::new(OrderSide::Buy, dec!(150), dec!(1));
        assert!(order_list.update(updated_order.clone()));
        assert_eq!(order_list.list[0], updated_order);
        assert!(!order_list.update(Order::new(OrderSide::Buy, dec!(250), dec!(1))));
    }

    #[test]
    fn test_sort() {
        let mut order_list = OrderList::new();
        order_list.append(Order::new(OrderSide::Buy, dec!(100), dec!(1)));
        order_list.append(Order::new(OrderSide::Sell, dec!(200), dec!(2)));
        order_list.append(Order::new(OrderSide::Sell, dec!(150), dec!(3)));
        order_list.sort();
        assert_eq!(order_list.list[0].price, dec!(200));
        assert_eq!(order_list.list[1].price, dec!(150));
        assert_eq!(order_list.list[2].price, dec!(100));
    }

    #[test]
    fn test_append() {
        let mut order_list = OrderList::new();
        let order1 = Order::new(OrderSide::Buy, dec!(100), dec!(1));
        let order2 = Order::new(OrderSide::Sell, dec!(200), dec!(2));
        order_list.append(order1.clone());
        order_list.append(order2.clone());
        assert_eq!(order_list.len(), 2);
        assert_eq!(order_list.list[0], order1);
        assert_eq!(order_list.list[1], order2);
    }

    #[test]
    fn test_remove() {
        let mut order_list = OrderList::new();
        let order1 = Order::new(OrderSide::Buy, dec!(100), dec!(1));
        let order2 = Order::new(OrderSide::Sell, dec!(200), dec!(2));
        order_list.append(order1.clone());
        order_list.append(order2.clone());
        assert!(order_list.remove(order1.clone()));
        assert_eq!(order_list.len(), 1);
        assert_eq!(order_list.list[0], order2);
        assert!(!order_list.remove(Order::new(OrderSide::Buy, dec!(250), dec!(1))));
    }

    #[test]
    fn test_remain_size() {
        let mut order_list = OrderList::new();
        order_list.append(Order::new(OrderSide::Buy, dec!(100), dec!(1)));
        order_list.append(Order::new(OrderSide::Sell, dec!(200), dec!(2)));
        order_list.append(Order::new(OrderSide::Sell, dec!(150), dec!(3)));
        assert_eq!(order_list.remain_size(), dec!(6));
    }


}
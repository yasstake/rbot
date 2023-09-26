use crate::common::{Order, OrderSide, OrderStatus, Trade};
use polars_lazy::dsl::first;
use pyo3::{pyclass, pymethods, PyResult};
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_derive::Serialize;

#[pyclass]
#[derive(Debug, Clone, Serialize)]
pub struct OrderList {
    pub asc: bool,
    pub list: Vec<Order>,
}

#[pymethods]
impl OrderList {
    #[new]
    pub fn new(order_side: OrderSide) -> Self {
        let asc = match order_side {
            OrderSide::Buy => false,
            OrderSide::Sell => true,
            _ => {
                log::error!("OrderList.new: invalid order_side={:?}", order_side);
                true
            }
        };

        return Self {
            asc,
            list: Vec::new(),
        };
    }

    /// Returns the index of the given order in the list, if it exists.
    ///
    /// # Arguments
    ///
    /// * `order` - A reference to the `Order` to search for in the list.
    ///
    /// # Returns
    ///
    /// An `Option` containing the index of the order in the list, or `None` if the order is not found.
    pub fn index(&self, order: &Order) -> Option<usize> {
        return self.list.iter().position(|x| x.order_id == order.order_id);
    }

    /// Clears the list of orders.
    pub fn clear(&mut self) {
        self.list.clear();
    }

    /// Updates an existing order in the list.
    ///
    /// # Arguments
    ///
    /// * `order` - The order to update.
    ///
    /// # Returns
    ///
    /// Returns `true` if the order was successfully updated, `false` otherwise.
    pub fn update(&mut self, order: Order) -> bool {
        match self.index(&order) {
            Some(index) => {
                self.list[index] = order;
                self.sort();
                return true;
            }
            None => {
                return false;
            }
        }
    }

    /// Sorts the order list in ascending or descending order based on the `asc` field and create_time.
    pub fn sort(&mut self) {
        self.list.sort_by(|a, b| {
            if a.price == b.price {
                a.create_time.cmp(&b.create_time)
            } else {
                if self.asc {
                    a.price.cmp(&b.price)
                } else {
                    b.price.cmp(&a.price)
                }
            }
        });
    }

    /// Appends an order to the list and sorts it.
    pub fn append(&mut self, order: Order) {
        self.list.push(order);
        self.sort();
    }

    /// Removes the given order from the list and returns true if successful, false otherwise.
    pub fn remove(&mut self, order: &Order) -> bool {
        match self.index(order) {
            Some(index) => {
                self.list.remove(index);
                true
            }
            None => false,
        }
    }

    /// Returns the number of orders in the list.
    pub fn len(&self) -> usize {
        return self.list.len();
    }

    /// Returns the total remaining size of all orders in the list.
    pub fn remain_size(&self) -> Decimal {
        return self
            .list
            .iter()
            .fold(dec![0.0], |acc, x| acc + x.remain_size);
    }

    pub fn __repr__(&self) -> String {
        self.__str__()
    }

    pub fn __str__(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

impl OrderList {
    /// Consumes a trade and returns a list of orders that were filled by the trade.
    /// The orders in the list are updated to reflect the remaining size of the order after the trade.
    /// For Buy Trade, it consumes the Sell order list which below the trade price only.
    /// For Sell Trade, it consumes the Buy order list which above the trade price only.

    pub fn consume_trade(&mut self, mut trade: &Trade) -> Vec<Order> {
        // first check if the order is in the list. If not return emply list.
        let order_len = self.len();

        if order_len == 0 {
            return Vec::new();
        }

        let mut filled_orders: Vec<Order> = Vec::new();
        let mut remain_size: Decimal = trade.size;

        loop {
            if self.len()== 0 {
                break;
            }

            if trade.order_side == self.list[0].order_side {
                log::error!("OrderList.consume_trade: trade and order side is same. trade={:?}, order={:?}", trade, self.list[0]);
                break;
            }

            // Buy Order will sonsumme Sell Trade which below the trade price only.
            //　買いオーダーは高い売りトレードがあっても影響を受けない
            if (trade.price >= self.list[0].price) && (self.list[0].order_side == OrderSide::Buy) {
                break;
            }

            // Sell Order will sonsumme Buy Trade which above the trade price only.
            //　売りオーダーは安い買いトレードがあっても影響を受けない
            if (trade.price <= self.list[0].price) && (self.list[0].order_side == OrderSide::Sell) {
                break;
            }

            if remain_size < self.list[0].remain_size {
                // consume all remain_size, order is not filled.
                self.list[0].status = OrderStatus::PartiallyFilled;
                self.list[0].remain_size -= remain_size;
                remain_size = 0.into();
                filled_orders.push(self.list[0].clone());

                // TODO: calc fills and profit

                break;
            } else {
                // Order is filled.
                self.list[0].status = OrderStatus::Filled;
                self.list[0].remain_size = 0.into();

                remain_size -= self.list[0].remain_size;

                filled_orders.push(self.list[0].clone());
                // TODO: calc fills and profit

                self.list.remove(0);                
            }
        }

        filled_orders
    }

    /// update or insert order
    pub fn update_or_insert(&mut self, order: &Order) {
        // TODO: check timestamp and update
        match self.index(order) {
            Some(index) => {
                self.list[index] = order.clone();
            }
            None => {
                self.list.push(order.clone());
            }
        }
        self.sort();
    }
}


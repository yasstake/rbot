use crate::common::Order;
use pyo3::{pyclass, pymethods};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

#[pyclass]
#[derive(Debug, Clone)]
pub struct OrderList {
    pub asc: bool,
    pub list: Vec<Order>,
}

#[pymethods]
impl OrderList {
    #[new]
    /// Creates a new `OrderList` instance with the specified sort order.
    ///
    /// # Arguments
    ///
    /// * `asc` - A boolean value indicating whether the list should be sorted in ascending order.
    ///
    /// # Returns
    ///
    /// A new `OrderList` instance with the specified sort order and an empty list.
    pub fn new(asc: bool) -> Self {
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

    /// Sorts the order list in ascending or descending order based on the `asc` field.
    pub fn sort(&mut self) {
        if self.asc {
            self.list.sort_by(|a, b| a.price.cmp(&b.price));
        } else {
            self.list.sort_by(|a, b| b.price.cmp(&a.price));
        }
    }

    /// Appends an order to the list and sorts it.
    pub fn append(&mut self, order: Order) {
        self.list.push(order);
        self.sort();
    }

    /// Removes the given order from the list and returns true if successful, false otherwise.
    pub fn remove(&mut self, order: Order) -> bool {
        match self.index(&order) {
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
}

#[pyclass(name = "_Session")]
struct Session {}

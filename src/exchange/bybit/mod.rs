
pub mod market;
pub use market::*;



pub mod rest;
pub mod ws;
pub mod message;
pub mod config;

use crate::common::OrderStatus;

pub fn bybit_order_status(status: &str) -> OrderStatus {
    match status {
        "New" => OrderStatus::New,
        "PartiallyFilled" => OrderStatus::PartiallyFilled,
        "Cancelled" | "PartiallyFilledCanceled" => OrderStatus::Canceled,
        "Filled" => OrderStatus::Filled,
        _ => OrderStatus::Unknown,
    /*
    "Created", 
    "Untriggered"
    "Triggered"
    "Deactivated"
    "Rejected"
    */
    }
}

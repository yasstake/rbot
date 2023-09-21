

use std::sync::mpsc;
use crossbeam_channel::Sender;
use crossbeam_channel::Receiver;
use crossbeam_channel::unbounded;
use openssl::x509::AccessDescription;
use pyo3::pyclass;
use super::order::Order;
use super::order::Trade;
use anyhow::Result;


#[pyclass]
#[derive(Debug, Clone)]
pub struct MarketMessage {
    pub trade: Option<Trade>,
    pub order: Option<Order>
//    OrderBook(OrderBook),
    /*
    Position(Position),
    Account(AccessDescription),
    */
}

impl MarketMessage {
    pub fn new() -> Self {
        Self {
            trade: None,
            order: None,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub reciver: Receiver<MarketMessage>,
}

impl MarketStream {
    pub fn open() -> (Sender<MarketMessage>, MarketStream) { 
        let (sender, receiver) = unbounded();
        (sender, Self {
            reciver: receiver,
        })
    }
}


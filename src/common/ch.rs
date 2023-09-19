

use std::sync::mpsc;
use crossbeam_channel::Sender;
use crossbeam_channel::Receiver;
use crossbeam_channel::unbounded;
use openssl::x509::AccessDescription;
use pyo3::pyclass;
use super::order::Order;
use super::order::Trade;
use anyhow::Result;



pub enum MarketMessage {
    Trade(Trade),
    Order(Order),
//    OrderBook(OrderBook),
    /*
    Position(Position),
    Account(AccessDescription),
    */
}

#[pyclass]
pub struct MarketStream {
    pub reciver: Receiver<MarketMessage>,
}

impl MarketStream {
    pub fn open() -> (Sender<MarketMessage>, MarketStream) { 
        let (sender, reciver) = unbounded();
        (sender, Self {
            reciver,
        })
    }
}

#[cfg(test)]
mod test {


}

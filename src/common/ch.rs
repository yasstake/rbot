

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use pyo3::pyclass;
use super::order::Order;
use super::order::Trade;

use anyhow::Result;

pub enum ChannelMessage {
    Order(Order),
    Trade(Trade),
}

pub type ExchangeReceiver = Receiver<ChannelMessage>;
pub type ExchangeSender = Sender<ChannelMessage>;


#[pyclass]
#[derive(Debug)]
pub struct Channel {
    rx: ExchangeReceiver
}

impl Channel {
    pub fn create() -> (Self, ExchangeSender) {
        let (tx, rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) = mpsc::channel();

        return (Self{rx}, tx);
    }
}


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_send_message()  -> anyhow::Result<()>{
//        let (tx, rx): (Sender<Order>, Receiver<Order>) = unbounded();
//let (tx, rx): (Sender<String>, Receiver<String>) = unbounded();
    let (tx, rx): (Sender<ChannelMessage>, Receiver<ChannelMessage>) = mpsc::channel();

    Ok(())
    }

    #[test]
    fn test_create_channel() -> anyhow::Result<()> {
        let (channel, tx) = Channel::create();

        Ok(())
    }



}

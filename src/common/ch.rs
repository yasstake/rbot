use super::order::Order;
use super::order::Trade;
use super::AccountStatus;
use anyhow::Result;
use crossbeam_channel::bounded;
use crossbeam_channel::unbounded;
use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use pyo3::pyclass;

#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub struct MarketMessage {
    pub trade: Option<Trade>,
    pub order: Option<Order>,
    pub account: Option<AccountStatus>,
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
            account: None,
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
        (sender, Self { reciver: receiver })
    }
}

#[derive(Debug)]
struct Channel {
    sender: Sender<MarketMessage>,
    valid: bool,
}

#[derive(Debug)]
pub struct MultiChannel {
    channels: Vec<Channel>,
}

impl MultiChannel {
    pub fn new() -> Self {
        Self {
            channels: Vec::new(),
        }
    }

    pub fn close(&mut self) {
        loop {
            match self.channels.pop() {
                Some(channel) => drop(channel),
                None => break,
            }
        }
    }

    pub fn add_channel(&mut self, channel: Sender<MarketMessage>) {
        self.channels.push(Channel {
            sender: channel,
            valid: true,
        });
    }

    pub fn open_channel(&mut self, buffer_size: usize) -> MarketStream {
        let (sender, receiver) = 
            if buffer_size == 0 {
                unbounded()
            }
            else {
                bounded(buffer_size)
            };
        self.add_channel(sender);

        MarketStream { reciver: receiver }
    }

    pub fn send(&mut self, message: MarketMessage) -> Result<()> {
        let mut has_error: bool = false;

        for channel in self.channels.iter_mut() {
            let result = channel.sender.send(message.clone());

            if result.is_err() {
                has_error = true;
            }
        }

        // remove invalid channels
        if has_error {
            log::warn!("Send ERROR: removing invalid channels");
            self.channels.retain(|x| x.valid);
        }

        Ok(())
    }
}

#[cfg(test)]
mod channel_test {
    use crate::common::{init_debug_log, init_log};

    use super::*;

    #[test]
    fn test_channel() {
        let mut channel = MultiChannel::new();
        let receiver = channel.open_channel(0);

        let message = MarketMessage {
            trade: None,
            order: None,
            account: None,
        };
        channel.send(message.clone()).unwrap();

        let result = receiver.reciver.recv();
        assert_eq!(result.unwrap(), message);
    }

    #[test]
    fn test_channel_full() {
        init_log();
        init_debug_log();
        let mut channel = MultiChannel::new();
        let _receiver = channel.open_channel(0);

        for _ in 0..1024 {
            let message = MarketMessage {
                trade: None,
                order: None,
                account: None,
            };
            channel.send(message.clone()).unwrap();
        }

        let message = MarketMessage {
            trade: None,
            order: None,
            account: None,
        };
        let result = channel.send(message.clone());
        assert_eq!(result.is_err(), true);
    }

    #[test]
    fn test_channel_disconnect() {
        init_log();
        let mut channel = MultiChannel::new();
        let receiver = channel.open_channel(0);

        let message = MarketMessage {
            trade: None,
            order: None,
            account: None,
        };
        channel.send(message.clone()).unwrap();

        drop(receiver);

        let message = MarketMessage {
            trade: None,
            order: None,
            account: None,
        };
        let result = channel.send(message.clone());
        assert_eq!(result.is_err(), true);

        // send again, should be ok
        let _result = channel.send(message.clone());
    }
}

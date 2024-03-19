// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use anyhow::Result;

use pyo3::pyclass;
use pyo3::pymethods;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use super::order::Order;
use super::order::Trade;
use super::AccountCoins;
use super::AccountPair;
use super::BoardTransfer;
use super::MarketConfig;
use super::OrderBookRaw;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlMessage {
    pub status: bool,
    pub operation: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MarketMessage {
    Trade(Trade),
    Order(Order),
    Account(AccountCoins),
    Orderbook(OrderBookRaw),
    Control(ControlMessage),
    Message(String),
    ErrorMessage(String)
}

impl MarketMessage {
    pub fn update_config(&mut self, config: &MarketConfig) {
        match self {
            MarketMessage::Trade(_trade) => {
                //                
            }
            MarketMessage::Order(order) => {
                order.update_balance(config);
            }
            MarketMessage::Account(_account) => {
                //
            }
            MarketMessage::Orderbook(_orderbook) => {
                //
            }
            _ => {}
        }
    }

    pub fn from_trade(trade: Trade) -> Self {
        MarketMessage::Trade(trade)
    }

    pub fn from_order(order: Order) -> Self {
        MarketMessage::Order(order)
    }

    pub fn from_account(account: AccountCoins) -> Self {
        MarketMessage::Account(account)
    }

    pub fn from_orderbook(orderbook: OrderBookRaw) -> Self {
        MarketMessage::Orderbook(orderbook)
    }

    pub fn from_message(message: String) -> Self {
        MarketMessage::Message(message)
    }

    pub fn from_control(message: ControlMessage) -> Self {
        MarketMessage::Control(message)
    }

    pub fn make_message(m: &str) -> Self {
        MarketMessage::Message(m.to_string())
    }

    pub fn make_error_message(m: &str) -> Self {
        MarketMessage::ErrorMessage(m.to_string())
    }   
}

//pub type MultiMarketMessage = Vec<MarketMessage>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MultiMarketMessage {
    Trade(Vec<Trade>),
    Order(Vec<Order>),
    Account(AccountCoins),
    Orderbook(BoardTransfer),
    Message(String),
    Control(ControlMessage),
}


const CHANNEL_SIZE: usize = 4096;


#[pyclass]
#[derive(Debug, Clone)]
pub struct MarketStream {
    pub reciver: Receiver<MarketMessage>,
}

use crossbeam_channel::Sender;
use crossbeam_channel::Receiver;
use crossbeam_channel::unbounded;
use crossbeam_channel::bounded;

impl MarketStream {
    pub fn open() -> (Sender<MarketMessage>, MarketStream) {
        let (sender, receiver) = bounded(CHANNEL_SIZE);
        (sender, Self { reciver: receiver })
    }
}


/*



#[derive(Debug)]
struct Channel<T> {
    sender: Sender<T>,
    valid: bool,
}

#[derive(Debug)]
pub struct BroadcastChannel<T>
where
    T: Clone,
{
    channels: Vec<Channel<T>>,
}

impl<T> BroadcastChannel<T>
where
    T: Clone,
{
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

    pub fn add_channel(&mut self, channel: Sender<T>) {
        self.channels.push(Channel {
            sender: channel,
            valid: true,
        });
    }

    pub fn open_channel(&mut self, buffer_size: usize) -> Receiver<T> {
        let (sender, receiver) = if buffer_size == 0 {
            unbounded()
        } else {
            bounded(buffer_size)
        };
        self.add_channel(sender);

        receiver
    }

    pub fn send(&mut self, message: T) -> Result<()> {
        let mut has_error: bool = false;

        for channel in self.channels.iter_mut() {
            let result = channel.sender.send(message.clone());

            if result.is_err() {
                log::warn!("Send ERROR: {:?}. remove channel", result);
                channel.valid = false;
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
*/
/*
#[cfg(test)]
mod channel_test {
    use rust_decimal_macros::dec;

    use super::*;
    use crate::common::{init_debug_log, init_log, LogStatus, OrderSide, OrderStatus};

    #[test]
    /// チャネルを開いて、メッセージを送信する。
    /// 送信したメッセージと同じものが帰ってきているか確認する。
    /// かつ、複数チャネルへ同じメッセージが来ていることを確認する。
    fn test_channel() {
        let mut channel = BroadcastChannel::new();
        let receiver1 = channel.open_channel(0);
        let receiver2 = channel.open_channel(0);

        let trade = Trade::new(
            1111,
            OrderSide::Buy,
            dec![1.0],
            dec![2.0],
            LogStatus::UnFix,
            "ORDERID",
        );

        let message = MarketMessage::Trade(trade.clone());
        channel.send(message.clone()).unwrap();

        let result = receiver1.recv();
        assert!(result.is_ok());

        let result = result.unwrap();
        match result {
            MarketMessage::Trade(t) => {
                assert_eq!(t, trade);
            }
            _ => {
                assert!(false);
            }
        }

        let result = receiver2.recv();
        match result.unwrap() {
            MarketMessage::Trade(t) => {
                assert_eq!(t, trade);
            }
            _ => {
                assert!(false);
            }
        }
    }

    #[test]
    /// 4096個のメッセージを送信して、最後のメッセージが送信できるか確認する。
    fn test_channel_full() {
        // init_log();

        let mut channel = BroadcastChannel::new();
        let _receiver = channel.open_channel(0);

        for i in 0..4096 {
            let message = MarketMessage::Message(format!("test message {:?}", i));
            channel.send(message.clone()).unwrap();
        }

        let message = MarketMessage::Message("LAST test message 4096".to_string());

        let result = channel.send(message.clone());
        assert_eq!(result.is_ok(), true);
    }

    #[test]
    fn test_channel_disconnect() {
        //init_log();
        let mut channel = BroadcastChannel::new();
        let receiver = channel.open_channel(0);

        let message = MarketMessage::Message("test message1".to_string());
        channel.send(message.clone()).unwrap();
        drop(receiver);

        let message = MarketMessage::Message("test message2".to_string());
        let result = channel.send(message.clone());
        assert_eq!(result.is_ok(), true);

        let receiver = channel.open_channel(0);
        // send again, should be ok
        let result = channel.send(message.clone());
        let _m = receiver.recv().unwrap();
        assert_eq!(result.is_ok(), true);
    }
*/



#[tokio::test]
async fn test_handling_lag() {
    use tokio::sync::broadcast;

    let (tx, mut rx) = broadcast::channel(2);

    // send mote than buffer size
    tx.send(10).unwrap();

    let mut rx2 = tx.subscribe();

    tx.send(20).unwrap();
    tx.send(30).unwrap();

    // The receiver lagged behind
    assert!(rx.recv().await.is_err());

    // At this point, we can abort or continue with lost messages

    assert_eq!(20, rx.recv().await.unwrap());
    assert_eq!(30, rx.recv().await.unwrap());

    assert_eq!(20, rx2.recv().await.unwrap());
    assert_eq!(30, rx2.recv().await.unwrap());
    println!("rx2: {:?}", rx2.recv().await);
    println!("rx2: {:?}", rx2.recv().await);

    for _ in 0..1000 {
        tx.send(40).unwrap();
    }
}

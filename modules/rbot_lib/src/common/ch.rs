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

    pub fn recv(&self) -> anyhow::Result<MarketMessage> {
        let r = self.reciver.recv()?;

        Ok(r)        
    }
}

#[cfg(test)]
mod test_market_stream {
    use crate::common::Trade;

    use super::{MarketMessage, MarketStream};

    #[test]
    fn test_market_stream() -> anyhow::Result<()>{
        let (sender, ms) = MarketStream::open();

        let mut trade = Trade::default();
        trade.id = "test".to_string();
        let message1 = MarketMessage::from_trade(trade);
        sender.send(message1.clone())?;

        let mut trade = Trade::default();
        trade.id = "test".to_string();
        let message2 = MarketMessage::from_trade(trade);
        sender.send(message2.clone())?;

        let r = ms.reciver.recv()?;
        assert_eq!(message1, r);

        let r = ms.reciver.recv()?;
        assert_eq!(message2, r);

        Ok(())
    }


}
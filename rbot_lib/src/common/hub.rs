use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;

use crate::net::BroadcastMessage;

use once_cell::sync::Lazy;

pub struct MarketHub {
    tx: Sender<BroadcastMessage>,
    _rx: Receiver<BroadcastMessage>,
}

const CHANNEL_SIZE: usize = 1024;

pub static MARKET_HUB: Lazy<MarketHub> = Lazy::new(|| MarketHub::open());

impl MarketHub {
    pub fn open() -> Self {
        let (tx, _rx) = broadcast::channel(CHANNEL_SIZE);
        Self { tx, _rx }
    }

    pub fn subscribe(&self) -> Receiver<BroadcastMessage> {
        return self.tx.subscribe();
    }

    pub fn publish(&self, message: BroadcastMessage) -> anyhow::Result<()> {
        self.tx.send(message)?;
        Ok(())
    }

    pub fn clone_channel(&self) -> Sender<BroadcastMessage> {
        self.tx.clone()
    }
}

#[cfg(test)]
mod test_market_hub {
    use super::*;
    use crate::common::hub::MARKET_HUB;
    use crate::common::init_debug_log;
    use crate::common::MarketMessage;
    use crate::common::Trade;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::Sender;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_market_hub() {
        init_debug_log();
        let tx = MARKET_HUB.clone_channel();

        for i in 0..CHANNEL_SIZE * 2 {

            let msg = BroadcastMessage{
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i))
            };
            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }

        let mut rx = MARKET_HUB.subscribe();
        let mut rx2 = MARKET_HUB.subscribe();        

        for i in 10..20 {
            let msg = BroadcastMessage{
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i))
            };

            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }
        
        for _i in 10..20 {
            let r = rx.recv().await;
            if r.is_err() {
                println!("error: {:?}", r);
            } else {
                let r = r.unwrap();
                println!("message: {:?}", r);
            }
        }

        for _i in 10..20 {
            let r = rx2.recv().await;
            if r.is_err() {
                println!("error: {:?}", r);
            } else {
                let r = r.unwrap();
                println!("message: {:?}", r);
            }
        }

    }
}

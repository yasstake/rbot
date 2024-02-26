use futures::Stream;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;

use crate::net::BroadcastMessage;

use async_stream::stream;
use once_cell::sync::Lazy;

use super::MarketMessage;

use futures::StreamExt;
use tokio::task::spawn;

pub struct MarketHub {
    tx: Sender<BroadcastMessage>,
    _rx: Receiver<BroadcastMessage>,
}

const CHANNEL_SIZE: usize = 1024;

pub static MARKET_HUB: Lazy<MarketHub> = Lazy::new(|| MarketHub::new());

pub fn stream_receiver(
    stream: impl Stream<Item = anyhow::Result<MarketMessage>> + Send + 'static,
) -> (crossbeam_channel::Receiver<MarketMessage>, JoinHandle<()>) {
    let (tx, rx) = crossbeam_channel::unbounded();

    let handle = tokio::spawn(async move {
        let mut stream = Box::pin(stream);

        while let Some(message) = stream.next().await {
            match message {
                Ok(msg) => {
                    let r = tx.send(msg);
                    if r.is_err() {
                        log::error!("send message error: {:?}", r);
                    }
                }
                Err(e) => {
                    log::error!("receive message error: {:?}", e);
                }
            }
        }
    });

    (rx, handle)
}

impl MarketHub {
    pub fn new() -> Self {
        let (tx, _rx) = broadcast::channel(CHANNEL_SIZE);
        Self { tx, _rx }
    }

    pub fn subscribe(&self,
        exchange: &str,
        category: &str,
        symbol: &str,
        agent_id: &str,
    ) -> anyhow::Result<crossbeam_channel::Receiver<MarketMessage>> {
        let exchange = exchange.to_string();
        let category = category.to_string();
        let symbol = symbol.to_string();
        let agent_id = agent_id.to_string();

        let (tx, rx) = crossbeam_channel::unbounded();
        let mut ch = self.tx.subscribe();

        std::thread::spawn(move ||{
            let runtime = Runtime::new().unwrap();

            runtime.block_on(async move {
                loop {
                    let msg = ch.recv().await;
    
                    if msg.is_err() {
                        break;
                    }
    
                    let msg = msg.unwrap();
    
                    if msg.filter(&exchange, &category, &symbol) {
                        let market_message = msg.msg.clone();
    
                        match market_message {
                            MarketMessage::Order(ref order) => {
                                if order.is_my_order(&agent_id) {
                                    let r = tx.send(market_message.clone());
                                    if r.is_err() {
                                        log::error!("open_channel: {}/{:?}", r.err().unwrap(), msg);
                                        break;
                                    }
                                }
                            }
                            _ => {
                                let r = tx.send(market_message.clone());
                                if r.is_err() {
                                    log::error!("open_channel: {}/{:?}", r.err().unwrap(), msg);
                                    break;
                                }
                            }
                        }
                    }
                }
            });
        });

        Ok(rx)
    }


    pub fn subscribe_all(&self,
    ) -> anyhow::Result<crossbeam_channel::Receiver<BroadcastMessage>> {
        let mut ch = self.tx.subscribe();
        let (tx, rx) = crossbeam_channel::unbounded();        

        std::thread::spawn(move ||{
            let runtime = Runtime::new().unwrap();

            runtime.block_on(async move {
                loop {
                    let msg = ch.recv().await;
    
                    if msg.is_err() {
                        break;
                    }

                    if tx.send(msg.unwrap()).is_err() {
                        log::error!("send message error");
                        break;
                }
            }
            });
        });

        Ok(rx)
    }







    pub async fn subscribe_stream<'a>(
        &self,
        exchange: &'a str,
        category: &'a str,
        symbol: &'a str,
        agent_id: &'a str,
    ) -> impl Stream<Item = anyhow::Result<MarketMessage>> + 'a {
        let mut ch = self.tx.subscribe();

        stream! {
            loop {
                let msg = ch.recv().await?;

                if msg.filter(exchange, category, symbol) {
                    match msg.msg {
                        MarketMessage::Order(ref order) => {
                            if order.is_my_order(agent_id) {
                                yield Ok(msg.msg);
                            }
                        }
                        _ => {
                            yield Ok(msg.msg);
                        }
                    }
                }
            }
        }
    }
    pub fn publish(&self, message: BroadcastMessage) -> anyhow::Result<()> {
        self.tx.send(message)?;
        Ok(())
    }

    pub fn open_channel(&self) -> Sender<BroadcastMessage> {
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
    use futures::StreamExt;
    use tokio::sync::broadcast::Receiver;
    use tokio::sync::broadcast::Sender;
    use tokio::time::sleep;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_receive_channel() {
        init_debug_log();
        let tx = MARKET_HUB.open_channel();

        let rx = MARKET_HUB.subscribe_stream("a", "b", "c", "").await;
        let mut rx = Box::pin(rx);

        for i in 0..CHANNEL_SIZE {
            let msg = BroadcastMessage {
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i)),
            };
            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }

        log::debug!("send message done");

        for _i in 0..10 {
            if let Some(r) = rx.next().await {
                println!("ch: m.essage: {:?}", r);
            }
        }
    }

    #[test]
    fn test_market_hub() {
        init_debug_log();
        let tx = MARKET_HUB.open_channel();
        let rx = MARKET_HUB.subscribe("a", "b", "c", "").unwrap();
        let rx2 = MARKET_HUB.subscribe("a", "b", "c", "").unwrap();

        for i in 0..CHANNEL_SIZE * 2 {
            let msg = BroadcastMessage {
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i)),
            };
            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }


        for i in 10..20 {
            let msg = BroadcastMessage {
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i)),
            };

            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }

        log::debug!("-------rx1--------");

        for _i in 10..20 {
            let r = rx.recv();
            if r.is_err() {
                println!("error: {:?}", r);
            } else {
                let r = r.unwrap();
                println!("message: {:?}", r);
            }
        }

        log::debug!("-------rx2--------");

        for _i in 10..20 {
            let r = rx2.recv();
            if r.is_err() {
                println!("error: {:?}", r);
            } else {
                let r = r.unwrap();
                println!("message: {:?}", r);
            }
        }
    }

    #[tokio::test]
    async fn test_stream_receiver() {
        let tx = MARKET_HUB.open_channel();

        let rx = MARKET_HUB.subscribe_stream("a", "b", "c", "").await;

        let (rx2, handle2) = stream_receiver(rx);

        for i in 0..10 {
            let msg = BroadcastMessage {
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i)),
            };
            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }

        for _i in 0..10 {
            let r = rx2.recv().unwrap();
            println!("ch: message: {:?}", r);
        }

        handle2.await.unwrap();

        sleep(Duration::from_secs(5)).await;
    }

    /*
    #[tokio::test]


    async fn test_receiver() {

        init_debug_log();
        let tx = MARKET_HUB.open_channel();
        let mut rx2 = MARKET_HUB.subscribe();

        let handle = spawn(async {
            let rx = MARKET_HUB.subscribe_stream("a", "b", "c", "").await;
            let (rx, handle) = rx.unwrap();

            for _i in 0..10 {
                let r = rx.recv().unwrap();
                println!("ch: message: {:?}", r);
            }
        });
        let rx = MARKET_HUB.subscribe_stream("a", "b", "c", "").await;
        let (rx, handle) = rx.unwrap();

        for i in 0..CHANNEL_SIZE {
            let msg = BroadcastMessage {
                exchange: "a".to_string(),
                category: "b".to_string(),
                symbol: "c".to_string(),
                msg: MarketMessage::make_message(&format!("message {}", i)),
            };
            let r = tx.send(msg.clone());
            if r.is_err() {
                println!("error: {:?}", r);
            }
        }

        log::debug!("send message done");
        for _i in 10..20 {
            let r = rx2.recv().await;
            if r.is_err() {
                println!("error: {:?}", r);
            } else {
                let r = r.unwrap();
                println!("message: {:?}", r);
            }
        }

        for _i in 0..10 {
            let r = rx.recv().unwrap();
            println!("ch: message: {:?}", r);
        }

        sleep(Duration::from_secs(5)).await;
    }
    */
}

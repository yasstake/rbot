use async_stream::stream;
use futures::{Stream, StreamExt};
use pubnub::{subscribe::{EventEmitter, EventSubscriber, SubscriptionParams, Update}, Keyset, PubNubClient, PubNubClientBuilder};
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;


use rbot_lib::{
    common::{ExchangeConfig, MarketConfig, MultiMarketMessage, Trade, BoardTransfer, ControlMessage},
    net::{AutoConnectClient, ReceiveMessage, WsOpMessage, WebSocketClient},
};

use crate::{BitbankPrivateStreamKey, BitbankPrivateWsMessage, BitbankPublicWsMessage, BitbankRestApi };

const PING_INTERVAL_SEC: i64 = 15;
const SWITCH_INTERVAL_SEC: i64 = 60 * 60;
const SYNC_WAIT_RECORDS: i64 = 100;
const SYNC_WAIT_RECORDS_FOR_PRIVATE: i64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BitbankWsOpMessage {
}

impl WsOpMessage for BitbankWsOpMessage {
    fn new() -> Self {
        BitbankWsOpMessage {}
    }

    fn add_params(&mut self, _params: &Vec<String>) {
    }

    fn make_message(&self) -> Vec<String> {
        vec![        
            r#"42["join-room","depth_diff_xrp_jpy"]"#.to_string(),
        ]
    }

    fn to_string(&self) -> String {
        return "".to_string();
    }
}


pub struct BitbankPublicWsClient {
    ws: AutoConnectClient<BitbankWsOpMessage>,
    server: ExchangeConfig,
    config: MarketConfig,
    _handler: Option<JoinHandle<()>>,
}

impl BitbankPublicWsClient {
    pub async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        let api = BitbankRestApi::new(server);
        let url = server.get_public_ws_server();

        log::debug!("url: {}", url);

        let public_ws = AutoConnectClient::new(
            server,
            config,
            &url,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
            true,
        );

        Self {
            ws: public_ws,
            server: server.clone(),
            config: config.clone(),
            _handler: None,
        }
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a + Send {
        let mut s = Box::pin(self.ws.open_stream().await);

        stream! {
            let mut try_count = 0;
            while let Some(message) = s.next().await {
                try_count += 1;
                if try_count > 10 {
                    yield Err("try_count > 10".to_string());
                }

                match message {
                    Ok(m) => {
                        if let ReceiveMessage::Text(m) = m {
                            println!("message: {:?}", m);

                            match Self::parse_message(m) {
                                Err(e) => {
                                    println!("Parse Error: {:?}", e);
                                    continue;
                                }
                                Ok(m) => {
                                    let market_message = Self::convert_ws_message(m);

                                    match market_message
                                    {
                                        Err(e) => {
                                            println!("Convert Error: {:?}", e);
                                            continue;
                                        }
                                        Ok(m) => {
                                            try_count = 0;
                                            yield Ok(m);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Receive Error: {:?}", e);
                    }
                }
            }
        }
    }
}

impl BitbankPublicWsClient{
    fn parse_message(message: String) -> anyhow::Result<BitbankPublicWsMessage> {
        log::debug!("message: {:?}", message);

        let message = BitbankPublicWsMessage::from_str(&message)?;

        Ok(message)
    }

    // TODO: implement
    fn convert_ws_message(message: BitbankPublicWsMessage) -> anyhow::Result<MultiMarketMessage> {
        Ok(message.into())  
    }
}

impl WebSocketClient for BitbankPublicWsClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        Self::new(server, config).await
    }

    async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + Send + 'a {
        self.open_stream().await
    }
}

const PUBNUB_SUB_KEY: &str = "sub-c-ecebae8e-dd60-11e6-b6b1-02ee2ddab7fe";

pub struct BitbankPrivateStreamClient {
    _system_handler: Option<JoinHandle<()>>,
}

impl BitbankPrivateStreamClient {
    pub async fn new(server: &ExchangeConfig) -> Self {
        Self {
            _system_handler: None,
        }
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a + Send {
        let server = match ExchangeConfig::open("bitbank", true) {
            Ok(s) => s,
            Err(e) => return futures::stream::once(async move { Err(format!("Failed to open config: {}", e)) }).boxed(),
        };

        let keys = BitbankRestApi::new(&server)
            .get_private_stream_key()
            .await.unwrap();

        let pubnub = PubNubClientBuilder::with_reqwest_transport()
            .with_keyset(Keyset {
                subscribe_key: PUBNUB_SUB_KEY,
                publish_key: None,
                secret_key: None,
            })
            .with_user_id(&keys.pubnub_channel)
            .build().unwrap();

        pubnub.set_token(keys.pubnub_token);

        let subscription = pubnub.subscription(
            SubscriptionParams{
                channels: Some(&[&keys.pubnub_channel]),
                channel_groups: None,
                options: None,
            }
        );

        subscription.subscribe();
        
        tokio::spawn(
            pubnub
                .status_stream()
                .for_each(|status| async move { 
                    println!("\nStatus: {:?}", status) 
                }),
        );

        Box::pin(stream! {
            while let Some(message) = subscription.stream().next().await {
                match message {
                    Update::Message(message) => {
                        if let Ok(utf8_message) = String::from_utf8(message.data.clone()) {
                            println!("\nmessage: {:?}", utf8_message);
                            match Self::parse_message(utf8_message) {
                                Ok(parsed) => match Self::convert_ws_message(parsed) {
                                    Ok(converted) => yield Ok(converted),
                                    Err(e) => yield Err(format!("Failed to convert message: {}", e)),
                                },
                                Err(e) => yield Err(format!("Failed to parse message: {}", e)),
                            }
                        }
                    }
                    Update::Signal(message) => {
                    }
                    _ => {
                    }
                }
            }
        })
    }
}

impl BitbankPrivateStreamClient {
    fn parse_message(message: String) -> anyhow::Result<BitbankPrivateWsMessage> {
        let message: BitbankPrivateWsMessage = serde_json::from_str(&message)?;

        Ok(message)
    }

    fn convert_ws_message(message: BitbankPrivateWsMessage) -> anyhow::Result<MultiMarketMessage> {
        Ok(message.into())
    }
}

impl WebSocketClient for BitbankPrivateStreamClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        Self::new(server).await
    }

    async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a + Send {
        self.open_stream().await
    }
}


#[cfg(test)]
mod tests {
    use rbot_lib::common::init_debug_log;

    use super::*;

    #[tokio::test]
    async fn test_public_ws_client() -> anyhow::Result<()> {
        init_debug_log();

        let server = ExchangeConfig::open("bitbank", true)?;
        log::debug!("server: {:?}", server);

        let config = MarketConfig::default();
        let mut client = BitbankPublicWsClient::new(&server, &config).await;
        let stream = client.open_stream().await;
        let mut stream = Box::pin(stream);


        for _i in 0..10 {
            let message = stream.next().await;
            println!("{:?}", message.unwrap());
        }
        Ok(())
    }
}
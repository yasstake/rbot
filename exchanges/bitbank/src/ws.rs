use std::time::Duration;
use async_stream::stream;
use futures::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio::time::sleep;

use anyhow::anyhow;

use rbot_lib::{
    common::{ExchangeConfig, MarketConfig, MultiMarketMessage, Trade, BoardTransfer, ControlMessage},
    net::{AutoConnectClient, ReceiveMessage, WsOpMessage, WebSocketClient},
};

use crate::{BitbankPrivateWsMessage, BitbankPublicWsRoomMessage, BitbankRestApi, BitbankWsRawMessage};
use rbot_blockon::BLOCK_ON;

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
    fn parse_message(message: String) -> anyhow::Result<BitbankPublicWsRoomMessage> {
        log::debug!("message: {:?}", message);

        let m = serde_json::from_str::<Vec<BitbankWsRawMessage>>(&message)
            .map_err(|e| {
                log::warn!("Error in serde_json::from_str: {:?}", message);
                anyhow!("Error in serde_json::from_str: {:?}", message)
            })?;

        todo!()
    }

    // TODO: implement
    fn convert_ws_message(message: BitbankPublicWsRoomMessage) -> anyhow::Result<MultiMarketMessage> {
        // TODO: implement
        Ok(MultiMarketMessage::Message(format!("{:#?}", message.message)))
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


pub struct BitbankPrivateWsClient {
    ws: AutoConnectClient<BitbankWsOpMessage>,
    server: ExchangeConfig,
    _handler: Option<JoinHandle<()>>,
    api: BitbankRestApi,
}

impl BitbankPrivateWsClient {
    pub async fn new(server: &ExchangeConfig) -> Self {
        todo!()
        /*
        let api = BitbankRestApi::new(server);
        let url = server.get_public_ws_server();

        let public_ws = AutoConnectClient::new(
            server,
            None,
            &url,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
        );
        Self {
            ws: public_ws,
            server: server.clone(),
            _handler: None,
            api: api,
        }
        */
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a + Send {
        let mut s = Box::pin(self.ws.open_stream().await);

        stream! {
            while let Some(message) = s.next().await {
                match message {
                    Ok(m) => {
                        if let ReceiveMessage::Text(m) = m {


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

impl BitbankPrivateWsClient {
    fn parse_message(message: String) -> anyhow::Result<BitbankPrivateWsMessage> {
        todo!()
    }

    fn convert_ws_message(message: BitbankPrivateWsMessage) -> anyhow::Result<MultiMarketMessage> {
        todo!()
    }
}

impl WebSocketClient for BitbankPrivateWsClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        Self::new(server).await
    }

    async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + Send + 'a {
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
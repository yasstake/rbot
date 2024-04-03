use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use rbot_lib::net::ReceiveMessage;
// use serde::{Deserialize as _, Serialize as _};
use tokio::task::JoinHandle;

use async_stream::stream;

use rbot_lib::{
    common::{MarketConfig, MultiMarketMessage, ServerConfig, NOW},
    net::{AutoConnectClient, WsOpMessage},
};
use tokio::time::sleep;

use crate::BinanceRestApi;
use crate::BinanceUserWsMessage;
use crate::BinanceWsRawMessage;
use crate::{BinancePublicWsMessage, BinanceServerConfig};

use serde_derive::{Deserialize, Serialize};

use anyhow::anyhow;

/// https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
/// Ping/Keep-alive a ListenKey (USER_STREAM)
/// PUT /api/v3/userDataStream
/// Keepalive a user data stream to prevent a time out.
/// User data streams will close after 60 minutes.
/// It's recommended to send a ping about every 30 minutes.

// const KEY_EXTEND_INTERVAL: MicroSec = 5 * 60 * MICRO_SECOND; // 30 min

pub const PING_INTERVAL_SEC: i64 = 60 * 3; // every 3 min
pub const SWITCH_INTERVAL_SEC: i64 = 60 * 60 * 12; // 12 hours
const SYNC_WAIT_RECORDS_FOR_PUBLIC: i64 = 3; // no overlap
const SYNC_WAIT_RECORDS_FOR_PRIVATE: i64 = 0; // no overlap

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceWsOpMessage {
    method: String,
    params: Vec<String>,
    id: i64,
}

impl WsOpMessage for BinanceWsOpMessage {
    fn new() -> Self {
        BinanceWsOpMessage {
            method: "SUBSCRIBE".to_string(),
            params: vec![],
            id: NOW() % 1000,
        }
    }

    fn add_params(&mut self, params: &Vec<String>) {
        log::debug!("add_params: {:?} / {:?}", self.params, params);
        self.params.extend(params.clone());
    }

    fn make_message(&self) -> Vec<String> {
        vec![self.to_string()]
    }

    fn to_string(&self) -> String {
        if self.params.len() == 0 {
            return "".to_string();
        } else {
            return serde_json::to_string(&self).unwrap();
        }
    }
}

pub struct BinancePublicWsClient {
    ws: AutoConnectClient<BinanceServerConfig, BinanceWsOpMessage>,
    _handler: Option<JoinHandle<()>>,
}

impl BinancePublicWsClient {
    pub async fn new(server: &BinanceServerConfig, config: &MarketConfig) -> Self {
        let mut public_ws = AutoConnectClient::new(
            server,
            config,
            &server.get_public_ws_server(),
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS_FOR_PUBLIC,
            None,
            None,
        );

        public_ws.subscribe(&config.public_subscribe_channel).await;

        Self {
            ws: public_ws,
            _handler: None,
        }
    }

    pub async fn connect(&mut self) {
        self.ws.connect().await
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a {
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

    fn parse_message(message: String) -> anyhow::Result<BinancePublicWsMessage> {
        let m = serde_json::from_str::<BinanceWsRawMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(anyhow!("Error in serde_json::from_str: {:?}", message));
        }

        let m = m.unwrap();

        Ok(m.into())
    }

    // TODO: implement
    fn convert_ws_message(message: BinancePublicWsMessage) -> anyhow::Result<MultiMarketMessage> {
        Ok(message.into())
    }
}

pub struct BinancePrivateWsClient {
    ws: AutoConnectClient<BinanceServerConfig, BinanceWsOpMessage>,
    server: BinanceServerConfig,
    _handler: Option<JoinHandle<()>>,
    listen_key: String,
    key_update_handler: Option<JoinHandle<()>>,
}

impl BinancePrivateWsClient {
    pub async fn new(server: &BinanceServerConfig) -> Self {
        let dummy_config = MarketConfig::new("dummy", "dummy", "dummy", 0, 0, 0);

        let listen_key = Self::make_listen_key(server).await.unwrap();
        let url = Self::make_connect_url(server, &listen_key).unwrap();

        let private_ws = AutoConnectClient::new(
            server,
            &dummy_config,
            &url,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS_FOR_PRIVATE,
            None,
            None,
        );

        Self {
            server: server.clone(),
            ws: private_ws,
            _handler: None,
            listen_key: listen_key,
            key_update_handler: None,
        }
    }

    pub async fn connect(&mut self) {
        self.ws.connect().await;

        let key = self.listen_key.clone();
        let server = self.server.clone();

        let hander = tokio::task::spawn(async move {
            loop {
                sleep(Duration::from_secs(60*60)).await;
                let r = BinanceRestApi::extend_listen_key(&server, &key).await;
                log::info!("Extend listen key");
                if r.is_err() {
                    log::error!("Failed to extend listen key: {:?}", r);
                }
            }
        });

        self.key_update_handler = Some(hander);
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a {
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

    fn parse_message(message: String) -> anyhow::Result<BinanceUserWsMessage> {
        let m = serde_json::from_str::<BinanceUserWsMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(anyhow!("Error in serde_json::from_str: {:?}", message));
        }

        Ok(m.unwrap())
    }

    fn convert_ws_message(message: BinanceUserWsMessage) -> anyhow::Result<MultiMarketMessage> {
        Ok(message.convert_multimarketmessage("SPOT"))
    }

    async fn make_listen_key(server: &BinanceServerConfig) -> anyhow::Result<String> {
        let key = BinanceRestApi::create_listen_key(server).await?;

        Ok(key)
    }

    fn make_connect_url(server: &BinanceServerConfig, key: &str) -> anyhow::Result<String> {
        let url = format!("{}/ws/{}", server.get_user_ws_server(), key);

        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BinanceConfig;
    use rbot_lib::common::init_debug_log;

    #[tokio::test]
    async fn test_binance_public_ws_client() {
        let server = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut client = BinancePublicWsClient::new(&server, &config).await;

        client.connect().await;

        let stream = client.open_stream().await;

        let mut stream = Box::pin(stream);

        let mut count = 0;

        while let Some(message) = stream.next().await {
            match message {
                Ok(m) => {
                    println!("Message: {:?}", m);
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            }

            if 100 < count {
                break;
            }
            count += 1;
        }
    }

    #[tokio::test]
    async fn test_make_connect_url() {
        let server = BinanceServerConfig::new(false);
        let _config = BinanceConfig::BTCUSDT();

        let key = BinanceRestApi::create_listen_key(&server).await.unwrap();
        let url = BinancePrivateWsClient::make_connect_url(&server, &key);

        println!("URL: {:?}", url);
    }

    #[tokio::test]
    async fn test_binance_private_ws_client() {
        init_debug_log();
        
        let server = BinanceServerConfig::new(false);
        let mut client = BinancePrivateWsClient::new(&server).await;

        client.connect().await;

        let stream = client.open_stream().await;

        let mut stream = Box::pin(stream);

        let mut count = 0;

        while let Some(message) = stream.next().await {
            match message {
                Ok(m) => {
                    println!("Message: {:?}", m);
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            }

            if 100 < count {
                break;
            }
            count += 1;
        }
    }
}

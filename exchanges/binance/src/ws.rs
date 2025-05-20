use std::time::Duration;

use futures::Stream;
use futures::StreamExt;
use rbot_lib::net::ReceiveMessage;
use rbot_lib::net::WebSocketClient;
// use serde::{Deserialize as _, Serialize as _};
use tokio::task::JoinHandle;

use async_stream::stream;

use rbot_lib::{
    common::{MarketConfig, MultiMarketMessage, ExchangeConfig, NOW},
    net::{AutoConnectClient, WsOpMessage},
};
use tokio::time::sleep;

use crate::BinanceRestApi;
use crate::BinanceUserWsMessage;
use crate::BinanceWsRawMessage;
use crate::BinancePublicWsMessage;
use serde_derive::{Deserialize, Serialize};

use anyhow::anyhow;

/// https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
/// Ping/Keep-alive a ListenKey (USER_STREAM)
/// PUT /api/v3/userDataStream
/// Keepalived a user data stream to prevent a time out.
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
    ws: AutoConnectClient<BinanceWsOpMessage>,
    _handler: Option<JoinHandle<()>>,
}

impl WebSocketClient for BinancePublicWsClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
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

        public_ws.subscribe(&vec![
            format!("{}@trade", config.trade_symbol.to_lowercase()),
            format!("{}@depth@100ms",  config.trade_symbol.to_lowercase())
        ]).await;

        Self {
            ws: public_ws,
            _handler: None,
        }
    }

    async fn open_stream<'a>(
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

impl BinancePublicWsClient{
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
    ws: AutoConnectClient<BinanceWsOpMessage>,
    server: ExchangeConfig,
    _handler: Option<JoinHandle<()>>,
    listen_key: String,
    key_update_handler: Option<JoinHandle<()>>,
    api: BinanceRestApi,
}

impl BinancePrivateWsClient {
    pub async fn new(server: &ExchangeConfig) -> Self {
        let api = BinanceRestApi::new(server);

        let listen_key = api.create_listen_key().await.unwrap();
        let url = api.make_connect_url(&listen_key);

        let private_ws = AutoConnectClient::new(
            server,
            &MarketConfig::default(),
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
            api: BinanceRestApi::new(server)
        }
    }

    pub async fn connect(&mut self) {
        self.ws.connect().await;

        let key = self.listen_key.clone();
        let api = self.api.clone();

        let handler = tokio::task::spawn(async move {
            loop {
                sleep(Duration::from_secs(60 * 60)).await;
                let r = api.extend_listen_key(&key).await;
                log::info!("Extend listen key");
                if r.is_err() {
                    log::error!("Failed to extend listen key: {:?}", r);
                }
            }
        });

        self.key_update_handler = Some(handler);
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

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BinanceConfig;
    use crate::BinanceServerConfig;
    use rbot_lib::common::init_debug_log;

    #[tokio::test]
    async fn test_binance_public_ws_client() {
        let server = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut client = BinancePublicWsClient::new(&server, &config).await;

        // client.connect().await;

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
        let api = BinanceRestApi::new(&server);
        let _config = BinanceConfig::BTCUSDT();

        let key = api.create_listen_key().await.unwrap();
        let url = api.make_connect_url(&key);

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

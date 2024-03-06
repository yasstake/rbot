
use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;


use async_stream::stream;

use rbot_lib::{common::{
    MarketConfig, 
    MicroSec, MultiMarketMessage, ServerConfig, MICRO_SECOND, NOW}, net::{AutoConnectClient, WsOpMessage}};

use crate::{BinancePublicWsMessage, BinanceServerConfig};

use anyhow::anyhow;

/// https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
/// Ping/Keep-alive a ListenKey (USER_STREAM)
/// PUT /api/v3/userDataStream
/// Keepalive a user data stream to prevent a time out.
/// User data streams will close after 60 minutes.
/// It's recommended to send a ping about every 30 minutes.

const KEY_EXTEND_INTERVAL: MicroSec = 5 * 60 * MICRO_SECOND; // 30 min

pub const PING_INTERVAL_SEC: i64 = 60 * 3; // every 3 min
pub const SWITCH_INTERVAL_SEC: i64 = 60 * 60 * 12; // 12 hours
const SYNC_WAIT_RECORDS: i64 = 3; // no overlap

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
    handler: Option<JoinHandle<()>>,
}

impl BinancePublicWsClient {
    pub async fn new(server: &BinanceServerConfig, config: &MarketConfig) -> Self {
        let mut public_ws = AutoConnectClient::new(
            server,
            config,
            &server.get_public_ws_server(),
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
        );

        public_ws.subscribe(&config.public_subscribe_channel).await;

        Self {
            ws: public_ws,
            handler: None,
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
                    Err(e) => {
                        println!("Receive Error: {:?}", e);
                    }
                }
            }
        }
    }

    fn parse_message(message: String) -> anyhow::Result<BinancePublicWsMessage> {
        let m = serde_json::from_str::<BinancePublicWsMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(anyhow!("Error in serde_json::from_str: {:?}", message));
        }

        Ok(m.unwrap())
    }

    // TODO: implement
    fn convert_ws_message(message: BinancePublicWsMessage) -> anyhow::Result<MultiMarketMessage> {
        Ok(message.into())
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rbot_lib::common::MarketConfig;
    use crate::BinanceConfig;

    #[tokio::test]
    async fn test_binance_public_ws_client() {
        let server = BinanceServerConfig::new(true);
        let config = BinanceConfig::BTCUSDT();

        let mut client = BinancePublicWsClient::new(&server, &config).await;

        client.connect().await;

        let stream = client.open_stream().await;

        let mut stream = Box::pin(stream);

        while let Some(message) = stream.next().await {
            match message {
                Ok(m) => {
                    println!("Message: {:?}", m);
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                }
            }
        }
    }
}
#![allow(unused)]
use std::pin::Pin;

use async_stream::stream;
use futures::Stream;
use futures::StreamExt;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::AccountPair;
use rbot_lib::common::ControlMessage;
use rbot_lib::common::MarketMessage;
use rbot_lib::common::Order;
use rbot_lib::common::MARKET_HUB;
use rbot_lib::net::BroadcastMessage;
use rbot_lib::net::ReceiveMessage;
use rbot_lib::net::WebSocketClient;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use rbot_lib::common::{hmac_sign, MarketConfig, MultiMarketMessage, ExchangeConfig, NOW};

use rbot_lib::net::{AutoConnectClient, WsOpMessage};
use tokio::task::JoinHandle;

use crate::message::HyperliquidPublicWsMessage;
use crate::message::HyperliquidUserWsMessage;
use crate::HyperliquidConfig;

use super::config::HyperliquidServerConfig;

const PING_INTERVAL_SEC: i64 = 30; // 30 sec
const SWITCH_INTERVAL_SEC: i64 = 60 * 3; // 3min for test
const SYNC_WAIT_RECORDS: i64 = 0; // no overlap

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperliquidWsOpMessage {
    pub method: String,
    pub subscription: serde_json::Value,
    pub id: i64,
}

impl WsOpMessage for HyperliquidWsOpMessage {
    fn new() -> Self {
        HyperliquidWsOpMessage {
            method: "subscribe".to_string(),
            subscription: serde_json::Value::Null,
            id: NOW() % 1000,
        }
    }

    fn add_params(&mut self, params: &Vec<String>) {
        log::debug!("add_params: {:?}", params);
        // Build subscription object based on params
        if let Some(first_param) = params.first() {
            if first_param.contains("l2Book") {
                self.subscription = serde_json::json!({
                    "type": "l2Book",
                    "coin": params.get(1).unwrap_or(&"BTC".to_string())
                });
            } else if first_param.contains("trades") {
                self.subscription = serde_json::json!({
                    "type": "trades",
                    "coin": params.get(1).unwrap_or(&"BTC".to_string())
                });
            }
        }
    }

    fn make_message(&self) -> Vec<String> {
        vec![self.to_string()]
    }

    fn get_ping_message() -> String {
        r#"{"method": "ping"}"#.to_string()
    }

    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

pub struct HyperliquidPublicWsClient {
    ws: AutoConnectClient<HyperliquidWsOpMessage>,
    handler: Option<tokio::task::JoinHandle<()>>,
}

impl WebSocketClient for HyperliquidPublicWsClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        let public_ws_url = server.get_public_ws_server().clone();
        let mut public_ws = AutoConnectClient::new(
            server,
            config,
            &public_ws_url,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
            false,
        );

        Self {
            ws: public_ws,
            handler: None,
        }
    }

    async fn open_stream(&mut self) -> Pin<Box<dyn Stream<Item = MultiMarketMessage> + Send>> {
        let receiver = self.ws.connect().await;

        let stream = stream! {
            loop {
                match receiver.recv().await {
                    Ok(ReceiveMessage::Text(message)) => {
                        log::debug!("Hyperliquid WS received: {}", message);
                        
                        if let Ok(ws_message) = serde_json::from_str::<HyperliquidPublicWsMessage>(&message) {
                            match ws_message.channel.as_str() {
                                "l2Book" => {
                                    // Handle order book updates
                                    // Parse and convert to MarketMessage
                                }
                                "trades" => {
                                    // Handle trade updates
                                    // Parse and convert to MarketMessage
                                }
                                _ => {
                                    log::debug!("Unknown channel: {}", ws_message.channel);
                                }
                            }
                        }
                    }
                    Ok(ReceiveMessage::Ping(_)) => {
                        log::debug!("Received ping message");
                    }
                    Ok(ReceiveMessage::Pong(_)) => {
                        log::debug!("Received pong message");
                    }
                    Err(e) => {
                        log::error!("WebSocket error: {:?}", e);
                        let control_msg = ControlMessage {
                            status: false,
                            operation: "ws_error".to_string(),
                            message: format!("WebSocket error: {:?}", e),
                        };
                        yield MultiMarketMessage::Control(control_msg);
                    }
                }
            }
        };

        Box::pin(stream)
    }

    async fn subscribe(&mut self, subscribe_keys: Vec<String>) {
        self.ws.subscribe(subscribe_keys).await;
    }

    fn close(&mut self) {
        if let Some(handler) = self.handler.take() {
            handler.abort();
        }
        // WebSocket cleanup handled by dropping
    }
}

pub struct HyperliquidPrivateWsClient {
    ws: AutoConnectClient<HyperliquidWsOpMessage>,
    handler: Option<tokio::task::JoinHandle<()>>,
}

impl WebSocketClient for HyperliquidPrivateWsClient {
    async fn new(server: &ExchangeConfig, config: &MarketConfig) -> Self {
        let private_ws_url = server.get_private_ws_server().clone();
        let mut private_ws = AutoConnectClient::new(
            server,
            config,
            &private_ws_url,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_WAIT_RECORDS,
            None,
            None,
            true,
        );

        Self {
            ws: private_ws,
            handler: None,
        }
    }

    async fn open_stream(&mut self) -> Pin<Box<dyn Stream<Item = MultiMarketMessage> + Send>> {
        let receiver = self.ws.connect().await;

        let stream = stream! {
            loop {
                match receiver.recv().await {
                    Ok(ReceiveMessage::Text(message)) => {
                        log::debug!("Hyperliquid Private WS received: {}", message);
                        
                        if let Ok(ws_message) = serde_json::from_str::<HyperliquidUserWsMessage>(&message) {
                            match ws_message.channel.as_str() {
                                "user" => {
                                    // Handle user updates (orders, fills, etc.)
                                    // Parse and convert to MarketMessage
                                }
                                _ => {
                                    log::debug!("Unknown private channel: {}", ws_message.channel);
                                }
                            }
                        }
                    }
                    Ok(ReceiveMessage::Ping(_)) => {
                        log::debug!("Received ping message");
                    }
                    Ok(ReceiveMessage::Pong(_)) => {
                        log::debug!("Received pong message");
                    }
                    Err(e) => {
                        log::error!("Private WebSocket error: {:?}", e);
                        let control_msg = ControlMessage {
                            status: false,
                            operation: "private_ws_error".to_string(),
                            message: format!("Private WebSocket error: {:?}", e),
                        };
                        yield MultiMarketMessage::Control(control_msg);
                    }
                }
            }
        };

        Box::pin(stream)
    }

    async fn subscribe(&mut self, subscribe_keys: Vec<String>) {
        self.ws.subscribe(subscribe_keys).await;
    }

    fn close(&mut self) {
        if let Some(handler) = self.handler.take() {
            handler.abort();
        }
        // WebSocket cleanup handled by dropping
    }
}
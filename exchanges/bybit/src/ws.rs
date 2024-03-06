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
use serde_derive::Deserialize;
use serde_derive::Serialize;

use rbot_lib::common::{hmac_sign, MarketConfig, MultiMarketMessage, ServerConfig, NOW};

use rbot_lib::net::{AutoConnectClient, WsOpMessage};
use tokio::task::JoinHandle;

use crate::message::convert_coin_to_account_status;
use crate::message::merge_order_and_execution;
use crate::message::BybitExecution;
use crate::message::BybitOrderStatus;
use crate::message::BybitPublicWsMessage;
use crate::message::BybitUserMessage;
use crate::message::BybitUserWsMessage;

use super::config::BybitServerConfig;

const PING_INTERVAL_SEC: i64 = 30; // 30 sec
const SWITCH_INTERVAL_SEC: i64 = 60 * 3; // 3min for test
const SYNC_WAIT_RECORDS: i64 = 0; // no overlap

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsOpMessage {
    pub op: String,
    pub args: Vec<String>,
    pub id: i64,
}

impl WsOpMessage for BybitWsOpMessage {
    fn new() -> Self {
        BybitWsOpMessage {
            op: "subscribe".to_string(),
            args: vec![],
            id: NOW() % 1000,
        }
    }

    fn add_params(&mut self, params: &Vec<String>) {
        log::debug!("add_params: {:?} / {:?}", self.args, params);
        self.args.extend(params.clone());
    }

    fn make_message(&self) -> Vec<String> {
        let mut messages: Vec<String> = vec![];
        for arg in &self.args {
            let m = BybitWsOpMessage {
                op: "subscribe".to_string(),
                args: vec![arg.clone()],
                id: NOW() % 1000,
            };
            messages.push(m.to_string());
        }

        messages
    }

    fn get_ping_message() -> String {
        r#"{"op": "ping"}"#.to_string()
    }

    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

pub struct BybitPublicWsClient {
    ws: AutoConnectClient<BybitServerConfig, BybitWsOpMessage>,
    handler: Option<tokio::task::JoinHandle<()>>,
}

impl BybitPublicWsClient {
    fn public_url(server: &BybitServerConfig, config: &MarketConfig) -> String {
        format!(
            "{}/{}",
            server.get_public_ws_server(),
            config.trade_category
        )
    }

    pub async fn new(server: &BybitServerConfig, config: &MarketConfig) -> Self {
        let mut public_ws = AutoConnectClient::new(
            server,
            config,
            &Self::public_url(server, config),
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

    fn parse_message(message: String) -> Result<BybitPublicWsMessage, String> {
        let m = serde_json::from_str::<BybitPublicWsMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(format!("Error in serde_json::from_str: {:?}", message));
        }

        Ok(m.unwrap())
    }

    fn convert_ws_message(message: BybitPublicWsMessage) -> Result<MultiMarketMessage, String> {
        Ok(message.into())
    }
}

pub struct BybitPrivateWsClient {
    ws: AutoConnectClient<BybitServerConfig, BybitWsOpMessage>,
}

impl BybitPrivateWsClient {
    pub async fn new(server: &BybitServerConfig) -> Self {
        let dummy_config = MarketConfig::new("dummy", "dummy", "dummy", 0, 0, 0);

        let mut private_ws = AutoConnectClient::new(
            server,
            &dummy_config,
            &server.private_ws,
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            0,
            Some(Self::make_auth_message),
            None,
        );

        private_ws
            .subscribe(&vec![
                "execution".to_string(),
                "order".to_string(),
                "wallet".to_string(),
            ])
            .await;

        Self { ws: private_ws }
    }

    fn make_auth_message(server: &BybitServerConfig) -> String {
        let api_key = server.get_api_key().extract();
        let secret_key = server.get_api_secret().extract();
        let time_stamp = (NOW() / 1_000) + 1_000 * 10; // 10 seconds in the future
        let sign = hmac_sign(&secret_key, &format!("GET/realtime{}", time_stamp));

        let message = BybitWsOpMessage {
            op: "auth".to_string(),
            args: vec![api_key.to_string(), time_stamp.to_string(), sign],
            id: NOW() % 1_000,
        };

        message.to_string()
    }

    pub async fn connect(&mut self) {
        self.ws.connect().await
    }

    pub async fn open_stream<'a>(
        &'a mut self,
    ) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a {
        let mut s = Box::pin(self.ws.open_stream().await);

        stream! {
            let mut last_orders: Vec<BybitOrderStatus> = vec![];
            let mut last_executions: Vec<BybitExecution> = vec![];


            while let Some(message) = s.next().await {
                match message {
                    Ok(m) => {
                        if let ReceiveMessage::Text(m) = m {
                            let m = Self::parse_message(m);

                            match m {
                                Err(e) => {
                                    println!("Parse Error: {:?}", e);
                                    continue;
                                }
                                Ok(m) => {
                                    match m {
                                        BybitUserWsMessage::status(s) => {
                                            let control: ControlMessage = s.into();
                                            log::debug!("status message: {:?}", control);
                                            yield Ok(MultiMarketMessage::Control(control));
                                        }
                                        BybitUserWsMessage::pong(p) => {
                                            log::debug!("pong message: {:?}", p);
                                        }
                                        BybitUserWsMessage::message(m) => {
                                            match m {
                                                BybitUserMessage::order {
                                                    id,
                                                    creationTime,
                                                    mut data,
                                                } => {
                                                    println!("{}", serde_json::to_string(&data).unwrap().to_string());
                                                    if last_orders.len() == 0 {
                                                        last_orders.append(&mut data);
                                                    }
                                                    if last_executions.len() != 0 {
                                                        let order = merge_order_and_execution(&last_orders, &last_executions);
                                                        last_orders.clear();
                                                        last_executions.clear();

                                                        yield Ok(MultiMarketMessage::Order(order));
                                                    }
                                                }
                                                BybitUserMessage::execution {
                                                    id,
                                                    creationTime,
                                                    mut data,
                                                } => {
                                                    println!("{}", serde_json::to_string(&data).unwrap().to_string());
                                                    if last_executions.len() == 0 {
                                                        last_executions.append(&mut data);
                                                    }
                                                    if last_orders.len() != 0 {
                                                        let order = merge_order_and_execution(&last_orders, &last_executions);
                                                        last_orders.clear();
                                                        last_executions.clear();

                                                        yield Ok(MultiMarketMessage::Order(order));
                                                    }
                                                }
                                                BybitUserMessage::wallet {
                                                    id,
                                                    creationTime,
                                                    data,
                                                } => {
                                                    let mut coins = AccountCoins::new();
                                                    for account in data {
                                                        let mut account_coins: AccountCoins = account.into();
                                                        coins.append(account_coins);
                                                    }
                                                    yield Ok(MultiMarketMessage::Account(coins));
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        println!("Data Receive Error: {:?}", e);
                        continue;
                    }
                }
            }
        }
    }

    fn parse_message(message: String) -> Result<BybitUserWsMessage, String> {
        let m = serde_json::from_str::<BybitUserWsMessage>(&message);

        if m.is_err() {
            log::warn!("Error in serde_json::from_str: {:?}", message);
            return Err(format!("Error in serde_json::from_str: {:?}", message));
        }

        Ok(m.unwrap())
    }

    fn merge_order_and_execution(
        orders: &Vec<BybitOrderStatus>,
        executions: &Vec<BybitExecution>,
    ) -> Vec<Order> {
        merge_order_and_execution(orders, executions)
    }
}

#[cfg(test)]
mod bybit_ws_test {
    use crate::config::BybitConfig;
    use crate::config::BybitServerConfig;
    use crate::ws::BybitPublicWsClient;
    use futures::StreamExt;
    use rbot_lib::common::init_debug_log;

    use super::BybitPrivateWsClient;

    #[tokio::test]
    async fn test_bybit_public_ws() {
        init_debug_log();
        let server = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let mut ws = BybitPublicWsClient::new(&server, &config).await;

        ws.connect().await;

        let mut stream = Box::pin(ws.open_stream().await);

        let mut i = 0;
        while let Some(message) = stream.next().await {
            // println!("{:?}", message);
            i += 1;
            if 1000 < i {
                break;
            }
        }
    }

    #[tokio::test]
    async fn test_bybit_user_ws() {
        init_debug_log();
        let server = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let mut ws = BybitPrivateWsClient::new(&server).await;

        ws.connect().await;

        let mut stream = Box::pin(ws.open_stream().await);

        let mut i = 0;
        while let Some(message) = stream.next().await {
            println!("{:?}", message);
            i += 1;
            if 10 < i {
                break;
            }
        }
    }
}

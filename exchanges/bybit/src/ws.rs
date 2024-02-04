use async_stream::stream;
use futures::Stream;
use futures::StreamExt;
use polars_core::utils::rayon::vec;
use polars_core::utils::rayon::yield_local;
use rbot_lib::common::AccountStatus;
use rbot_lib::common::MarketMessage;
use rbot_lib::common::Order;
use serde_derive::Deserialize;
use serde_derive::Serialize;

use rbot_lib::common::{hmac_sign, MarketConfig, MultiMarketMessage, ServerConfig, NOW};

use rbot_lib::net::{AutoConnectClient, WsOpMessage};

use crate::market;
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

/*
pub fn make_ws_stream<'a>(
    ws: &'a mut AutoConnectClient<BybitServerConfig, BybitWsOpMessage>,
    parse_message: fn(String) -> Result<BybitUserWsMessage, String>,
) -> impl Stream<Item = Result<MultiMarketMessage, String>> + 'a {
    let mut s = Box::pin(ws.open_stream().await);

    stream! {
        while let Some(message) = s.next().await {
            match message {
                Ok(m) => {
                    let m = parse_message(m);
                    match m {
                        Err(e) => {
                            println!("Parse Error: {:?}", e);
                            continue;
                        }
                        Ok(m) => {
                            match m {
                                BybitUserWsMessage::status(s) => {
                                    log::debug!("Status message : {:?}", s);
                                }
                                BybitUserWsMessage::message(m) => {
                                    let m = Self::convert_ws_message(m);
                                    yield m;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Data Receive Error: {:?}", e);
                }
            }
        }
    }
}
*/

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

        Self { ws: public_ws }
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
                        let m = Self::parse_message(m);

                        match m {
                            Err(e) => {
                                println!("Parse Error: {:?}", e);
                                continue;
                            }
                            Ok(m) => {
                                match m {
                                    BybitPublicWsMessage::Status(s) => {
                                        log::debug!("status message: {:?}", s);
                                    }
                                    _ => {
                                        let m = Self::convert_ws_message(m);
                                        yield m;
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

struct BybitPrivateWsClient {
    ws: AutoConnectClient<BybitServerConfig, BybitWsOpMessage>,
}

impl BybitPrivateWsClient {
    pub async fn new(server: &BybitServerConfig, config: &MarketConfig) -> Self {
        let mut private_ws = AutoConnectClient::new(
            server,
            config,
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

        Self {
            ws: private_ws,
        }
    }

    /*
    fn update_last_order(&mut self, mut orders: Vec<BybitOrderStatus>) -> Vec<Order> {
        if self.last_orders.is_none() {
            self.last_orders = Some(orders);
        } else {
            log::error!("Multiple order status message received: {:?}", orders);
            self.last_orders.as_mut().unwrap().append(&mut orders);
        }

        if self.last_executions.is_none() {
            return vec![];
        }

        self.merge_orders()
    }

    fn update_last_execution(&mut self, mut execution: Vec<BybitExecution>) -> Vec<Order> {
        if self.last_executions.is_none() {
            self.last_executions = Some(execution);
            return vec![];
        } else {
            log::error!("Multiple execution message received: {:?}", execution);
            self.last_executions
                .as_mut()
                .unwrap()
                .append(&mut execution);
        }

        if self.last_orders.is_none() {
            return vec![];
        }

        self.merge_orders()
    }
    */

    fn make_auth_message(server: &BybitServerConfig) -> String {
        let api_key = server.get_api_key();
        let secret_key = server.get_api_secret();
        let time_stamp = (NOW() / 1_000) + 1_000 * 10; // 10 seconds in the future
        let sign = hmac_sign(&secret_key, &format!("GET/realtime{}", time_stamp));

        let message = BybitWsOpMessage {
            op: "auth".to_string(),
            args: vec![api_key, time_stamp.to_string(), sign],
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
                        let m = Self::parse_message(m);

                        match m {
                            Err(e) => {
                                println!("Parse Error: {:?}", e);
                                continue;
                            }
                            Ok(m) => {
                                match m {
                                    BybitUserWsMessage::status(s) => {
                                        log::debug!("status message: {:?}", s);
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
                                                if last_orders.len() == 0 {
                                                    last_orders.append(&mut data);
                                                }
                                                if last_executions.len() != 0 {
                                                    let order = merge_order_and_execution(&last_orders, &last_executions);
                                                    last_orders.clear();
                                                    last_executions.clear();

                                                    let mut market_message = MultiMarketMessage::new();
                                                    for o in order.iter() {
                                                        market_message.push(MarketMessage::Order(o.clone()));
                                                    }
                                                    yield Ok(market_message);
                                                }
                                            }
                                            BybitUserMessage::execution {
                                                id,
                                                creationTime,
                                                mut data,
                                            } => {
                                                if last_executions.len() == 0 {
                                                    last_executions.append(&mut data);
                                                }
                                                if last_orders.len() != 0 {
                                                    let order = merge_order_and_execution(&last_orders, &last_executions);
                                                    last_orders.clear();
                                                    last_executions.clear();

                                                    let mut market_message = MultiMarketMessage::new();
                                                    for o in order.iter() {
                                                        market_message.push(MarketMessage::Order(o.clone()));
                                                    }
                                                    yield Ok(market_message);
                                                }
                                            }
                                            BybitUserMessage::wallet {
                                                id,
                                                creationTime,
                                                data,
                                            } => {
                                                let mut market_message = MultiMarketMessage::new();
                                                for account in data.iter() {
                                                    let a: AccountStatus = account.into();
                                                    market_message.push(MarketMessage::Account(a));
                                                }
                                                yield Ok(market_message);
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

    /*
    fn convert_to_multimarketmessage(
        &mut self,
        m: BybitUserMessage,
    ) -> Result<MultiMarketMessage, String> {
        match m {
            BybitUserMessage::order {
                id,
                creationTime,
                data,
            } => {
                let order = self.update_last_order(data);

                if order.len() == 0 {
                    return Err("No order message".to_string());
                }
                let mut market_message = MultiMarketMessage::new();
                for o in order.iter() {
                    market_message.push(MarketMessage::Order(o.clone()));
                }
                Ok(market_message)
            }
            BybitUserMessage::execution {
                id,
                creationTime,
                data,
            } => {
                let order = self.update_last_execution(data);

                if order.len() == 0 {
                    return Err("No execution message".to_string());
                }
                let mut market_message = MultiMarketMessage::new();
                for o in order.iter() {
                    market_message.push(MarketMessage::Order(o.clone()));
                }
                Ok(market_message)
            }
            BybitUserMessage::wallet {
                id,
                creationTime,
                data,
            } => {
                let mut market_message = MultiMarketMessage::new();
                for account in data.iter() {
                    let a: AccountStatus = account.into();
                    market_message.push(MarketMessage::Account(a));
                }
                Ok(market_message)
            }
        }
    }
    */
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

        let mut ws = BybitPrivateWsClient::new(&server, &config).await;

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

/*
    pub fn connect(&mut self) {
        BLOCK_ON(async {
        BLOCK_ON(async {
            self.ws.connect(|message|{
                let m = serde_json::from_str::<BybitWsMessage>(&message);

                if m.is_err() {
                    log::warn!("Error in serde_json::from_str: {:?}", message);
                    println!("ERR: {:?}", message);
                }

                let m = m.unwrap();

                return m.into();
            }).await
        })
    }

    pub fn open_channel(&mut self) -> Receiver<MultiMarketMessage> {
        BLOCK_ON(async {
            self.ws.open_channel().await
        })
    }
}
*/

/*
pub async fn listen_userdata_stream<F>(
    config: &BybitServerConfig,
    mut f: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(BybitUserStreamMessage) + Send + 'static,
{
    let url = config.private_ws.clone();

    let mut websocket: AutoConnectClient<BybitServerConfig, BybitWsOpMessage> =
        AutoConnectClient::new(

            config,
            &url,
            // Arc::new(RwLock::new(message)),
            PING_INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_RECORDS,
            Some(make_auth_message),
        );

    websocket
        .subscribe(&vec![
            "execution".to_string(),
            "order".to_string(),
            "wallet".to_string(),
        ])
        .await;

    websocket.connect().await;

    let handle = tokio::task::spawn(async move {
        loop {
            let msg = websocket.receive_text().await;
            if msg.is_err() {
                log::warn!("Error in websocket.receive_message: {:?}", msg);
                continue;
            }
            let msg = msg.unwrap();
            log::debug!("raw msg: {}", msg);
            let msg = serde_json::from_str::<BybitUserStreamMessage>(msg.as_str());
            if msg.is_err() {
                log::warn!("Error in serde_json::from_str: {:?}", msg);
                continue;
            }
            let msg = msg.unwrap();
            f(msg);
        }
    });

    handle
}

*/

/*
#[cfg(test)]
mod test_ws {
    use crate::common::init_debug_log;

    #[test]
    fn test_make_auth_message() {
        use crate::exchange::bybit::config::BybitServerConfig;
        let config = BybitServerConfig::new(false);
        let msg = super::make_auth_message(&config);
        println!("{}", msg);
    }

    #[test]
    fn test_connect() {
        use crate::exchange::bybit::config::BybitServerConfig;
        use crate::exchange::bybit::ws::listen_userdata_stream;
        let config = BybitServerConfig::new(true);

        init_debug_log();

        listen_userdata_stream(&config, |msg| {
            println!("{:?}", msg);
        });

        std::thread::sleep(std::time::Duration::from_secs(5));
    }
}

/*
TODO: reconnet ws when disconnected.
[2024-01-20T04:55:43Z WARN  rbot::exchange::bybit::ws] Error in websocket.receive_message: Err("Disconnected wss://stream-testnet.bybit.com/v5/private: Trying to work with closed connection")
[2024-01-20T04:55:43Z ERROR rbot::exchange::ws] Disconnected from server
*/

*/

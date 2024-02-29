

use futures::Stream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;


use async_stream::stream;

use rbot_lib::{common::{MarketConfig, MicroSec, MultiMarketMessage, ServerConfig, MICRO_SECOND, NOW}, net::{AutoConnectClient, WsOpMessage}};

use crate::{BinanceConfig, BinancePublicWsMessage, BinanceServerConfig};

use anyhow::anyhow;

/// https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
/// Ping/Keep-alive a ListenKey (USER_STREAM)
/// PUT /api/v3/userDataStream
/// Keepalive a user data stream to prevent a time out.
/// User data streams will close after 60 minutes.
/// It's recommended to send a ping about every 30 minutes.

const KEY_EXTEND_INTERVAL: MicroSec = 5 * 60 * MICRO_SECOND; // 30 min

fn make_user_stream_endpoint(server: &BinanceServerConfig, key: String) -> String {
    let url = format!("{}/{}", server.get_public_ws_server(), key);

    return url;
}

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
    handler: Option<tokio::task::JoinHandle<()>>,
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
        //Ok(message.into())
        todo!();
    }
}


#[cfg(test)]
mod test_binance_ws{
    use super::*;
    use rbot_lib::common::MarketConfig;
    use rust_decimal_macros::dec;
    use std::time::Duration;
    use tokio::time::sleep;

    use crate::{BinanceConfig, BinanceServerConfig};

    /*
    #[tokio::test]
    async fn test_binance_ws() {
        let server = BinanceServerConfig::new(false);
        let market = BinanceConfig::BTCUSDT();

        let mut ws = BinancePublicWsClient::new(&server, &market).await;
        ws.connect().await;

        let mut stream = ws.open_stream().await;

        let pin_stream = Box::pin(stream);

        loop {
            let message = pin_stream.next().await;
            println!("message: {:?}", message);
            sleep(Duration::from_secs(1)).await;
        }
    }
    */
}







/*








pub fn listen_userdata_stream<F>(config: &BinanceConfig, mut f: F) -> JoinHandle<()>
where
    F: FnMut(BinanceUserStreamMessage) + Send + 'static,
{
    let key = create_listen_key(&config).unwrap();
    let url = make_user_stream_endpoint(config, key.clone());

    let message = BinanceWsOpMessage::new();

    let mut websocket: AutoConnectClient<BinanceConfig, BinanceWsOpMessage> =
        AutoConnectClient::new(
            &config,
            url.as_str(),
            // Arc::new(RwLock::new(message)),
            PING_INTERVAL,
            SWITCH_INTERVAL,
            SYNC_RECORDS_FOR_USER,
            None,
        );

    websocket.connect();

    let now = NOW();
    let mut key_extend_timer: MicroSec = now;

    let cc = config.clone();

    // TODO: change to async
    let handle = tokio::task::spawn(async move {
        let config = cc;
        loop {
            let msg = websocket.receive_text().await;

            if msg.is_err() {
                log::warn!("Error in websocket.receive_message: {:?}", msg);
                continue;
            }

            let msg = msg.unwrap();
            log::debug!("raw msg: {}", msg);

            let msg = serde_json::from_str::<BinanceUserStreamMessage>(msg.as_str());

            if msg.is_err() {
                log::warn!("Error in serde_json::from_str: {:?}", msg);
                continue;
            }

            let msg = msg.unwrap();
            f(msg);

            let now = NOW();

            if key_extend_timer + KEY_EXTEND_INTERVAL < now {
                match extend_listen_key(&config, &key.clone()) {
                    Ok(key) => {
                        log::debug!("Key extend success: {:?}", key);
                    }
                    Err(e) => {
                        let key = create_listen_key(&config);

                        websocket.url = make_user_stream_endpoint(&config, key.unwrap());
                        log::error!("Key extend error: {}  / NEW url={}", e, websocket.url);
                    }
                }
                key_extend_timer = now;
            }
        }
    });

    return handle;
}
*/

/*
#[test]
fn test_listen_userdata_stream() {
    use crate::common::init_debug_log;
    use crate::common::OrderSide;
    use crate::exchange::binance::rest::new_limit_order;
    use crate::exchange::binance::ws::listen_userdata_stream;
    use crate::exchange::binance::BinanceConfig;
    use rust_decimal_macros::dec;
    use std::thread::sleep;
    use std::time::Duration;

    let config = BinanceConfig::TESTSPOT("BTC", "BUSD");
    init_debug_log();
    listen_userdata_stream(&config, |msg| {
        println!("msg: {:?}", msg);
    });

    new_limit_order(
        &config,
        OrderSide::Buy,
        dec![25000.0],
        dec![0.001],
        Some(&"TestForWS"),
    )
    .unwrap();

    sleep(Duration::from_secs(60 * 1));
}

*/

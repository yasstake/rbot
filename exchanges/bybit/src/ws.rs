
use rbot_lib::common::{
    NOW,SEC,
    ServerConfig,
    hmac_sign, 
};
    
use rbot_lib::net::{
        AutoConnectClient, 
        WebSocketClient,
        WsOpMessage,
};
use serde_derive::Serialize;
use serde_derive::Deserialize;


use crate::message::BybitUserStreamMessage;


use super::config::BybitServerConfig;

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

    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}







pub fn make_auth_message(config: &BybitServerConfig) -> String {
    let api_key = config.get_api_key();
    let secret_key = config.get_api_secret();
    let time_stamp = (NOW() / 1_000) + 1_000 * 10; // 10 seconds in the future
    let sign = hmac_sign(&secret_key, &format!("GET/realtime{}", time_stamp));

    let message = BybitWsOpMessage {
        op: "auth".to_string(),
        args: vec![api_key, time_stamp.to_string(), sign],
        id: NOW() % 1_000,
    };

    message.to_string()
}

const INTERVAL_SEC: i64 = 20;
//const SWITCH_INTERVAL: i64 = 60 * 60 * 12; // 12 hours
const SWITCH_INTERVAL_SEC: i64 = 60 * 3; // 3min for test
pub const SYNC_RECORDS: i64 = 0; // no overlap


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
            INTERVAL_SEC,
            SWITCH_INTERVAL_SEC,
            SYNC_RECORDS,
            Some(make_auth_message),
        );

    websocket.subscribe(&vec![
        "execution".to_string(),
        "order".to_string(),
        "wallet".to_string(),
    ]).await;

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

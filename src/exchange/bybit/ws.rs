use std::{
    sync::{Arc, RwLock},
    thread::{self, JoinHandle},
};

use openssl::sign;

use crate::{
    common::NOW,
    exchange::{hmac_sign, AutoConnectClient, BybitWsOpMessage, WsOpMessage, bybit::message::BybitUserStreamMessage},
};

use super::config::BybitServerConfig;

fn make_auth_message(config: &BybitServerConfig) -> String {
    let api_key = config.api_key.clone();
    let secret_key = config.api_secret.clone();
    let time_stamp = (NOW() / 1_000) + 1_000 * 10; // 10 seconds in the future
    let sign = hmac_sign(&secret_key, &format!("GET/realtime{}", time_stamp));

    let message = BybitWsOpMessage {
        op: "auth".to_string(),
        args: vec![api_key, time_stamp.to_string(), sign],
        id: NOW() % 1_000,
    };

    message.to_string()
}

pub fn listen_userdata_stream<F>(config: &BybitServerConfig, mut f: F) -> JoinHandle<()> 
where
    F: FnMut(BybitUserStreamMessage) + Send + 'static
{

    let url = config.private_ws.clone();

    let mut message = BybitWsOpMessage::new();
    message.add_params(&vec!["execution".to_string(), "wallet".to_string()]);

    let auth_message = make_auth_message;

    let mut websocket: AutoConnectClient<BybitServerConfig, BybitWsOpMessage> =
        AutoConnectClient::new(
            config,
            &url,
            Arc::new(RwLock::new(message)),
            Some(make_auth_message),
        );

    websocket.connect();
    let handle = thread::spawn(move || loop {
        let msg = websocket.receive_message();
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
    });
    handle
}

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

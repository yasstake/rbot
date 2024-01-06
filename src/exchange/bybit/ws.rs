use std::sync::{Arc, RwLock};

use openssl::sign;

use crate::{
    common::NOW,
    exchange::{hmac_sign, AutoConnectClient, BybitWsOpMessage, WsOpMessage},
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



pub fn listen_userdata_stream<F>(config: &BybitServerConfig, mut f: F) {
    let url = config.private_ws.clone();

    let mut message = BybitWsOpMessage::new();
    message.add_params(&vec!["execution".to_string()]);

    let auth_message = make_auth_message;

    let mut websocket: AutoConnectClient<BybitServerConfig, BybitWsOpMessage> = AutoConnectClient::new(
        config,
        &url,
        Arc::new(RwLock::new(message)),
        None
    );


    /*

        let message = BybitWsOpMessage::new();
        let mut websocket: AutoConnectClient<BybitWsOpMessage> = AutoConnectClient::new(
            url.as_str(),
            Arc::new(RwLock::new(message)),
        );
        websocket.connect();
        let now = NOW();
        let mut key_extend_timer: MicroSec = now;
        let cc = config.clone();
        let handle = thread::spawn(move || {
            let config = cc;
            loop {
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
        handle

    */
}

#[cfg(test)]
mod test_ws {
    #[test]
    fn test_make_auth_message() {
        use crate::exchange::bybit::config::BybitServerConfig;
        let config = BybitServerConfig::new(false);
        let msg = super::make_auth_message(&config);
        println!("{}", msg);
    }
}

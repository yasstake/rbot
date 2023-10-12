use futures::future::Join;
use reqwest::Url;
use rust_decimal_macros::dec;
use serde_json::json;
use tungstenite::WebSocket;
use tungstenite::connect;
use tungstenite::Message;
use tungstenite::stream::MaybeTlsStream;

use std::net::TcpStream;
use std::thread;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::time::Duration;

use crate::common::init_debug_log;
use crate::common::OrderSide;
use crate::common::MICRO_SECOND;
use crate::common::MicroSec;
use crate::common::NOW;
use crate::exchange::AutoConnectClient;

use crate::exchange::binance::message::BinanceUserStreamMessage;
use crate::exchange::binance::rest::extend_listen_key;
use crate::exchange::binance::rest::new_limit_order;

use super::BinanceConfig;
use super::rest::create_listen_key;

/// TODO: Optimize this interval.
const KEY_EXTEND_INTERVAL:MicroSec = 5 * 60 * MICRO_SECOND;    // 24 min

fn make_user_stream_endpoint(config: &BinanceConfig, key: String) -> String {
    let url = format!("{}/{}", config.private_ws_endpoint, key);

    return url;
}

pub fn listen_userdata_stream<F>(config: &BinanceConfig, mut f: F) -> JoinHandle<()>
where
    F: FnMut(BinanceUserStreamMessage) + Send + 'static
{
    let key = create_listen_key(&config).unwrap();
    let url = make_user_stream_endpoint(config, key.clone());

    let mut websocket = AutoConnectClient::new(
            url.as_str(),
            None);
    
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
                    },
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

#[test]
fn test_listen_userdata_stream() {
    use crate::exchange::binance::BinanceConfig;
    use crate::exchange::binance::ws::listen_userdata_stream;

    let config = BinanceConfig::TESTSPOT("BTC", "BUSD");
    init_debug_log();
    listen_userdata_stream(&config, |msg| {
        println!("msg: {:?}", msg);
    });

    new_limit_order(&config, OrderSide::Buy, dec![25000.0], dec![0.001], Some(&"TestForWS")).unwrap();

    sleep(Duration::from_secs(60*1));
}



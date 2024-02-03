use std::sync::Arc;
use std::sync::RwLock;
use std::thread;
use tokio::task::JoinHandle;

use tokio::runtime::Runtime;

use crate::common::MicroSec;
use crate::common::MICRO_SECOND;
use crate::common::NOW;
use crate::exchange::AutoConnectClient;

use crate::exchange::binance::message::BinanceUserStreamMessage;
use crate::exchange::binance::rest::extend_listen_key;
use crate::exchange::BinanceWsOpMessage;
use crate::exchange::WsOpMessage;

use super::rest::create_listen_key;
use super::BinanceConfig;

/// https://binance-docs.github.io/apidocs/spot/en/#listen-key-spot
/// Ping/Keep-alive a ListenKey (USER_STREAM)
/// PUT /api/v3/userDataStream
/// Keepalive a user data stream to prevent a time out.
/// User data streams will close after 60 minutes.
/// It's recommended to send a ping about every 30 minutes.
const KEY_EXTEND_INTERVAL: MicroSec = 5 * 60 * MICRO_SECOND; // 30 min

fn make_user_stream_endpoint(config: &BinanceConfig, key: String) -> String {
    let url = format!("{}/{}", config.private_ws_endpoint, key);

    return url;
}

pub const PING_INTERVAL: i64 = 60 * 3; // every 3 min
pub const SWITCH_INTERVAL: i64 = 60 * 60 * 12; // 12 hours
const SYNC_RECORDS_FOR_USER: i64 = 0; // no overlap

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

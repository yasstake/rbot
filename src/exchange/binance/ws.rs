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

use crate::common::order::OrderSide;
use crate::exchange::AutoConnectClient;

use crate::exchange::binance::message::BinanceUserStreamMessage;
use crate::exchange::binance::rest::new_limit_order;

use super::BinanceConfig;
use super::rest::create_listen_key;


pub fn listen_userdata_stream(config: &BinanceConfig) {
    let key = create_listen_key(&config);

    let url = format!("{}/{}", config.private_ws_endpoint, key.unwrap());

    let mut websocket = AutoConnectClient::new(
            url.as_str(),
            None);
    
    websocket.connect();

    let mut count = 0;

    thread::spawn(move || {
        loop {
            let msg = websocket.receive_message();
            let msg = msg.unwrap();
            println!("raw msg: {}", msg);

            let msg = serde_json::from_str::<BinanceUserStreamMessage>(msg.as_str());
            println!("cooked : {:?}", msg.unwrap());            
        }
    });
}

#[test]
fn test_listen_userdata_stream() {
    use crate::exchange::binance::BinanceConfig;
    use crate::exchange::binance::ws::listen_userdata_stream;

    let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

    listen_userdata_stream(&config);

    sleep(Duration::from_secs(1));

    new_limit_order(&config, OrderSide::Buy, dec![25000.0], dec![0.001]);

    sleep(Duration::from_secs(10));
}



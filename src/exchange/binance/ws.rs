use reqwest::Url;
use serde_json::json;
use tungstenite::WebSocket;
use tungstenite::connect;
use tungstenite::Message;
use tungstenite::stream::MaybeTlsStream;

use std::net::TcpStream;
use std::thread::JoinHandle;
use std::thread::sleep;
use std::time::Duration;

const ENDPOINT: &str = "wss://stream.binance.com";

const PING_INTERVAL: u64 = 30;

// start websocket loop in a new thread
fn start_websocket_loop() {
    std::thread::spawn(move || {
        websocket_loop();
    });
}

// connect to ws server ()
fn websocket_loop() {
    let url = format!("{}{}", ENDPOINT, "/ws");

    let (mut socket, response) =
        tungstenite::connect(Url::parse(&url).unwrap()).expect("Can't connect");

    println!("Connected to the server");

    println!("Response HTTP code: {}", response.status());
    println!("Response contains the following headers:");

    for (ref header, _value) in response.headers() {
        println!("* {}", header);
    }

    let subscribe = json!({
        "method": "SUBSCRIBE",
        "params": [
            "btcusdt@trade",
            "btcusdt@depth"
        ],
        "id": 1
    });

    sleep(Duration::from_secs(1));

    println!("Sending subscribe message: {}", subscribe.to_string());

    let result = socket.write(Message::Text(subscribe.to_string()));

    socket.flush().unwrap();

    socket.write(Message::Ping(vec![]));

    socket.flush().unwrap();

    match result {
        Ok(_) => {
            println!("Sent subscribe message");
        }
        Err(e) => {
            println!("Error: {:?}", e.to_string());
        }
    }

    loop {
        let msg = socket.read().unwrap();

        match msg {
            Message::Text(t) => {
                println!("TEXT: {}", t);
            }
            Message::Binary(b) => {
                println!("BINARY: {:?}", b);
            }
            Message::Ping(p) => {
                println!("PING: {:?}", p);
            }
            Message::Pong(p) => {
                println!("PONG: {:?}", p);
            }
            Message::Close(c) => {
                println!("CLOSE: {:?}", c);
            }
            Message::Frame(f) => {
                println!("FRAME: {:?}", f);
            }
        }
    }
}

#[cfg(test)]
mod binance_ws_tests {
    use super::*;

    #[test]
    fn test_websocket_loop() {
        start_websocket_loop();

        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_websocket_connect() {
        // Connect to the Binance WebSocket API
        let result = connect("wss://stream.binance.com:9443/ws/btcusdt@trade");

        match result {
            Ok(stream) => {}
            Err(e) => {
                println!("Error: {:?}", e.to_string());
            }
        }
    }
}

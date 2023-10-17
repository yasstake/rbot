//pub mod bb;
//pub mod binance;
//pub mod orderbook;

//pub use orderbook::*;

// pub mod ftx;

use core::panic;
use std::{
    fs::File,
    io::{copy, BufReader, Cursor, Write},
    net::TcpStream,
    path::Path,
};

use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use reqwest::Method;
use serde_json::Value;
use tempfile::tempdir;
use url::Url;
use zip::ZipArchive;


use crate::common::{Trade,MicroSec, HHMM, MICRO_SECOND, NOW, SEC};

use crossbeam_channel::Sender;
// use crossbeam_channel::Receiver;

use tungstenite::Message;
use tungstenite::{connect, stream::MaybeTlsStream};
use tungstenite::{http::request, protocol::WebSocket};


pub struct WebSocketClient {
    connection: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    url: String,
    subscribe_message: Option<Value>,
}

impl WebSocketClient {
    pub fn new(url: &str, subscribe_message: Option<Value>) -> Self {
        WebSocketClient {
            connection: None,
            url: url.to_string(),
            subscribe_message: subscribe_message,
        }
    }

    pub fn connect(&mut self) {
        let url = Url::parse(&self.url).unwrap();

        let result = connect(url);

        if result.is_err() {
            log::error!("Can't connect to {}", self.url);
            panic!("Can't connect to {}", self.url);
        }

        let (mut socket, response) = result.unwrap();

        log::debug!("Connected to the server");
        log::debug!("Response HTTP code: {}", response.status());
        log::debug!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            log::debug!("* {}", header);
        }

        self.connection = Some(socket);

        if self.subscribe_message.is_some() {
            self.send_message(self.subscribe_message.clone().unwrap().to_string().as_str());
        }
        self.flush();
    }

    pub fn send_message(&mut self, message: &str) {
        // TODO: check if connection is established.
        let connection = self.connection.as_mut().unwrap();
        let result = connection.write(Message::Text(message.to_string()));

        match result {
            Ok(_) => {
                log::debug!("Sent message {}", message);
            }
            Err(e) => {
                log::error!("Error: {:?}", e.to_string());
            }
        }
    }

    pub fn send_ping(&mut self) {
        log::debug!("*>PING*>");
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Ping(vec![]));
        self.flush();
    }

    pub fn send_pong(&mut self, message: Vec<u8>) {
        log::debug!("*>PONG*>: {:?}", message);
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Pong(message));
        self.flush();
    }

    pub fn close(&mut self) {
        let connection = self.connection.as_mut().unwrap();
        connection.close(None);
    }

    pub fn flush(&mut self) {
        let connection = self.connection.as_mut().unwrap();
        connection.flush().unwrap();
    }

    pub fn has_message(&self) -> bool {
        let connection = self.connection.as_ref().unwrap();
        connection.can_read()
    }

    pub fn receive_message(&mut self) -> Result<String, String> {
        let connection = self.connection.as_mut().unwrap();

        let message = connection.read();

        if message.is_err() {
            log::error!("Disconnected from server");
            return Err(format!("Disconnected {}: {}", self.url, message.unwrap_err()));
        }

        let message = message.unwrap();

        match message {
            Message::Text(t) => {
                return Ok(t);
            }
            Message::Binary(b) => {
                log::debug!("BINARY: {:?}", b);
            }
            Message::Ping(p) => {
                log::debug!("<PING<: {:?}", p);
                self.send_pong(p);
            }
            Message::Pong(p) => {
                log::debug!("<PONG<: {:?}", p);
            }
            Message::Close(c) => {
                log::debug!("CLOSE: {:?}", c);
                return Err("Closed".to_string());
            }
            Message::Frame(_) => {}
        }

        return self.receive_message();
    }
}

pub struct AutoConnectClient {
    client: Option<WebSocketClient>,
    next_client: Option<WebSocketClient>,
    pub url: String,
    subscribe_message: Option<Value>,
    last_message: String,
    last_connect_time: MicroSec,
    last_ping_time: MicroSec,
    sync_interval: MicroSec,
    sync_records: i64,
    sync_mode: bool,
}

const SYNC_RECORDS: i64 = 3;
// TODO: change sync interval (possibly 12 hours)
const SYNC_INTERVAL: MicroSec = MICRO_SECOND * 60 * 5; // every 5 min for test
const PING_INTERVAL: MicroSec = MICRO_SECOND * 60 * 3; // every 3 min

impl AutoConnectClient {
    pub fn new(url: &str, message: Option<Value>) -> Self {
        AutoConnectClient {
            client: None,
            next_client: None,
            url: url.to_string(),
            subscribe_message: message.clone(),
            last_message: "".to_string(),
            last_connect_time: 0,
            last_ping_time: NOW(),
            sync_interval: SYNC_INTERVAL,
            sync_records: 0,
            sync_mode: false,
        }
    }

    pub fn connect(&mut self) {
        self.client = Some(WebSocketClient::new(
            self.url.as_str(),
            self.subscribe_message.clone(),
        ));
        self.client.as_mut().unwrap().connect();
        self.last_connect_time = NOW();
    }

    pub fn connect_next(&mut self, url: Option<String>) {
        if url.is_some() {
            self.url = url.unwrap();
        }

        self.next_client = Some(WebSocketClient::new(
            self.url.as_str(),
            self.subscribe_message.clone(),
        ));
        self.next_client.as_mut().unwrap().connect();
        self.last_connect_time = NOW();        
    }

    pub fn switch(&mut self) {
        self.client.as_mut().unwrap().close();
        self.client = self.next_client.take();
        self.next_client = None;

        log::info!("------WS switched-{}-----", self.url);
    }

    pub fn receive_message(&mut self) -> Result<String, String> {
        let now = NOW();

        // if connection exceed sync interval, reconnect
        if (self.last_connect_time + self.sync_interval < now) && self.next_client.is_none() {
            self.connect_next(None);
        }


        if self.last_ping_time + PING_INTERVAL < now {
            self.client.as_mut().unwrap().send_ping();
            self.last_ping_time = now;
        }

        // if the connection_next is not None, receive message
        if self.next_client.is_some() && self.next_client.as_ref().unwrap().has_message() {
            self.sync_mode = true;
        }

        if self.sync_mode {
            if SYNC_RECORDS < self.sync_records {
                self.switch();

                loop {
                    let message = self._receive_message();

                    if message.is_err() {
                        log::warn!("Disconnected from server before sync complete {}: {:?}", self.url, message.unwrap_err());
                        self.client.as_mut().unwrap().close();
                        self.client = None;

                        self.last_message = "".to_string();                        
                        self.sync_mode = false;
                        self.sync_records = 0;
                        log::warn!("few records may be lost");
                        break;
                    }                    

                    let message = message.unwrap();

                    log::debug!("SYNC: {} / {}", message, self.last_message);

                    if message == self.last_message
                    {
                        self.last_message = "".to_string();
                        self.sync_mode = false;
                        self.sync_records = 0;

                        break;
                    }

                    if self.sync_records == 0 {
                        self.last_message = "".to_string();
                        self.sync_mode = false;
                        self.sync_records = 0;

                        break;
                    }

                    self.sync_records -= 1;
                }
            }
            else {
                log::info!("SYNC:({})/ {}", self.url, self.sync_records);
                let message = self._receive_message();

                if message.is_ok() {
                    let m = message.unwrap();
                    self.last_message = m.clone();

                    self.sync_records += 1;

                    return Ok(m);
                }

                log::warn!("Disconnected from server before sync start {}: {:?}", self.url, message);
                self.last_message = "".to_string();
                self.sync_mode = false;
                self.sync_records = 0;

                return message;                
            }
        }

        return self._receive_message();
    }

    fn _receive_message(&mut self) -> Result<String, String> {
        let client = self.client.as_mut();
        if client.is_none() {
            self.connect();
        }        

        let result = self.client.as_mut().unwrap().receive_message();

        match result {
            Ok(_) => {
                return result;
            }
            Err(e) => {
                log::debug!("recive error{}", e);
                Err(e)
            }
        }
    }

    pub fn send_ping(&mut self) {
        self.client.as_mut().unwrap().send_ping();
    }
}

#[cfg(test)]
mod test_exchange_ws {
    use serde_json::json;

    use super::*;
    use crate::common::init_debug_log;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn ws_loop() {
        let mut ws1 = WebSocketClient::new(
            "wss://stream.binance.com/ws",
            std::option::Option::Some(json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        "btcusdt@depth"
                    ],
                    "id": 1
                }
            )),
        );

        ws1.connect();

        let mut ws2 = WebSocketClient::new(
            "wss://stream.binance.com/ws",
            Some(json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        "btcusdt@depth"
                    ],
                    "id": 1
                }
            )),
        );

        ws2.connect();

        spawn(move || loop {
            let m = ws1.receive_message();
            println!("1: {}", m.unwrap());

            let m = ws2.receive_message();
            println!("2: {}", m.unwrap());
        });

        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_reconnect() {
        let mut ws = AutoConnectClient::new(
            "wss://stream.binance.com/ws",

            Some(json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        // "btcusdt@depth"
                    ],
                    "id": 1
                }
            ))
        );

        ws.connect();

        spawn(move || loop {
            let m = ws.receive_message();
            println!("{}", m.unwrap());
        });

        sleep(Duration::from_secs(20));
    }
}


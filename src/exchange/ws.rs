// Copyright(c) 2022-2023. yasstake. All rights reserved.

use core::panic;
use crossbeam_channel::{Receiver, Sender};
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::net::TcpStream;
use std::rc::Rc;
use std::sync::{Arc, Mutex, RwLock};
use url::Url;

use crate::common::{MicroSec, MultiChannel, MICRO_SECOND, NOW};
use tungstenite::protocol::WebSocket;
use tungstenite::Message;
use tungstenite::{connect, stream::MaybeTlsStream};


pub trait WsMessage {
    fn new() -> Self;
    fn add_params(&mut self, params: &Vec<String>);
    fn to_string(&self) -> String;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceWsMessage {
    method: String,
    params: Vec<String>,
    id: i64,
}

impl WsMessage for BinanceWsMessage {
    fn new() -> Self {
        BinanceWsMessage {
            method: "SUBSCRIBE".to_string(),
            params: vec![],
            id: NOW() % 1000,
        }
    }

    fn add_params(&mut self, params: &Vec<String>) {
        log::debug!("add_params: {:?} / {:?}", self.params, params);
        self.params.extend(params.clone());
    }

    fn to_string(&self) -> String {
        if self.params.len() == 0 {
            return "".to_string();
        } else {
            return serde_json::to_string(&self).unwrap();
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BybitWsMessage {
    op: String,
    args: Vec<String>,
    id: i64,
}

impl WsMessage for BybitWsMessage {
    fn new() -> Self {
        BybitWsMessage {
            op: "subscribe".to_string(),
            args: vec![],
            id: NOW() % 1000,
        }
    }

    fn add_params(&mut self, params: &Vec<String>) {
        log::debug!("add_params: {:?} / {:?}", self.args, params);
        self.args.extend(params.clone());
    }

    fn to_string(&self) -> String {
        if self.args.len() == 0 {
            return "".to_string();
        } else {
            return serde_json::to_string(&self).unwrap();
        }
    }
}



pub struct WebSocketClient<T>
where T: WsMessage + Send + Sync + 'static
{
    url: String,
    handle: Option<std::thread::JoinHandle<()>>,
    connection: Arc<RwLock<AutoConnectClient<T>>>,
    subscribe_list: Arc<RwLock<T>>,
    control_ch: MultiChannel<String>, // send sbscribe message
}

impl <T>WebSocketClient<T>
where T: WsMessage + Send + Sync + 'static
{
    pub fn new(url: &str, subscribe: Vec<String>) -> Self {
        let subscrive_list = Arc::new(RwLock::new(T::new()));
        subscrive_list.as_ref().write().unwrap().add_params(&subscribe);

        WebSocketClient {
            url: url.to_string(),
            handle: None,
            connection: Arc::new(RwLock::new(AutoConnectClient::new(
                url,
                subscrive_list.clone(),
            ))),
            subscribe_list: subscrive_list.clone(),
            control_ch: MultiChannel::new(),
        }
    }

    /// connect to websocket server
    /// start listening thread
    pub fn connect(&mut self) -> Receiver<String> {
        // where T: Clone {
        let subscribe_list = self.subscribe_list.clone();
        let control_ch = self.control_ch.open_channel(0);

        let mut receive_ch: MultiChannel<String> = MultiChannel::new();
        let ch = receive_ch.open_channel(100);

        let mut websocket = self.connection.clone();

        let handle = std::thread::spawn(move || {
            let mut ws= websocket.write().unwrap();
            ws.connect();
            drop(ws);
            
            loop {
                let mut ws = websocket.write().unwrap();
                let message = ws.receive_message();
                drop(ws);

                receive_ch.send(message.unwrap());
            }
        });

        self.handle = Some(handle);

        return ch;
    }


    /// Append subscribe list and send subscribe message to send queue
    pub fn subscribe(&mut self, message: &Vec<String>) {
        log::debug!("subscribe: {:?}", message);

        let mut lock = self.subscribe_list.as_ref().write().unwrap();
        lock.add_params(message);
        drop(lock);

        let message = self.subscribe_list.as_ref().read().unwrap().to_string();

        log::debug!("call subscribe: {:?}", message);

        let websocket = self.connection.clone();
        let mut lock = websocket.write().unwrap();
        lock.send_message(&message);
        drop(lock);
    }

    pub fn switch(&mut self) {
        let websocket = self.connection.clone();
        let mut lock = websocket.write().unwrap();
        lock.switch();
        drop(lock);
    }

    /// get receive queue
    pub fn open_channel(&mut self, channel: &str) {}
}

pub struct SimpleWebsocket<T> {
    connection: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    url: String,
    subscribe_message: Arc<RwLock<T>>,
}

impl <T>SimpleWebsocket<T>
    where T: WsMessage
{
    pub fn new(url: &str, subscribe_message: Arc<RwLock<T>>) -> Self {
        SimpleWebsocket {
            connection: None,
            url: url.to_string(),
            subscribe_message: subscribe_message.clone(),
        }
    }

    pub fn connect(&mut self) {
        let url = Url::parse(&self.url).unwrap();

        let result = connect(url);

        if result.is_err() {
            log::error!("Can't connect to {}", self.url);
            panic!("Can't connect to {}", self.url);
        }

        let (socket, response) = result.unwrap();

        log::debug!("Connected to the server");
        log::debug!("Response HTTP code: {}", response.status());
        log::debug!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            log::debug!("* {}", header);
        }

        self.connection = Some(socket);

        let message: String = self.subscribe_message.read().unwrap().to_string();

        if message != "" {
            self.send_message(&message);
        }
        self.flush();
    }

    pub fn send_message(&mut self, message: &str) {
        if message == "" {
            log::warn!("Empty message");
            return;
        }

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
        let _ = connection.write(Message::Ping(vec![]));
        self.flush();
    }

    pub fn send_pong(&mut self, message: Vec<u8>) {
        log::debug!("*>PONG*>: {:?}", message);
        let connection = self.connection.as_mut().unwrap();
        let _ = connection.write(Message::Pong(message));
        self.flush();
    }

    pub fn close(&mut self) {
        let connection = self.connection.as_mut().unwrap();
        let _ = connection.close(None);
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
            return Err(format!(
                "Disconnected {}: {}",
                self.url,
                message.unwrap_err()
            ));
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

pub struct AutoConnectClient<T> {
    client: Option<SimpleWebsocket<T>>,
    next_client: Option<SimpleWebsocket<T>>,
    pub url: String,
    subscribe_message: Arc<RwLock<T>>,
    last_message: String,
    last_connect_time: MicroSec,
    last_ping_time: MicroSec,
    sync_interval: MicroSec,
    sync_records: i64,
    sync_mode: bool,
}

const SYNC_RECORDS: i64 = 3;

// TODO: tuning sync interval (possibly 6-12 hours)
const SYNC_INTERVAL: MicroSec = MICRO_SECOND * 60 * 60 * 6; // every 6 hours
const PING_INTERVAL: MicroSec = MICRO_SECOND * 60 * 3; // every 3 min

impl <T>AutoConnectClient<T>
    where T: WsMessage
     {
    pub fn new(url: &str, message: Arc<RwLock<T>>) -> Self {
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
        self.client = Some(SimpleWebsocket::new(
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

        self.next_client = Some(SimpleWebsocket::new(
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

    pub fn send_message(&mut self, message: &str) {
        if self.client.is_some() {
            log::debug!("SEND main: {:?}", message);
            self.client.as_mut().unwrap().send_message(message);
        }

        if self.next_client.is_some() {
            log::debug!("SEND next: {:?}", message);
            self.next_client.as_mut().unwrap().send_message(message);
        }
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
                        log::warn!(
                            "Disconnected from server before sync complete {}: {:?}",
                            self.url,
                            message.unwrap_err()
                        );
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

                    if message == self.last_message {
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
            } else {
                log::info!("SYNC:({})/ {}", self.url, self.sync_records);
                let message = self._receive_message();

                if message.is_ok() {
                    let m = message.unwrap();
                    self.last_message = m.clone();

                    self.sync_records += 1;

                    return Ok(m);
                }

                log::warn!(
                    "Disconnected from server before sync start {}: {:?}",
                    self.url,
                    message
                );
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

    use crate::common::init_debug_log;

    use super::*;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn ws_loop() {
        let mut message = BinanceWsMessage::new();
        
        message.add_params(&vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        let mut ws1 = SimpleWebsocket::new(
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message)),
        );
        ws1.connect();

        let mut message2 = BinanceWsMessage::new();
        
        message2.add_params(&vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        let mut ws2 = SimpleWebsocket::new(
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message2)),
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
        let mut message = BinanceWsMessage::new();
        
        message.add_params(&vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        let mut ws = AutoConnectClient::new(
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message)),
        );

        ws.connect();

        spawn(move || loop {
            let m = ws.receive_message();
            println!("{}", m.unwrap());
        });

        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_wsmessage() {
        let message = BinanceWsMessage {
            method: "SUBSCRIBE".to_string(),
            params: vec!["btcusdt@trade".to_string(), "btcusdt@depth".to_string()],
            id: 1,
        };

        let message = serde_json::to_string(&message);

        if message.is_err() {
            println!("Error: {:?}", message);
            return;
        }

        let message = message.unwrap();

        println!("{}", message);
    }

    #[test]
    fn test_websocket_client() {
        init_debug_log();

        let mut ws: WebSocketClient<BinanceWsMessage>  = WebSocketClient::new(
                "wss://stream.binance.com/ws", vec![]);

        log::debug!("subscribe");
        ws.subscribe(&mut vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        log::debug!("connect");
        let ch = ws.connect();

        for _ in 0..30 {
            let message = ch.recv();

            println!("{}", message.unwrap());
        }

        ws.subscribe(&mut vec!["btcusdt@depth".to_string()]);
        log::debug!("----- subscribe depth-----");

        for _ in 0..30 {
            let message = ch.recv();

            println!("{}", message.unwrap());
        }

        ws.switch();

        for _ in 0..30 {
            let message = ch.recv();

            println!("{}", message.unwrap());
        }
    }

    #[test]
    pub fn bybit_ws_connect_test() {
        let mut ws: WebSocketClient<BybitWsMessage> = WebSocketClient::new(
            "wss://stream.bybit.com/v5/public/spot",
            vec!["publicTrade.BTCUSDT".to_string()],
        );

        // ws.subscribe(&mut vec!["publicTrade.BTCUSDT".to_string()]);

        log::debug!("connect");
        let ch = ws.connect();

        for _ in 0..3 {
            let message = ch.recv();

            println!("{}", message.unwrap());
        }
    }

    #[test]
    pub fn bybit_ws_connect_test2() {
        let mut ws: WebSocketClient<BybitWsMessage> = WebSocketClient::new(
            "wss://stream.bybit.com/v5/public/spot",
            vec!["orderbook.200.BTCUSDT".to_string()],
        );

//        log::debug!("subscribe");
        //ws.subscribe(&mut vec!["publicTrade.BTCUSDT".to_string()]);

        log::debug!("connect");
        let ch = ws.connect();

        for _ in 0..3 {
            let message = ch.recv();

            println!("{}", message.unwrap());
        }
    }

}

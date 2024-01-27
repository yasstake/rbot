// Copyright(c) 2022-2023. yasstake. All rights reserved.

use core::panic;
use crossbeam_channel::Receiver;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use futures::SinkExt;
use futures::StreamExt;
use serde_derive::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_tungstenite::WebSocketStream;

use std::sync::Arc;
use tokio::sync::RwLock;
use url::Url;

use crate::common::MultiMarketMessage;
use crate::common::{MicroSec, MultiChannel, MICRO_SECOND, NOW};
use crate::RUNTIME;
use tokio_tungstenite::{connect_async, MaybeTlsStream};
use tungstenite::Error;
use tungstenite::Message;

pub trait WsOpMessage {
    fn new() -> Self;
    fn add_params(&mut self, params: &Vec<String>);
    fn to_string(&self) -> String;
    fn make_message(&self) -> Vec<String>;
}

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

#[derive(Debug)]
/// Tは、WsMessageを実装した型。Subscribeメッセージの取引所の差分を実装する。
/// WebSocketClientは、文字列のメッセージのやり取りを行う。
pub struct WebSocketClient<T, U>
where
    T: Send + Clone + 'static,
    U: WsOpMessage + Sync + Send + Sync + Clone + 'static,
{
    // url: String,
    //handle: Option<tokio::task::JoinHandle<()>>,
    connection: Arc<Mutex<AutoConnectClient<T, U>>>,
    message: Arc<Mutex<MultiChannel<MultiMarketMessage>>>,
    //  control_ch: MultiChannel<String>, // send sbscribe message
}

impl<T, U> WebSocketClient<T, U>
where
    T: Send + Sync + Clone,
    U: WsOpMessage + Send + Sync + Clone + 'static,
{
    pub fn new(
        config: &T,
        url: &str,
        subscribe: Vec<String>,
        ping_interval_sec: i64,
        switch_interval_sec: i64,
        sync_wait_records: i64,
        init_fn: Option<fn(&T) -> String>,
    ) -> Self {
        RUNTIME.block_on(async {
            let mut client: AutoConnectClient<T, U> = AutoConnectClient::new(
                &config,
                url,
                ping_interval_sec,
                switch_interval_sec,
                sync_wait_records,
                init_fn,
            );

            client.subscribe(&subscribe).await;

            WebSocketClient {
                // handle: None,
                connection: Arc::new(Mutex::new(client)),
                message: Arc::new(Mutex::new(MultiChannel::new())),
            }
        })
    }

    pub fn connect<F>(&mut self, convert: F)
    where
        F: Fn(String) -> MultiMarketMessage + Send + Sync + Clone + 'static,
    {
        log::debug!("blocking connect start");

        RUNTIME.block_on(async {
            self._connect(convert).await;
        });
    }

    pub fn connect_websocket(&mut self) {
        log::debug!("blocking connect start");

        RUNTIME.block_on(async {
            self._connect_websocket().await;
        });
    }

    pub async fn _connect_websocket(&mut self) {
        log::debug!("connect start");

        let websocket = self.connection.clone();
        let mut websocket = websocket.lock().await;
        log::debug!("connect start2. get lock.");
        websocket.connect().await;
        log::debug!("connected");
        drop(websocket);
    }

    pub fn receive_text(&mut self) -> Result<String, String> {
        let result = RUNTIME.block_on(async { Self::_receive_text(&self.connection).await });

        result
    }

    pub async fn _receive_text(
        connection: &Arc<Mutex<AutoConnectClient<T, U>>>,
    ) -> Result<String, String> {
        let mut lock = connection.as_ref().lock().await;
        let message = lock.receive_text().await;
        drop(lock);

        if message.is_err() {
            log::error!("No message");
            return Err("No message".to_string());
        }

        let message = message.unwrap();

        return Ok(message.to_string());
    }

    /// connect to websocket server
    /// start listening thread
    pub async fn _connect<F>(&mut self, convert: F) -> tokio::task::JoinHandle<()>
    where
        F: Fn(String) -> MultiMarketMessage + Send + Sync + Clone + 'static,
    {
        self._connect_websocket().await;

        let websocket = self.connection.clone();
        let message_ch = self.message.clone();

        let handle = tokio::spawn(async move {
            loop {
                let message = Self::_receive_text(&websocket).await;
                if message.is_err() {
                    log::warn!("Error in websocket.receive_message: {:?}", message);
                    continue;
                }
                let m = message.unwrap();

                let m = convert(m);
                let result = Self::send_message_channel(&message_ch, m).await;
                if result.is_err() {
                    log::warn!("Error in websocket.receive_message: {:?}", result);
                    continue;
                }
            }
        });

        handle
    }

    pub async fn send_message_channel(
        ch: &Arc<Mutex<MultiChannel<MultiMarketMessage>>>,
        message: MultiMarketMessage,
    ) -> Result<(), anyhow::Error> {
        //    log::debug!("send_message_channel: {:?}", message);
        let mut lock = ch.as_ref().lock().await;
        //        log::debug!("send_message_channel: lock ok");
        lock.send(message)
    }

    /*
    /// Append subscribe list and send subscribe message to send queue
    pub async fn subscribe(&mut self, message: &Vec<String>) {
        log::debug!("subscribe: {:?}", message);

        let mut lock = self.subscribe_list.as_ref().write().unwrap();
        lock.add_params(message);
        drop(lock);

        let message = self.subscribe_list.as_ref().read().unwrap().make_message();

        log::debug!("call subscribe: {:?}", message);

        let websocket = self.connection.clone();
        let mut lock = websocket.lock().await;

        for m in &message {
            lock.send_text(m);

            // TODO: receive status
        }

        drop(lock);
    }
    */

    pub async fn switch(&mut self) {
        let websocket = self.connection.clone();
        let mut lock = websocket.lock().await;
        lock.switch();
        drop(lock);
    }

    pub fn open_channel(&mut self) -> Receiver<MultiMarketMessage> {
        RUNTIME.block_on(async { self._open_channel().await })
    }

    /// get receive queue
    pub async fn _open_channel(&mut self) -> Receiver<MultiMarketMessage> {
        log::debug!("open_channel");
        let mut lock = self.message.as_ref().lock().await;
        log::debug!("open_channel lock ok");
        lock.open_channel(0)
    }
}

const MAX_RETRY: usize = 5;

#[derive(Debug)]
pub struct SimpleWebsocket<T, U> {
    read_stream: Option<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    write_sream: Option<Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>>,
    url: String,
    subscribe_message: Arc<RwLock<U>>,
    init_fn: Option<fn(&T) -> String>,
    ping_interval_sec: i64,
    ping_thread: Option<tokio::task::JoinHandle<()>>,
    config: T,
}

impl<T, U> SimpleWebsocket<T, U>
where
    T: Clone,
    U: WsOpMessage,
{
    pub fn new(
        config: &T,
        url: &str,
        subscribe_message: Arc<RwLock<U>>,
        ping_interval_sec: i64,
        init_fn: Option<fn(&T) -> String>,
    ) -> Self {
        log::debug!("new SimpleWebsocket");
        SimpleWebsocket {
            read_stream: None,
            write_sream: None,
            url: url.to_string(),
            subscribe_message: subscribe_message.clone(),
            init_fn,
            ping_interval_sec,
            ping_thread: None,
            config: config.clone(),
        }
    }

    pub async fn connect(&mut self) {
        log::debug!("start connect: {}", self.url);
        let url = Url::parse(&self.url).unwrap();

        let client = connect_async(url).await;
        if client.is_err() {
            log::error!("Can't connect to {}", self.url);
            panic!("Can't connect to {}", self.url);
        }

        log::debug!("Connected to the server");

        let (socket, response) = client.unwrap();

        log::debug!("Connected to the server");
        log::debug!("Response HTTP code: {}", response.status());
        log::debug!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            log::debug!("* {}", header);
        }

        let (write_stream, read_stream) = socket.split();
        self.write_sream = Some(Arc::new(Mutex::new(write_stream)));
        self.read_stream = Some(read_stream);
        log::debug!("setup split stream");

        // self.connection = Some(socket);

        if self.init_fn.is_some() {
            let message = (self.init_fn.as_ref().unwrap())(&self.config);
            log::debug!("init message: {}", message);
            self.send_text(message).await;

            // receive accept message
            let accept = self.receive_text().await;
            log::debug!("accept message: {:?}", accept);
        }

        let message: String = self.subscribe_message.read().await.to_string();

        if message != "" {
            self.send_text(message).await;
        }

        self.ping_thread = Some(self.spawn_ping_task());
    }

    pub fn spawn_ping_task(&mut self) -> tokio::task::JoinHandle<()> {
        let mut write_stream = self.write_sream.as_mut().unwrap().clone();
        let ping_interval = self.ping_interval_sec as u64;
        log::debug!("start ping: interval: {}", ping_interval);
        let mut random = 0;

        let handle = tokio::spawn(async move {
            // let write_stream = write_stream.clone();
            loop {
                let random = NOW() % 5_000;
                let mut sleeptime = (ping_interval as i64) * 1_000 + random - 2_500;
                if sleeptime < 0 {
                    sleeptime = 2_500;
                }
                tokio::time::sleep(Duration::from_millis(sleeptime as u64)).await;
                log::debug!("SEND PING {:?}", sleeptime);
                let message = Self::ping_message();
                Self::_send_message(&mut write_stream, message).await;
            }
        });

        handle
    }

    pub async fn send_message(&mut self, message: Message) {
        let write_stream = self.write_sream.as_mut();

        if write_stream.is_none() {
            log::error!("No write stream");
            return;
        }
        // TODO: check if connection is established.
        //        let connection = self.connection.as_mut().unwrap();
        let write_stream = write_stream.unwrap();

        Self::_send_message(write_stream, message).await;
    }

    pub async fn send_text(&mut self, message: String) {
        let message = Message::Text(message);
        self.send_message(message).await;
    }

    async fn _send_message(
        write_stream: &mut Arc<
            Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
        >,
        message: Message,
    ) {
        log::debug!("Sent message {:?}", message);
        let mut write_stream = write_stream.lock().await;
        let result = write_stream.send(message).await;
        match result {
            Ok(_) => {}
            Err(e) => {
                log::error!("Error: {:?}", e.to_string());
            }
        }
        let r = write_stream.flush().await;
        match r {
            Ok(_) => {}
            Err(e) => {
                log::error!("Websocket write Error: {:?}", e.to_string());
            }
        }
    }

    pub async fn send_ping(&mut self) {
        log::debug!("*>PING*>");
        let t = NOW();
        let message = format!("{:?}", t).into_bytes();
        self.send_message(Message::Ping(message)).await;
    }

    pub fn ping_message() -> Message {
        let t = NOW();
        let message = format!("{:?}", t).into_bytes();
        Message::Ping(message)
    }

    pub async fn send_pong(&mut self, message: Vec<u8>) {
        log::debug!("*>PONG*>: {:?}", message);
        self.send_message(Message::Pong(message)).await;
    }

    pub async fn close(&mut self) {
        log::debug!(">>>Close connection<<<");
        self.send_message(Message::Close(None)).await;
        self.write_sream = None;
        self.read_stream = None;
        self.ping_thread.as_mut().unwrap().abort();
        self.ping_thread = None;
    }

    pub async fn receive_text(&mut self) -> Result<String, String> {
        loop {
            let message = self.read_stream.as_mut().unwrap().next().await;

            if message.is_none() {
                log::error!("No message");
                return Err("No message".to_string());
            }

            let message = message.unwrap();
            if message.is_err() {
                log::error!("Error: {:?}", message);
                return Err(format!("Error: {:?}", message));
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
                    self.send_pong(p).await;
                }
                Message::Pong(p) => {
                    log::debug!("<PONG<: {:?}", p);
                }
                Message::Close(c) => {
                    log::debug!("CLOSE: {:?}", c);
                    self.write_sream = None;
                    self.read_stream = None;

                    return Err("Closed".to_string());
                }
                Message::Frame(message) => {
                    log::warn!("Unknown frame {:?}", message);
                }
            };
        }
    }
}

#[derive(Debug)]
pub struct AutoConnectClient<T, U> {
    client: Option<SimpleWebsocket<T, U>>,
    next_client: Option<SimpleWebsocket<T, U>>,
    pub url: String,
    subscribe_message: Arc<RwLock<U>>,
    last_message: String,
    last_connect_time: MicroSec,
    switch_interval: MicroSec,
    sync_records: i64, // current sync records
    sync_mode: bool,
    sync_wait_records: i64, // setting for number of records to sync
    ping_interval: MicroSec,
    init_fn: Option<fn(&T) -> String>,
    config: T,
}

const SYNC_RECORDS: i64 = 3;

// TODO: tuning sync interval (possibly 6-12 hours)
const SYNC_INTERVAL: MicroSec = MICRO_SECOND * 60 * 60 * 6; // every 6 hours

impl<T, U> AutoConnectClient<T, U>
where
    T: Clone,
    U: WsOpMessage,
{
    pub fn new(
        config: &T,
        url: &str,
        ping_interval: MicroSec,
        switch_interval_sec: i64,
        sync_wait_records: i64,
        init_fn: Option<fn(&T) -> String>,
    ) -> Self {
        AutoConnectClient {
            client: None,
            next_client: None,
            url: url.to_string(),
            subscribe_message: Arc::new(RwLock::new(U::new())),
            last_message: "".to_string(),
            last_connect_time: 0,
            switch_interval: switch_interval_sec * MICRO_SECOND,
            sync_records: 0,
            sync_mode: false,
            sync_wait_records: sync_wait_records,
            ping_interval,
            init_fn: init_fn,
            config: config.clone(),
        }
    }

    pub async fn connect(&mut self) {
        log::debug!("connect: {}", self.url);

        self.client = Some(SimpleWebsocket::new(
            &self.config,
            self.url.as_str(),
            self.subscribe_message.clone(),
            self.ping_interval,
            self.init_fn,
        ));
        self.client.as_mut().unwrap().connect().await;
        self.last_connect_time = NOW();
    }

    pub async fn connect_next(&mut self, url: Option<String>) {
        if url.is_some() {
            self.url = url.unwrap();
        }

        self.next_client = Some(SimpleWebsocket::new(
            &self.config,
            self.url.as_str(),
            self.subscribe_message.clone(),
            self.ping_interval,
            self.init_fn,
        ));
        self.next_client.as_mut().unwrap().connect().await;
        self.last_connect_time = NOW();
    }

    pub async fn switch(&mut self) {
        self.client.as_mut().unwrap().close().await;
        self.client = self.next_client.take();
        self.next_client = None;

        log::info!("------WS switched-{}-----", self.url);
    }

    pub async fn send_text(&mut self, message: &str) {
        if self.client.is_some() {
            log::debug!("SEND main: {:?}", message);
            self.client
                .as_mut()
                .unwrap()
                .send_text(message.to_string())
                .await;
        }

        if self.next_client.is_some() {
            log::debug!("SEND next: {:?}", message);

            self.next_client
                .as_mut()
                .unwrap()
                .send_text(message.to_string())
                .await;
        }
    }

    pub async fn receive_text(&mut self) -> Result<String, String> {
        let client = self.client.as_mut();
        if client.is_none() {
            log::debug!("Try reconnect");
            self.connect().await;
        }

        let now = NOW();

        // if connection exceed sync interval, reconnect
        if (self.last_connect_time + self.switch_interval < now) && self.next_client.is_none() {
            log::info!("create new websocket: {}", self.url);
            self.connect_next(None).await;
        }

        // if the connection_next is not None, receive message
        if self.next_client.is_some() {
            self.sync_mode = true;
        }

        // in sync mode, receive message and compare with last message.
        if self.sync_mode {
            // no sync, just switch,
            if self.sync_wait_records == 0 {
                self.switch().await;
                self.sync_mode = false;
                log::info!("SWITCH");
            }
            // in sync mode, read forward sync_wait_records.
            else if self.sync_records <= self.sync_wait_records {
                log::info!("SYNC:({})/ {}", self.url, self.sync_records);
                let message = self._receive_message().await;
                if message.is_err() {
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

                let m = message.unwrap();
                self.last_message = m.clone();

                self.sync_records += 1;

                return Ok(m);
            } else {
                self.switch().await;

                loop {
                    let message = self._receive_message().await;

                    if message.is_err() {
                        log::warn!(
                            "Disconnected from server before sync complete {}: {:?}",
                            self.url,
                            message.unwrap_err()
                        );
                        self.client.as_mut().unwrap().close().await;
                        self.client = None;

                        self.last_message = "".to_string();
                        self.sync_mode = false;
                        self.sync_records = 0;
                        log::warn!("few records may be lost");
                        break;
                    }

                    let message = message.unwrap();

                    if message == self.last_message {
                        log::debug!(
                            "SYNC complete: {} / {} ({})",
                            message,
                            self.last_message,
                            self.sync_records
                        );
                        self.last_message = "".to_string();
                        self.sync_mode = false;
                        self.sync_records = 0;

                        break;
                    }

                    if self.sync_records == 0 {
                        log::warn!("SYNC timeup: {} / {}", message, self.last_message);
                        self.last_message = "".to_string();
                        self.sync_mode = false;
                        self.sync_records = 0;

                        break;
                    }

                    self.sync_records -= 1;
                }
            }
        }

        return self._receive_message().await;
    }

    async fn _receive_message(&mut self) -> Result<String, String> {
        let mut websocket = self.client.as_mut();
        if websocket.is_none() {
            log::warn!("No websocket, try reconnect");
            self.connect().await;
            websocket = self.client.as_mut();
        }

        let result = websocket.unwrap().receive_text().await;

        match result {
            Ok(_) => {
                return result;
            }
            Err(e) => {
                log::debug!("recive error{}, try reconnect!!", e);

                self.client.as_mut().unwrap().close().await;
                self.client = None;
                Err(e)
            }
        }
    }

    pub async fn subscribe(&mut self, message: &Vec<String>) {
        self.subscribe_message
            .as_ref()
            .write()
            .await
            .add_params(message);
    }
}

#[cfg(test)]
mod test_exchange_ws {
    use crate::common::init_debug_log;
    use crate::exchange::binance::ws::PING_INTERVAL;
    use crate::exchange::binance::BinanceConfig;
    use crate::exchange::bybit::config::BybitServerConfig;
    use crate::exchange::bybit::message::BybitWsMessage;

    use super::*;
    use std::thread;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;

    #[test]
    fn test_ping_message() {
        let message = SimpleWebsocket::<BybitServerConfig, BybitWsOpMessage>::ping_message();
        println!("PING={:?}", message);
    }

    #[tokio::test]
    async fn simple_connect() {
        init_debug_log();

        let config = BinanceConfig::BTCUSDT();
        let mut message = BinanceWsOpMessage::new();

        message.add_params(&vec![
            "btcusdt@trade".to_string(),
            // "btcusdt@depth".to_string(),
        ]);

        let mut ws = SimpleWebsocket::new(
            &config,
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message)),
            1,
            None,
        );

        ws.connect().await;

        let message = ws.receive_text().await;
        println!("{}", message.unwrap());

        for i in 0..100 {
            let message = ws.receive_text().await;
            println!("{}", message.unwrap());
        }
    }

    #[tokio::test]
    async fn ws_loop() {
        init_debug_log();

        let config = BinanceConfig::BTCUSDT();
        let mut message = BinanceWsOpMessage::new();

        message.add_params(&vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        let mut ws1 = SimpleWebsocket::new(
            &config,
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message)),
            PING_INTERVAL,
            None,
        );

        ws1.connect().await;
        println!("connect ws1");

        let mut message2 = BinanceWsOpMessage::new();

        message2.add_params(&vec![
            "btcusdt@trade".to_string(),
            "btcusdt@depth".to_string(),
        ]);

        let mut ws2 = SimpleWebsocket::new(
            &config,
            "wss://stream.binance.com/ws",
            Arc::new(RwLock::new(message2)),
            PING_INTERVAL,
            None,
        );

        ws2.connect().await;
        println!("connect ws2");

        tokio::spawn(async move {
            for i in 0..100 {
                let message = ws1.receive_text().await;
                println!("1: {}", message.unwrap());
            }
        });

        //tokio::spawn(async move {
        for i in 0..100 {
            let message = ws2.receive_text().await;
            println!("2: {}", message.unwrap());
        }
        //});

        //sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_wsmessage() {
        let message = BinanceWsOpMessage {
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

    #[tokio::test]
    async fn test_auto_connect_client() {
        init_debug_log();

        let config = BybitServerConfig::new(false);

        let mut ws: AutoConnectClient<BybitServerConfig, BybitWsOpMessage> = AutoConnectClient::new(
            &config,
            "wss://stream.bybit.com/v5/public/spot",
            PING_INTERVAL,
            60,
            0,
            None,
        );

        log::debug!("subscribe");
        ws.subscribe(&mut vec!["publicTrade.BTCUSDT".to_string()]);

        ws.connect().await;

        for _ in 0..30 {
            let message = ws.receive_text().await;

            println!("{:?}", message.unwrap());
        }
    }

    /*
        #[tokio::test]
        async fn test_websocket_client() {
            init_debug_log();

            let config = BinanceConfig::BTCUSDT();

            let mut ws: WebSocketClient<BinanceConfig, BinanceWsOpMessage> = WebSocketClient::new(
                &config,
                "wss://stream.binance.com/ws",
                vec!["btcusdt@trade".to_string(), "btcusdt@depth".to_string()],
                PING_INTERVAL,
                None,
            );

            log::debug!("connect");
            ws._connect(|message| {
                let message: BybitWsMessage = serde_json::from_str(&message).unwrap();
                return message.into();
            })
            .await;

            let ch = ws.open_channel().await;

            for _ in 0..30 {
                let message = ch.recv();

                println!("{:?}", message.unwrap());
            }

            for _ in 0..30 {
                let message = ch.recv();

                println!("{:?}", message.unwrap());
            }

            ws.switch();

            for _ in 0..30 {
                let message = ch.recv();

                println!("{:?}", message.unwrap());
            }
        }
    */
    #[tokio::test]
    pub async fn bybit_ws_connect_test() {
        init_debug_log();
        let config = BybitServerConfig::new(false);

        let mut ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage> = WebSocketClient::new(
            &config,
            "wss://stream.bybit.com/v5/public/spot",
            vec!["publicTrade.BTCUSDT".to_string()],
            PING_INTERVAL,
            60,
            0,
            None,
        );

        // ws.subscribe(&mut vec!["publicTrade.BTCUSDT".to_string()]);

        log::debug!("connect");
        ws._connect(|message| {
            let message = serde_json::from_str::<BybitWsMessage>(&message);
            if message.is_err() {
                log::error!("Error in serde_json::from_str: {:?}", message);
            }
            return message.unwrap().into();
        })
        .await;

        let ch = ws._open_channel().await;

        for _ in 0..3 {
            let message = ch.recv();

            println!("{:?}", message.unwrap());
        }
    }

    #[test]
    pub fn bybit_ws_connect_test2() {
        init_debug_log();

        let config = BybitServerConfig::new(false);

        let mut ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage> = WebSocketClient::new(
            &config,
            "wss://stream.bybit.com/v5/public/linear",
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
            PING_INTERVAL,
            60,
            0,
            None,
        );

        //        log::debug!("subscribe");
        //ws.subscribe(&mut vec!["publicTrade.BTCUSDT".to_string()]);

        log::debug!("connect");
        ws.connect(|message| {
            let message: BybitWsMessage = serde_json::from_str(&message).unwrap();
            return message.into();
        });

        println!("-OPEN CHANNEL-");
        let ch = ws.open_channel();

        println!("start receive");

        for _ in 0..5 {
            let message = ch.recv();

            println!("{:?}", message.unwrap());
        }

        //        ws.subscribe(&mut vec!["orderbook.200.BTCUSDT".to_string()]);

        for _ in 0..10 {
            let message = ch.recv();

            println!("{:?}", message.unwrap());
        }
    }

    #[test]
    fn simple_websocket_connect() {
        init_debug_log();

        let config = BybitServerConfig::new(false);

        let mut ws: WebSocketClient<BybitServerConfig, BybitWsOpMessage> = WebSocketClient::new(
            &config,
            "wss://stream.bybit.com/v5/public/linear",
            vec![
                "publicTrade.BTCUSDT".to_string(),
                "orderbook.200.BTCUSDT".to_string(),
            ],
            10,
            30,
            0,
            None,
        );

        ws.connect_websocket();

        for i in 0..3000 {
            let message = ws.receive_text();

            //println!("{:?}", message.unwrap());
        }
    }
}

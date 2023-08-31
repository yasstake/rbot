// Copyright(c) 2023. yasstake. All rights reserved.
// Abloultely no warranty.

pub mod bb;
pub mod binance;
// pub mod ftx;

use std::{
    fs::File,
    io::{copy, BufReader, Cursor, Write},
    net::TcpStream,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use openssl::ssl::ConnectConfiguration;
use polars_core::utils::rayon::prelude::IndexedParallelIterator;
use serde::{de, Deserialize, Deserializer};
use serde_json::{json, Value};
use tempfile::tempdir;
use url::Url;
use zip::ZipArchive;

use crate::common::{order::Trade, time::{MicroSec, NOW, HHMM, SEC, MICRO_SECOND}};

use std::sync::mpsc::Sender;
use std::thread::sleep;
use std::thread::spawn;
use std::thread::JoinHandle;
use std::time::Duration;

use tungstenite::protocol::WebSocket;
use tungstenite::Message;
use tungstenite::{connect, stream::MaybeTlsStream};

fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse f64")),
    }
}

fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<i64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse i64")),
    }
}

pub fn log_download_tmp(url: &str, tmp_dir: &Path) -> Result<String, String> {
    let client = reqwest::blocking::Client::new();

    let response = match client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let fname = tmp_dir.join(fname);

    let mut target = match File::create(&fname) {
        Ok(t) => t,
        Err(e) => {
            return Err(e.to_string());
        }
    };

    let file_name = fname.to_str().unwrap();
    let content = match response.bytes() {
        Ok(c) => c,
        Err(e) => {
            log::error!("{}", e.to_string());
            return Err(e.to_string());
        }
    };
    let mut cursor = Cursor::new(content);

    if copy(&mut cursor, &mut target).is_err() {
        return Err(format!("write error"));
    }

    let _r = target.flush();

    log::debug!("download size {}", target.metadata().unwrap().len());

    Ok(file_name.to_string())
}

pub fn log_download<F>(url: &str, has_header: bool, f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("Downloading ...[{}]", url);

    let tmp_dir = match tempdir() {
        Ok(tmp) => tmp,
        Err(e) => {
            log::error!("create tmp dir error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    let result = log_download_tmp(url, tmp_dir.path());

    let file_path: String;
    match result {
        Ok(path) => {
            file_path = path;
        }
        Err(e) => {
            return Err(e);
        }
    }

    log::debug!("let's extract = {}", file_path);

    if url.ends_with("gz") || url.ends_with("GZ") {
        log::debug!("extract gzip = {}", file_path);
        return extract_gzip_log(&file_path, has_header, f);
    } else if url.ends_with("zip") || url.ends_with("ZIP") {
        log::debug!("extract zip = {}", file_path);
        return extract_zip_log(&file_path, has_header, f);
    } else {
        log::error!("unknown file suffix {}", url);
        return Err(format!("").to_string());
    }

    // remove tmp file
}

#[allow(unused)]
fn gzip_log_download<F>(
    response: reqwest::blocking::Response,
    has_header: bool,
    mut f: F,
) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    let mut rec_count = 0;

    match response.bytes() {
        Ok(b) => {
            let gz = GzDecoder::new(b.as_ref());

            let mut reader = csv::Reader::from_reader(gz);
            if has_header {
                reader.has_headers();
            }

            for rec in reader.records() {
                if let Ok(string_rec) = rec {
                    f(&string_rec);
                    rec_count += 1;
                }
            }
        }
        Err(e) => {
            log::error!("{}", e);
            return Err(e.to_string());
        }
    }
    Ok(rec_count)
}

#[allow(unused)]
fn zip_log_download<F>(
    response: reqwest::blocking::Response,
    has_header: bool,
    mut f: F,
) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    let mut rec_count = 0;

    match response.bytes() {
        Ok(b) => {
            let reader = std::io::Cursor::new(b);
            let mut zip = zip::ZipArchive::new(reader).unwrap();

            for i in 0..zip.len() {
                let mut file = zip.by_index(i).unwrap();

                if file.name().ends_with("csv") == false {
                    log::debug!("Skip file {}", file.name());
                    continue;
                }

                let mut csv_reader = csv::Reader::from_reader(file);
                if has_header {
                    csv_reader.has_headers();
                }
                for rec in csv_reader.records() {
                    if let Ok(string_rec) = rec {
                        f(&string_rec);
                        rec_count += 1;
                    }
                }
            }
        }
        Err(e) => {
            log::error!("{}", e);
            return Err(e.to_string());
        }
    }
    Ok(rec_count)
}

fn extract_zip_log<F>(path: &String, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("extract zip = {}", path);
    let mut rec_count = 0;

    let file_path = Path::new(path);

    if file_path.exists() == false {
        log::error!("File Not Found {}", path);
        return Err(format!("File Not Found {}", path));
    }

    let tmp_file = File::open(file_path).unwrap();
    let bufreader = BufReader::new(tmp_file);

    let mut zip = match ZipArchive::new(bufreader) {
        Ok(z) => z,
        Err(e) => {
            return Err(e.to_string());
        }
    };

    for i in 0..zip.len() {
        let file = zip.by_index(i).unwrap();

        if file.name().to_lowercase().ends_with("csv") == false {
            log::debug!("Skip file {}", file.name());
            continue;
        } else {
            log::debug!("processing {}", file.name());
        }

        let mut csv_reader = csv::Reader::from_reader(file);
        if has_header {
            csv_reader.has_headers();
        }
        for rec in csv_reader.records() {
            if let Ok(string_rec) = rec {
                f(&string_rec);
                rec_count += 1;
            }
        }
    }

    Ok(rec_count)
}

fn extract_gzip_log<F>(path: &String, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord),
{
    log::debug!("extract gzip = {}", path);
    let mut rec_count = 0;

    let file_path = Path::new(path);

    if file_path.exists() == false {
        log::error!("File Not Found {}", path);
        return Err(format!("File Not Found {}", path));
    }

    let tmp_file = File::open(file_path).unwrap();
    let bufreader = BufReader::new(tmp_file);
    let gzip_reader = std::io::BufReader::new(GzDecoder::new(bufreader));

    let mut csv_reader = csv::Reader::from_reader(gzip_reader);

    if has_header {
        csv_reader.has_headers();
    }

    for rec in csv_reader.records() {
        if let Ok(string_rec) = rec {
            f(&string_rec);
            rec_count += 1;
        }
    }

    Ok(rec_count)
}

fn make_download_url_list<F>(name: &str, days: Vec<i64>, f: F) -> Vec<String>
where
    F: Fn(&str, i64) -> String,
{
    let mut urls: Vec<String> = vec![];
    for day in days {
        urls.push(f(name, day));
    }
    urls
}

fn download_log<F>(urls: Vec<String>, tx: Sender<Vec<Trade>>, has_header: bool, f: F) -> i64
where
    F: Fn(&StringRecord) -> Trade,
{
    let mut download_rec = 0;

    for url in urls {
        log::debug!("download url = {}", url);

        let mut buffer: Vec<Trade> = vec![];

        let result = log_download(url.as_str(), has_header, |rec| {
            let trade = f(&rec);

            buffer.push(trade);

            if 2000 < buffer.len() {
                let result = tx.send(buffer.to_vec());

                match result {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("{:?}", e);
                    }
                }
                buffer.clear();
            }
        });

        if buffer.len() != 0 {
            let result = tx.send(buffer.to_vec());
            match result {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{:?}", e);
                }
            }

            buffer.clear();
        }

        match result {
            Ok(count) => {
                log::debug!("Downloaded rec = {} ", count);
                println!("Downloaded rec = {} ", count);
                download_rec += count;
            }
            Err(e) => {
                log::error!("extract err = {}", e.as_str());
            }
        }
    }

    log::debug!("download rec = {}", download_rec);

    return download_rec;
}

fn rest_get(server: &str, path: &str) -> Result<String, String> {
    let url = format!("{}{}", server, path);
    let client = reqwest::blocking::Client::new();

    let response = match client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    Ok(response.text().unwrap())
}

fn restapi<F>(server: &str, path: &str, f: F) -> Result<(), String>
where
    F: Fn(String) -> Result<(), String>,
{
    let url = format!("{}{}", server, path);
    let client = reqwest::blocking::Client::new();

    let response = match client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return Err(e.to_string());
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    f(response.text().unwrap())
}

pub struct WebSocketClient {
    connection: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    url: String,
    subscribe_message: Value,
}

impl WebSocketClient {
    pub fn new(url: &str, subscribe_message: Value) -> Self {
        WebSocketClient {
            connection: None,
            url: url.to_string(),
            subscribe_message: subscribe_message,
        }
    }

    pub fn connect(&mut self) {
        let (mut socket, response) =
            connect(Url::parse(&self.url).unwrap()).expect("Can't connect");

        println!("Connected to the server");

        println!("Response HTTP code: {}", response.status());
        println!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            println!("* {}", header);
        }

        self.connection = Some(socket);

        self.send_message(self.subscribe_message.to_string().as_str());
        self.flush();
    }

    pub fn send_message(&mut self, message: &str) {
        let connection = self.connection.as_mut().unwrap();
        let result = connection.write(Message::Text(message.to_string()));

        match result {
            Ok(_) => {
                println!("Sent message {}", message);
            }
            Err(e) => {
                println!("Error: {:?}", e.to_string());
            }
        }
    }

    pub fn send_ping(&mut self) {
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Ping(vec![]));
    }

    pub fn send_pong(&mut self, message: Vec<u8>) {
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Pong(message));
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

        let message = connection.read().unwrap();

        match message {
            Message::Text(t) => {
                println!("TEXT: {}", t);
                return Ok(t);
            }
            Message::Binary(b) => {
                println!("BINARY: {:?}", b);
            }
            Message::Ping(p) => {
                println!("PING: {:?}", p);
                self.send_pong(p);
            }
            Message::Pong(p) => {
                println!("PONG: {:?}", p);
            }
            Message::Close(c) => {
                println!("CLOSE: {:?}", c);
                return Err("Closed".to_string());
            }
            Message::Frame(_) => {}
        }

        return self.receive_message();
    }
}

struct AutoConnectClient {
    client: Option<WebSocketClient>,
    next_client: Option<WebSocketClient>,
    url: String,
    subscribe_message: Value,
    last_message: String,
    last_connect_time: MicroSec,
    sync_interval: MicroSec,
    sync_records: i64,
}

const SYNC_RECORDS: i64 = 100;
const SYNC_INTERVAL: MicroSec = MICRO_SECOND * 600; // 600 sec (10 min)

impl AutoConnectClient {
    fn new(url: &str, subscribe_message: Value) -> Self {
        AutoConnectClient {
            client: Some(WebSocketClient::new(url, subscribe_message.clone())),
            next_client: None,
            url: url.to_string(),
            subscribe_message: subscribe_message,
            last_message: "".to_string(),
            last_connect_time: 0,
            sync_interval: SEC(SYNC_INTERVAL), 
            sync_records: 0,
        }
    }

    fn connect(&mut self) {
        self.client.as_mut().unwrap().connect();
        self.last_connect_time = NOW();
    }

    fn connect_next(&mut self) {
        self.next_client = Some(WebSocketClient::new(
            self.url.as_str(),
            self.subscribe_message.clone(),
        ));
        self.next_client.as_mut().unwrap().connect();
    }

    fn switch(&mut self) {
        self.client.as_mut().unwrap().close();
        self.client = self.next_client.take();
        self.next_client = None;
        self.last_connect_time = NOW();
        println!("------switched------");
    }

    fn receive_message(&mut self) -> Result<String, String> {

        // if connection exceed sync interval, reconnect
        if self.last_connect_time + self.sync_interval < NOW() && self.next_client.is_none() {
            self.connect_next();
        }

        // if the connection_next is not None, receive message
        if self.next_client.is_some() {
            if self.sync_records < SYNC_RECORDS {
                self.sync_records += 1;
                println!("SYNC {}", self.sync_records);
                let message = self._receive_message();
                let m = message.unwrap();
                self.last_message = m.clone();

                return Ok(m);
            }
            else {
                self.sync_records = 0;
                self.switch();

                loop {
                    let message = self._receive_message().unwrap();

                    println!("{} / {}", message, self.last_message);

                    if (message == self.last_message) || (! self.client.as_ref().unwrap().has_message()) {
                        self.last_message = "".to_string();
                        break;
                    }
                }
            }
        }

        return self._receive_message();        
    }

    fn _receive_message(&mut self) -> Result<String, String> {

        let result = self.client.as_mut().unwrap().receive_message();

        match result {
            Ok(_) => {
                return result;
            }
            Err(e) => {
                println!("reconnect");
                self.connect_next();
                self.switch();

                Err(e)
            }
        }
    }
}



/*
pub struct WebSocketClient {
    connection: Option<Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>>,
    url: String,
    pub message_hander: Arc<dyn FnMut(String) -> Result<(), String>>,
    recv_handler: Option<JoinHandle<()>>,
    control_handler: Option<JoinHandle<()>>,
}

const PING_INTERVAL: u64 = 30;


impl WebSocketClient {
    pub fn new<F>(url: &str, f: F) -> Self
        where
            F: FnMut(String) -> Result<(), String> + 'static, {
        WebSocketClient {
            connection: None,
            url: url.to_string(),
            message_hander: Arc::new(f),
            recv_handler: None,
            control_handler: None,
        }
    }

    pub fn connect(&mut self) -> Result<(), String> {
        let (mut socket, response) =
            connect(Url::parse(&self.url).unwrap()).expect("Can't connect");

        println!("Connected to the server");

        println!("Response HTTP code: {}", response.status());
        println!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            println!("* {}", header);
        }

        self.connection = Some(Arc::new(Mutex::new(socket)));

        Ok(())
    }

    pub fn send_message(&mut self, message: &str) -> Result<(), String> {
        let mut guard = self.connection.as_ref().unwrap().lock().unwrap();

        let result = guard.write(Message::Text(message.to_string()));

        match result {
            Ok(_) => {
                println!("Sent subscribe message");
            }
            Err(e) => {
                println!("Error: {:?}", e.to_string());
            }
        }

        guard.flush().unwrap();

        Ok(())
    }

    /*
    fn send_ping(&self) -> Result<(), tungstenite::error::Error> {
        let mut guard = self.connection.as_ref().clone().unwrap().lock().unwrap();
        guard.write(Message::Ping(vec![]))
    }
    */

    fn send_ping(socket: &Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>) -> Result<(), tungstenite::error::Error> {
        let mut guard = socket.lock().unwrap();
        guard.write(Message::Ping(vec![]))
    }

    fn send_pong(socket: &Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>, message: Vec<u8>) -> Result<(), tungstenite::error::Error> {
        let mut guard = socket.lock().unwrap();
        guard.write(Message::Pong(message))
    }

    fn control_loop(socket: &Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>) {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(PING_INTERVAL));
            Self::send_ping(socket);
        }
    }

    fn recv_loop(socket: Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>, message_handler: Arc<dyn FnMut(String) -> Result<(), String>>) -> Result<(), String> {
        loop {
            let mut guard = socket.lock().unwrap();
            let msg = guard.read().unwrap();
            drop(guard);

            match msg {
                Message::Text(s) => {
                    let result = {
                        let h = message_handler.clone();
                        //let handler = h.as_ref();
                        let message_handler = Arc::new(Mutex::new(h.clone()));
                        let handler = message_handler.lock().unwrap();
                        (handler)(s.to_string())
                    };
                    match result {
                        Ok(_) => {}
                        Err(e) => {
                            log::error!("message handler error {}", e);
                        }
                    }
                }
                Message::Binary(_) => {}
                Message::Ping(ping_message) => {
                    log::debug!("PING: {:?}", ping_message);
                    Self::send_pong(&socket, ping_message).unwrap();
                }
                Message::Pong(_) => {}
                Message::Close(_) => {
                    log::debug!("close message");
                    break;
                }
                Message::Frame(_) => {}
            }
        }

        log::debug!("recv loop end");
        Ok(())
    }

    fn start(&mut self) {
        let socket = self.connection.as_ref().unwrap().clone();
        let message_handler = self.message_hander.clone();

         //let message_handler: Box<dyn FnMut(std::string::String) -> Result<(), std::string::String> + 'static> =
 //       let mut message_handler = self.message_hander.as_ref().clone();

        let handler = Some(thread::spawn(move || {
            //Self::recv_loop(socket, message_handler);
        }));
    }
}

use std::sync::{Arc, Mutex};

struct MyClass {
    connection: Option<Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>>,
    pub url: String,
    counter: i32,
    handle: Option<JoinHandle<()>>,
}

impl MyClass {
    fn new(url: &str) -> Self {
        MyClass {
            connection: None,
            url: url.to_string(),
            counter: 0,
            handle: None,
        }
    }

    fn open(&mut self) -> Result<(), String> {
        let (socket, response) = connect(Url::parse(&self.url).unwrap()).expect("Can't connect");

        println!("Connected to the server");

        println!("Response HTTP code: {}", response.status());
        println!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            println!("* {}", header);
        }

        self.connection = Some(Arc::new(Mutex::new(socket)));

        Ok(())
    }

    fn do_something(&mut self) {
        self.counter += 1;
        println!("Doing something! Counter: {}", self.counter);
    }

    fn start(&mut self) {
        let mut inner_counter = self.counter;

        let socket = self.connection.as_ref().unwrap().clone();

        self.handle = Some(thread::spawn(move || {
            loop {
                sleep(Duration::from_secs(60));
                Self::send_ping(socket.clone()).unwrap();
                /*
                inner_counter += 1;
                println!("Doing something in thread! Counter: {}", inner_counter);
                thread::sleep(Duration::from_secs(2));
                connection_b.unwrap();
                connection_b.unwrap().write(Message::Ping(vec![]));
                */
            }
        }));
    }

    fn send_ping(socket: Arc<Mutex<WebSocket<MaybeTlsStream<TcpStream>>>>) -> Result<(), tungstenite::error::Error> {
        let mut guard = socket.lock().unwrap();
        guard.write(Message::Ping(vec![]))
    }


    fn stop(self) {
        if let Some(handle) = self.handle {
            // Here you could send a signal to the thread to stop it gracefully
            // but for the sake of this example, we will just join the thread.
            handle.join().unwrap();
        }
    }
}
*/

#[cfg(test)]
mod test_exchange {
    use super::*;
    use crate::common::init_debug_log;

    #[test]
    fn ws_loop() {
        let mut ws1 = WebSocketClient::new(
            "wss://stream.binance.com/ws",
            json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        "btcusdt@depth"
                    ],
                    "id": 1
                }
            ),
        );

        ws1.connect();

        let mut ws2 = WebSocketClient::new(
            "wss://stream.binance.com/ws",
            json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        "btcusdt@depth"
                    ],
                    "id": 1
                }
            ),
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
    fn reconnect() {
        let mut ws = AutoConnectClient::new(
            "wss://stream.binance.com/ws",
            json!(
                {
                    "method": "SUBSCRIBE",
                    "params": [
                        "btcusdt@trade",
                        // "btcusdt@depth"
                    ],
                    "id": 1
                }
            ),
        );

        ws.connect();

        spawn(move || loop {
            let m = ws.receive_message();
            println!("{}", m.unwrap());
        });

        sleep(Duration::from_secs(20));
    }

    #[test]
    fn test_log_download() {
        init_debug_log();
    }

    #[test]
    fn log_download_temp_test() {
        init_debug_log();
        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path());
    }

    #[test]
    fn log_download_temp_test_bb() {
        init_debug_log();
        //let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";

        let url = "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2020-05-18.csv.gz";

        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path());
    }

    #[test]
    fn test_rest_api() {
        restapi(
            "https://api.binance.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=1000",
            |s| {
                println!("{}", s);
                Ok(())
            },
        )
        .unwrap();
    }

    #[test]
    fn test_rest_get() {
        let s = rest_get(
            "https://api.binance.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=5",
        )
        .unwrap();
        println!("{}", s);
    }
}

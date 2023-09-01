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
    thread, iter::Map, collections::HashMap,
};

use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use ndarray::Array2;
use numpy::{PyArray2, IntoPyArray};
use pyo3::{PyResult, Py, Python};
use serde::{de, Deserialize, Deserializer};
use serde_derive::Serialize;
use serde_json::{json, Value};
use tempfile::tempdir;
use url::Url;
use zip::ZipArchive;

use crate::common::{
    order::Trade,
    time::{MicroSec, HHMM, MICRO_SECOND, NOW, SEC},
};

use std::sync::mpsc::Sender;

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

fn string_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(Decimal::from_f64(num).unwrap()),
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
        println!(">PING>");
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Ping(vec![]));
        self.flush();
    }

    pub fn send_pong(&mut self, message: Vec<u8>) {
        println!(">PONG>: {:?}", message);
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
                println!("<PING<: {:?}", p);
                self.send_pong(p);
            }
            Message::Pong(p) => {
                println!("<PONG<: {:?}", p);
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
    last_ping_time: MicroSec,
    sync_interval: MicroSec,
    sync_records: i64,
}

const SYNC_RECORDS: i64 = 100;
const SYNC_INTERVAL: MicroSec = MICRO_SECOND * 60 * 60 * 6; // every 6H
const PING_INTERVAL: MicroSec = MICRO_SECOND * 60 * 3; // every 3 min

impl AutoConnectClient {
    fn new(url: &str, subscribe_message: Value) -> Self {
        AutoConnectClient {
            client: Some(WebSocketClient::new(url, subscribe_message.clone())),
            next_client: None,
            url: url.to_string(),
            subscribe_message: subscribe_message,
            last_message: "".to_string(),
            last_connect_time: 0,
            last_ping_time: NOW(),
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

        if self.last_ping_time + PING_INTERVAL < NOW() {
            self.client.as_mut().unwrap().send_ping();
            self.last_ping_time = NOW();
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
            } else {
                self.sync_records = 0;
                self.switch();

                loop {
                    let message = self._receive_message().unwrap();

                    println!("{} / {}", message, self.last_message);

                    if (message == self.last_message)
                        || (!self.client.as_ref().unwrap().has_message())
                    {
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
Order book management.
https://binance-docs.github.io/apidocs/spot/en/#diff-depth-stream

1. Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.
2. Buffer the events you receive from the stream.
3. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .
4. Drop any event where u is <= lastUpdateId in the snapshot.
5. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
6. While listening to the stream, each new event's U should be equal to the previous event's u+1.
7. The data in each event is the absolute quantity for a price level.
8. If the quantity is 0, remove the price level.
9. Receiving an event that removes a price level that is not in your local order book can happen and is normal.
*/

use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, Serialize, Deserialize)]
pub struct BoardItem {
    #[serde(deserialize_with = "string_to_decimal")]
    pub  price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]    
    pub size: Decimal
}

impl BoardItem {
    pub fn from_f64(price: f64, size: f64) -> Self {
        BoardItem {
            price: Decimal::from_f64(price).unwrap(),
            size: Decimal::from_f64(size).unwrap(),
        }
    }
}

#[derive(Debug)]
pub struct Board {
    step: Decimal,    
    asc: bool,
    board: HashMap<Decimal, Decimal>,
}

impl Board {
    pub fn new(step: Decimal, asc: bool) -> Self {
        Board {
            step,
            asc,
            board: HashMap::new(),
        }
    }

    pub fn set(&mut self, price: &Decimal, size: &Decimal) {
        if *size == dec!(0.0) {
            self.board.remove(price);
            return;
        }

        self.board.insert(*price, *size);
    }

    fn step(&self) -> Decimal {
        if self.asc {
            self.step
        }
        else {
            - self.step
        }
    }

    // Keyをソートして、Vecにして返す
    // ascがtrueなら昇順、falseなら降順
    // stepサイズごとで0の値も含めて返す
    // stepサイズが０のときは、stepサイズを無視して返す
    pub fn get(&self) -> Vec<(Decimal, Decimal)>{
        let mut v: Vec<(Decimal, Decimal)> = vec![];

        let mut keys: Vec<_> = self.board.keys().collect();
        if self.asc {
            keys.sort_by(|a, b| a.partial_cmp(&b).unwrap());
        }
        else {
            keys.sort_by(|a, b| b.partial_cmp(&a).unwrap());
        }

        let mut current_value = dec!(0.0);

        for k in keys {
            if current_value == dec!(0.0) {
                current_value = *k;
            }

            while current_value != *k {
                v.push((current_value, dec!(0.0)));
                current_value += self.step();
            }
            
            v.push((*k, self.board[k]));
            current_value += self.step();            
        }

        v
    }

    pub fn get_array(&self) -> Array2<f64> {
        return Self::to_ndarray(&self.get());
    }

    pub fn clear(&mut self) {
        self.board.clear();
    }

    // convert to ndarray
    pub fn to_ndarray(board: &Vec<(Decimal, Decimal)>) -> Array2<f64>{
        let shape = (board.len(), 2);
        let mut array_vec: Vec<f64>= Vec::with_capacity(shape.0 * shape.1);

        for (a, b) in board.iter() {
            array_vec.push(a.to_f64().unwrap());
            array_vec.push(b.to_f64().unwrap());
        }

        let array: Array2<f64> = Array2::from_shape_vec(shape, array_vec).unwrap();

        array
    }

    pub fn to_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.get_array();
        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);
            py_array2.to_owned()
        });

        return Ok(r);
    }
}


struct OrderBook {
    symbol: String,
    last_update_id: i64,
    bids: Board,
    asks: Board,
}

impl OrderBook {
    pub fn new(symbol: String, step: Decimal) -> Self {
        OrderBook {
            symbol: symbol,
            last_update_id: 0,
            bids: Board::new(step, false),
            asks: Board::new(step, true),
        }
    }

    pub fn update(&mut self, update_id: i64, bids: Vec<(Decimal, Decimal)>, asks: Vec<(Decimal, Decimal)>) {
        if self.last_update_id == 0 {
            self.last_update_id = update_id;
        }

        if self.last_update_id + 1 != update_id {
            log::error!("update_id error {} / {}", self.last_update_id, update_id);
            return;
        }

        self.last_update_id = update_id;

        for (price, size) in bids {
            self.bids.set(&price, &size);
        }

        for (price, size) in asks {
            self.asks.set(&price, &size);
        }
    }



    // get all data from rest api
    pub fn reflesh(&mut self) {
        self.bids.clear();
        self.asks.clear();

        let path = format!("/api/v3/depth?symbol={}&limit=1000", self.symbol);
        let s = rest_get("https://api.binance.com", path.as_str());



        match s {
            Ok(s) => {

                let v: Value = serde_json::from_str(s.as_str()).unwrap();
                println!("{:?}", v.to_string());                

                let update_id = v["lastUpdateId"].as_i64().unwrap();
                let mut bids: Vec<(Decimal, Decimal)> = vec![];
                let mut asks: Vec<(Decimal, Decimal)> = vec![];

                for bid in v["bids"].as_array().unwrap() {
                    let price = bid[0].as_str().unwrap().parse::<Decimal>().unwrap();
                    let size = bid[1].as_str().unwrap().parse::<Decimal>().unwrap();
                    bids.push((price, size));
                }

                for ask in v["asks"].as_array().unwrap() {
                    let price = ask[0].as_str().unwrap().parse::<Decimal>().unwrap();
                    let size = ask[1].as_str().unwrap().parse::<Decimal>().unwrap();
                    asks.push((price, size));
                }

                self.update(update_id, bids, asks);
            }
            Err(e) => {
                log::error!("{}", e);
            }
        }
    }
}













#[cfg(test)]
mod test_exchange {
    use super::*;
    use crate::common::init_debug_log;
    use std::thread::sleep;
    use std::thread::spawn;
    use std::time::Duration;


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


    #[test]
    fn test_board_set() {
        let mut b = Board::new(dec!(0.5), true);

        b.set(&dec!(10.0), &dec!(1.0));
        println!("{:?}", b.get());

        b.set(&dec!(9.0), &dec!(1.5));
        println!("{:?}", b.get());

        b.set(&dec!(11.5), &dec!(2.0));
        println!("{:?}", b.get());

        b.set(&dec!(9.0), &dec!(0.0));
        println!("{:?}", b.get());

        let mut b = Board::new(dec!(0.5), false);

        println!("---------desc----------");

        b.set(&dec!(10.0), &dec!(1.0));
        println!("{:?}", b.get());

        b.set(&dec!(9.0), &dec!(1.5));
        println!("{:?}", b.get());

        b.set(&dec!(11.5), &dec!(2.0));
        println!("{:?}", b.get());

        b.set(&dec!(9.0), &dec!(0.0));
        println!("{:?}", b.get());

        println!("---------clear----------");
        b.clear();
        println!("{:?}", b.get());

    }

    #[test]
    fn to_ndarray() {
        let mut b = Board::new(dec!(0.5), true);
        b.set(&dec!(10.0), &dec!(1.0));
        b.set(&dec!(12.5), &dec!(1.0));        

        let array = b.get_array();
        println!("{:?}", array);
    }

    #[test]
    fn update_data() {
        let mut b = OrderBook::new("BTCBUSD".to_string(), dec!(0.5));
        b.reflesh();
        println!("{:?}", b.bids.get());
        println!("{:?}", b.asks.get());
    }
}

// Copyright(c) 2023. yasstake. All rights reserved.
// Abloultely no warranty.

pub mod bb;
pub mod binance;
// pub mod ftx;

use core::panic;
use std::{
    collections::HashMap,
    fs::File,
    io::{copy, BufReader, Cursor, Write},
    iter::Map,
    net::TcpStream,
    path::Path,
    sync::{Arc, Mutex},
    thread,
};

use chrono::format;
use csv::{self, StringRecord};
use flate2::bufread::GzDecoder;
use ndarray::Array2;
use numpy::{IntoPyArray, PyArray2};
use pyo3::{Py, PyResult, Python};
use reqwest::Method;
use serde::{de, Deserialize, Deserializer};
use serde_derive::Serialize;
use serde_json::{json, Value};
use tempfile::tempdir;
use url::Url;
use zip::ZipArchive;

use crate::{
    common::{
        order::Trade,
        time::{MicroSec, HHMM, MICRO_SECOND, NOW, SEC},
    },
    db::sqlite::TradeTable,
    exchange::binance::{BinanceConfig, BinanceMarket},
};

use std::sync::mpsc::Sender;

use tungstenite::Message;
use tungstenite::{connect, stream::MaybeTlsStream};
use tungstenite::{http::request, protocol::WebSocket};

pub fn open_db(exchange_name: &str, trade_type: &str, trade_symbol: &str) -> TradeTable {
    match exchange_name.to_uppercase().as_str() {
        "BN" => match trade_type.to_uppercase().as_str() {
            "SPOT" => {
                log::debug!("open_db: Binance / {}", trade_symbol);
                let config = BinanceConfig::SPOT(trade_symbol.to_string());
                let binance = BinanceMarket::new(&config);

                return binance.db;
            }
            "MARGIN" => {
                log::debug!("open_db: Binance / {}", trade_symbol);
                panic!("Not implemented yet");
            }
            _ => {
                panic!("Unknown trade type {}", trade_type);
            }
        },
        /*
        "BB" => {
            log::debug!("open_db: ByBit / {}", market_name);
            let bb = BBMarket::new(market_name, true);

            return bb.db;
        },
        */
        _ => {
            panic!("Unknown exchange {}", exchange_name);
        }
    }
}

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

pub fn do_rest_request(
    method: Method,
    url: &str,
    headers: Vec<(&str, &str)>,
    body: &str
) -> Result<String, String> {
    let client = reqwest::blocking::Client::new();
    
    log::debug!("method({:?}) / URL = {} / path{}", method, url, body);
    
    let mut request_builder = client.request(method, url);

    // make request builder as a common function.
    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    if body !=  "" {
        request_builder = request_builder.body(body.to_string());
    }

    request_builder = request_builder
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html");

    let response = match request_builder.send() {
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

pub fn rest_get(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    param: Option<&str>,
    body: Option<&str>
) -> Result<String, String> {
    let mut url = format!("{}{}", server, path);
    if param.is_some() {
        url = format!("{}?{}", url, param.unwrap());
    }

    let body_string = match body {
        Some(b) => b,
        None => "",
    };

    do_rest_request(Method::GET, &url, headers, body_string)
}

pub fn rest_post(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::POST, &url, headers, body)
}

pub fn rest_delete(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::DELETE, &url, headers, body)
}

pub fn rest_put(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> Result<String, String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::PUT, &url, headers, body)
}


pub fn restapi<F>(server: &str, path: &str, f: F) -> Result<(), String>
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

pub fn check_exist(url: &str) -> bool {
    let client = reqwest::blocking::Client::new();

    let response = match client
        .head(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
    {
        Ok(r) => r,
        Err(e) => {
            log::error!("URL get error {}", e.to_string());
            return false;
        }
    };

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    if response.status().as_str() == "200" {
        return true;
    } else {
        return false;
    }
}

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
        let (mut socket, response) =
            connect(Url::parse(&self.url).unwrap()).expect("Can't connect");

        log::debug!("Connected to the server");

        log::debug!("Response HTTP code: {}", response.status());
        log::debug!("Response contains the following headers:");

        for (ref header, _value) in response.headers() {
            println!("* {}", header);
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
        log::debug!(">PING>");
        let connection = self.connection.as_mut().unwrap();
        connection.write(Message::Ping(vec![]));
        self.flush();
    }

    pub fn send_pong(&mut self, message: Vec<u8>) {
        log::debug!(">PONG>: {:?}", message);
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
                return Ok(t);
            }
            Message::Binary(b) => {
                log::debug!("BINARY: {:?}", b);
            }
            Message::Ping(p) => {
                log::debug!(">PING>: {:?}", p);
                self.send_pong(p);
            }
            Message::Pong(p) => {
                log::debug!(">PONG>: {:?}", p);
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

struct AutoConnectClient {
    client: Option<WebSocketClient>,
    next_client: Option<WebSocketClient>,
    url: String,
    subscribe_message: Option<Value>,
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
    pub fn new(url: &str, message: Option<Value>) -> Self {
        AutoConnectClient {
            client: Some(WebSocketClient::new(url, message.clone())),
            next_client: None,
            url: url.to_string(),
            subscribe_message: message.clone(),
            last_message: "".to_string(),
            last_connect_time: 0,
            last_ping_time: NOW(),
            sync_interval: SEC(SYNC_INTERVAL),
            sync_records: 0,
        }
    }

    pub fn connect(&mut self) {
        self.client.as_mut().unwrap().connect();
        self.last_connect_time = NOW();
    }

    pub fn connect_next(&mut self) {
        self.next_client = Some(WebSocketClient::new(
            self.url.as_str(),
            self.subscribe_message.clone(),
        ));
        self.next_client.as_mut().unwrap().connect();
    }

    pub fn switch(&mut self) {
        self.client.as_mut().unwrap().close();
        self.client = self.next_client.take();
        self.next_client = None;
        self.last_connect_time = NOW();

        log::debug!("------switched------");
    }

    pub fn receive_message(&mut self) -> Result<String, String> {
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
                log::debug!("SYNC {}", self.sync_records);
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
                log::debug!("reconnect");
                self.connect_next();
                self.switch();

                Err(e)
            }
        }
    }
}

use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

#[derive(Debug, Serialize, Deserialize)]
pub struct BoardItem {
    #[serde(deserialize_with = "string_to_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub size: Decimal,
}

impl BoardItem {
    pub fn from_f64(price: f64, size: f64) -> Self {
        BoardItem {
            price: Decimal::from_f64(price).unwrap(),
            size: Decimal::from_f64(size).unwrap(),
        }
    }

    pub fn from_decimal(price: Decimal, size: Decimal) -> Self {
        BoardItem {
            price: price,
            size: size,
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

    pub fn set(&mut self, price: Decimal, size: Decimal) {
        if size == dec!(0.0) {
            self.board.remove(&price);
            return;
        }

        self.board.insert(price, size);
    }

    fn step(&self) -> Decimal {
        if self.asc {
            self.step
        } else {
            -self.step
        }
    }

    // Keyをソートして、Vecにして返す
    // ascがtrueなら昇順、falseなら降順
    // stepサイズごとで0の値も含めて返す
    // stepサイズが０のときは、stepサイズを無視して返す
    pub fn get(&self) -> Vec<BoardItem> {
        let mut vec: Vec<BoardItem> = Vec::from_iter(
            self.board
                .iter()
                .map(|(k, v)| BoardItem::from_decimal(*k, *v)),
        );

        if self.asc {
            vec.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
        } else {
            vec.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        }

        vec
    }

    pub fn get_with_fill(&self) -> Vec<BoardItem> {
        let mut v: Vec<BoardItem> = vec![];
        let mut current_price = dec!(0.0);

        for item in self.get() {
            if current_price == dec!(0.0) {
                current_price = item.price;
            }

            while current_price != item.price {
                v.push(BoardItem::from_decimal(current_price, dec!(0.0)));
                current_price += self.step();
            }

            v.push(BoardItem::from_decimal(item.price, item.size));
            current_price += self.step();
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
    pub fn to_ndarray(board: &Vec<BoardItem>) -> Array2<f64> {
        let shape = (board.len(), 2);
        let mut array_vec: Vec<f64> = Vec::with_capacity(shape.0 * shape.1);

        for item in board {
            array_vec.push(item.price.to_f64().unwrap());
            array_vec.push(item.size.to_f64().unwrap());
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

#[derive(Debug)]
struct Depth {
    bids: Board,
    asks: Board,
}

impl Depth {
    pub fn new(step: Decimal) -> Self {
        Depth {
            bids: Board::new(step, false),
            asks: Board::new(step, true),
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.asks.get()
    }

    pub fn get_asks_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.asks.to_pyarray()
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.bids.get()
    }

    pub fn get_bids_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.bids.to_pyarray()
    }

    pub fn set_bids(&mut self, price: Decimal, size: Decimal) {
        self.bids.set(price, size);
    }

    pub fn set_asks(&mut self, price: Decimal, size: Decimal) {
        self.asks.set(price, size);
    }
}

#[derive(Debug)]
pub struct OrderBook {
    symbol: String,
    depth: Depth,
}

impl OrderBook {
    pub fn new(symbol: String, step: Decimal) -> Self {
        OrderBook {
            symbol: symbol,
            depth: Depth::new(step),
        }
    }

    pub fn update(&mut self, bids_diff: &Vec<BoardItem>, asks_diff: &Vec<BoardItem>, force: bool) {
        if force {
            self.depth.clear();
        }

        for item in bids_diff {
            self.depth.set_bids(item.price, item.size);
        }

        for item in asks_diff {
            self.depth.set_asks(item.price, item.size);
        }
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.depth.bids.get()
    }

    pub fn get_bids_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.depth.bids.to_pyarray()
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.depth.asks.get()
    }

    pub fn get_asks_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.depth.asks.to_pyarray()
    }
}

#[cfg(test)]
mod test_exchange {
    use super::*;
    use crate::common::init_debug_log;
    use crate::exchange::binance::BinanceMarket;
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
    fn reconnect() {
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
            vec![],
            None,
            None,
        )
        .unwrap();
        println!("{}", s);
    }

    #[test]
    fn test_board_set() {
        let mut b = Board::new(dec!(0.5), true);

        b.set(dec!(10.0), dec!(1.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(1.5));
        println!("{:?}", b.get());

        b.set(dec!(11.5), dec!(2.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(0.0));
        println!("{:?}", b.get());

        let mut b = Board::new(dec!(0.5), false);

        println!("---------desc----------");

        b.set(dec!(10.0), dec!(1.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(1.5));
        println!("{:?}", b.get());

        b.set(dec!(11.5), dec!(2.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(0.0));
        println!("{:?}", b.get());

        println!("---------clear----------");
        b.clear();
        println!("{:?}", b.get());
    }

    #[test]
    fn to_ndarray() {
        let mut b = Board::new(dec!(0.5), true);
        b.set(dec!(10.0), dec!(1.0));
        b.set(dec!(12.5), dec!(1.0));

        let array = b.get_array();
        println!("{:?}", array);
    }
}

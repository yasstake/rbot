// Copyright(c) 2023-4. yasstake. All rights reserved.
// Abloultely no warranty.

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use chrono::Datelike;

// use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use csv::StringRecord;
use flate2::read::GzDecoder;
use polars::chunked_array::ops::ChunkCast;
use polars::datatypes::DataType;
use polars::frame::DataFrame;
use polars::io::csv::read::CsvReadOptions;
use polars::io::parquet::write::ParquetWriter;
use polars::io::SerReader;
use polars::prelude::Series;
use reqwest::Method;
use rust_decimal::Decimal;
use std::io::BufRead;
use std::io::BufWriter;
use std::path::PathBuf;
use std::str::FromStr;
use std::{
    fs::File,
    io::{copy, BufReader, Cursor, Write},
    path::Path,
    thread::sleep,
    time::Duration,
};
use tempfile::tempdir;
//use tokio::spawn;
use zip::ZipArchive;

use crate::common::time_string;
use crate::common::AccountCoins;
use crate::common::{
    flush_log, to_naive_datetime, BoardTransfer, Kline, LogStatus, MarketConfig, MicroSec, Order,
    OrderSide, OrderType, ServerConfig, Trade, DAYS, FLOOR_DAY, TODAY,
};
use crate::db::KEY;
//use crate::db::KEY::low;
use async_trait::async_trait;

pub trait RestApi {
    async fn get_board_snapshot(
        &self,
        config: &MarketConfig,
    ) -> anyhow::Result<BoardTransfer>;

    async fn get_recent_trades(
        &self,
        config: &MarketConfig,
    ) -> anyhow::Result<Vec<Trade>>;

    async fn get_trade_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<Vec<Kline>>;

    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>>;
    async fn cancel_order(
        &self,
        config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order>;
    async fn open_orders(
        &self,
        config: &MarketConfig,
    ) -> anyhow::Result<Vec<Order>>;

    async fn get_account(&self)
        -> anyhow::Result<AccountCoins>;

    async fn has_archive(
        &self,
        config: &MarketConfig,
        date: MicroSec,
    ) -> anyhow::Result<bool>;

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String;
    fn archive_has_header(&self) -> bool;
    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame>;
}

#[async_trait]
pub trait RestTrait: RestApi + Send + Sync + Clone {}

/*
async fn download_archive_log<F>(
    url: &str,
    tx: &Sender<Vec<Trade>>,
    has_header: bool,
    low_priority: bool,
    verbose: bool,
    f: F,
) -> anyhow::Result<i64>
where
    F: Fn(&StringRecord) -> Trade,
{
    log::debug!("Downloading ...[{}]", url);

    let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

    let file_path = log_download_tmp(url, tmp_dir.path())
        .await
        .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

    let mut buffer: Vec<Trade> = vec![];
    let mut is_first_record = true;
    let mut download_rec = 0;

    let file_path = PathBuf::from_str(&file_path)?;

    read_csv_archive(&file_path, has_header, |rec| {
        let mut trade = f(&rec);
        download_rec += 1;
        trade.status = LogStatus::FixArchiveBlock;

        if MAX_BUFFER_SIZE <= buffer.len() {
            if is_first_record {
                buffer[0].status = LogStatus::FixBlockStart;
                is_first_record = false;
            }

            if low_priority && LOW_QUEUE_SIZE < tx.len() {
                sleep(Duration::from_millis(100));
            }

            let result = tx.send(buffer.to_vec());

            match result {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{:?}", e);
                }
            }
            buffer.clear();
        }
        buffer.push(trade);

        Ok(())
    });

    let buffer_len = buffer.len();
    ensure!(buffer_len != 0, "download rec = 0");

    buffer[buffer_len - 1].status = LogStatus::FixBlockEnd;

    tx.send(buffer.to_vec())
        .with_context(|| format!("channel send error"))?;

    buffer.clear();

    log::debug!("download rec = {}", download_rec);
    if verbose {
        println!(" download complete rec = {}", download_rec);
        flush_log();
    }

    std::fs::remove_file(&file_path).with_context(|| format!("remove file error {:?}", file_path))?;

    Ok(download_rec)
}

pub fn read_csv_archive<F>(file_path: &PathBuf, has_header: bool, mut f: F) -> anyhow::Result<()>
where
    F: FnMut(&StringRecord) -> anyhow::Result<()>,
{
    log::debug!("read_csv_archive = {:?}", file_path);

//    let file_path = Path::new(file_path);
    let file = File::open(file_path)?;

    match file_path.extension().unwrap().to_str().unwrap() {
        "gz" | "GZ" => {
            let gzip_reader = BufReader::new(file);
            let gzip_reader = GzDecoder::new(gzip_reader);
            let mut csv_reader = csv::Reader::from_reader(gzip_reader);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }

            Ok(())
        }

        "zip" | "ZIP" => {
            let mut zip = ZipArchive::new(file)?;

            let file = zip.by_index(0).unwrap();
            let mut csv_reader = csv::Reader::from_reader(file);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }
            Ok(())
        }
        _ => {
            let mut csv_reader = csv::Reader::from_reader(file);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }
            Ok(())
        }
    }
}

pub fn read_line_archive<F>(file_path: &str, has_header: bool, mut f: F) -> anyhow::Result<()>
where
    F: FnMut(&str),
{
    log::debug!("read_csv_archive = {}", file_path);

    let file_path = Path::new(file_path);
    let file = File::open(file_path)?;

    match file_path.extension().unwrap().to_str().unwrap() {
        "gz" | "GZ" => {
            let gzip_reader = GzDecoder::new(file);
            let mut reader = BufReader::new(gzip_reader);
            if has_header {
                let mut buf = String::new();
                reader.read_line(&mut buf);
                log::debug!("csv header\n{}",buf);
            }
            for line in reader.lines() {
                let line = line?;
                f(&line);
            }
            Ok(())
        }
        "zip" | "ZIP" => {
            let mut zip = ZipArchive::new(file)?;
            let zip_file = zip.by_index(0).unwrap();
            let mut reader = BufReader::new(zip_file);
            if has_header {
                let mut buf = String::new();
                reader.read_line(&mut buf);
                log::debug!("csv header\n{}",buf);
            }
            for line in reader.lines() {
                let line = line?;
                f(&line);
            }
            Ok(())
        }
        _ => {
            let mut reader = BufReader::new(file);
            if has_header {
                let mut buf = String::new();
                reader.read_line(&mut buf);
                log::debug!("csv header\n{}",buf);
            }
            for line in reader.lines() {
                let line = line?;
                f(&line);
            }
            Ok(())
        }
    }
}

pub fn make_download_url_list<F>(name: &str, days: Vec<i64>, f: F) -> Vec<String>
where
    F: Fn(&str, i64) -> String,
{
    let mut urls: Vec<String> = vec![];
    for day in days {
        urls.push(f(name, day));
    }
    urls
}
*/

const MAX_BUFFER_SIZE: usize = 4096;
const LOW_QUEUE_SIZE: usize = 5;

pub async fn do_rest_request(
    method: Method,
    url: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> anyhow::Result<String> {
    let client = reqwest::Client::new();

    let mut request_builder = client.request(method.clone(), url);

    // make request builder as a common function.
    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    if body != "" {
        request_builder = request_builder.body(body.to_string());
    }

    request_builder = request_builder
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html");

    let response = request_builder
        .send()
        .await
        .with_context(|| format!("URL get error {url:}"))?;

    if response.status().as_str() != "200" {
        return Err(anyhow!(
            "Response code = {} / download size {:?} / method({:?}) /  response body = {}",
            response.status().as_str(),
            response.content_length(),
            method,
            &body,
        ));
    }

    let body = response
        .text()
        .await
        .with_context(|| format!("response text error"))?;

    Ok(body)
}

pub async fn rest_get(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    param: Option<&str>,
    body: Option<&str>,
) -> anyhow::Result<String> {
    let mut url = format!("{}{}", server, path);
    if param.is_some() {
        url = format!("{}?{}", url, param.unwrap());
    }

    let body_string = match body {
        Some(b) => b,
        None => "",
    };

    do_rest_request(Method::GET, &url, headers, body_string).await
}

pub async fn rest_post(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> anyhow::Result<String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::POST, &url, headers, body).await
}

pub async fn rest_delete(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> anyhow::Result<String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::DELETE, &url, headers, body).await
}

pub async fn rest_put(
    server: &str,
    path: &str,
    headers: Vec<(&str, &str)>,
    body: &str,
) -> anyhow::Result<String> {
    let url = format!("{}{}", server, path);

    do_rest_request(Method::PUT, &url, headers, body).await
}

pub async fn check_exist(url: &str) -> anyhow::Result<bool> {
    let client = reqwest::Client::new();

    let response = client
        .head(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
        .await
        .with_context(|| format!("URL get error {}", url))?;

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap()
    );

    anyhow::ensure!(
        response.status().as_str() == "200",
        "URL get response error {}/code={}",
        url,
        response.status()
    );

    Ok(true)
}

// TODO: remove this function
async fn has_archive<F>(date: MicroSec, f: &F) -> bool
where
    F: Fn(MicroSec) -> String,
{
    let url = f(date);

    let result = check_exist(url.as_str()).await;

    if result.is_err() {
        return false;
    }

    result.unwrap()
}

// TODO: remove this function (move to )
pub async fn latest_archive_date<F>(f: &F) -> Result<MicroSec, String>
where
    F: Fn(MicroSec) -> String,
{
    let mut latest = TODAY();
    let mut i = 0;

    loop {
        let has_archive = has_archive(latest, f).await;

        if has_archive {
            log::debug!("latest archive date = {}({})", time_string(latest), latest);
            return Ok(latest);
        }

        latest -= DAYS(1);
        i += 1;

        if 5 < i {
            return Err(format!("get_latest_archive max retry error"));
        }
    }
}

#[cfg(test)]
mod test_exchange {
    use crate::net::rest_get;

    #[tokio::test]
    async fn test_rest_get_err() -> anyhow::Result<()> {
        let r = rest_get(
            "https://example.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=5",
            vec![],
            None,
            None,
        )
        .await;

        println!("{:?}", r);

        Ok(())
    }
}

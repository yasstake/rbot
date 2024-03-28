// Copyright(c) 2023-4. yasstake. All rights reserved.
// Abloultely no warranty.

use anyhow::anyhow;
use anyhow::ensure;
use anyhow::Context;
use chrono::Datelike;

// use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;
use csv::StringRecord;
use flate2::bufread::GzDecoder;
use reqwest::Method;
use rust_decimal::Decimal;
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
//use crate::db::KEY::low;

pub trait RestApi<T>
where
    T: ServerConfig,
{
    async fn get_board_snapshot(server: &T, config: &MarketConfig)
        -> anyhow::Result<BoardTransfer>;

    async fn get_recent_trades(server: &T, config: &MarketConfig) -> anyhow::Result<Vec<Trade>>;

    fn get_trade_klines(
        server: &T,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<Kline>>> + Send;
    fn new_order(
        server: &T,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<Order>>> + Send;
    fn cancel_order(
        server: &T,
        config: &MarketConfig,
        order_id: &str,
    ) -> impl std::future::Future<Output = anyhow::Result<Order>> + Send;
    fn open_orders(
        server: &T,
        config: &MarketConfig,
    ) -> impl std::future::Future<Output = anyhow::Result<Vec<Order>>> + Send;

    fn get_account(
        server: &T,
    ) -> impl std::future::Future<Output = anyhow::Result<AccountCoins>> + Send;

    fn history_web_url(server: &T, config: &MarketConfig, date: MicroSec) -> String {
        let history_web_base = server.get_historical_web_base();
        let category = &config.trade_category;
        let symbol = &config.trade_symbol;

        let timestamp = to_naive_datetime(date);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        Self::format_historical_data_url(&history_web_base, &category, &symbol, yyyy, mm, dd)
    }

    fn format_historical_data_url(
        history_web_base: &str,
        category: &str,
        symbol: &str,
        yyyy: i64,
        mm: i64,
        dd: i64,
    ) -> String;

    /// Convert archived CSV trade log record to Trade struct
    /// timestamp,      symbol,side,size,price,  tickDirection,trdMatchID,                          grossValue,  homeNotional,foreignNotional
    /// 1620086396.8268,BTCUSDT,Buy,0.02,57199.5,ZeroMinusTick,224061a0-e105-508c-9696-b53ab4b5bb03,114399000000.0,0.02,1143.99    
    fn rec_to_trade(rec: &StringRecord) -> Trade;
    fn archive_has_header() -> bool;

    async fn latest_archive_date(server: &T, config: &MarketConfig) -> anyhow::Result<MicroSec> {
        let f = |date: MicroSec| -> String { Self::history_web_url(server, config, date) };
        let mut latest = TODAY();
        let mut i = 0;

        loop {
            log::debug!("check log exist = {}({})", time_string(latest), latest);

            if has_archive(latest, &f).await {
                return Ok(latest);
            }

            latest -= DAYS(1);
            i += 1;

            if 5 < i {
                return Err(anyhow!(
                    "Find archive retry over {}/{}/{}",
                    i,
                    latest,
                    f(latest)
                ));
            }
        }
    }

    fn download_archive(
        server: &T,
        config: &MarketConfig,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        low_priority: bool,
        verbose: bool,
    ) -> impl std::future::Future<Output = anyhow::Result<i64>> {
        async move {
            let date = FLOOR_DAY(date);
            let url = Self::history_web_url(server, config, date);
            let has_header = Self::archive_has_header();

            download_archive_log(
                &url,
                tx,
                has_header,
                low_priority,
                verbose,
                &Self::rec_to_trade,
            )
            .await
        }
    }
}

pub async fn log_download_tmp(url: &str, tmp_dir: &Path) -> anyhow::Result<String> {
    let client = reqwest::Client::new();

    let response = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
        .await
        .with_context(|| format!("URL get error {}", url))?;

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        response.content_length().unwrap_or_default() // if error, return 0
    );

    if !response.status().is_success() {
        return Err(anyhow!("Err: response code {}", response.status().as_str()));
    }

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let fname = tmp_dir.join(fname);
    let file_name = fname.to_str().unwrap();

    let mut target = File::create(&fname)
        .with_context(|| format!("file create error {}", fname.to_str().unwrap()))?;

    let content = response
        .bytes()
        .await
        .with_context(|| format!("response bytes error {}", url))?;
    let mut cursor = Cursor::new(content);

    let size =
        copy(&mut cursor, &mut target).with_context(|| format!("write error {}", file_name))?;
    ensure!(size > 0, "file is empty {}", file_name);

    let _r = target.flush();

    log::debug!("download size {}", target.metadata().unwrap().len());

    Ok(file_name.to_string())
}

/*
pub async fn log_download<T, F>(url: &str, sender: &mut Sender<T>, has_header: bool, f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord)->T,
{
    log::debug!("Downloading ...[{}]", url);

    let tmp_dir = match tempdir() {
        Ok(tmp) => tmp,
        Err(e) => {
            log::error!("create tmp dir error {}", e.to_string());
            return Err(format!("create tmp dir error {}", e.to_string()));
        }
    };

    let result = log_download_tmp(url, tmp_dir.path()).await;

    let file_path = match result {
        Ok(path) => path,
        Err(e) => {
            log::error!("download error {}", e.to_string());
            return Err(format!("download error{}", e));
        }
    };

    log::debug!("let's extract = {}", file_path);

    if url.ends_with("gz") || url.ends_with("GZ") {
        log::debug!("extract gzip = {}", file_path);
        return extract_gzip_log(&file_path, has_header, f);
    } else if url.ends_with("zip") || url.ends_with("ZIP") {
        log::debug!("extract zip = {}", file_path);
        return extract_zip_log(&file_path, has_header, f);
    } else {
        log::error!("unknown file suffix {}", url);
        return Err(format!("unknown file suffix").to_string());
    }

    // remove tmp file
}
*/

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

    Ok(download_rec)
}

fn read_csv_archive<F>(file_path: &str, has_header: bool, mut f: F)
where
    F: FnMut(&StringRecord),
{
    log::debug!("read_csv_archive = {}", file_path);

    let file_path = Path::new(file_path);
    match file_path.extension().unwrap().to_str().unwrap() {
        "gz" | "GZ" => {
            let file = File::open(file_path).unwrap();
            let bufreader = BufReader::new(file);
            let gzip_reader = std::io::BufReader::new(GzDecoder::new(bufreader));
            let mut csv_reader = csv::Reader::from_reader(gzip_reader);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }
        }

        "zip" | "ZIP" => {
            let file = File::open(file_path).unwrap();
            let bufreader = BufReader::new(file);
            let mut zip = match ZipArchive::new(bufreader) {
                Ok(z) => z,
                Err(e) => {
                    log::error!("extract zip log error {}", e.to_string());
                    return;
                }
            };

            let file = zip.by_index(0).unwrap();
            let mut csv_reader = csv::Reader::from_reader(file);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }
        }
        _ => {
            let file = File::open(file_path).unwrap();
            let bufreader = BufReader::new(file);
            let mut csv_reader = csv::Reader::from_reader(bufreader);
            if has_header {
                csv_reader.has_headers();
            }

            for csv_rec in csv_reader.records() {
                let rec = csv_rec.unwrap();
                f(&rec);
            }
        }
    }
}

/*
async fn extract_zip_log<T, F>(path: &String, sender: &Sender<T>, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord)->T,
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
            return Err(format!("extract zip log error {}", e.to_string()));
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
                sender.send(f(&string_rec)).await;
                rec_count += 1;
            }
        }
    }

    Ok(rec_count)
}

async fn extract_gzip_log<T, F>(path: &String, sender: &Sender<T>, has_header: bool, mut f: F) -> Result<i64, String>
where
    F: FnMut(&StringRecord)->T,
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
            sender.send(f(&string_rec)).await;
            rec_count += 1;
        }
    }

    Ok(rec_count)
}
*/

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

const MAX_BUFFER_SIZE: usize = 4096;

const LOW_QUEUE_SIZE: usize = 5;

/*
pub async fn download_archive_log<F>(
    url: &String,
    tx: &Sender<Vec<Trade>>,
    has_header: bool,
    low_priority: bool,
    verbose: bool,
    f: &F,
) -> Result<i64, String>
where
    F: Fn(&StringRecord) -> Trade,
{
    let queue_capacity = tx.max_capacity();

    // TODO:  レコードが割り切れる場合、最後のレコードのstatusをFixBlockEndにする。
    if verbose {
        print!("log download (url = {})", url);
        flush_log();
    }

    let max_queue = if low_priority {
        LOW_QUEUE_SIZE
    } else {
        MAX_QUEUE_SIZE
    };

    let mut download_rec = 0;

    let mut buffer: Vec<Trade> = vec![];
    let mut is_first_record = true;



    let result = log_download::<Vec<Trade>>(url.as_str(), &mut tx, has_header, |rec| {
        let mut trade = f(&rec);
        trade.status = LogStatus::FixArchiveBlock;

        buffer.push(trade);

        if MAX_BUFFER_SIZE < buffer.len() {
            if is_first_record {
                buffer[0].status = LogStatus::FixBlockStart;
                is_first_record = false;
            }

            while max_queue < queue_capacity - tx.capacity() {
                sleep(Duration::from_millis(100));
            }

            let result = tx.send(buffer.to_vec()).await;

            match result {
                Ok(_) => {}
                Err(e) => {
                    log::error!("{:?}", e);
                }
            }
            buffer.clear();
        }
    })
    .await;

    let buffer_len = buffer.len();

    if buffer_len != 0 {
        buffer[buffer_len - 1].status = LogStatus::FixBlockEnd;

        let result = tx.send(buffer.to_vec()).await;
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
            return Err(format!("extract err = {}", e.as_str()));
        }
    }

    log::debug!("download rec = {}", download_rec);
    if verbose {
        println!(" download complete rec = {}", download_rec);
        flush_log();
    }

    return Ok(download_rec);
}
*/

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
    // const MAX_QUEUE_SIZE: usize = 100;

    use super::*;
    use crate::common::init_debug_log;
    // use crossbeam_channel::bounded;

    #[tokio::test]
    async fn log_download_temp_test() -> anyhow::Result<()> {
        init_debug_log();
        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        let tmp_dir = tempdir().unwrap();
        let _r = log_download_tmp(url, tmp_dir.path()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_log_download() -> anyhow::Result<()> {
        init_debug_log();

        let url = "https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip";
        // let (tx, rx) = bounded::<StringRecord>(MAX_QUEUE_SIZE);

        let tmp_dir = tempdir()?;
        let path = log_download_tmp(url, tmp_dir.path()).await?;
        log::debug!("log_download_temp: {}", path);

        let mut rec_no = 0;

        read_csv_archive(&path, true, |_rec| {
            rec_no += 1;
        });

        log::debug!("rec_no = {}", rec_no);

        Ok(())
    }

    #[tokio::test]
    async fn test_rest_get() -> anyhow::Result<()> {
        let r = rest_get(
            "https://api.binance.com",
            "/api/v3/trades?symbol=BTCBUSD&limit=5",
            vec![],
            None,
            None,
        )
        .await?;

        println!("{}", r);

        Ok(())
    }

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

    /*
    use crate::exchange::binance::BinanceMarket;

    #[test]
    fn test_has_archive() {
        init_debug_log();

        let date = NOW() - DAYS(1);
        let config = crate::exchange::binance::config::BinanceConfig::BTCUSDT();

        let f = |date: MicroSec| -> String {
            BinanceMarket::make_historical_data_url_timestamp(&config, date)
        };

        let result = has_archive(date, &f);

        assert!(result.is_ok());

        let result = result.unwrap();

        assert_eq!(result, true);
    }
    */
}

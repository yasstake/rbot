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
    fn line_to_trade(rec: &str) -> Trade;
    fn convert_archive_line(line: &str) -> String;
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

    async fn has_archive(server: &T, config: &MarketConfig, date: MicroSec) -> bool {
        has_archive(date, &|d| Self::history_web_url(server, config, d)).await
    }

    /// read csv.gz file and convert to DataFrame
    fn logfile_to_df(logfile: &str) -> anyhow::Result<DataFrame> {
        let has_header = Self::archive_has_header();

        log::debug!("read into DataFrame {}", logfile);

        let df = CsvReadOptions::default()
            .with_has_header(has_header)
            .try_into_reader_with_file_path(Some(logfile.into()))
            .with_context(|| format!("polars csv read error {}", logfile))?
            .finish()
            .with_context(|| format!("polars error {}", logfile))?;

        log::debug!("read into DataFrame complete");
        log::debug!("{:?}", df);

        Ok(df)
    }

    /// create DataFrame with columns;
    ///  KEY:time_stamp(Int64), KEY:order_side(Bool), KEY:price(f64), KEY:size(f64)
    fn logdf_to_archivedf(df: &DataFrame) -> anyhow::Result<DataFrame> {
        // bybit はデータをタイムスタンプでソートしてはいけない。
        let df = df.clone();

        let timestamp = df.column("timestamp")?.f64()? * 1_000_000.0;
        let timestamp = Series::from(timestamp);

        let mut id = df.column("trdMatchID")?.clone();
        id.rename(KEY::id);

        let side = df.column("side")?;
        let price = df.column("price")?;
        let size = df.column("size")?;

        let df = DataFrame::new(vec![
            timestamp,
            side.clone(),
            price.clone(),
            size.clone(),
            id,
        ])?;

        Ok(df)
    }

    fn archivedf_to_file(mut df: DataFrame, archive_file: &str) -> anyhow::Result<()> {
        log::debug!("write to parquet file {}", archive_file);

        let file = BufWriter::new(
            File::create(archive_file)
                .with_context(|| format!("file create error {}", archive_file))?,
        );

        ParquetWriter::new(file).finish(&mut df)?;

        Ok(())
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
    F: FnMut(&StringRecord),
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
        let path = PathBuf::from_str(&path)?;
        log::debug!("log_download_temp: {:?}", path);

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

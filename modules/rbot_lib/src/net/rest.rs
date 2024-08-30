// Copyright(c) 2023-4. yasstake. All rights reserved.
// Abloultely no warranty.

use std::path::PathBuf;

use anyhow::anyhow;
use anyhow::Context;
use polars::lazy::frame::LazyFrame;
use reqwest::StatusCode;
use tempfile::tempdir;

// use crossbeam_channel::Receiver;
use crate::common::time_string;
use crate::common::AccountCoins;
use crate::common::ExchangeConfig;
use crate::common::Kline;
use crate::common::{
    BoardTransfer, MarketConfig, MicroSec, Order, OrderSide, OrderType, Trade, DAYS, TODAY,
};
use crate::db::csv_to_df;
use crate::db::df_to_parquet;
use crate::db::log_download_tmp;
use polars::frame::DataFrame;
use reqwest::Method;
use rust_decimal::Decimal;
//use crate::db::KEY::low;
use async_trait::async_trait;


#[derive(PartialEq, Debug)]
pub enum RestPage {
    New,
    Done,
    Time(MicroSec),
    Int(i64),
    Key(String)
}

pub trait RestApi {
    fn get_exchange(&self) -> ExchangeConfig;

    async fn get_board_snapshot(&self, config: &MarketConfig) -> anyhow::Result<BoardTransfer>;

    async fn get_recent_trades(&self, config: &MarketConfig) -> anyhow::Result<Vec<Trade>>;

    async fn get_trades(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage
    ) -> anyhow::Result<(Vec<Trade>, RestPage)>;

    async fn get_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage
    )-> anyhow::Result<(Vec<Kline>, RestPage)>;

    fn klines_width(&self) -> i64;

    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>>;
    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order>;
    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>>;

    async fn get_account(&self) -> anyhow::Result<AccountCoins>;

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String;
    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame>;

    async fn has_web_archive(&self, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool> {
        let url = self.history_web_url(config, date);
        let result = check_exist(url.as_str()).await;
    
        if result.is_err() {
            log::info!("archive not found: url = {}", url);
            return Ok(false);
        }
    
        Ok(result.unwrap())
    }

    async fn web_archive_to_parquet<F>(
        &self,
        config: &MarketConfig,
        parquet_file: &PathBuf,
        date: MicroSec,
        f: F,
    ) -> anyhow::Result<i64>
    where
        F: FnMut(i64, i64),
    {
        let url = self.history_web_url(config, date);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path(), f)
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        let file_path = PathBuf::from(file_path);

        let suffix = file_path.extension().unwrap_or_default();
        let suffix = suffix.to_ascii_lowercase();

        if suffix == "gz" || suffix == "csv" || suffix == "zip" {
            log::debug!("read log csv to df");
            let df = csv_to_df(&file_path)?;

            let mut archive_df = self.logdf_to_archivedf(&df)?;
            log::debug!("archive df shape={:?}", archive_df.shape());

            log::debug!("store paquet");
            let rec = df_to_parquet(&mut archive_df, &parquet_file)?;
            log::debug!("done {} [rec]", rec);

            return Ok(rec)
        }

        Err(anyhow!("Unknown file type {:?}", file_path))
    }



}


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

    if response.status().as_str() == "200" {
        let body = response
            .text()
            .await
            .with_context(|| format!("response text error"))?;

        return Ok(body);
    }

    // -----------other errors---------------
    let status = response.status();
    match status {
        StatusCode::NOT_FOUND => {
            log::error!("NOT FOUND url={}, {}", url, body);
            println!("NOT FOUND url={}, {}", url, body);
        },
        StatusCode::FORBIDDEN |
        StatusCode::UNAUTHORIZED => {
            log::error!("AUTH ERROR url={}, {}", url, body);
            println!("AUTH ERROR url={}, {}", url, body);
            println!("Please check access key and token");
        }
        _ => {
            let code = status.as_u16();

            if code == 10001 {
                print!("status code 10001. please check access key and token");
                log::error!("status code 10001. please check access key and token");
            } 

            log::error!("request error code={} / body={}", status, body)
        }
    }

    Err(anyhow!(
        "Response code = {} / download size {:?} / method({:?}) /  response body = {}",
        response.status().as_str(),
        response.content_length(),
        method,
        &body,
    ))
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

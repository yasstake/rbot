use crate::{
    common::{
        date_string, parse_date, time_string, MarketConfig, MicroSec, OrderSide, Trade, DAYS,
        FLOOR_DAY, MIN, NOW, TODAY,
    },
    db::{append_df, csv_to_df, df_to_parquet, parquet_to_df, KEY},
    net::{check_exist, RestApi},
};
use anyhow::{anyhow, Context};
use futures::StreamExt;
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use reqwest::Client;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio::io::AsyncWriteExt as _;
// Import the `anyhow` crate and the `Result` type.
use super::{db_path_root, select_df};
use polars::lazy::{
    dsl::{col, lit},
    frame::IntoLazy,
};
use polars::prelude::{DataFrame, NamedFrom};
use polars::series::Series;

use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    str::FromStr,
    vec,
};
use tempfile::tempdir;

const EXTENSION: &str = "parquet";

///
///Archive CSV format
///
///│ timestamp ┆ order_side  | price ┆ size │ id      |
///
///Archive DataFrame
///┌───────────┬───────┬──────┬────────────┐
///│ timestamp ┆ price ┆ size ┆ order_side │ id      |
///│ ---       ┆ ---   ┆ ---  ┆ ---        │         |
///│ i64       ┆ f64   ┆ f64  ┆ bool       │string   |
///└───────────┴───────┴──────┴────────────┘
///
///
/// log_df    ->   raw archvie file it may be different from exchanges.
/// archive_df -> archvie file that is stored in the local directory
/// chache_df -> df to use TradeTable's cache.
pub struct TradeArchive {
    config: MarketConfig,
    production: bool,
    last_archive_check_time: MicroSec,
    latest_archive_date: MicroSec,
    start_time: MicroSec,
    end_time: MicroSec,
}

impl Clone for TradeArchive {
    fn clone(&self) -> Self {
        let mut archive = Self {
            config: self.config.clone(),
            production: self.production.clone(),
            last_archive_check_time: self.last_archive_check_time.clone(),
            latest_archive_date: self.latest_archive_date.clone(),
            start_time: self.start_time.clone(),
            end_time: self.end_time.clone(),
        };

        let r = archive.analyze();
        if r.is_err() {
            log::warn!("trade archive analyze error");
        }

        archive
    }
}

impl TradeArchive {
    pub fn new(config: &MarketConfig, production: bool) -> Self {
        let mut my = Self {
            config: config.clone(),
            production: production,
            last_archive_check_time: 0,
            latest_archive_date: 0,
            start_time: 0,
            end_time: 0,
        };

        let r = my.analyze();
        if r.is_err() {
            log::debug!("Archive analyze error {:?}", r);
        }

        log::debug!(
            "S: {:?} ({:?})",
            my.start_time(),
            time_string(my.start_time().unwrap_or_default())
        );
        log::debug!(
            "E: {:?} ({:?})",
            my.end_time(),
            time_string(my.end_time().unwrap_or_default())
        );

        return my;
    }

    pub fn start_time(&self) -> anyhow::Result<MicroSec> {
        if self.start_time == 0 {
            return Err(anyhow!("no data in archive"));
        }

        Ok(self.start_time)
    }

    pub fn end_time(&self) -> anyhow::Result<MicroSec> {
        if self.end_time == 0 {
            return Err(anyhow!("no data in archive"));
        }

        Ok(self.end_time)
    }

    // check if the log data is already stored in local.
    pub fn has_local_archive(&self, date: MicroSec) -> bool {
        let archive_path = self.file_path(date);

        log::debug!("check file exists {:?}", archive_path);

        return archive_path.exists();
    }

    /// download historical data from the web and store csv in the Archive directory
    pub async fn download<T>(
        &mut self,
        api: &T,
        ndays: i64,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64>
    where
        T: RestApi,
    {
        let mut date = FLOOR_DAY(NOW());

        log::debug!("download log from {:?}({:?})", date_string(date), date);

        let mut count = 0;
        let mut i = 0;

        while i <= ndays {
            count += self
                .web_archive_to_parquet(api, date, force, verbose)
                .await?;
            date -= DAYS(1);

            i += 1;
        }

        self.analyze()?;

        Ok(count)
    }

    /// check the lates date in archive web site
    /// check the latest check time, within 60 min call this function, reuse cache value.
    pub async fn latest_archive_date<T>(&mut self, api: &T) -> anyhow::Result<MicroSec>
    where
        T: RestApi,
    {
        let now = NOW();
        if now - self.last_archive_check_time < MIN(60) {
            return Ok(self.latest_archive_date);
        }
        self.last_archive_check_time = now;

        let mut latest = TODAY();
        let mut i = 0;

        loop {
            log::debug!("check log exist = {}({})", time_string(latest), latest);

            if has_web_archive(api, &self.config, latest).await? {
                self.latest_archive_date = latest;
                return Ok(latest);
            }
            latest -= DAYS(1);
            i += 1;
            if 5 < i {
                return Err(anyhow!(
                    "Find archive retry over {}/{}/{}",
                    i,
                    latest,
                    time_string(latest)
                ));
            }
        }
    }

    /// generate 0 row empty cache(stored in memory) df
    pub fn make_empty_cachedf() -> DataFrame {
        let time = Series::new(KEY::time_stamp, Vec::<MicroSec>::new());
        let price = Series::new(KEY::price, Vec::<f64>::new());
        let size = Series::new(KEY::size, Vec::<f64>::new());
        let order_side = Series::new(KEY::order_side, Vec::<bool>::new());

        let df = DataFrame::new(vec![time, order_side, price, size]).unwrap();

        return df;
    }

    /// generate 0 row empty archive(stored in disk) df
    pub fn load_archive_df(&self, date: MicroSec) -> anyhow::Result<DataFrame> {
        let parquet_file = self.file_path(date);
        log::debug!(
            "read archive file into DataFrame {} ({})",
            date_string(date),
            date
        );
        let df = parquet_to_df(&parquet_file)?;

        Ok(df)
    }

    /// load from archived paquet file retrive specifed time frame.
    pub fn select_cachedf(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<DataFrame> {
        let dates = self.select_dates(start_time, end_time)?;

        let mut df = Self::make_empty_cachedf();

        for date in dates {
            log::debug!("{:?}", date_string(date));

            let new_df = self.load_cache_df(date)?;

            df = append_df(&df, &new_df)?;
        }

        df = select_df(&df, start_time, end_time);

        Ok(df)
    }

    /// load from parquet file and retrive as cachedf.
    pub fn load_cache_df(&self, date: MicroSec) -> anyhow::Result<DataFrame> {
        let date = FLOOR_DAY(date);

        if date < self.start_time()? {
            log::warn!(
                "Not found in archive[too early] query={:?} < start_time{:?}",
                date_string(date),
                date_string(self.start_time()?)
            );
            return Ok(Self::make_empty_cachedf());
        }

        if self.end_time()? <= date {
            log::warn!(
                "Not found in archive[too new] query={:?} >= end_time{:?}",
                date_string(date),
                date_string(self.end_time()?)
            );
            return Ok(Self::make_empty_cachedf());
        }

        let df = self.load_archive_df(date)?;

        log::debug!("{:?}", df);

        let df = df
            .lazy()
            .with_column(col(KEY::order_side).eq(lit("Buy")).alias(KEY::order_side))
            .select([
                col(KEY::time_stamp),
                col(KEY::order_side),
                col(KEY::price),
                col(KEY::size),
            ])
            .collect()?;

        log::debug!("read into DataFrame complete");

        Ok(df)
    }

    /// execute f for each rec in archive within specifed time frame.
    pub fn foreach<F>(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
        f: &mut F,
    ) -> anyhow::Result<i64>
    where
        F: FnMut(&Trade) -> anyhow::Result<()>,
    {
        let dates = self.select_dates(start_time, end_time)?;

        let mut count: i64 = 0;
        for d in dates {
            count += self.foreach_paquet(d, f)?;
        }

        Ok(count)
    }

    /// execite f for each rec(trade) specifed a date.
    pub fn foreach_paquet<F>(&self, date: MicroSec, f: &mut F) -> anyhow::Result<i64>
    where
        F: FnMut(&Trade) -> anyhow::Result<()>,
    {
        let mut count = 0;

        let file = self.file_path(date);
        let file = File::open(file)?;
        let reader = SerializedFileReader::new(file)?;

        // スキーマを取得
        let schema = reader.metadata().file_metadata().schema();
        println!("File Schema: {:?}", schema);

        for row in reader {
            let row = row?;

            let timestamp = row.get_long(0)?;
            let order_side = row.get_string(1)?;
            let price = row.get_double(2)?;
            let size = row.get_double(3)?;
            let id = row.get_string(4)?;

            let trade = Trade::new(
                timestamp,
                OrderSide::from(order_side.as_str()),
                Decimal::from_f64(price).unwrap(),
                Decimal::from_f64(size).unwrap(),
                crate::common::LogStatus::FixArchiveBlock,
                id,
            );

            f(&trade)?;
            // log::debug!("{:?}" , trade);

            count += 1;
        }

        Ok(count)
    }

    ///
    pub fn select_dates(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<Vec<MicroSec>> {
        let start_time = if start_time == 0 || start_time < self.start_time()? {
            self.start_time()?
        } else {
            FLOOR_DAY(start_time)
        };

        let end_time = if end_time == 0 || self.end_time()? <= end_time {
            FLOOR_DAY(self.end_time()? - 1)
        } else {
            FLOOR_DAY(end_time)
        };

        let all_dates = self.list_dates()?;

        let mut dates: Vec<MicroSec> = all_dates
            .into_iter()
            .filter(|date| start_time <= *date && *date <= end_time)
            .collect();

        dates.sort();

        return Ok(dates);
    }

    /// get the date of the file. 0 if the file does not exist
    /// File name
    pub fn file_date(&self, path: &PathBuf) -> anyhow::Result<MicroSec> {
        let file_stem = path.file_stem().unwrap().to_str().unwrap();

        let date: Vec<&str> = file_stem.split("-").collect();

        let date = date[date.len() - 1];

        let timestamp = parse_date(date)?;

        Ok(timestamp)
    }

    /// analyze archive directory, find start_time, end_time
    /// if there is fragmented date, delete it.
    /// Note: endday means next day's begining(archive date 24:00 = next day 00:00)
    pub fn analyze(&mut self) -> anyhow::Result<()> {
        let mut dates = self.list_dates()?;
        dates.reverse();
        log::debug!("analyze dates = {:?}", dates);

        let number_of_entry = dates.len();

        if number_of_entry == 0 {
            self.start_time = 0;
            self.end_time = 0;

            return Ok(());
        }

        let mut detect_gap = false;
        let mut last_time: MicroSec = 0;

        for i in 0..number_of_entry {
            let d = dates[i];
            log::debug!("{:?}({:?})", date_string(d), d);

            if i == 0 {
                log::debug!("setup first");
                self.end_time = d + DAYS(1);
                self.start_time = d;
            } else {
                if last_time - d == DAYS(1) {
                    log::debug!("next day OK{:?}", date_string(d));
                    self.start_time = d;
                } else {
                    log::debug!("have gap   {:?}", date_string(d));
                    detect_gap = true;
                }

                if detect_gap {
                    self.delete(d)?;
                }
            }

            last_time = d;
        }

        Ok(())
    }

    /// get list of archive date in order(older first)
    pub fn list_dates(&self) -> anyhow::Result<Vec<MicroSec>> {
        let mut dates: Vec<MicroSec> = vec![];

        let directory = self.archive_directory();

        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries {
                if let Ok(ent) = entry {
                    let ent = ent.path();
                    let path = ent.file_name().unwrap();
                    let path_str = path.to_str().unwrap();
                    if path_str.ends_with(EXTENSION) {
                        log::debug!("entry= {:?}", path_str);
                        if let Ok(date) = self.file_date(&PathBuf::from_str(&path_str)?) {
                            dates.push(date);
                        }
                    }
                }
            }
        }

        dates.sort();

        Ok(dates)
    }

    /// get archive directory for each exchagen and trading pair.
    fn archive_directory(&self) -> PathBuf {
        let db_path_root = db_path_root(
            &self.config.exchange_name,
            &self.config.trade_category,
            &self.config.trade_symbol,
            self.production,
        );

        let archive_dir = db_path_root.join("ARCHIVE");
        let _ = fs::create_dir_all(&archive_dir);

        return archive_dir;
    }

    /// get file path for the date
    pub fn file_path(&self, date: MicroSec) -> PathBuf {
        let archive_directory = self.archive_directory();

        let date = FLOOR_DAY(date);
        let date = date_string(date);

        let archive_name = format!("{}-{}.{}", self.config.trade_symbol, date, EXTENSION);

        let archive_path = archive_directory.join(archive_name);

        return archive_path;
    }

    /// delete archive file at date
    fn delete(&self, date: MicroSec) -> anyhow::Result<()> {
        let path = self.file_path(date);

        log::warn!(
            "delete old archive file date{:?}({:?}) = {:?}",
            date,
            time_string(date),
            path
        );

        std::fs::remove_file(&path).with_context(|| format!("remove file error {:?}", path))?;

        Ok(())
    }

    pub async fn web_archive_to_parquet<T>(
        &mut self,
        api: &T,
        date: MicroSec,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64>
    where
        T: RestApi,
    {
        let config = &self.config.clone();

        let date = FLOOR_DAY(date);

        let has_csv_file = self.has_local_archive(date);

        if has_csv_file && !force {
            if verbose {
                log::debug!("archive csv file exist {}", time_string(date));
                println!("archive csv file exist {}", time_string(date));
                return Ok(0);
            }
        }

        let latest = { self.latest_archive_date(api).await? };

        if latest < date {
            log::warn!(
                "no data {} (archive latest={})",
                date_string(date),
                date_string(latest)
            );
            return Ok(0);
        }

        if verbose && force {
            log::debug!("force download");
            println!("force download");
        }

        let url = api.history_web_url(config, date);

        log::debug!("Downloading ...[{}]", url);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path())
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        let file_path = PathBuf::from(file_path);

        // load to paquet
        log::debug!("read log csv to df");
        let has_header = api.archive_has_header();
        let df = csv_to_df(&file_path, has_header)?;
        log::debug!("load to df {:?}", df.shape());

        let mut archive_df = api.logdf_to_archivedf(&df)?;
        log::debug!("archive df shape={:?}", archive_df.shape());

        log::debug!("store paquet");
        let paquet_file = self.file_path(date);
        let rec = df_to_parquet(&mut archive_df, &paquet_file)?;
        log::debug!("done {} [rec]", rec);

        Ok(rec)
    }
}

pub async fn log_download_tmp(url: &str, tmp_dir: &Path) -> anyhow::Result<PathBuf> {
    let client = Client::new();

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
        return Err(anyhow!("Download error response={:?}", response));
    }

    let total_size = response.content_length().unwrap_or(0);

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let path = tmp_dir.join(fname);

    let mut file = tokio::fs::File::create(path.clone()).await?;
    let mut downloaded: u64 = 0;
    let mut stream = response.bytes_stream();

    log::debug!("start reading from web");

    while let Some(item) = stream.next().await {
        let chunk = item?;
        file.write_all(&chunk).await?;
        let new = std::cmp::min(downloaded + (chunk.len() as u64), total_size);
        downloaded = new;
    }

    file.flush().await?;

    Ok(path)
}

/// check if achive date is avairable at specified date
async fn has_web_archive<T>(api: &T, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool>
where
    T: RestApi,
{
    let url = api.history_web_url(config, date);
    let result = check_exist(url.as_str()).await;

    if result.is_err() {
        return Ok(false);
    }

    Ok(result.unwrap())
}

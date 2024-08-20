use crate::{
    common::{
        date_string, parse_date, time_string, MarketConfig, MicroSec, OrderSide, PyFileBar, Trade,
        DAYS, FLOOR_DAY, MIN, NOW, TODAY,
    },
    db::{append_df, csv_to_df, df_to_parquet, parquet_to_df, KEY},
    net::{check_exist, RestApi},
};
use anyhow::{anyhow, Context};
use arrow::temporal_conversions::MICROSECONDS;
use futures::StreamExt;
use parquet::{file::reader::SerializedFileReader, record::RowAccessor};
use reqwest::Client;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio::io::{AsyncWriteExt as _, BufWriter};
// Import the `anyhow` crate and the `Result` type.
use super::{db_path_root, select_df_lazy};
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

const ARCHIVE_CHECK_INTERVAL: MicroSec = 10 * 60 * MICROSECONDS;

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

    end_time_update_t: MicroSec,
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
            end_time_update_t: 0,
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
            end_time_update_t: 0,
        };

        let r = my.analyze();
        if r.is_err() {
            log::debug!("Archive analyze error {:?}", r);
        }

        log::debug!(
            "S: {:?} ({:?})",
            my.start_time(),
            time_string(my.start_time())
        );
        log::debug!("E: {:?} ({:?})", my.end_time(), time_string(my.end_time()));

        return my;
    }

    pub fn start_time(&self) -> MicroSec {
        self.start_time
    }

    pub fn end_time(&mut self) -> MicroSec {
        let _ = self.update_end_time();
        self.end_time
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
        let mut bar = PyFileBar::new();

        if verbose {
            bar.print(&format!(
                "downloading web archvie from [{}]days before. force=[{}]",
                ndays, force
            ));
        }

        let mut count = 0;
        let mut total_files = -1;

        for i in 0..ndays {
            if force
                || (!self.has_local_archive(date) && date < self.latest_archive_date(api).await?)
            {
                if total_files == -1 {
                    total_files = ndays - i;

                    if verbose {
                        bar.init(total_files, true, true);
                        bar.set_total_files(total_files);
                    }
                }

                let url = api.history_web_url(&self.config, date);
                bar.next_file(&url, 10_000);
                bar.print(&url);

                let mut file_size = 0;

                count += self
                    .web_archive_to_parquet(api, date, force, verbose, |count, content_len| {
                        if verbose {
                            if file_size == 0 {
                                bar.set_file_size(content_len);
                            }
                            file_size = content_len;

                            bar.set_file_progress(count);
                        }
                    })
                    .await?;
            } else {
                if verbose {
                    // text_bar.set_message(format!("skip download [{}]", date_time_string(date)));
                }
            }
            date -= DAYS(1);
        }

        self.analyze()?;

        if verbose {
            bar.print(&format!(
                "Archived data: from:[{}] to:[{}]",
                time_string(self.start_time()),
                time_string(self.end_time())
            ));
        }

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
        let time = Series::new(KEY::timestamp, Vec::<MicroSec>::new());
        let price = Series::new(KEY::price, Vec::<f64>::new());
        let size = Series::new(KEY::size, Vec::<f64>::new());
        let order_side = Series::new(KEY::order_side, Vec::<String>::new());
        let id = Series::new(KEY::id, Vec::<String>::new());

        let df = DataFrame::new(vec![time, order_side, price, size, id]).unwrap();

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
    pub fn fetch_cachedf(
        &mut self,
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

        df = select_df_lazy(&df, start_time, end_time).collect()?;

        Ok(df)
    }

    /// load from parquet file and retrive as cachedf.
    pub fn load_cache_df(&mut self, date: MicroSec) -> anyhow::Result<DataFrame> {
        let date = FLOOR_DAY(date);

        if date < self.start_time() {
            log::warn!(
                "Not found in archive[too early] query={:?} < start_time{:?}",
                date_string(date),
                date_string(self.start_time())
            );
            return Ok(Self::make_empty_cachedf());
        }

        if self.end_time() <= date {
            log::warn!(
                "Not found in archive[too new] query={:?} >= end_time{:?}",
                date_string(date),
                date_string(self.end_time())
            );
            return Ok(Self::make_empty_cachedf());
        }

        let df = self.load_archive_df(date)?;

        log::debug!("{:?}", df);


        log::debug!("read into DataFrame complete");

        Ok(df)
    }

    /// execute f for each rec in archive within specifed time frame.
    pub fn foreach<F>(
        &mut self,
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
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<Vec<MicroSec>> {
        let start_time = if start_time == 0 || start_time < self.start_time() {
            self.start_time()
        } else {
            FLOOR_DAY(start_time)
        };

        let end_time = if end_time == 0 || self.end_time() <= end_time {
            FLOOR_DAY(self.end_time() - 1)
        } else {
            FLOOR_DAY(end_time)
        };

        let all_dates = self.list_dates()?;

        if all_dates.len() == 0 {
            return Err(anyhow!(
                "no data in archive from {} -> to {}",
                date_string(start_time),
                date_string(end_time)
            ));
        }

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

    pub fn update_end_time(&mut self) -> anyhow::Result<()> {
        if self.end_time_update_t < NOW() + ARCHIVE_CHECK_INTERVAL {
            return Ok(());
        }

        let dates = self.list_dates()?;

        let len = dates.len();
        if len == 0 {
            return Err(anyhow!("no data in archive"));
        }

        self.end_time = dates[len - 1] + DAYS(1);

        self.end_time_update_t = NOW();
        Ok(())
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
                    log::warn!("Delete too old log {:?}", date_string(d));

                    // TODO: for some exchange, this must be moved(should be backed up the data)
                    self.delete(d)?;
                }
            }

            last_time = d;
        }

        Ok(())
    }

    /// get list of archive date in order
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

    pub async fn web_archive_to_parquet<T, F>(
        &mut self,
        api: &T,
        date: MicroSec,
        force: bool,
        verbose: bool,
        f: F,
    ) -> anyhow::Result<i64>
    where
        T: RestApi,
        F: FnMut(i64, i64),
    {
        let config = &self.config.clone();

        let has_csv_file = self.has_local_archive(date);

        if has_csv_file && !force {
            return Ok(0);
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
        }

        let url = api.history_web_url(config, date);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path(), f)
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        let file_path = PathBuf::from(file_path);

        let suffix = file_path.extension().unwrap_or_default();
        let suffix = suffix.to_ascii_lowercase();

        if suffix == "gz" || suffix == "csv" || suffix == "zip" {
            log::debug!("read log csv to df");
            let has_header = api.archive_has_header();
            let df = csv_to_df(&file_path, has_header)?;

            let mut archive_df = api.logdf_to_archivedf(&df)?;
            log::debug!("archive df shape={:?}", archive_df.shape());

            log::debug!("store paquet");
            let paquet_file = self.file_path(date);
            let rec = df_to_parquet(&mut archive_df, &paquet_file)?;
            log::debug!("done {} [rec]", rec);

            return Ok(rec)
        }

        Err(anyhow!("Unknown file type {:?}", file_path))
    }
}

const BUFFER_SIZE: usize = 8 * 1024 * 1024;

pub async fn log_download_tmp<F>(
    url: &str,
    tmp_dir: &Path,
    mut progress: F,
) -> anyhow::Result<PathBuf>
where
    F: FnMut(i64, i64),
{
    let client = Client::new();

    let response = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0")
        .header("Accept", "text/html")
        .send()
        .await
        .with_context(|| format!("URL get error {}", url))?;

    let content_length = response.content_length().unwrap_or_default();

    log::debug!(
        "Response code = {} / download size {}",
        response.status().as_str(),
        content_length // if error, return 0
    );

    if !response.status().is_success() {
        return Err(anyhow!("Download error response={:?}", response));
    }

    let fname = response
        .url()
        .path_segments()
        .and_then(|segments| segments.last())
        .and_then(|name| if name.is_empty() { None } else { Some(name) })
        .unwrap_or("tmp.bin");

    let path = tmp_dir.join(fname);
    let file = tokio::fs::File::create(path.clone()).await?;
    let mut file_buffer = BufWriter::with_capacity(BUFFER_SIZE, file);
    let mut stream = response.bytes_stream();

    log::debug!("start reading from web");

    let mut count: i64 = 0;
    let mut last_count = 0;
    let count_interval = (content_length / 100) as i64;

    while let Some(item) = stream.next().await {
        let chunk = item?;
        let len = chunk.len() as i64;
        count += len;
        last_count += len;

        if count_interval < last_count {
            progress(count, content_length as i64);
            last_count = 0;
        }

        file_buffer.write_all(&chunk).await?;
    }

    file_buffer.flush().await?;

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
        log::info!("archive not found: url = {}", url);
        return Ok(false);
    }

    Ok(result.unwrap())
}

#[cfg(test)]
mod archive_test {
    use std::{path::PathBuf, str::FromStr};

    use crate::{common::{init_debug_log, NOW}, db::TradeArchive};

    use super::log_download_tmp;

    #[tokio::test]
    async fn test_download() -> anyhow::Result<()> {
        let path_buf = PathBuf::from_str("/tmp")?;
        let path = path_buf.as_path();

        init_debug_log();

        log::debug!("start download");
        let now = NOW();
        let file = log_download_tmp(
            "https://public.bybit.com/trading/BTCUSDT/BTCUSDT2024-07-16.csv.gz",
            path,
            |count, _content_len| {
                println!("{}", count);
            },
        )
        .await?;

        log::debug!("done");
        log::debug!("file={:?} :  {}[msec]", file, (NOW() - now) / 1_000);

        Ok(())
    }
}

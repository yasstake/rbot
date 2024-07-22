use crate::{
    common::{
        date_string, parse_date, time_string, MarketConfig, MicroSec, OrderSide, ServerConfig, Trade, DAYS, FLOOR_DAY, MIN, NOW, TODAY
    },
    db::{csv_to_df, df_to_parquet, merge_df, parquet_to_df, KEY},
    net::{check_exist, log_download_tmp, read_csv_archive, RestApi},
};
use anyhow::{anyhow, Context};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::RowAccessor,
};
use rust_decimal::{prelude::FromPrimitive, Decimal};
// Import the `anyhow` crate and the `Result` type.
use super::db_path_root;
use polars::lazy::{
    dsl::{col, lit},
    frame::IntoLazy,
};
use polars::prelude::{DataFrame, NamedFrom};
use polars::series::Series;
use std::{
    fs::{self, File},
    io::BufWriter,
    path::PathBuf,
    str::FromStr,
    vec,
};
use tempfile::tempdir;

use parquet::record::Row;
use std::sync::Arc;

const PARQUET: bool = true;
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
#[derive(Debug)]
pub struct TradeArchive<T, U>
where
    T: RestApi<U>,
    U: ServerConfig + Clone,
{
    server: U,
    config: MarketConfig,
    last_archive_check_time: MicroSec,
    latest_archive_date: MicroSec,
    start_time: MicroSec,
    end_time: MicroSec,
    _dummy: Option<T>,
}

impl<T, U> TradeArchive<T, U>
where
    T: RestApi<U>,
    U: ServerConfig + Clone,
{
    pub fn new(server: &U, config: &MarketConfig) -> Self {
        let mut my = Self {
            server: server.clone(),
            config: config.clone(),
            last_archive_check_time: 0,
            latest_archive_date: 0,
            start_time: 0,
            end_time: 0,
            _dummy: None,
        };

        let r = my.analyze();
        if r.is_err() {
            log::debug!("Archive analyze error {:?}", r);
        }

        log::debug!("S: {} ({})", my.start_time(), time_string(my.start_time()));
        log::debug!("E: {} ({})", my.end_time(), time_string(my.end_time()));

        return my;
    }

    pub fn start_time(&self) -> MicroSec {
        self.start_time
    }

    pub fn end_time(&self) -> MicroSec {
        self.end_time
    }

    /// check the lates date in archive web site
    /// check the latest check time, within 60 min call this function, reuse cache value.
    pub async fn latest_archive_date(&mut self) -> anyhow::Result<MicroSec> {
        let now = NOW();
        if now - self.last_archive_check_time < MIN(60) {
            return Ok(self.latest_archive_date);
        }
        self.last_archive_check_time = now;

        let mut latest = TODAY();
        let mut i = 0;

        loop {
            log::debug!("check log exist = {}({})", time_string(latest), latest);

            if self.has_web_archive(latest).await? {
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

    /// check if achive date is avairable at specified date
    async fn has_web_archive(&self, date: MicroSec) -> anyhow::Result<bool> {
        let url = T::history_web_url(&self.server, &self.config, date);
        let result = check_exist(url.as_str()).await;

        if result.is_err() {
            return Ok(false);
        }

        Ok(result.unwrap())
    }

    // check if the log data is already stored in local.
    pub fn has_local_archive(&self, date: MicroSec) -> bool {
        let archive_path = self.file_path(date);

        log::debug!("check file exists {:?}", archive_path);

        return archive_path.exists();
    }

    /// download historical data from the web and store csv in the Archive directory
    pub async fn download(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        let mut date = FLOOR_DAY(NOW());

        log::debug!("download log from {:?}({:?})", date_string(date), date);

        let mut count = 0;
        let mut i = 0;

        while i <= ndays {
            count += self.web_archive_to_parquet(date, force, verbose).await?;
            date -= DAYS(1);

            i += 1;
        }

        self.analyze()?;

        Ok(count)
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

    /// load from parquet file and retrive as cachedf.
    pub fn load_cache_df(&self, date: MicroSec) -> anyhow::Result<DataFrame> {
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

    /// load from archived paquet file retrive specifed time frame.
    pub fn select_cache_df(
        &self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<DataFrame> {
        let dates = self.select_dates(start_time, end_time)?;

        let mut df = Self::make_empty_cachedf();

        for date in dates {
            log::debug!("{:?}", date_string(date));

            let new_df = self.load_cache_df(date)?;

            df = merge_df(&df, &new_df)?;
        }

        Ok(df)
    }

    /// execute f for each rec in archive within specifed time frame.
    pub fn foreach<F>(&self, start_time: MicroSec, end_time: MicroSec, f: &mut F) -> anyhow::Result<i64>
    where
    F: FnMut(&Trade) -> anyhow::Result<()>
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
    F: FnMut(&Trade) -> anyhow::Result<()>
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

            let trade = Trade::new(timestamp, 
                OrderSide::from(order_side.as_str()), 
                Decimal::from_f64(price).unwrap(), 
                Decimal::from_f64(size).unwrap(), crate::common::LogStatus::FixArchiveBlock, id);

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
            self.server.is_production(),
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

    pub async fn web_archive_to_parquet(
        &mut self,
        date: MicroSec,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        let server = &self.server.clone();
        let config = &self.config.clone();

        let date = FLOOR_DAY(date);

        let has_csv_file = self.has_local_archive(date);

        if has_csv_file && !force {
            if verbose {
                println!("archive csv file exist {}", time_string(date));
                return Ok(0);
            }
        }

        let latest = { self.latest_archive_date().await? };

        if latest < date {
            log::warn!(
                "no data {} (archive latest={})",
                date_string(date),
                date_string(latest)
            );
            return Ok(0);
        }

        if verbose && force {
            println!("force download")
        }

        let url = T::history_web_url(server, config, date);

        log::debug!("Downloading ...[{}]", url);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path())
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        let file_path = PathBuf::from(file_path);

        // load to paquet
        log::debug!("read log csv to df");
        let has_header = T::archive_has_header();
        let df = csv_to_df(&file_path, has_header)?;
        log::debug!("load to df {:?}", df.shape());

        let mut archive_df = T::logdf_to_archivedf(&df)?;
        log::debug!("archive df shape={:?}", archive_df.shape());

        log::debug!("store paquet");
        let paquet_file = self.file_path(date);
        let rec = df_to_parquet(&mut archive_df, &paquet_file)?;
        log::debug!("done {} [rec]", rec);

        Ok(rec)
    }
}


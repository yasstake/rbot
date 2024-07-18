use crate::{
    common::{
        date_string, time_string, MarketConfig, MicroSec, OrderSide, ServerConfig, DAYS, FLOOR_DAY,
        MIN, NOW, TODAY,
    },
    net::{check_exist, log_download_tmp, read_csv_archive, RestApi},
};
use anyhow::{anyhow, Context}; // Import the `anyhow` crate and the `Result` type.
use polars::prelude::DataFrame;
use std::{
    fs::{self, File},
    path::PathBuf,
    vec,
};
use tempfile::tempdir;

use super::db_path_root;

///
///Archive CSV format
///┌───────────┬───────┬──────┬────────────┐
///│ timestamp ┆ price ┆ size ┆ order_side │ id      |
///│ ---       ┆ ---   ┆ ---  ┆ ---        │         |
///│ i64       ┆ f64   ┆ f64  ┆ bool       │string   |
///└───────────┴───────┴──────┴────────────┘
struct TradeTableArchive<T, U>
where
    T: RestApi<U>,
    U: ServerConfig,
{
    server: U,
    config: MarketConfig,
    last_archive_check_time: MicroSec,
    latest_archive_date: MicroSec,
    start_time: MicroSec,
    end_time: MicroSec,
    dummy: Option<T>,
}

impl<T, U> TradeTableArchive<T, U>
where
    T: RestApi<U>,
    U: ServerConfig,
{
    pub fn new(server: U, config: &MarketConfig) -> Self {
        let mut my = Self {
            server: server,
            config: config.clone(),
            last_archive_check_time: 0,
            latest_archive_date: 0,
            start_time: 0,
            end_time: 0,
            dummy: None,
        };

        return my;
    }

    fn start_time(&self) -> MicroSec {
        self.start_time
    }

    fn end_time(&self) -> MicroSec {
        self.end_time
    }

    async fn latest_archive_date(&mut self) -> anyhow::Result<MicroSec> {
        let now = NOW();
        if now - self.last_archive_check_time < MIN(60) {
            return Ok(self.latest_archive_date);
        }
        self.last_archive_check_time = now;

        let mut latest = TODAY();
        let mut i = 0;

        loop {
            log::debug!("check log exist = {}({})", time_string(latest), latest);

            if self.has_archive_web(latest).await? {
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

    async fn has_archive_web(&self, date: MicroSec) -> anyhow::Result<bool> {
        let url = T::history_web_url(&self.server, &self.config, date);
        let result = check_exist(url.as_str()).await;

        if result.is_err() {
            return Ok(false);
        }

        Ok(result.unwrap())
    }

    pub fn has_archive_file(&self, date: MicroSec) -> bool {
        let archive_path = self.file_path(date);

        return archive_path.exists();
    }

    /// download historical data from the web and store csv in the Archive directory
    pub fn download(ndays: i64) -> i64 {
        0
    }

    pub fn load_df(&self, date: MicroSec) -> anyhow::Result<DataFrame> {
        Err(anyhow!("Not implemented"))
    }

    pub fn select_df(&self, start: MicroSec, end: MicroSec) -> anyhow::Result<DataFrame> {
        Err(anyhow!("Not implemented"))
    }

    /// get the date of the file. 0 if the file does not exist
    /// File name
    pub fn file_date(&self, path: &PathBuf) -> MicroSec {
        0
    }

    /// select files that are within the start and end time
    fn select_files(&self, start: MicroSec, end: MicroSec) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = vec![];

        let directory = self.archive_directory();

        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("gz")
                        && path
                            .file_name()
                            .and_then(|s| s.to_str())
                            .map_or(false, |s| s.ends_with(".csv.gz"))
                    {
                        files.push(path);
                    }
                }
            }
        }

        files.sort();
        files
    }

    fn select<F>(&self, start: MicroSec, end: MicroSec, f: F) -> anyhow::Result<()>
    where
        F: Fn(&DataFrame) -> Vec<(MicroSec, f64, f64, bool)>,
    {
        Err(anyhow!("Not implemented"))
    }

    pub fn archive_directory(&self) -> PathBuf {
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

    pub fn file_path(&self, date: MicroSec) -> PathBuf {
        let archive_directory = self.archive_directory();

        let date = FLOOR_DAY(date);
        let date = date_string(date);

        let archive_name = format!(
            "{}-{}-{}.csv.gz",
            date, self.config.trade_category, self.config.trade_symbol
        );

        let archive_path = archive_directory.join(archive_name);

        return archive_path;
    }

    async fn archive_to_csv(
        &self,
        date: MicroSec,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        let server = &self.server;
        let config = &self.config;

        let has_csv_file = self.has_archive_file(date);

        if has_csv_file && !force {
            if verbose {
                println!("archive csv file exist {}", time_string(date));
            }
            return Ok(0);
        }

        if !T::has_archive(server, config, date).await {
            if verbose {
                println!("archive NOT exist {}", time_string(date));
            }

            return Ok(0);
        }

        let date = FLOOR_DAY(date);
        let url = T::history_web_url(server, config, date);
        let has_header = T::archive_has_header();

        log::debug!("Downloading ...[{}]", url);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path())
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        log::debug!("read into DataFrame");
        let df = T::logfile_to_df(&file_path)?;
        log::debug!("file_path = {}", file_path);

        log::debug!("convert to csv.gz file");

        let gzip_csv_file = self.file_path(date);

        let gzip_csv_tmp = gzip_csv_file.with_extension("tmp");

        let gzip_file = File::create(&gzip_csv_tmp).with_context(|| {
            format!("gzip file create error {}", gzip_csv_file.to_str().unwrap())
        })?;
        let encoder = flate2::write::GzEncoder::new(gzip_file, flate2::Compression::default());

        let mut csv_writer = csv::Writer::from_writer(encoder);
        csv_writer
            .write_record(&["timestamp", "side", "price", "size", "id"])
            .unwrap();
        let mut download_rec: i64 = 0;

        read_csv_archive(&file_path, has_header, |rec| {
            let trade = T::rec_to_trade(&rec);

            let time = trade.time;
            let side = if trade.order_side == OrderSide::Buy {
                1
            } else {
                0
            };
            let price = trade.price;
            let size = trade.size;
            let id = trade.id;

            csv_writer
                .write_record(&[
                    time.to_string(),
                    side.to_string(),
                    price.to_string(),
                    size.to_string(),
                    id.to_string(),
                ])
                .unwrap();
            download_rec += 1;
        });

        csv_writer.flush().unwrap();

        std::fs::remove_file(&file_path)
            .with_context(|| format!("remove file error {}", file_path))?;
        let r = std::fs::rename(gzip_csv_tmp, gzip_csv_file);
        if r.is_err() {
            if verbose {
                println!("rename error {:?}", r);
            }
            log::error!("rename error {:?}", r);
        }

        log::debug!("download rec = {}", download_rec);

        Ok(download_rec)
    }
}


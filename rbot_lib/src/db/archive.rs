use crate::{
    common::{
        date_string, parse_date, time_string, MarketConfig, MicroSec, OrderSide, ServerConfig,
        Trade, DAYS, FLOOR_DAY, MIN, NOW, TODAY,
    },
    net::{check_exist, log_download_tmp, read_csv_archive, read_line_archive, RestApi},
};
use anyhow::{anyhow, Context};
// Import the `anyhow` crate and the `Result` type.
use polars::prelude::DataFrame;
use std::{
    fs::{self, File}, io::{BufWriter, Write}, path::PathBuf, str::FromStr, vec
};
use tempfile::tempdir;

use super::{db_path_root, log_convert};

///
///Archive CSV format
///┌───────────┬───────┬──────┬────────────┐
///│ timestamp ┆ price ┆ size ┆ order_side │ id      |
///│ ---       ┆ ---   ┆ ---  ┆ ---        │         |
///│ i64       ┆ f64   ┆ f64  ┆ bool       │string   |
///└───────────┴───────┴──────┴────────────┘
#[derive(Debug)]
pub struct TradeTableArchive<T, U>
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

impl<T, U> TradeTableArchive<T, U>
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

        return my;
    }

    pub fn start_time(&self) -> MicroSec {
        self.start_time
    }

    pub fn end_time(&self) -> MicroSec {
        self.end_time
    }

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
    pub fn file_date(&self, path: &PathBuf) -> anyhow::Result<MicroSec> {
        let mut file_stem = path.file_stem().unwrap().to_str().unwrap();

        if file_stem.ends_with(".csv") {
            file_stem = file_stem.trim_end_matches(".csv");
        }

        if file_stem.ends_with(".csv.gz") {
            file_stem = file_stem.trim_end_matches(".csv.gz");
        }

        let date: Vec<&str> = file_stem.split("-").collect();
   
        let date = date[date.len()-1];

        let timestamp = parse_date(date)?;

        Ok(timestamp)
    }

    /// analyze archive directory, find start_time, end_time
    /// if there is fragmented date, delete it.
    pub fn analyze(&mut self) -> anyhow::Result<()> {
        let dates = self.list_dates()?;

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

            if i == 0 {
                self.end_time = d;
                self.start_time = d;
            }
            else {
                if detect_gap {
                    self.delete(d)?;
                }
                if last_time - d == DAYS(1)  {
                    self.start_time = d;
                }
                else {
                    detect_gap = true;
                    self.delete(d)?;
                }

            } 

            last_time = d;        
        }

        Ok(())
    }
    

    /// get list of archive date in reverse order(newer first)
    pub fn list_dates(&self) -> anyhow::Result<Vec<MicroSec>> {
        let mut dates: Vec<MicroSec> = vec![];

        let directory = self.archive_directory();

        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries {
                if let Ok(ent) = entry {
                    let ent = ent.path();
                    let path = ent.file_name().unwrap();
                    let path_str = path.to_str().unwrap();
                    if path_str.ends_with(".csv.gz") {
                        log::debug!("entry= {:?}", path_str);
                        if let Ok(date) = self.file_date(&PathBuf::from_str(&path_str)?) {
                            dates.push(date);
                        }
                    }
                }
            }
        }

        dates.reverse();
        
        Ok(dates)
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

        let archive_name = format!("{}-{}.csv.gz", self.config.trade_symbol, date);

        let archive_path = archive_directory.join(archive_name);

        return archive_path;
    }

    pub fn delete(&self, date: MicroSec) -> anyhow::Result<()> {
        let path = self.file_path(date);

        log::debug!("delete archive file date{:?}({:?}) = {:?}", date, time_string(date), path);

        std::fs::remove_file(&path).with_context(|| format!("remove file error {:?}", path))?;

        Ok(())
    }

    pub async fn archive_to_csv(
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

        log::debug!("Downloading ...[{}]", url);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path())
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;

        let file_path = PathBuf::from(file_path);

        log::debug!("convert to csv.gz file");
        let gzip_csv_file = self.file_path(date);
        let gzip_csv_tmp = gzip_csv_file.with_extension("tmp");

        let download_rec = self.write_csv(&file_path, &gzip_csv_tmp)?;

        std::fs::remove_file(&file_path)
            .with_context(|| format!("remove file error {:?}", file_path))?;
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

    /*/

    fn dump_csv(&self, src_file: &PathBuf, target_file: &PathBuf) -> anyhow::Result<i64> {
        let now = NOW();
        log::debug!("dump start");

        log::debug!("read into DataFrame");
        let mut df = T::logfile_to_df(&src_file.to_str().unwrap())?;
        log::debug!("file_path = {:?}", src_file);

        let mut archivedf = T::logdf_to_archivedf(&df)?;
        log::debug!("converted\n {:?}", df);

        let gzip_file = File::create(&target_file).with_context(|| {
            format!("gzip file create error {}", target_file.to_str().unwrap())
        })?;
        let encoder = flate2::write::GzEncoder::new(gzip_file, flate2::Compression::default());

        let writer = CsvWriter::new(encoder);
        let mut writer = writer.include_header(true);

        let r = writer.finish(&mut archivedf)?;
        log::debug!("write file done{:?}", r);

        log::debug!("Dump end {}[usec]", NOW() - now);

        Ok(archivedf.shape().0 as i64)
    }
    */

    fn write_csv(&self, src_file: &PathBuf, target_file: &PathBuf) -> anyhow::Result<i64> {
        let now = NOW();
        log::debug!("write start");

        let gzip_file = File::create(&target_file)
            .with_context(|| format!("gzip file create error {}", target_file.to_str().unwrap()))?;
        let encoder = flate2::write::GzEncoder::new(gzip_file, flate2::Compression::default());
        let encoder = BufWriter::new(encoder);

        let mut csv_writer = csv::Writer::from_writer(encoder);
        csv_writer
            .write_record(&["timestamp", "side", "price", "size", "id"])
            .unwrap();
        let mut download_rec: i64 = 0;

        read_csv_archive(src_file.to_str().unwrap(), true, |rec| {
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
        })?;

        csv_writer.flush().unwrap();

        log::debug!("write done {}[usec]", NOW() - now);

        Ok(download_rec)
    }

    fn write_csv2(&self, src_file: &PathBuf, target_file: &PathBuf) -> anyhow::Result<i64> {
        let now = NOW();
        log::debug!("write2 start");

        let gzip_file = File::create(&target_file)
            .with_context(|| format!("gzip file create error {}", target_file.to_str().unwrap()))?;
        let encoder = flate2::write::GzEncoder::new(gzip_file, flate2::Compression::default());
        let mut writer = BufWriter::new(encoder);
        let r = writer.write_all(Trade::csv_header().as_bytes());

        let mut download_rec: i64 = 0;

        read_line_archive(src_file.to_str().unwrap(), true, |rec| {
            let line = T::convert_archive_line(rec);

            let r = writer.write_all(line.as_bytes());
            download_rec += 1;
        })?;

        writer.flush().unwrap();

        log::debug!("write2 done {}[usec]", NOW() - now);

        Ok(download_rec)
    }

    async fn write_csv3(
        &self,
        source_file: &PathBuf,
        target_file: &PathBuf,
    ) -> anyhow::Result<i64> {
        let now = NOW();
        log::debug!("write3 start");

        log_convert(source_file.clone(), target_file.clone(), true, |line| {
            T::convert_archive_line(&line)
        })
        .await?;

        log::debug!("write3 end {}[usec]", NOW() - now);

        Ok(0)
    }
}

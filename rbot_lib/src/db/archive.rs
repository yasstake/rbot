use std::{f32::consts::E, path::PathBuf, vec};
use crate::common::{MarketConfig, MicroSec};
use super::archive_directory;
use anyhow::{anyhow, Result}; // Import the `anyhow` crate and the `Result` type.
use polars::prelude::DataFrame;


///
///Archive CSV format
///┌───────────┬───────┬──────┬────────────┐
///│ timestamp ┆ price ┆ size ┆ order_side │
///│ ---       ┆ ---   ┆ ---  ┆ ---        │
///│ i64       ┆ f64   ┆ f64  ┆ bool       │
///╞═══════════╪═══════╪══════╪════════════╡
///└───────────┴───────┴──────┴────────────┘)
struct TradeTableArchive {
    exchange_name: String,
    config: MarketConfig,
    path: PathBuf,
}

impl TradeTableArchive {
    pub fn new(exchange_name: &str, config: &MarketConfig) -> Self {
        let path = archive_directory(&config.exchange_name, &config.trade_category, &config.trade_symbol, false); // archive has only production

        Self {
            exchange_name: exchange_name.to_string(),
            config: config.clone(),
            path
        }
    }

    /// download historical data from the web and store csv in the Archive directory
    pub fn download(ndays: i64) -> i64{
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

        if let Ok(entries) = std::fs::read_dir(&self.path) {
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.extension().and_then(|s| s.to_str()) == Some("gz") && path.file_name().and_then(|s| s.to_str()).map_or(false, |s| s.ends_with(".csv.gz")) {
                        
                        files.push(path);
                    }
                }
            }
        }

        files.sort();
        files
    }

    fn select<F>(
        &self,
        start: MicroSec,
        end: MicroSec,
        f: F
    ) -> anyhow::Result<()>
    where
        F: Fn(&DataFrame) -> Vec<(MicroSec, f64, f64, bool)>
    {

        Err(anyhow!("Not implemented"))
    }
}


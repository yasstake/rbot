use std::path::PathBuf;
use crate::common::MarketConfig;
use super::archive_directory;

/*
Archive CSV format
┌───────────┬───────┬──────┬────────────┐
│ timestamp ┆ price ┆ size ┆ order_side │
│ ---       ┆ ---   ┆ ---  ┆ ---        │
│ i64       ┆ f64   ┆ f64  ┆ bool       │
╞═══════════╪═══════╪══════╪════════════╡
└───────────┴───────┴──────┴────────────┘)
*/

struct Archive {
    config: MarketConfig,
    path: PathBuf,
}

impl Archive {
    pub fn new(config: &MarketConfig) -> Self {
        let path = archive_directory(&config.exchange_name, &config.trade_category, &config.trade_symbol, false); // archive has only production

        Archive {
            config: config.clone(),
            path
        }
    }
}


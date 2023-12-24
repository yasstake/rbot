
#![allow(non_snake_case)]
use pyo3::prelude::*;
use rust_decimal_macros::dec;

use crate::{common::{MarketConfig, PriceType, FeeType}, fs::db_full_path};


#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitConfig {
    pub exchange_name: String,
    pub testnet: bool,
    pub trade_category: String,
    pub trade_symbol: String,

    pub rest_endpoint: String,
    pub history_web_base: String,

    pub db_base_dir: String,
    pub market_config: MarketConfig,

    pub public_stream_endpoint: String,
    pub private_stream_endpoint: String,
}

#[pymethods]
impl BybitConfig {

    pub fn get_db_path(&self) -> String {
        let mut exchange_name = self.exchange_name.clone();

        if self.testnet {
            exchange_name = format!("{}-TESTNET", exchange_name);
        }

        let db_path = db_full_path(&exchange_name, &self.trade_category, &self.trade_symbol, &self.db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }


    #[classattr]
    pub fn SPOT_BTCUSDT() -> Self {
        return BybitConfig {
            exchange_name: "BYBIT".to_string(),
            testnet: false,
            trade_category: "spot".to_string(),
            trade_symbol: "BTCUSDT".to_string(),
            rest_endpoint: "https://api.bybit.com".to_string(),
            history_web_base: "https://public.bybit.com".to_string(),
            db_base_dir: "".to_string(),
            public_stream_endpoint: "wss://stream.bybit.com/v5/public/spot".to_string(),
            private_stream_endpoint: "wss://stream.bybit.com/v5/private".to_string(),
            market_config: MarketConfig {
                price_unit: dec![0.05],
                price_scale: 3,
                size_unit: dec![0.001],
                size_scale: 4,
                maker_fee: dec![0.00_01],
                taker_fee: dec![0.00_01],
                price_type: PriceType::Home,
                fee_type: FeeType::Home,
                home_currency: "USDT".to_string(),
                foreign_currency: "BTC".to_string(),
                market_order_price_slip: dec![0.01],
                board_depth: 250,
            }
        };
    }
}


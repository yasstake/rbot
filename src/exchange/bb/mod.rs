// Copyright(c) 2022-2023. yasstake. All rights reserved.
// ABSOLUTELY NO WARRANTY

pub mod message;
pub mod rest;
pub mod ws;

use chrono::Datelike;
use csv::StringRecord;
use numpy::PyArray2;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;
use rust_decimal::Decimal;
use rust_decimal::prelude::FromPrimitive;

use crate::common::{OrderSide, Trade, DAYS};
use crate::common::NOW;
use crate::common::{to_naive_datetime, MicroSec};
use crate::db::sqlite::{TradeTable, TradeTableQuery};
use crate::fs::db_full_path;


use super::{download_log, make_download_url_list};

// TODO: implement ByBit Config.
#[pyclass]
struct BybitConfig {
    pub exchange_name: String,
    pub market_name: String,
}


#[derive(Debug)]
#[pyclass(name = "_ByBitMarket")]
pub struct BBMarket {
    name: String,
    pub db: TradeTable,
}


#[pymethods]
impl BBMarket {
    #[new]
    pub fn new(market_name: &str, dummy: bool) -> Self {
        // TODO: SPOTにのみ対応しているのを変更する。
        let db_name = Self::db_path(&market_name).unwrap();

        println!("create TradeTable: {}", db_name);

        let db = TradeTable::open(db_name.as_str()).expect("cannot open db");

        db.create_table_if_not_exists();

        return BBMarket {
            name: market_name.to_string(),
            db,
        };
    }

    #[staticmethod]
    pub fn db_path(market_name: &str) -> PyResult<String> {
        let db_name = db_full_path("BB", "trade", market_name);

        return Ok(db_name.as_os_str().to_str().unwrap().to_string());
    }



    #[getter]
    pub fn get_cache_duration(&self) -> MicroSec {
        return self.db.get_cache_duration();
    }

    pub fn reset_cache_duration(&mut self) {
        self.db.reset_cache_duration();
    }

    pub fn cache_all_data(&mut self) {
        self.db.update_cache_all();
    }

    pub fn download(&mut self, ndays: i64, force: bool) -> i64 {
        let latest_time = NOW() - DAYS(1);
        let days_gap = self.db.make_time_days_chunk_from_days(ndays, latest_time, force);
        let urls: Vec<String> = make_download_url_list(self.name.as_str(), days_gap, Self::make_historical_data_url_timestamp);
        let tx = self.db.start_thread();
        let download_rec = download_log(urls, tx, true, BBMarket::rec_to_trade);

        return download_rec;
    }

    pub fn select_trades_a(
        &mut self,
        start_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_select_trades(start_time, to_time);
    }

    pub fn ohlcvv_a(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcvv(start_time, end_time, window_sec);
    }

    pub fn ohlcv_a(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<Py<PyArray2<f64>>> {
        return self.db.py_ohlcv(start_time, end_time, window_sec);
    }


    pub fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_select_trades_polars(start_time, end_time);
    }

    pub fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcvv_polars(start_time, end_time, window_sec);
    }

    pub fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        return self.db.py_ohlcv_polars(start_time, end_time, window_sec);
    }


    pub fn info(&mut self) -> String {
        return self.db.info();
    }

    #[getter]
    fn get_file_name(&self) -> String {
        return self.db.get_file_name();
    }

    fn vaccum(&self) {
        self.db.vaccum();
    }

    pub fn _repr_html_(&self) -> String {
        return format!("<b>Binance DB ({})</b>{}", self.name, self.db._repr_html_());
    }
}

const HISTORY_WEB_BASE: &str = "https://public.bybit.com";

impl BBMarket {
    fn make_historical_data_url_timestamp(name: &str, t: MicroSec) -> String {
        let timestamp = to_naive_datetime(t);

        let yyyy = timestamp.year() as i64;
        let mm = timestamp.month() as i64;
        let dd = timestamp.day() as i64;

        return format!(
            "{}/trading/{}/{}{:04}-{:02}-{:02}.csv.gz",
            HISTORY_WEB_BASE, name, name, yyyy, mm, dd
        );
    }

    /*
        0         1      2    3    4     5             6          7          8            9                                    
        timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
        1692748800.279,BTCUSD,Sell,641,26027.00,ZeroMinusTick,80253109-efbb-58ca-9adc-d458b66201e9,2.462827064202559e+06,641,0.02462827064202559        
     */
    fn rec_to_trade(rec: &StringRecord) -> Trade {
        let timestamp = rec
            .get(0)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default()
            * 1_000_000.0;
        
        let timestamp = timestamp as MicroSec;

        let order_side = match rec.get(2).unwrap_or_default() {
            "Buy" => OrderSide::Buy,
            "Sell" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

        let id = rec.get(6).unwrap_or_default().to_string();

        let price = rec
            .get(4)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let price: Decimal = Decimal::from_f64(price).unwrap();

        let size = rec
            .get(3)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let size = Decimal::from_f64(size).unwrap();

        let trade = Trade::new(timestamp, order_side, price, size, id);

        return trade;
    }
}



/*
*
ByBitAPI：
    https://bybit-exchange.github.io/docs/
   

Bybitのメッセージフォーマットについて

約定履歴は、過去ログ、RESTAPI、WSの３つの方法で取得できるが
それぞれ、取得可能時間、メッセージフォーマットが異なる。

・過去ログ (昨日以前のものが取得可能)
＜サンプル＞
https://public.bybit.com

https://public.bybit.com/trading/BTCUSDT/BTCUSDT2023-08-21.csv.gz

timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
1651449601,BTCUSD,Sell,258,38458.00,MinusTick,a0dd4504-db3c-535f-b43b-4de38f581b79,670861.7192781736,258,0.006708617192781736


・RESTAPI　（直近1000レコード分＝おおよそ３分程度のログが取得できる）
＜サンプル＞
・リクエスト
https://api.bybit.com/v5/market/recent-trade?category=linear&symbol=BTCUSDT&limit=1000&start=1692828100000

・レスポンス
{"ret_code":0,"ret_msg":"OK","ext_code":"","ext_info":"","result":[{"id":66544931,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:53.165Z"},{"id":66544930,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:53.058Z"},{"id":66544929,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.954Z"},{"id":66544928,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.85Z"},{"id":66544927,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.747Z"},{"id":66544926,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.646Z"},{"id":66544925,"symbol":"BTCUSD","price":29558,"qty":100,"side":"Sell","time":"2022-06-03T13:45:52.536Z"},


KLine形式で１分足ならばもっと長期間のログが取得可能。


・WS（リアルタイム：過去は取得不可。タイムスタンプがMS単位）

{"topic":"ParseTradeMessage.BTCUSD",
 "data":[
       {"trade_time_ms":1619398389868,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":2000,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"8241a632-9f07-5fa0-a63d-06cefd570d75","cross_seq":6169452432},
       {"trade_time_ms":1619398389947,"timestamp":"2021-04-26T00:53:09.000Z","symbol":"BTCUSD","side":"Sell","size":200,"price":50703.5,"tick_direction":"ZeroMinusTick","trade_id":"ff87be41-8014-5a33-b4b1-3252a6422a41","cross_seq":6169452432}]}
]
}

*/

#[cfg(test)]
mod bbmarket_test{
    use csv::StringRecord;
    use rust_decimal_macros::dec;

    use crate::common::{NOW, time_string};

    #[test]
    fn test_make_historical_data_url_timestamp() {
        println!("{}", super::BBMarket::make_historical_data_url_timestamp("BTCUSD", 0));

        println!("{}", NOW());

        let url = super::BBMarket::make_historical_data_url_timestamp("BTCUSD", 1);
        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSD/BTCUSD1970-01-01.csv.gz"
        );

        let url = super::BBMarket::make_historical_data_url_timestamp("BTCUSD", 1692841687658323);
        assert_eq!(
            url,
            "https://public.bybit.com/trading/BTCUSD/BTCUSD2023-08-24.csv.gz"
        );
    }


    /*
    timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional
    1692748800.279,BTCUSD,Sell,641,26027.00,ZeroMinusTick,80253109-efbb-58ca-9adc-d458b66201e9,2.462827064202559e+06,641,0.02462827064202559
    1692748800.279,BTCUSD,Sell,737,26027.00,ZeroMinusTick,7db24799-4be3-540a-bdad-5c51e7d027d7,2.831674799246936e+06,737,0.02831674799246936
     */
    #[test]
    fn test_rec_to_trade() {
        let rec = "1692748800.279,BTCUSD,Sell,641,26027.00,ZeroMinusTick,80253109-efbb-58ca-9adc-d458b66201e9,2.462827064202559e+06,641,0.02462827064202559".to_string();
        let rec = rec.split(',').collect::<Vec<&str>>();
        let rec = StringRecord::from(rec);
        let trade = super::BBMarket::rec_to_trade(&rec);

        assert_eq!(trade.time, 1692748800279000);
        assert_eq!(trade.order_side, super::OrderSide::Sell);
        assert_eq!(trade.price, dec![26027.0]);
        assert_eq!(trade.size, dec![641.0]);
        assert_eq!(trade.id, "80253109-efbb-58ca-9adc-d458b66201e9");
    }

    #[test]
    fn test_last_day() {
        println!("{}", time_string(NOW()));        

        println!("{}", time_string(1692920542792000));
        println!("{}", time_string(1692920496811000));
    }

}
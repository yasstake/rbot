use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crossbeam_channel::Sender;
use once_cell::sync::Lazy;
use polars::frame::DataFrame;
use pyo3_polars::PyDataFrame;

use crate::{
    common::{time_string, MarketConfig, MicroSec, Trade, DAYS, FLOOR_DAY, NOW},
    db::{
        append_df, end_time_df, make_empty_ohlcvv, merge_df, ohlcv_start, ohlcvv_df, select_df,
        start_time_df, TradeBuffer, KEY,
    },
    net::RestApi,
};

use super::{
    convert_timems_to_datetime, ohlcv_df, ohlcv_floor_fix_time, ohlcv_from_ohlcvv_df,
    ohlcvv_from_ohlcvv_df, vap_df, TradeArchive, TradeDb,
};
use anyhow::anyhow;

//static EXCHANGE_DB_CACHE: Lazy<Mutex<HashMap<String, Arc<Mutex<TradeDataFrame>>>>> =
static TRADE_DATAFRAME_CACHE: Lazy<Mutex<HashMap<String, Arc<Mutex<TradeDataFrame>>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn insert_trade_dataframe_cache(
    config: &MarketConfig,
    production: bool,
    trade_dataframe: TradeDataFrame,
) -> anyhow::Result<Arc<Mutex<TradeDataFrame>>> {
    let key = config.key_string(production);

    let mut lock = TRADE_DATAFRAME_CACHE.lock().unwrap();
    let dataframe = Arc::new(Mutex::new(trade_dataframe));
    lock.insert(key, dataframe.clone());

    Ok(dataframe)
}

fn get_trade_dataframe_cache(
    config: &MarketConfig,
    production: bool,
) -> anyhow::Result<Arc<Mutex<TradeDataFrame>>> {
    let key = config.key_string(production);

    let lock = TRADE_DATAFRAME_CACHE.lock().unwrap();
    let trade_dataframe = lock.get(&key);

    if trade_dataframe.is_some() {
        return Ok(trade_dataframe.unwrap().clone());
    }

    Err(anyhow!("no TradeDataFrame [key={}] found", key))
}

pub const OHLCV_WINDOW_SEC: i64 = 60; // min

pub struct TradeDataFrame {
    db: TradeDb,
    archive: TradeArchive,

    cache_df: DataFrame,
    cache_ohlcvv: DataFrame,
    cache_duration: MicroSec,
}

impl TradeDataFrame {
    pub fn archive_emd_default() -> MicroSec {
        NOW() - DAYS(2)
    }

    pub fn get(config: &MarketConfig, production: bool) -> anyhow::Result<Arc<Mutex<Self>>> {
        let trade_dataframe = get_trade_dataframe_cache(config, production);

        if trade_dataframe.is_ok() {
            return Ok(trade_dataframe.unwrap());
        }

        let trade_dataframe = TradeDataFrame::open(config, production)?;
        let trade_data_frame = insert_trade_dataframe_cache(config, production, trade_dataframe)?;

        Ok(trade_data_frame)
    }

    pub fn vacuum(&self) -> anyhow::Result<()> {
        self.db.vacuum()
    }

    pub fn get_archive_start_time(&self)-> MicroSec {
        self.archive.start_time()
    }

    pub fn get_archive_end_time(&self) -> MicroSec {
        self.archive.end_time()
    }

    pub fn get_db_start_time(&self, since_time: MicroSec) -> MicroSec {
        self.db.start_time(since_time)
    }

    pub fn get_db_end_time(&self, from_time: MicroSec) -> MicroSec {
        self.db.end_time(from_time)
    }

    pub fn get_archive(&self) -> TradeArchive {
        self.archive.clone()
    }

    pub fn start_time(&self) -> MicroSec {
        let archive_start = self.get_archive_start_time();

        if archive_start != 0 {
            return archive_start;
        }

        let db_start = self.db.start_time(Self::archive_emd_default());

        db_start
    }

    pub fn end_time(&self) -> MicroSec {
        let archive_end = self.archive.end_time();

        let db_end = self.get_db_end_time(Self::archive_emd_default());

        if db_end != 0 && archive_end < db_end {
            return db_end;
        }

        return archive_end;
    }

    /// create new expire control message(from latest fix time to now)
    /// if there is not fix record in 2 days, return error.
    pub fn make_expire_control_message(
        &mut self,
        now: MicroSec,
        force: bool,
    ) -> anyhow::Result<Vec<Trade>> {
        self.db.make_expire_control_message(now, force)
    }

    pub fn set_cache_ohlcvv(&mut self, df: DataFrame) -> anyhow::Result<()> {
        let start_time: MicroSec = df
            .column(KEY::time_stamp)
            .unwrap()
            .min()
            .unwrap()
            .unwrap_or(0);
        let end_time: MicroSec = df
            .column(KEY::time_stamp)
            .unwrap()
            .max()
            .unwrap()
            .unwrap_or(0);

        let head = select_df(&self.cache_ohlcvv, 0, start_time);
        let tail = select_df(&self.cache_ohlcvv, end_time, 0);

        log::debug!(
            "set_cache_ohlcvv head {} /tail {}/ df {}",
            head.shape().0,
            tail.shape().0,
            df.shape().0
        );

        log::debug!("df {:?}", df.head(Some(2)));
        log::debug!("head {:?}", head.head(Some(2)));
        log::debug!("tail {:?}", tail.head(Some(2)));

        let df = append_df(&head, &df)?;
        let df = append_df(&df, &tail)?;

        self.cache_ohlcvv = df;

        Ok(())
    }

    // TODO: rename to open_channel
    pub fn open_channel(&mut self) -> anyhow::Result<Sender<Vec<Trade>>> {
        self.db.open_channel()
    }

    pub async fn download_archive<T>(
        &mut self,
        api: &T,
        ndays: i64,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64>
    where
        T: RestApi,
    {
        self.archive.download(api, ndays, force, verbose).await
    }


    pub fn get_cache_duration(&self) -> MicroSec {
        return self.cache_duration;
    }

    pub fn reset_cache_duration(&mut self) {
        self.cache_duration = 0;
    }

    pub fn select_cachedf(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<DataFrame> {
        let archive_end = self.get_archive_end_time();

        let df = if start_time < archive_end {
            let df1 = self.archive.select_cachedf(start_time, end_time)?;
            let df2 = self.db.select_cachedf(archive_end, end_time)?;
            append_df(&df1, &df2)
        } else {
            self.db.select_cachedf(start_time, end_time)
        };

        df
    }

    pub fn load_cachedf(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<i64> {
        self.cache_df = self.select_cachedf(start_time, end_time)?;

        Ok(self.cache_df.shape().0 as i64)
    }

    pub fn update_cache_all(&mut self) -> anyhow::Result<()> {
        self.update_cache_df(0, 0)
    }

    pub fn expire_cache_df(&mut self, forget_before: MicroSec) {
        let forget_before = FLOOR_DAY(forget_before); // expire by date.
        log::debug!("Expire cache {}", time_string(forget_before));
        let cache_timing = ohlcv_start(forget_before);
        self.cache_df = select_df(&self.cache_df, cache_timing, 0);
        self.cache_ohlcvv = select_df(&self.cache_ohlcvv, cache_timing, 0);
    }

    pub fn update_cache_df(
        &mut self,
        start_time: MicroSec,
        mut end_time: MicroSec,
    ) -> anyhow::Result<()> {
        log::debug!("update_cache_df {} -> {}", start_time, end_time);

        let df_start_time: i64;

        if end_time == 0 {
            end_time = NOW();
        }

        let cache_time = end_time - start_time;
        if self.cache_duration < cache_time {
            log::debug!("update cache duration {}", self.cache_duration);
            self.cache_duration = cache_time;
        }

        match start_time_df(&self.cache_df) {
            Some(t) => {
                df_start_time = t;
            }
            _ => {
                log::debug!(
                    "cache update all {} -> {}",
                    time_string(start_time),
                    time_string(end_time)
                );
                // no cache / update all
                self.load_cachedf(start_time, end_time)?;

                let df_start_time = if let Some(t) = start_time_df(&self.cache_df) {
                    t
                } else {
                    return Ok(());
                };

                let df_end_time = if let Some(t) = end_time_df(&self.cache_df) {
                    t
                } else {
                    return Ok(());
                };

                // update ohlcv
                self.cache_ohlcvv = ohlcvv_df(
                    &self.cache_df,
                    ohlcv_start(df_start_time),
                    df_end_time,
                    OHLCV_WINDOW_SEC,
                )?;
                return Ok(()); // update cache all.
            }
        }

        let df_end_time = end_time_df(&self.cache_df).unwrap();

        // load data and merge cache
        if start_time < df_start_time {
            let df1 = &self.select_cachedf(start_time, df_start_time)?;

            let len = df1.shape().0;
            // データがあった場合のみ更新
            if 0 < len {
                let new_df_start_time = start_time_df(&df1).unwrap();
                let new_df_end_time = end_time_df(&df1).unwrap();

                self.cache_df = merge_df(&df1, &self.cache_df)?;

                // update ohlcv
                let ohlcv1_start = ohlcv_start(new_df_start_time);
                let ohlcv1_end = ohlcv_start(new_df_end_time); // only use fixed data

                log::debug!(
                    "ohlcVV cache update diff before {} -> {}",
                    time_string(ohlcv1_start),
                    time_string(ohlcv1_end)
                );
                let ohlcv1 = ohlcvv_df(&self.cache_df, ohlcv1_start, ohlcv1_end, OHLCV_WINDOW_SEC)?;

                if ohlcv1.shape().0 != 0 {
                    let ohlcv2 = select_df(&self.cache_ohlcvv, ohlcv1_end, 0);
                    self.cache_ohlcvv = merge_df(&ohlcv1, &ohlcv2)?;
                }
            }
        } else {
            // expire cache ducarion * 2
            if df_start_time < start_time - self.cache_duration * 2 {
                self.expire_cache_df(start_time - self.cache_duration);
            }
        }

        if df_end_time < end_time {
            // 3日先までキャッシュを先読み
            let df2 = &self.select_cachedf(df_end_time, FLOOR_DAY(end_time + DAYS(4)))?;

            log::debug!(
                "load data after cache(2days) df1={:?} df2={:?}",
                self.cache_df.shape(),
                df2.shape()
            );

            if df2.shape().0 != 0 {
                let new_df_start_time = start_time_df(&df2).unwrap();
                let new_df_end_time = end_time_df(&df2).unwrap();
                // update ohlcv
                let ohlcv2_start = ohlcv_start(new_df_start_time);
                let ohlcv2_end = ohlcv_start(new_df_end_time); // use only fix data

                log::debug!(
                    "load data AFTER cache df1={:?} df2={:?}",
                    self.cache_df.shape(),
                    df2.shape()
                );
                self.cache_df = merge_df(&self.cache_df, &df2)?;

                log::debug!(
                    "ohlcVV cache update diff after {} ",
                    time_string(ohlcv2_start),
                );
                let ohlcv1 = select_df(&self.cache_ohlcvv, 0, ohlcv2_start);
                let ohlcv2 = ohlcvv_df(&self.cache_df, ohlcv2_start, ohlcv2_end, OHLCV_WINDOW_SEC)?;

                self.cache_ohlcvv = merge_df(&ohlcv1, &ohlcv2)?;
            }
        }

        Ok(())
    }

    fn _ohlcvv_df(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> anyhow::Result<DataFrame> {
        start_time = ohlcv_floor_fix_time(start_time, time_window_sec); // 開始tickは確定足、終了は未確定足もOK.

        self.update_cache_df(start_time, end_time)?;

        if time_window_sec % OHLCV_WINDOW_SEC == 0 {
            ohlcvv_from_ohlcvv_df(&self.cache_ohlcvv, start_time, end_time, time_window_sec)
        } else {
            ohlcvv_df(&self.cache_df, start_time, end_time, time_window_sec)
        }
    }

    pub fn py_ohlcvv_polars(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        start_time = ohlcv_floor_fix_time(start_time, window_sec); // 開始tickは確定足、終了は未確定足もOK.

        let mut df = self._ohlcvv_df(start_time, end_time, window_sec)?;

        convert_timems_to_datetime(&mut df)?;

        return Ok(PyDataFrame(df));
    }

    fn _ohlcv_df(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        time_window_sec: i64,
    ) -> anyhow::Result<DataFrame> {
        start_time = ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        self.update_cache_df(start_time, end_time)?;

        if time_window_sec % OHLCV_WINDOW_SEC == 0 {
            ohlcv_from_ohlcvv_df(&self.cache_ohlcvv, start_time, end_time, time_window_sec)
        } else {
            ohlcv_df(&self.cache_df, start_time, end_time, time_window_sec)
        }
    }

    pub fn py_ohlcv_polars(
        &mut self,
        mut start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        start_time = ohlcv_start(start_time); // 開始tickは確定足、終了は未確定足もOK.

        let mut df = self._ohlcv_df(start_time, end_time, window_sec)?;
        convert_timems_to_datetime(&mut df)?;
        let df = PyDataFrame(df);

        return Ok(df);
    }

    pub fn py_vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> anyhow::Result<PyDataFrame> {
        let df = self.vap(start_time, end_time, price_unit)?;

        let py_df = PyDataFrame(df);

        Ok(py_df)
    }

    pub fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> anyhow::Result<DataFrame> {
        self.update_cache_df(start_time, end_time)?;
        let df = vap_df(&self.cache_df, start_time, end_time, price_unit);

        Ok(df)
    }

    pub fn py_select_trades_polars(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let mut df = self.db.select_cachedf(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;
        let df = PyDataFrame(df);

        return Ok(df);
    }

    pub fn info(&self) -> String {
        let min = self.start_time();
        let max = self.end_time();

        return format!(
            "{{\"start\": {}, \"end\": {}}}",
            time_string(min),
            time_string(max)
        );
    }

    pub fn _repr_html_(&self) -> String {
        let min = self.start_time();
        let max = self.end_time();

        let archive_min = self.get_archive_start_time();
        let archive_max = self.get_archive_end_time();

        let db_min = self.get_db_start_time(0);
        let db_max = self.get_db_end_time(0);

        return format!(
            r#"
            <table>
            <caption>Trade Database info table</caption>
            <tr><th>start</th><th>end</th></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td><b>days=</b></td><td>{}</td></tr>                
            </table>                
            <table>
            <caption>Archive Data</caption>
            <tr><th>start</th><th>end</th></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td><b>days=</b></td><td>{}</td></tr>                
            </table>                
            <table>
            <caption>DataBase Data</caption>
            <tr><th>start</th><th>end</th></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td>{:?}</td><td>{:?}</td></tr>
            <tr><td><b>days=</b></td><td>{}</td></tr>                
            </table>                


            "#,
            min,
            max,
            time_string(min),
            time_string(max),
            (max - min) / DAYS(1),

            archive_min,
            archive_max,
            time_string(archive_min),
            time_string(archive_max),
            (archive_max - archive_min) / DAYS(1),

            db_min,
            db_max,
            time_string(db_min),
            time_string(db_max),
            (db_max - db_min) / DAYS(1),

        );
    }

    pub fn insert_records(&mut self, trades: &Vec<Trade>) -> anyhow::Result<i64> {
        return self.db.insert_records(trades);
    }

    pub fn find_latest_gap(&mut self, force: bool) -> anyhow::Result<(MicroSec, MicroSec)> {
        let start_time = NOW() - DAYS(2);

        let fix_time = self.latest_fix_time(start_time, force)?;
        let unfix_time = self.first_unfix_time(fix_time)?;

        Ok((fix_time, unfix_time))
    }

    pub fn db_start_up_rec(&mut self) -> Option<Trade> {
        self.db.get_last_start_up_rec()
    }

    pub fn latest_db_rec(&mut self, search_before: MicroSec) -> Option<Trade> {
        self.db.get_latest_rec(search_before)
    }

    pub fn latest_fix_trade(
        &mut self,
        start_time: MicroSec,
        force: bool,
    ) -> anyhow::Result<Option<Trade>> {
        self.db.latest_fix_trade(start_time, force)
    }

    pub fn latest_fix_time(
        &mut self,
        start_time: MicroSec,
        force: bool,
    ) -> anyhow::Result<MicroSec> {
        self.db.latest_fix_time(start_time, force)
    }

    pub fn first_unfix_trade(&mut self, start_time: MicroSec) -> anyhow::Result<Option<Trade>> {
        self.db.first_unfix_trade(start_time)
    }

    pub fn first_unfix_time(&mut self, start_time: MicroSec) -> anyhow::Result<MicroSec> {
        self.db.first_unfix_time(start_time)
    }
}

impl TradeDataFrame {
    fn open(config: &MarketConfig, production: bool) -> anyhow::Result<Self> {
        let conn = TradeDb::open(&config, production)?;
        log::debug!("db open success");
        let archive = TradeArchive::new(config, production);

        // setup cache
        let df = TradeBuffer::new().to_dataframe();
        let ohlcv = make_empty_ohlcvv();

        Ok(TradeDataFrame {
            db: conn,
            archive: archive,

            cache_df: df,
            cache_ohlcvv: ohlcv,
            cache_duration: 0,
        })
    }
}

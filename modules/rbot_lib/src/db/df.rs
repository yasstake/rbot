// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::fs::File;
use std::io::{BufReader, Cursor, Read};
use std::path::{Path, PathBuf};

use crate::common::{OrderSide, Trade};
use crate::common::{time_string, MicroSec, SEC};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use polars::prelude::DataFrame;
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use polars::prelude::SortMultipleOptions;

use polars::lazy::frame::pivot::pivot;
use polars::lazy::prelude::IntoLazy;
use polars::lazy::prelude::{col, LazyFrame};
use polars::time::ClosedWindow;

use anyhow::anyhow;

#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
pub mod KEY {
    use polars::prelude::PlSmallStr;

    pub const timestamp: &str = "timestamp";

    // for trade
    pub const price: &str = "price";
    pub const size: &str = "size";
    pub const order_side: &str = "order_side";
    // pub const liquid: &str = "liquid";
    #[allow(unused)]
    pub const id: &str = "id";

    // for ohlcv
    pub const open: &str = "open";
    pub const high: &str = "high";
    pub const low: &str = "low";
    pub const close: &str = "close";
    pub const volume: &str = "volume";
    #[allow(unused)]
    pub const sell_volume: &str = "sell_volume";
    #[allow(unused)]
    pub const sell_count: &str = "sell_count";
    #[allow(unused)]
    pub const buy_volume: &str = "buy_volume";
    #[allow(unused)]
    pub const buy_count: &str = "buy_count";
    pub const start_time: &str = "start_time";
    pub const end_time: &str = "end_time";
    pub const count: &str = "count";
}

/// Convert DataFrame to Parquet format and save it to the specified path.
pub fn df_to_parquet(df: &mut DataFrame, target_path: &PathBuf) -> anyhow::Result<i64> {
    let mut target_path = target_path.clone();
    target_path.set_extension("parquet");
    let tmp = target_path.clone();
    tmp.with_extension("tmp");

    let mut file = File::create(&tmp).expect("could not create file");

    ParquetWriter::new(&mut file).finish(df)?;

    std::fs::rename(&tmp, &target_path)?;

    Ok(df.shape().0 as i64)
}

/// This function reads a Parquet file and converts it into a DataFrame.
pub fn parquet_to_df(path: &PathBuf) -> anyhow::Result<DataFrame> {
    let file = File::open(path).expect("file not found");

    let df = ParquetReader::new(file).finish()?;

    Ok(df)
}


fn has_csv_header(source_path: &PathBuf) -> anyhow::Result<bool> {
    let suffix = source_path.extension().unwrap_or_default();
    let suffix = suffix.to_ascii_lowercase();

    if suffix == "csv" {
        let file = File::open(source_path)?;
        let reader = BufReader::new(file);

        let csv = ReaderBuilder::new().has_headers(true).from_reader(reader);
        return Ok(csv.has_headers())
    }
    else if suffix == "gz" {
        let file = File::open(source_path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);

        let csv = ReaderBuilder::new().has_headers(true).from_reader(reader);
        return Ok(csv.has_headers())
    }
    else if suffix == "zip" {
        let file = File::open(source_path)?;
        let mut archive = ZipArchive::new(file)?;

        // Assuming there's only one file in the zip
        let csv_file = archive.by_index(0)?;
        let reader = BufReader::new(csv_file);

        let csv = ReaderBuilder::new().has_headers(true).from_reader(reader);
        return Ok(csv.has_headers())
    }

    Err(anyhow!("unsupported file type {:?}", source_path))
}


/// import csv format into dataframe
pub fn csv_to_df(source_path: &PathBuf) -> anyhow::Result<DataFrame> {
    let has_header = has_csv_header(source_path)?;

    log::debug!("reading csv file = {:?}, header = {:?}", source_path, has_header);

    let suffix = source_path.extension().unwrap_or_default();
    let suffix = suffix.to_ascii_lowercase();

    if suffix == "gz" || suffix == "csv" {
        let df = CsvReadOptions::default()
            .with_has_header(has_header)
            .try_into_reader_with_file_path(Some(source_path.clone()))?
            .finish()?;

        return Ok(df);
    } else if suffix == "zip" {
        let file = File::open(source_path)?;
        let mut archive = ZipArchive::new(file)?;

        // Assuming there's only one file in the zip
        let mut csv_file = archive.by_index(0)?;
        let mut csv_data = Vec::new();

        csv_file.read_to_end(&mut csv_data)?;
        let cursor = Cursor::new(csv_data);

        let df = CsvReadOptions::default()
            .with_has_header(has_header)
            .into_reader_with_file_handle(cursor)
            .finish()?;

        return Ok(df);
    }

    return Err(anyhow!("Unknown file type {:?}", source_path));
    //let lazy = LazyCsvReader::new(source_path).with_has_header(has_header).finish()?;
}

/*
/// Cutoff start_time to end_time(not include)
pub fn select_df(df: &DataFrame, start_time: MicroSec, end_time: MicroSec) -> DataFrame {
    log::debug!(
        "Select from {} -> {}",
        time_string(start_time),
        time_string(end_time)
    );
    if start_time == 0 && end_time == 0 {
        log::debug!("preserve select df");
        return df.clone();
    }

    let mask: ChunkedArray<BooleanType>;

    if start_time == 0 {
        mask = df.column(KEY::timestamp).unwrap().lt(end_time).unwrap();
    } else if end_time == 0 {
        mask = df
            .column(KEY::timestamp)
            .unwrap()
            .gt_eq(start_time)
            .unwrap();
    } else {
        mask = df
            .column(KEY::timestamp)
            .unwrap()
            .gt_eq(start_time)
            .unwrap()
            & df.column(KEY::timestamp).unwrap().lt(end_time).unwrap();
    }

    let df = df.filter(&mask).unwrap();

    return df;
}
*/

/// select df with lazy dataframe
pub fn select_df_lazy(df: &DataFrame, start_time: MicroSec, end_time: MicroSec) -> LazyFrame {
    log::debug!(
        "Select from {} -> {}",
        time_string(start_time),
        time_string(end_time)
    );

    let mut lazy_df = df.clone().lazy();

    if start_time == 0 && end_time == 0 {
        log::debug!("preserve select df");
        return lazy_df;
    }

    if 0 < start_time {
        lazy_df = lazy_df.filter(col(KEY::timestamp).gt_eq(start_time));
    }

    if start_time < 0 {
        let df_end = end_time_df(&df);

        if let Some(df_end) = df_end {
            lazy_df = lazy_df.filter(col(KEY::timestamp).gt_eq(df_end + start_time));
        }
    }

    if 0 < end_time {
        lazy_df = lazy_df.filter(col(KEY::timestamp).lt(end_time));
    }

    if end_time < 0 {
        let df_end = end_time_df(&df);

        if let Some(df_end) = df_end {
            lazy_df = lazy_df.filter(col(KEY::timestamp).lt(df_end + end_time));
        }
    }

    return lazy_df;
}

pub fn start_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::timestamp).unwrap().i64().unwrap().min().map(|x| x as MicroSec)
}

pub fn end_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::timestamp).unwrap().i64().unwrap().max().map(|x| x as MicroSec)
}

/// append df2 after df1
pub fn append_df(df1: &DataFrame, df2: &DataFrame) -> anyhow::Result<DataFrame> {
    if df1.shape().1 != df2.shape().1 {
        println!("shape mismatch df1={:?}, df2={:?}", df1.shape(), df2.shape());
        println!("{:?}", df1);
        println!("{:?}", df2);
    }

    let df = df1.vstack(df2)?;

    Ok(df)
}

/// merge 2 dataframe, if overlap df2 on df1, df1 will be trimmed(overwritten by df2)
pub fn merge_df(df1: &DataFrame, df2: &DataFrame) -> anyhow::Result<DataFrame> {
    log::debug!("merge df1={:?}  df2={:?}", df1.shape(), df2.shape());

    if df1.shape().0 == 0 {
        return Ok(df2.clone());
    }

    if df2.shape().0 == 0 {
        return Ok(df1.clone());
    }

    let df2_start_time = start_time_df(df2);
    if df2_start_time.is_none() {
        return Err(anyhow!("cannot find dataframe start timestamp {:?}", df2));
    }
    let df2_start_time = df2_start_time.unwrap();

    let df2_end_time = end_time_df(df2);
    if df2_end_time.is_none() {
        return Err(anyhow!("cannot find dataframe end timestamp {:?}", df2));
    }
    let df2_end_time = df2_end_time.unwrap();

    let df_before = select_df_lazy(df1, 0, df2_start_time).collect()?;
    let df = if df_before.shape().0 == 0 {
        df2.clone()
    } else {
        append_df(&df_before, df2)?
    };

    let df_after = select_df_lazy(df1, df2_end_time, 0).collect()?;
    let df = if df_after.shape().0 == 0 {
        df
    } else {
        append_df(&df, &df_after)?
    };

    Ok(df)
}

pub fn ohlcv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> anyhow::Result<DataFrame> {
    log::debug!(
        "ohlcv_df, from={} / to={}",
        time_string(start_time),
        time_string(end_time)
    );

    if df.shape().0 == 0 {
        log::debug!("empty ohlc");
        return Ok(make_empty_ohlcv());
    }

    let option = DynamicGroupOptions {
        index_column: KEY::timestamp.into(),
        every: Duration::new(SEC(time_window)), // グループ間隔
        period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
        offset: Duration::parse("0m"),
        // truncate: true,                    // タイムスタンプを切り下げてまとめる。
        include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
        closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
        // check_sorted: false,
        ..Default::default()
    };

    let df = select_df_lazy(df, start_time, end_time);

    let result = df
        .group_by_dynamic(col(KEY::timestamp), [], option)
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::volume),
            col(KEY::price).count().alias(KEY::count),
        ])
        .sort(
            vec![(KEY::timestamp).to_string()],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                maintain_order: true,
                multithreaded: true,
                limit: None,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return Ok(dataframe),
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return Ok(make_empty_ohlcv());
        }
    }
}

pub fn ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> anyhow::Result<DataFrame> {
    log::debug!(
        "ohlcv_df, from={} / to={}",
        time_string(start_time),
        time_string(end_time)
    );

    if df.shape().0 == 0 {
        log::debug!("empty ohlcv");
        return Ok(make_empty_ohlcvv());
    }

    let option = DynamicGroupOptions {
        index_column: KEY::timestamp.into(),
        every: Duration::new(SEC(time_window)), // グループ間隔
        period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
        offset: Duration::parse("0m"),
        // truncate: true,                    // タイムスタンプを切り下げてまとめる。
        include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
        closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
        ..Default::default()
    };

    let df = select_df_lazy(df, start_time, end_time);

    let result = df
        .group_by_dynamic(col(KEY::timestamp), [col(KEY::order_side)], option)
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::volume),
            col(KEY::price).count().alias(KEY::count),
            col(KEY::timestamp).min().alias(KEY::start_time),
            col(KEY::timestamp).max().alias(KEY::end_time),
        ])
        .sort(
            vec![KEY::timestamp.to_string()],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                maintain_order: true,
                multithreaded: true,
                limit: None,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return Ok(dataframe),
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return Ok(make_empty_ohlcvv());
        }
    }
}

pub fn ohlcv_from_ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> anyhow::Result<DataFrame> {
    log::debug!(
        "ohlc {:?} -> {:?}",
        time_string(start_time),
        time_string(end_time)
    );

    if df.shape().0 == 0 {
        log::debug!("empty ohlc");
        return Ok(make_empty_ohlcv());
    }

    let option = DynamicGroupOptions {
        index_column: KEY::timestamp.into(),
        every: Duration::new(SEC(time_window)), // グループ間隔
        period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
        offset: Duration::parse("0m"),
        // truncate: true,                    // タイムスタンプを切り下げてまとめる。
        include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
        closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
        ..Default::default()
    };

    let df = select_df_lazy(df, start_time, end_time);

    let result = df
        .group_by_dynamic(col(KEY::timestamp), [], option)
        .agg([
            col(KEY::open)
                .sort_by(
                    vec![col(KEY::start_time)],
                    SortMultipleOptions {
                        descending: vec![false],
                        nulls_last: vec![false],
                        multithreaded: true,
                        maintain_order: true,
                        limit: None,
                    },
                )
                .first()
                .alias(KEY::open),
            col(KEY::high).max().alias(KEY::high),
            col(KEY::low).min().alias(KEY::low),
            col(KEY::close)
                .sort_by(
                    vec![col(KEY::end_time)],
                    SortMultipleOptions {
                        descending: vec![false],
                        nulls_last: vec![false],
                        multithreaded: true,
                        maintain_order: true,
                        limit: None,
                    },
                )
                .last()
                .alias(KEY::close),
            col(KEY::volume).sum().alias(KEY::volume),
            col(KEY::count).sum().alias(KEY::count),
        ])
        .sort(
            vec![(KEY::timestamp).to_string()],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                maintain_order: true,
                multithreaded: true,
                limit: None,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return Ok(dataframe),
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return Ok(make_empty_ohlcv());
        }
    }
}

pub fn ohlcvv_from_ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> anyhow::Result<DataFrame> {
    log::debug!(
        "ohlc {:?} -> {:?}",
        time_string(start_time),
        time_string(end_time)
    );

    let option = DynamicGroupOptions {
        index_column: KEY::timestamp.into(),
        every: Duration::new(SEC(time_window)), // グループ間隔
        period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
        offset: Duration::parse("0m"),
        // truncate: true,                    // タイムスタンプを切り下げてまとめる。
        include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
        closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
        ..Default::default()
    };

    let df = select_df_lazy(df, start_time, end_time);

    let result = df
        .group_by_dynamic(col(KEY::timestamp), [col(KEY::order_side)], option)
        .agg([
            col(KEY::open).first().alias(KEY::open),
            col(KEY::high).max().alias(KEY::high),
            col(KEY::low).min().alias(KEY::low),
            col(KEY::close).last().alias(KEY::close),
            col(KEY::volume).sum().alias(KEY::volume),
            col(KEY::count).sum().alias(KEY::count),
            col(KEY::start_time).min().alias(KEY::start_time),
            col(KEY::end_time).max().alias(KEY::end_time),
        ])
        .sort(
            vec![(KEY::timestamp).to_string()],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                maintain_order: true,
                multithreaded: true,
                limit: None,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return Ok(dataframe),
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return Ok(make_empty_ohlcvv());
        }
    }
}

/// Calc Value At Price
/// group by unit price and order_side
pub fn vap_df(df: &DataFrame, start_time: MicroSec, end_time: MicroSec, size: i64) -> DataFrame {
    let df = select_df_lazy(df, start_time, end_time);

    let floor_price = col(KEY::price);

    let floor_price = (floor_price.floor_div(size.into()) * size.into()).floor();
    let vap_gb = df.group_by([col(KEY::order_side), floor_price]);

    let vap = vap_gb
        .agg([col(KEY::size).sum().alias(KEY::volume)])
        .collect()
        .unwrap();

    let vap = pivot(
        &vap,
        vec![KEY::volume],
        Some([KEY::price]),
        Some([KEY::order_side]),
        false,
        None,
        None,
    )
    .unwrap();

    let vap = vap
        .sort(
            vec![KEY::price.to_string()],
            SortMultipleOptions {
                descending: vec![false],
                nulls_last: vec![false],
                multithreaded: true,
                maintain_order: true,
                limit: None,
            },
        )
        .unwrap();

    let price = vap.column(KEY::price).unwrap().clone();
    let mut buy_vol = vap
        .column("true")
        .unwrap()
        .fill_null(FillNullStrategy::Zero)
        .unwrap();

    buy_vol.rename(KEY::buy_volume.into()).clone();


    let mut sell_vol = vap
        .column("false")
        .unwrap()
        .fill_null(FillNullStrategy::Zero)
        .unwrap();

    sell_vol.rename(KEY::sell_volume.into());

    let total_vol = buy_vol.clone() + sell_vol.clone();

    let df = DataFrame::new(vec![price.into(), buy_vol.into(), sell_vol.into(), total_vol.unwrap().into()]).unwrap();

    df
}

pub struct TradeBuffer {
    pub id: Vec<String>,
    pub time_stamp: Vec<MicroSec>,
    pub order_side: Vec<String>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
}

impl TradeBuffer {
    pub fn new() -> Self {
        return TradeBuffer {
            id: vec![],
            time_stamp: vec![],
            price: vec![],
            size: vec![],
            order_side: vec![],
        };
    }

    #[allow(unused)]
    pub fn clear(&mut self) {
        self.id.clear();
        self.time_stamp.clear();
        self.price.clear();
        self.size.clear();
        self.order_side.clear();
    }

    pub fn push(
        &mut self,
        timestamp: MicroSec,
        id: String,
        order_side: &OrderSide,
        price: f64,
        size: f64,
    ) {
        self.id.push(id);
        self.time_stamp.push(timestamp);
        self.order_side.push(order_side.to_string());
        self.price.push(price);
        self.size.push(size);
    }

    #[allow(unused)]
    pub fn push_trades(&mut self, trades: Vec<Trade>) {
        for trade in trades {
            self.push_trade(&trade);
        }
    }

    pub fn push_trade(&mut self, trade: &Trade) {
        self.id.push(trade.id.clone());
        self.time_stamp.push(trade.time);
        self.price.push(trade.price.to_f64().unwrap());
        self.size.push(trade.size.to_f64().unwrap());
        self.order_side.push(trade.order_side.to_string());
    }

    pub fn to_dataframe(&self) -> DataFrame {
        let id = Series::new(KEY::id.into(), self.id.to_vec());
        let time_stamp = Series::new(KEY::timestamp.into(), self.time_stamp.to_vec());
        let order_side = Series::new(KEY::order_side.into(), self.order_side.to_vec());
        let price = Series::new(KEY::price.into(), self.price.to_vec());
        let size = Series::new(KEY::size.into(), self.size.to_vec());

        let df = DataFrame::new(vec![time_stamp.into(), order_side.into(), price.into(), size.into(), id.into() ]).unwrap();

        return df;
    }
}

pub fn make_empty_ohlcvv() -> DataFrame {
    let time = Series::new(KEY::timestamp.into(), Vec::<MicroSec>::new());
    let order_side = Series::new(KEY::order_side.into(), Vec::<String>::new());
    let open = Series::new(KEY::open.into(), Vec::<f64>::new());
    let high = Series::new(KEY::high.into(), Vec::<f64>::new());
    let low = Series::new(KEY::low.into(), Vec::<f64>::new());
    let close = Series::new(KEY::close.into(), Vec::<f64>::new());
    let vol = Series::new(KEY::volume.into(), Vec::<f64>::new());
    let count = Series::new(KEY::count.into(), Vec::<i64>::new());
    let start_time = Series::new(KEY::start_time.into(), Vec::<MicroSec>::new());
    let end_time = Series::new(KEY::end_time.into(), Vec::<MicroSec>::new());

    let df = DataFrame::new(vec![
        time.into(), order_side.into(), open.into(), high.into(), low.into(), close.into(), vol.into(), count.into(), start_time.into(), end_time.into(),
    ])
    .unwrap();

    return df;
}

pub fn make_empty_ohlcv() -> DataFrame {
    let time = Series::new(KEY::timestamp.into(), Vec::<MicroSec>::new());
    let open = Series::new(KEY::open.into(), Vec::<f64>::new());
    let high = Series::new(KEY::high.into(), Vec::<f64>::new());
    let low = Series::new(KEY::low.into(), Vec::<f64>::new());
    let close = Series::new(KEY::close.into(), Vec::<f64>::new());
    let vol = Series::new(KEY::volume.into(), Vec::<f64>::new());
    let count = Series::new(KEY::count.into(), Vec::<i64>::new());

    let df = DataFrame::new(vec![time.into(), open.into(), high.into(), low.into(), close.into(), vol.into(), count.into()]).unwrap();

    return df;
}

pub trait AsDynamicGroupOptions {
    fn as_dynamic_group_options(&self) -> &DynamicGroupOptions;
}

impl AsDynamicGroupOptions for DynamicGroupOptions {
    fn as_dynamic_group_options(&self) -> &DynamicGroupOptions {
        self
    }
}

pub fn convert_timems_to_datetime(df: &mut DataFrame) -> anyhow::Result<()> {
    let time = df.column(KEY::timestamp)?.i64()?.clone();
    let date_time = time.into_datetime(TimeUnit::Microseconds, None);
    df.with_column(date_time)?;

    Ok(())
}

use tokio::time::error::Elapsed;
use ::zip::ZipArchive;
use polars::prelude::*;
use rust_decimal::prelude::ToPrimitive;

#[cfg(test)]
mod test_df {
    use super::*;
    use crate::common::{init_debug_log, DAYS};

    #[test]
    fn test_merge_and_append_df() -> anyhow::Result<()> {
        init_debug_log();

        let df1 = df![
            KEY::timestamp => [1, 2, 3, 4, 5],
            "value" => [11, 12, 13, 14, 15]
        ]?;

        let df2 = df![
            KEY::timestamp => [5, 6],
            "value" => [25, 26]
        ]?;

        let merged_df = df![
            KEY::timestamp => [1, 2, 3, 4, 5, 6],
            "value" => [11, 12, 13, 14, 25, 26]
        ]?;

        let df = merge_df(&df1, &df2)?;
        log::debug!("{:?}", df);
        assert_eq!(df, merged_df);

        let empty_df = select_df_lazy(&df1, 100, 101).collect()?;
        log::debug!("{:?}", empty_df);

        let df = merge_df(&empty_df, &df2)?;
        assert_eq!(df, df2);

        let df = merge_df(&df1, &empty_df)?;
        assert_eq!(df, df1);

        assert_eq!(
            merge_df(
                &df![
                    KEY::timestamp => [1, 2],
                    "value" => [11, 12]
                ]?,
                &df![
                    KEY::timestamp => [5, 6],
                    "value" => [25, 26]
                ]?,
            )?,
            df![
                KEY::timestamp => [1,2,5,6],
                "value" => [11, 12, 25,26]
            ]?
        );

        assert_eq!(
            append_df(
                &df![
                    KEY::timestamp => [1, 2],
                    "value" => [11, 12]
                ]?,
                &df![
                    KEY::timestamp => [2, 3],
                    "value" => [25, 26]
                ]?,
            )?,
            df![
                KEY::timestamp => [1,2,2,3],
                "value" => [11, 12, 25,26]
            ]?
        );

        Ok(())
    }

    #[test]
    fn test_simple_dynamic_group() {
        let option = DynamicGroupOptions {
            every: Duration::new(DAYS(1)),
            index_column: "date".into(),
            // check_sorted: true,
            start_by: StartBy::DataPoint,
            period: Duration::new(DAYS(1)), // データ取得の幅（グループ間隔と同じでOK)
            offset: Duration::parse("0m"),
            // truncate: true,                    // タイムスタンプを切り下げてまとめる。
            include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
            closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。

            ..Default::default()
        };

        // Make example of lazy group by
        let df = df!(
            "date" => [DAYS(100), DAYS(100), DAYS(101), DAYS(101), DAYS(102),DAYS(103)],
            "category" => ["A", "B", "A", "B", "A", "B"],
            "value" => [1, 2, 3, 4, 5, 6]
        )
        .unwrap();

        println!("{:?}", df);

        let groupby = df
            .lazy()
            .group_by_dynamic(
                col("date"),
                //[col("category")],
                [],
                option,
            )
            .agg([
                col("value").mean().alias("mean"),
                col("value").sum().alias("sum"),
            ])
            .sort(
                vec!["date".to_string()],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![false],
                    multithreaded: true,
                    maintain_order: false,
                    limit: None,
                },
            )
            .collect()
            .unwrap();

        println!("{:?}", groupby);
        let df = make_empty_ohlcv();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 7);
    }

    fn make_ohlcv_df() -> DataFrame {
        let df = df!(
            KEY::order_side => &[false, true, false, true],
            KEY::timestamp => &[DAYS(1), DAYS(2), DAYS(3), DAYS(3)],
            KEY::open => &[1.0, 2.0, 3.0, 4.0],
            KEY::high => &[1.0, 2.0, 3.0, 4.0],
            KEY::low => &[1.0, 2.0, 3.0, 4.0],
            KEY::close => &[1.0, 2.0, 3.0, 4.0],
            KEY::volume => &[1.0, 2.0, 3.0, 4.0],
            KEY::count => &[1, 2, 3, 4],
            KEY::start_time => &[DAYS(1), DAYS(2), DAYS(3), DAYS(3)],
            KEY::end_time => &[DAYS(2), DAYS(3), DAYS(4), DAYS(4)]
        );

        return df
            .unwrap()
            .sort(
                [KEY::timestamp],
                SortMultipleOptions {
                    descending: vec![false],
                    nulls_last: vec![false],
                    multithreaded: true,
                    maintain_order: true,
                    limit: None,
                },
            )
            .unwrap();
    }

    #[test]
    fn test_make_ohlcv() {
        let ohlc = make_ohlcv_df();

        println!("{:?}", ohlc);
        assert_eq!(ohlc.shape().0, 4);
        assert_eq!(ohlc.shape().1, 10);
    }

    #[test]
    fn test_make_ohlcvv_from_ohclv() -> anyhow::Result<()> {
        let ohlcv = make_ohlcv_df();

        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, 0, 0, 10)?;
        println!("{:?}", ohlcv2);

        assert_eq!(ohlcv, ohlcv2);

        Ok(())
    }

    #[test]
    fn test_make_ohlcv_from_ohclv() -> anyhow::Result<()> {
        let ohlcv = make_ohlcv_df();

        let ohlcv2 = ohlcv_from_ohlcvv_df(&ohlcv, 0, 0, 10)?;
        println!("{:?}", ohlcv2);

        assert_eq!(ohlcv2.shape().0, 3);
        assert_eq!(ohlcv2.shape().1, 7);

        Ok(())
    }

    #[test]
    fn test_make_empty_ohlcv() {
        let r = make_empty_ohlcvv();

        println!("{:?}", r);
        assert_eq!(r.shape(), (0, 10));
    }

    #[test]
    fn test_make_empty_ohlc() {
        let r = make_empty_ohlcv();

        println!("{:?}", r);
        assert_eq!(r.shape(), (0, 7));
    }

    #[test]
    fn test_make_ohlcv_from_empty_ohlcv() -> anyhow::Result<()> {
        let r = make_empty_ohlcvv();
        let r2 = ohlcvv_df(&r, 0, 0, 10)?;
        println!("{:?}", r2);
        assert_eq!(r2.shape(), (0, 10));

        Ok(())
    }

    #[test]
    fn test_make_ohlc_from_empty_ohlcv() -> anyhow::Result<()> {
        let r = make_empty_ohlcvv();
        let r2 = ohlcv_df(&r, 0, 0, 10)?;
        println!("{:?}", r2);
        assert_eq!(r2.shape(), (0, 7));

        Ok(())
    }

    #[test]
    fn test_make_ohlcv_from_ohlcv_empty_ohlcv() -> anyhow::Result<()> {
        let r = make_empty_ohlcvv();
        println!("{:?}", r);
        let r2 = ohlcvv_df(&r, 0, 0, 10)?;
        println!("{:?}", r2);
        let r3 = ohlcv_from_ohlcvv_df(&r2, 0, 0, 10);
        println!("{:?}", r3);

        Ok(())
    }

    #[test]
    fn test_make_ohlcv_datetime() {
        use polars::datatypes::TimeUnit;

        let time = Series::new(KEY::timestamp.into(), Vec::<MicroSec>::new());

        let t64 = time.i64().unwrap().clone();
        let date_time = t64.into_datetime(TimeUnit::Microseconds, None);

        let t = Series::new("time".into(), date_time);
        let open = Series::new(KEY::open.into(), Vec::<f64>::new());
        let high = Series::new(KEY::high.into(), Vec::<f64>::new());
        let low = Series::new(KEY::low.into(), Vec::<f64>::new());
        let close = Series::new(KEY::close.into(), Vec::<f64>::new());
        let vol = Series::new(KEY::volume.into(), Vec::<f64>::new());
        let count = Series::new(KEY::count.into(), Vec::<f64>::new());

        let df = DataFrame::new(vec![t.into(), open.into(), high.into(), low.into(), close.into(), vol.into(), count.into()]).unwrap();

        println!("{:?}", df);
    }

    #[test]
    fn test_convert_ohlc_datetime() -> anyhow::Result<()> {
        let mut df = make_empty_ohlcv();

        println!("{:?}", df);

        convert_timems_to_datetime(&mut df)?;

        println!("{:?}", df);

        Ok(())
    }

    #[test]
    fn test_ohlcv() {
        let mut trade_buffer = TradeBuffer::new();

        for i in 0..1000000 {
            trade_buffer.push(
                i * 1_00,
                "id-1".to_string(),
                &OrderSide::Buy,
                (i * 2) as f64,
                (i * 3) as f64,
            );
        }

        let df = trade_buffer.to_dataframe();

        println!("{:}", df);

        let mut ohlcv = ohlcv_df(&df, 123, 0, 10).unwrap();

        println!("{:?}", ohlcv);

        let _ = convert_timems_to_datetime(&mut ohlcv);

        println!("{:?}", ohlcv);
    }

    #[test]
    fn test_ohlcvv() {
        let mut trade_buffer = TradeBuffer::new();

        for i in 0..1000000 {
            trade_buffer.push(
                i * 1_00,
                "id2".to_string(),
                &OrderSide::Sell,
                (i * 2) as f64,
                (i * 3) as f64,
            );
        }

        let df = trade_buffer.to_dataframe();

        println!("{:}", df);

        let mut ohlcv = ohlcvv_df(&df, 123, 0, 10).unwrap();

        println!("{:?}", ohlcv);

        let _ = convert_timems_to_datetime(&mut ohlcv);

        println!("{:?}", ohlcv);
    }
}

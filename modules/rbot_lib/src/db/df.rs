// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::fs::File;
use std::path::PathBuf;

use crate::common::Trade;
use crate::common::{time_string, MicroSec, SEC};
use polars::prelude::BooleanType;
use polars::prelude::ChunkCompare;
use polars::prelude::ChunkedArray;
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

/// import csv format into dataframe
pub fn csv_to_df(source_path: &PathBuf, has_header: bool) -> anyhow::Result<DataFrame> {
    log::debug!("reading csv file = {:?}", source_path);

    let df = CsvReadOptions::default()
        .with_has_header(has_header)
        .try_into_reader_with_file_path(Some(source_path.clone()))?
        .finish()?;

    Ok(df)
}

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

/// select df with lazy dataframe
pub fn select_df_lazy(df: &DataFrame, start_time: MicroSec, end_time: MicroSec) -> LazyFrame {
    log::debug!(
        "Select from {} -> {}",
        time_string(start_time),
        time_string(end_time)
    );

    let mut df = df.clone().lazy();

    if start_time == 0 && end_time == 0 {
        log::debug!("preserve select df");
        return df;
    }

    if start_time == 0 {
        df = df.filter(col(KEY::timestamp).lt(end_time));
    } else if end_time == 0 {
        df = df.filter(col(KEY::timestamp).gt_eq(start_time));
    } else {
        df = df.filter(
            col(KEY::timestamp)
                .gt_eq(start_time)
                .and(col(KEY::timestamp).lt(end_time)),
        );
    }

    return df;
}

pub fn start_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::timestamp).unwrap().min().unwrap()
}

pub fn end_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::timestamp).unwrap().max().unwrap()
}

/// append df2 after df1
pub fn append_df(df1: &DataFrame, df2: &DataFrame) -> anyhow::Result<DataFrame> {
    let df = df1.vstack(df2)?;

    Ok(df)
}

/// merge 2 dataframe, if overlap df2 on df1, df1 will be trimmed(overritten by df2)
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
        return Err(anyhow!("cannot find time stamp {:?}", df2));
    }
    let df2_start_time = df2_start_time.unwrap();

    let df = select_df(df1, 0, df2_start_time);
    if df.shape().0 == 0 {
        return Ok(df2.clone());
    }

    let df = df.vstack(df2)?;

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
            },
        )
        .unwrap();

    let price = vap.column(KEY::price).unwrap().clone();
    let buy_vol = vap
        .column("true")
        .unwrap()
        .fill_null(FillNullStrategy::Zero)
        .unwrap()
        .rename(KEY::buy_volume)
        .clone();
    let sell_vol = vap
        .column("false")
        .unwrap()
        .fill_null(FillNullStrategy::Zero)
        .unwrap()
        .rename(KEY::sell_volume)
        .clone();

    let total_vol = (&buy_vol + &sell_vol).unwrap().rename(KEY::volume).clone();

    let df = DataFrame::new(vec![price, buy_vol, sell_vol, total_vol]).unwrap();

    df
}

pub struct TradeBuffer {
    pub time_stamp: Vec<MicroSec>,
    pub order_side: Vec<bool>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
}

impl TradeBuffer {
    pub fn new() -> Self {
        return TradeBuffer {
            time_stamp: Vec::new(),
            price: Vec::new(),
            size: Vec::new(),
            order_side: Vec::new(),
        };
    }

    #[allow(unused)]
    pub fn clear(&mut self) {
        self.time_stamp.clear();
        self.price.clear();
        self.size.clear();
        self.order_side.clear();
    }

    pub fn push(&mut self, timestamp: MicroSec, order_side: bool, price: f64, size: f64) {
        self.time_stamp.push(timestamp);
        self.order_side.push(order_side);
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
        self.time_stamp.push(trade.time);
        self.price.push(trade.price.to_f64().unwrap());
        self.size.push(trade.size.to_f64().unwrap());
        self.order_side.push(trade.order_side.is_buy_side());
    }

    pub fn to_dataframe(&self) -> DataFrame {
        let time_stamp = Series::new(KEY::timestamp, self.time_stamp.to_vec());
        let order_side = Series::new(KEY::order_side, self.order_side.to_vec());
        let price = Series::new(KEY::price, self.price.to_vec());
        let size = Series::new(KEY::size, self.size.to_vec());

        let df = DataFrame::new(vec![time_stamp, order_side, price, size]).unwrap();

        return df;
    }
}

pub fn make_empty_ohlcvv() -> DataFrame {
    let time = Series::new(KEY::timestamp, Vec::<MicroSec>::new());
    let order_side = Series::new(KEY::order_side, Vec::<bool>::new());
    let open = Series::new(KEY::open, Vec::<f64>::new());
    let high = Series::new(KEY::high, Vec::<f64>::new());
    let low = Series::new(KEY::low, Vec::<f64>::new());
    let close = Series::new(KEY::close, Vec::<f64>::new());
    let vol = Series::new(KEY::volume, Vec::<f64>::new());
    let count = Series::new(KEY::count, Vec::<i64>::new());
    let start_time = Series::new(KEY::start_time, Vec::<MicroSec>::new());
    let end_time = Series::new(KEY::end_time, Vec::<MicroSec>::new());

    let df = DataFrame::new(vec![
        time, order_side, open, high, low, close, vol, count, start_time, end_time,
    ])
    .unwrap();

    return df;
}

pub fn make_empty_ohlcv() -> DataFrame {
    let time = Series::new(KEY::timestamp, Vec::<MicroSec>::new());
    let open = Series::new(KEY::open, Vec::<f64>::new());
    let high = Series::new(KEY::high, Vec::<f64>::new());
    let low = Series::new(KEY::low, Vec::<f64>::new());
    let close = Series::new(KEY::close, Vec::<f64>::new());
    let vol = Series::new(KEY::volume, Vec::<f64>::new());
    let count = Series::new(KEY::count, Vec::<i64>::new());

    let df = DataFrame::new(vec![time, open, high, low, close, vol, count]).unwrap();

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

        let empty_df = select_df(&df1, 100, 101);
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

        let time = Series::new(KEY::timestamp, Vec::<MicroSec>::new());

        let t64 = time.i64().unwrap().clone();
        let date_time = t64.into_datetime(TimeUnit::Microseconds, None);

        let t = Series::new("time", date_time);
        let open = Series::new(KEY::open, Vec::<f64>::new());
        let high = Series::new(KEY::high, Vec::<f64>::new());
        let low = Series::new(KEY::low, Vec::<f64>::new());
        let close = Series::new(KEY::close, Vec::<f64>::new());
        let vol = Series::new(KEY::volume, Vec::<f64>::new());
        let count = Series::new(KEY::count, Vec::<f64>::new());

        let df = DataFrame::new(vec![t, open, high, low, close, vol, count]).unwrap();

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
            trade_buffer.push(i * 1_00, true, (i * 2) as f64, (i * 3) as f64);
        }

        let df = trade_buffer.to_dataframe();

        println!("{:}", df);

        let mut ohlcv = ohlcv_df(&df, 123, 0, 10).unwrap();

        println!("{:?}", ohlcv);

        convert_timems_to_datetime(&mut ohlcv);

        println!("{:?}", ohlcv);
    }

    #[test]
    fn test_ohlcvv() {
        let mut trade_buffer = TradeBuffer::new();

        for i in 0..1000000 {
            let side = i%2 == 0;

            trade_buffer.push(i * 1_00, side, (i * 2) as f64, (i * 3) as f64);
        }

        let df = trade_buffer.to_dataframe();

        println!("{:}", df);

        let mut ohlcv = ohlcvv_df(&df, 123, 0, 10).unwrap();

        println!("{:?}", ohlcv);

        convert_timems_to_datetime(&mut ohlcv);

        println!("{:?}", ohlcv);
    }

}

// Copyright(c) 2022-2023. yasstake. All rights reserved.

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
use polars::prelude::SortOptions;

use polars::lazy::frame::pivot::pivot;
use polars::lazy::prelude::{col, LazyFrame};
use polars::lazy::prelude::IntoLazy;
use polars::time::ClosedWindow;

#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
pub mod KEY {
    pub const time_stamp: &str = "timestamp";

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
        mask = df.column(KEY::time_stamp).unwrap().lt(end_time).unwrap();
    } else if end_time == 0 {
        mask = df
            .column(KEY::time_stamp)
            .unwrap()
            .gt_eq(start_time)
            .unwrap();
    } else {
        mask = df
            .column(KEY::time_stamp)
            .unwrap()
            .gt_eq(start_time)
            .unwrap()
            & df.column(KEY::time_stamp).unwrap().lt(end_time).unwrap();
    }

    let df = df.filter(&mask).unwrap();

    return df;
}

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
        df = df.filter(col(KEY::time_stamp).lt(end_time));
    } else if end_time == 0 {
        df = df.filter(col(KEY::time_stamp).gt_eq(start_time));
    } else {
        df = df.filter(
            col(KEY::time_stamp)
                .gt_eq(start_time)
                .and(col(KEY::time_stamp).lt(end_time)),
        );
    }

    return df;
}

pub fn start_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().min().unwrap()
}

pub fn end_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().max().unwrap()
}

pub fn merge_df(df1: &DataFrame, df2: &DataFrame) -> DataFrame {
    let df2_start_time = start_time_df(df2);

    if let Some(df2_start_time) = df2_start_time {
        let df = select_df(df1, 0, df2_start_time);

        log::debug!("merge len {}", df.shape().0);

        if df.shape().0 == 0 {
            df2.clone()
        } else {
            log::debug!("merge df1={:?}  df2={:?}", df1.shape(), df2.shape());

            let df = df.vstack(df2);
            
            if df.is_err() {
                log::error!("merge_df error {:?} and {:?}", df, df2);
                df2.clone()
            } else {
                df.unwrap()
            }
        }
    } else {
        df1.clone()
    }
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
        index_column: KEY::time_stamp.into(),
        every: Duration::new(SEC(time_window)), // グループ間隔
        period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
        offset: Duration::parse("0m"),
        // truncate: true,                    // タイムスタンプを切り下げてまとめる。
        include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
        closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
        start_by: StartBy::DataPoint,
        check_sorted: false,
        ..Default::default()
    };

    let df = select_df_lazy(df, start_time, end_time);

    let result = df
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
                maintain_order: true,
                multithreaded: true,
            },
        )
        .group_by_dynamic(col(KEY::time_stamp), [], option)
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::volume),
            col(KEY::price).count().alias(KEY::count),
        ])
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
        index_column: KEY::time_stamp.into(),
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
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
                maintain_order: true,
                multithreaded: true,
            },
        )
        .group_by_dynamic(col(KEY::time_stamp), [col(KEY::order_side)], option)
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::volume),
            col(KEY::price).count().alias(KEY::count),
            col(KEY::time_stamp).min().alias(KEY::start_time),
            col(KEY::time_stamp).max().alias(KEY::end_time),
        ])
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
        index_column: KEY::time_stamp.into(),
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
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
                maintain_order: true,
                multithreaded: true,
            },
        )
        .group_by_dynamic(col(KEY::time_stamp), [], option)
        .agg([
            col(KEY::open)
                .sort_by([KEY::start_time], [false])
                .first()
                .alias(KEY::open),
            col(KEY::high).max().alias(KEY::high),
            col(KEY::low).min().alias(KEY::low),
            col(KEY::close)
                .sort_by([KEY::end_time], [false])
                .last()
                .alias(KEY::close),
            col(KEY::volume).sum().alias(KEY::volume),
            col(KEY::count).sum().alias(KEY::count),
        ])
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
        index_column: KEY::time_stamp.into(),
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
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
                maintain_order: true,
                multithreaded: true,
            },
        )
        .group_by_dynamic(col(KEY::time_stamp), [col(KEY::order_side)], option)
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
pub fn vap_df_bak(df: &DataFrame, start_time: MicroSec, end_time: MicroSec) -> DataFrame {
    let df = select_df_lazy(df, start_time, end_time);

    let vap = df
        .group_by([KEY::price, KEY::order_side])
        .agg([col(KEY::size).sum().alias(KEY::size)])
        .collect()
        .unwrap();

    vap
}


/// Calc Value At Price
/// group by unit price and order_side
pub fn vap_df_bak2(df: &DataFrame, start_time: MicroSec, end_time: MicroSec) -> DataFrame {
    let df = select_df_lazy(df, start_time, end_time);

    //    let floor_price = col(KEY::price)
    //    .map(|v: f64| (v / 10.0).floor() * 10.0, Arc::new(DataType::Float64) as Arc<dyn FunctionOutputField>);

    //.div(10.0).alias("floor_price").cast(&DataType::Float64);

    //    let floor_price =
    //col(KEY::price).div(10.0).alias("floor_price").cast(&DataType::Float64);

    let floor_price = col(KEY::price).floor();

    let vap_gb = df.group_by([floor_price, col(KEY::order_side)]);

    let vap = vap_gb
        .agg([col(KEY::size).sum().alias(KEY::size)])
        .collect()
        .unwrap();

    vap
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
        [KEY::volume],
        [KEY::price],
        [KEY::order_side],
        false,
        //Some(col(KEY::volume).sum()),
        None,
        None,
    )
    .unwrap();

    let vap = vap.sort([KEY::price], false, true).unwrap();    

    let price = vap.column(KEY::price).unwrap().clone();
    let buy_vol = vap.column("true").unwrap().fill_null(FillNullStrategy::Zero).unwrap().rename(KEY::buy_volume).clone();
    let sell_vol = vap.column("false").unwrap().fill_null(FillNullStrategy::Zero).unwrap().rename(KEY::sell_volume).clone();
    let total_vol = (&buy_vol + &sell_vol).rename(KEY::volume).clone();

    let df = DataFrame::new(vec![price, buy_vol, sell_vol, total_vol]).unwrap();

    df
}

pub struct TradeBuffer {
    pub time_stamp: Vec<MicroSec>,
    pub price: Vec<f64>,
    pub size: Vec<f64>,
    pub order_side: Vec<bool>,
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
        let time_stamp = Series::new(KEY::time_stamp, self.time_stamp.to_vec());
        let price = Series::new(KEY::price, self.price.to_vec());
        let size = Series::new(KEY::size, self.size.to_vec());
        let order_side = Series::new(KEY::order_side, self.order_side.to_vec());

        let df = DataFrame::new(vec![time_stamp, price, size, order_side]).unwrap();

        return df;
    }
}

pub fn make_empty_ohlcvv() -> DataFrame {
    let time = Series::new(KEY::time_stamp, Vec::<MicroSec>::new());
    let order_side = Series::new(KEY::order_side, Vec::<f64>::new());
    let open = Series::new(KEY::open, Vec::<f64>::new());
    let high = Series::new(KEY::high, Vec::<f64>::new());
    let low = Series::new(KEY::low, Vec::<f64>::new());
    let close = Series::new(KEY::close, Vec::<f64>::new());
    let vol = Series::new(KEY::volume, Vec::<f64>::new());
    let count = Series::new(KEY::count, Vec::<f64>::new());
    let start_time = Series::new(KEY::start_time, Vec::<MicroSec>::new());
    let end_time = Series::new(KEY::end_time, Vec::<MicroSec>::new());

    let df = DataFrame::new(vec![
        time, order_side, open, high, low, close, vol, count, start_time, end_time,
    ])
    .unwrap();

    return df;
}

pub fn make_empty_ohlcv() -> DataFrame {
    let time = Series::new(KEY::time_stamp, Vec::<MicroSec>::new());
    let open = Series::new(KEY::open, Vec::<f64>::new());
    let high = Series::new(KEY::high, Vec::<f64>::new());
    let low = Series::new(KEY::low, Vec::<f64>::new());
    let close = Series::new(KEY::close, Vec::<f64>::new());
    let vol = Series::new(KEY::volume, Vec::<f64>::new());
    let count = Series::new(KEY::count, Vec::<f64>::new());

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

pub fn convert_timems_to_datetime(df: &mut DataFrame) -> &DataFrame {
    let time = df.column(KEY::time_stamp).unwrap().i64().unwrap().clone();
    let date_time = time.into_datetime(TimeUnit::Microseconds, None);
    let df = df.with_column(date_time).unwrap();

    df
}

use polars::prelude::*;
use rust_decimal::prelude::ToPrimitive;

#[cfg(test)]
mod test_df {
    use super::*;
    use crate::common::DAYS;

    #[test]
    fn test_simple_dynamic_group() {
        let option = DynamicGroupOptions {
            every: Duration::new(DAYS(1)),
            index_column: "date".into(),
            check_sorted: true,
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
            .sort(
                "date",
                SortOptions {
                    descending: false,
                    nulls_last: false,
                    multithreaded: true,
                    maintain_order: false,
                },
            )
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
            .collect()
            .unwrap();

        println!("{:?}", groupby);
        let df = make_empty_ohlcv();
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 7);
    }

    fn make_ohlcv_df() -> DataFrame {
        let df = df!(
            KEY::time_stamp => &[DAYS(1), DAYS(2), DAYS(3)],
            KEY::order_side => &[0, 1, 0],
            KEY::open => &[1.0, 2.0, 3.0],
            KEY::high => &[1.0, 2.0, 3.0],
            KEY::low => &[1.0, 2.0, 3.0],
            KEY::close => &[1.0, 2.0, 3.0],
            KEY::volume => &[1.0, 2.0, 3.0],
            KEY::count => &[1.0, 2.0, 3.0],
            KEY::start_time => &[DAYS(1), DAYS(2), DAYS(3)],
            KEY::end_time => &[DAYS(1), DAYS(2), DAYS(3)]
        );

        return df.unwrap().sort([KEY::time_stamp], false, true).unwrap();
    }

    #[test]
    fn test_make_ohlcv() {
        let ohlc = make_ohlcv_df();

        println!("{:?}", ohlc);
    }

    #[test]
    fn test_make_ohlcv_from_ohclv() {
        let ohlcv = make_ohlcv_df();

        let ohlcv2 = ohlcvv_from_ohlcvv_df(&ohlcv, 0, 0, 10);
        println!("{:?}", ohlcv2);
    }

    #[test]
    fn test_make_ohlc_from_ohclv() {
        let ohlcv = make_ohlcv_df();

        let ohlcv2 = ohlcv_from_ohlcvv_df(&ohlcv, 0, 0, 10);
        println!("{:?}", ohlcv2);
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
    fn test_make_ohlc_from_empty_ohlcv() -> anyhow::Result<()>{
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


        let time = Series::new(KEY::time_stamp, Vec::<MicroSec>::new());

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
    fn test_convert_ohlc_datetime() {
        let mut df = make_empty_ohlcv();

        println!("{:?}", df);

        let time = df.column("time_stamp").unwrap().i64().unwrap().clone();
        let date_time = time.into_datetime(TimeUnit::Microseconds, None);
        let df = df.with_column(date_time).unwrap();

        println!("{:?}", df);
    }
}

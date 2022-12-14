use crate::common::order::Trade;
use crate::common::time::{time_string, MicroSec, SEC};
use polars::prelude::BooleanType;
use polars::prelude::ChunkCompare;
use polars::prelude::ChunkedArray;
use polars::prelude::DataFrame;
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use polars_core::prelude::SortOptions;
use polars_lazy::prelude::col;
use polars_lazy::prelude::IntoLazy;
use polars_time::ClosedWindow;
// Copyright(c) 2022. yasstake. All rights reserved.

#[allow(non_upper_case_globals)]
#[allow(non_snake_case)]
pub mod KEY {
    pub const time_stamp: &str = "time_stamp";

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
    pub const vol: &str = "vol";
    #[allow(unused)]
    pub const sell_vol: &str = "sell_vol";
    #[allow(unused)]
    pub const sell_count: &str = "sell_count";
    #[allow(unused)]
    pub const buy_vol: &str = "buy_vol";
    #[allow(unused)]
    pub const buy_count: &str = "buy_count";
    pub const start_time: &str = "start_time";
    pub const end_time: &str = "end_time";
    pub const count: &str = "count";
}

/// Cutoff from_time to to_time(not include)
pub fn select_df(df: &DataFrame, from_time: MicroSec, to_time: MicroSec) -> DataFrame {
    log::debug!(
        "Select from {} -> {}",
        time_string(from_time),
        time_string(to_time)
    );
    if from_time == 0 && to_time == 0 {
        log::debug!("preserve select df");
        return df.clone();
    }

    let mask: ChunkedArray<BooleanType>;

    if from_time == 0 {
        mask = df.column(KEY::time_stamp).unwrap().lt(to_time).unwrap();
    } else if to_time == 0 {
        mask = df
            .column(KEY::time_stamp)
            .unwrap()
            .gt_eq(from_time)
            .unwrap();
    } else {
        mask = df
            .column(KEY::time_stamp)
            .unwrap()
            .gt_eq(from_time)
            .unwrap()
            & df.column(KEY::time_stamp).unwrap().lt(to_time).unwrap();
    }

    let df = df.filter(&mask).unwrap();

    return df;
}

pub fn start_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().min()
}

pub fn end_time_df(df: &DataFrame) -> Option<MicroSec> {
    df.column(KEY::time_stamp).unwrap().max()
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
            df.vstack(df2).unwrap()
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
) -> DataFrame {
    log::debug!(
        "ohlcv_df, from={} / to={}",
        time_string(start_time),
        time_string(end_time)
    );
    let df = select_df(df, start_time, end_time);

    if df.shape().0 == 0 {
        log::debug!("empty ohlc");
        return make_empty_ohlcv();
    }

    let result = df
        .lazy()
        .groupby_dynamic(
            [],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // ??????????????????
                period: Duration::new(SEC(time_window)), // ??????????????????????????????????????????????????????OK)
                offset: Duration::parse("0m"),
                truncate: true,            // ??????????????????????????????????????????????????????
                include_boundaries: false, // ???????????????????????????????????????????????????????????????(false???OK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       ???????????????Window??????????????????????????????????????????(CloseWindow::Left)???
            },
        )
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::vol),
            col(KEY::price).count().alias(KEY::count),
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect();
        match result {
            Ok(dataframe) => return dataframe,
            Err(e) => {
                log::error!("Polars error {}", e.to_string());
                println!("Polars error {}", e.to_string());
                return make_empty_ohlcv();
            }
        }

}

pub fn ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    log::debug!(
        "ohlcv_df, from={} / to={}",
        time_string(start_time),
        time_string(end_time)
    );
    let df = select_df(df, start_time, end_time);

    if df.shape().0 == 0 {
        log::debug!("empty ohlcv");
        return make_empty_ohlcvv();
    }

    let result = df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // ??????????????????
                period: Duration::new(SEC(time_window)), // ??????????????????????????????????????????????????????OK)
                offset: Duration::parse("0m"),
                truncate: true,            // ??????????????????????????????????????????????????????
                include_boundaries: false, // ???????????????????????????????????????????????????????????????(false???OK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       ???????????????Window??????????????????????????????????????????(CloseWindow::Left)???
            },
        )
        .agg([
            col(KEY::price).first().alias(KEY::open),
            col(KEY::price).max().alias(KEY::high),
            col(KEY::price).min().alias(KEY::low),
            col(KEY::price).last().alias(KEY::close),
            col(KEY::size).sum().alias(KEY::vol),
            col(KEY::price).count().alias(KEY::count),
            col(KEY::time_stamp).min().alias(KEY::start_time),
            col(KEY::time_stamp).max().alias(KEY::end_time),
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect();

        match result {
            Ok(dataframe) => return dataframe,
            Err(e) => {
                log::error!("Polars error {}", e.to_string());
                println!("Polars error {}", e.to_string());
                return make_empty_ohlcvv();
            }
        }
    }

pub fn ohlcv_from_ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    log::debug!(
        "ohlc {:?} -> {:?}",
        time_string(start_time),
        time_string(end_time)
    );
    let df = select_df(df, start_time, end_time);

    if df.shape().0 == 0 {
        log::debug!("empty ohlc");
        return make_empty_ohlcv();
    }

    let result = df
        .lazy()
        .groupby_dynamic(
            [],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // ??????????????????
                period: Duration::new(SEC(time_window)), // ??????????????????????????????????????????????????????OK)
                offset: Duration::parse("0m"),
                truncate: true,            // ??????????????????????????????????????????????????????
                include_boundaries: false, // ???????????????????????????????????????????????????????????????(false???OK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       ???????????????Window??????????????????????????????????????????(CloseWindow::Left)???
            },
        )
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
            col(KEY::vol).sum().alias(KEY::vol),
            col(KEY::count).sum().alias(KEY::count),
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return dataframe,
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return make_empty_ohlcv();
        }
    }
}

pub fn ohlcvv_from_ohlcvv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    log::debug!(
        "ohlc {:?} -> {:?}",
        time_string(start_time),
        time_string(end_time)
    );
    let df = select_df(df, start_time, end_time);

    let result = df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // ??????????????????
                period: Duration::new(SEC(time_window)), // ??????????????????????????????????????????????????????OK)
                offset: Duration::parse("0m"),
                truncate: true,            // ??????????????????????????????????????????????????????
                include_boundaries: false, // ???????????????????????????????????????????????????????????????(false???OK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       ???????????????Window??????????????????????????????????????????(CloseWindow::Left)???
            },
        )
        .agg([
            col(KEY::open).first().alias(KEY::open),
            col(KEY::high).max().alias(KEY::high),
            col(KEY::low).min().alias(KEY::low),
            col(KEY::close).last().alias(KEY::close),
            col(KEY::vol).sum().alias(KEY::vol),
            col(KEY::count).sum().alias(KEY::count),
            col(KEY::start_time).min().alias(KEY::start_time),
            col(KEY::end_time).max().alias(KEY::end_time),
        ])
        .sort(
            KEY::time_stamp,
            SortOptions {
                descending: false,
                nulls_last: false,
            },
        )
        .collect();

    match result {
        Ok(dataframe) => return dataframe,
        Err(e) => {
            log::error!("Polars error {}", e.to_string());
            println!("Polars error {}", e.to_string());
            return make_empty_ohlcvv();
        }
    }
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
        self.price.push(trade.price);
        self.size.push(trade.size);
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
    let vol = Series::new(KEY::vol, Vec::<f64>::new());
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
    let vol = Series::new(KEY::vol, Vec::<f64>::new());
    let count = Series::new(KEY::count, Vec::<f64>::new());

    let df = DataFrame::new(vec![time, open, high, low, close, vol, count]).unwrap();

    return df;
}

#[cfg(test)]
mod test_df {
    use super::*;
    use crate::common::time::DAYS;
    use polars::prelude::*;

    fn make_ohlcv_df() -> DataFrame {
        let df = df!(
            KEY::time_stamp => &[DAYS(1), DAYS(2), DAYS(3)],
            KEY::order_side => &[0, 1, 0],
            KEY::open => &[1.0, 2.0, 3.0],
            KEY::high => &[1.0, 2.0, 3.0],
            KEY::low => &[1.0, 2.0, 3.0],
            KEY::close => &[1.0, 2.0, 3.0],
            KEY::vol => &[1.0, 2.0, 3.0],
            KEY::count => &[1.0, 2.0, 3.0],
            KEY::start_time => &[DAYS(1), DAYS(2), DAYS(3)],
            KEY::end_time => &[DAYS(1), DAYS(2), DAYS(3)]
        );

        return df.unwrap();
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
    fn test_make_ohlcv_from_empty_ohlcv() {
        let r = make_empty_ohlcvv();
        let r2 = ohlcvv_df(&r, 0, 0, 10);
        println!("{:?}", r2);
        assert_eq!(r2.shape(), (0, 10));
    }

    #[test]
    fn test_make_ohlc_from_empty_ohlcv() {
        let r = make_empty_ohlcvv();
        let r2 = ohlcv_df(&r, 0, 0, 10);
        println!("{:?}", r2);
        assert_eq!(r2.shape(), (0, 7));
    }

    #[test]
    fn test_make_ohlcv_from_ohlcv_empty_ohlcv() {
        let r = make_empty_ohlcvv();
        println!("{:?}", r);
        let r2 = ohlcvv_df(&r, 0, 0, 10);
        println!("{:?}", r2);
        let r3 = ohlcv_from_ohlcvv_df(&r2, 0, 0, 10);
        println!("{:?}", r3);
    }
}

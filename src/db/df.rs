use crate::common::order::Trade;
use crate::common::time::{MicroSec, SEC, time_string};
use polars::prelude::ChunkCompare;
use polars::prelude::DataFrame;
use polars::prelude::Duration;
use polars::prelude::DynamicGroupOptions;
use polars::prelude::NamedFrom;
use polars::prelude::Series;
use polars::prelude::BooleanType;
use polars::prelude::ChunkedArray;
use polars_core::prelude::SortOptions;
use polars_lazy::prelude::IntoLazy;
use polars_lazy::prelude::col;
use polars_time::ClosedWindow;


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
    log::debug!("Select from {} -> {}", time_string(from_time), time_string(to_time));
    if from_time == 0 && to_time == 0 {
        log::debug!("preserve select df");
        return df.clone();
    }

    let mask: ChunkedArray<BooleanType>;

    if from_time == 0 {
        mask = df.column(KEY::time_stamp).unwrap().lt(to_time).unwrap();
    }
    else if to_time == 0 {
        mask = df.column(KEY::time_stamp).unwrap().gt_eq(from_time).unwrap();
    }
    else {
        mask = df.column(KEY::time_stamp).unwrap().gt_eq(from_time).unwrap()
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

    if df2_start_time.is_some() {
        let df = select_df(df1, 0, df2_start_time.unwrap());
        df.vstack(df2).unwrap()
    }
    else {
        df1.clone()
    }
}

pub fn ohlcv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    let df = select_df(df, start_time, end_time);

    return df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // グループ間隔
                period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
                offset: Duration::parse("0m"),
                truncate: true,            // タイムスタンプを切り下げてまとめる。
                include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
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
        .collect()
        .unwrap();
}


pub fn ohlcv_from_ohlcv_df(
    df: &DataFrame,
    start_time: MicroSec,
    end_time: MicroSec,
    time_window: i64,
) -> DataFrame {
    log::debug!("ohlc {:?} -> {:?}", time_string(start_time), time_string(end_time));
    let df = select_df(df, start_time, end_time);

    return df
        .lazy()
        .groupby_dynamic(
            [col(KEY::order_side)],
            DynamicGroupOptions {
                index_column: KEY::time_stamp.into(),
                every: Duration::new(SEC(time_window)), // グループ間隔
                period: Duration::new(SEC(time_window)), // データ取得の幅（グループ間隔と同じでOK)
                offset: Duration::parse("0m"),
                truncate: true,            // タイムスタンプを切り下げてまとめる。
                include_boundaries: false, // データの下限と上限を結果に含めるかどうか？(falseでOK)
                closed_window: ClosedWindow::Left, // t <=  x  < t+1       開始時間はWindowに含まれる。終了は含まれない(CloseWindow::Left)。
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
        .collect()
        .unwrap();
}

/*
///
/// SQL DBには、Tickデータを保存する。
/// インメモリーDBとしてPolarsを利用し、１秒足のOHLCV拡張を保存する。
/// 定期的に最新の情報をメモリに更新する（時間（秒）がインデックスキーとして上書きする）
pub struct OhlcvBuffer {
    pub time: Vec<f64>,
    pub open: Vec<f64>,
    pub high: Vec<f64>,
    pub low: Vec<f64>,
    pub close: Vec<f64>,
    pub vol: Vec<f64>,
    pub sell_vol: Vec<f64>,
    pub sell_count: Vec<f64>,
    pub buy_vol: Vec<f64>,
    pub buy_count: Vec<f64>,
    pub start_time: Vec<f64>,
    pub end_time: Vec<f64>,
}


impl OhlcvBuffer {
    pub fn new() -> Self {
        return OhlcvBuffer {
            time: Vec::new(),
            open: Vec::new(),
            high: Vec::new(),
            low: Vec::new(),
            close: Vec::new(),
            vol: Vec::new(),
            sell_vol: Vec::new(),
            sell_count: Vec::new(),
            buy_vol: Vec::new(),
            buy_count: Vec::new(),
            start_time: Vec::new(),
            end_time: Vec::new(),
        };
    }

    pub fn clear(&mut self) {
        self.time.clear();
        self.open.clear();
        self.high.clear();
        self.low.clear();
        self.close.clear();
        self.vol.clear();
        self.sell_vol.clear();
        self.sell_count.clear();
        self.buy_vol.clear();
        self.buy_count.clear();
        self.start_time.clear();
        self.end_time.clear();
    }

    pub fn push_trades(&mut self, trades: Vec<Ohlcvv>) {
        for trade in trades {
            self.push_trade(&trade);
        }
    }

    pub fn push_trade(&mut self, trade: &Ohlcvv) {
        self.time.push(trade.time);
        self.open.push(trade.open);
        self.high.push(trade.high);
        self.low.push(trade.low);
        self.close.push(trade.close);
        self.vol.push(trade.vol);
        self.sell_vol.push(trade.sell_vol);
        self.sell_count.push(trade.sell_count);
        self.buy_vol.push(trade.buy_vol);
        self.buy_count.push(trade.buy_count);
        self.start_time.push(trade.start_time);
        self.end_time.push(trade.end_time);
    }


    pub fn to_dataframe(&self) -> DataFrame {
        let time = Series::new(KEY::time_stamp, self.time.to_vec());
        let open = Series::new(KEY::open, self.open.to_vec());
        let high = Series::new(KEY::high, self.high.to_vec());
        let low = Series::new(KEY::low, self.low.to_vec());
        let close = Series::new(KEY::close, self.close.to_vec());
        let vol = Series::new(KEY::vol, self.vol.to_vec());
        let sell_vol = Series::new(KEY::sell_vol, self.sell_vol.to_vec());
        let sell_count = Series::new(KEY::sell_count, self.sell_count.to_vec());
        let buy_vol = Series::new(KEY::buy_vol, self.buy_vol.to_vec());
        let buy_count = Series::new(KEY::buy_count, self.buy_count.to_vec());
        let start_time = Series::new(KEY::start_time, self.start_time.to_vec());
        let end_time = Series::new(KEY::end_time, self.end_time.to_vec());

        let df = DataFrame::new(vec![
            time, open, high, low, close, vol, sell_vol, sell_count, buy_vol, buy_count,
            start_time, end_time,
        ])
        .unwrap();

        return df;
    }
}
*/
/*
/// Ohlcvのdfを内部にキャッシュとしてもつDataFrameクラス。
/// ・　生DFのマージ（あたらしいdfの期間分のデータを削除してから追加。重複がないようにマージする。
/// ・　OHLCVの生成
pub struct OhlcvDataFrame {
    df: DataFrame,
}

impl OhlcvDataFrame {
    /*
    // TODO: DFの最初の時間とおわりの時間を取得する。
    pub fn start_time() {

    }

    pub fn end_time() {

    }
    */

    //TODO: カラム名の変更
    pub fn select(&self, mut start_time_ms: i64, mut end_time_ms: i64) -> Self {
        if end_time_ms == 0 {
            end_time_ms = self.df.column("timestamp").unwrap().max().unwrap();
        }

        let mask = self
            .df
            .column("timestamp")
            .unwrap()
            .gt(start_time_ms)
            .unwrap();
        self.df
            .column("timestamp")
            .unwrap()
            .lt_eq(end_time_ms)
            .unwrap();

        let df = self.df.filter(&mask).unwrap();

        return OhlcvDataFrame { df };
    }
}
*/

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


pub fn make_empty_ohlcv() -> DataFrame {
    let time = Series::new(KEY::time_stamp, Vec::<MicroSec>::new());
    let open = Series::new(KEY::open, Vec::<f64>::new());
    let high = Series::new(KEY::high, Vec::<f64>::new());
    let low = Series::new(KEY::low, Vec::<f64>::new());
    let close = Series::new(KEY::close, Vec::<f64>::new());
    let vol = Series::new(KEY::vol, Vec::<f64>::new());
    let sell_vol = Series::new(KEY::sell_vol, Vec::<f64>::new());
    let sell_count = Series::new(KEY::sell_count, Vec::<f64>::new());
    let buy_vol = Series::new(KEY::buy_vol, Vec::<f64>::new());
    let buy_count = Series::new(KEY::buy_count, Vec::<f64>::new());
    let start_time = Series::new(KEY::start_time, Vec::<MicroSec>::new());
    let end_time = Series::new(KEY::end_time, Vec::<MicroSec>::new());

    let df = DataFrame::new(vec![
        time, open, high, low, close, vol, sell_vol, sell_count, buy_vol, buy_count,
        start_time, end_time,
    ])
    .unwrap();

    return df;
}
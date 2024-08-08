// Copyright(c) 2024. yasstake. All rights reserved.

use crossbeam_channel::Sender;
use pyo3::IntoPy;
use pyo3::Py;
use pyo3::PyAny;
use pyo3::PyResult;
use pyo3::Python;
use rbot_lib::common::convert_klines_to_trades;
use rbot_lib::common::flush_log;
use rbot_lib::common::time_string;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::LogStatus;
use rbot_lib::common::MarketMessage;

use rbot_lib::common::MultiMarketMessage;
use rbot_lib::common::ServerConfig;
use rbot_lib::common::FLOOR_SEC;
use rbot_lib::common::MICRO_SECOND;
use rbot_lib::db::convert_timems_to_datetime;
use rbot_lib::db::TradeDataFrame;
use rbot_lib::db::TradeDb;
use rbot_lib::net::BroadcastMessage;
use rbot_lib::net::RestPage;
use rbot_lib::net::WebSocketClient;
use rust_decimal_macros::dec;
use std::sync::{Arc, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::StreamExt as _;

use pyo3_polars::PyDataFrame;
use rbot_lib::common::BoardItem;
use rbot_lib::common::OrderBook;
use rbot_lib::net::RestApi;
use rust_decimal::Decimal;

use anyhow::anyhow;
#[allow(unused_imports)]
use anyhow::Context;

use rbot_lib::{
    common::{
        AccountPair, MarketConfig, MarketStream, MicroSec, Order, OrderSide, OrderType, Trade,
        MARKET_HUB, NOW,
    },
    db::df::KEY,
};

macro_rules! check_if_enable_order {
    ($s: expr) => {
        if !$s.get_enable_order_feature() {
            log::error!("Order feature is disabled.");
            return Err(anyhow!("Order feature is disabled, you can enable exchange property 'enable_order_with_my_own_risk' to True"));
        }
    };
}

pub trait OrderInterface {
    fn set_enable_order_feature(&mut self, enable_order: bool);
    fn get_enable_order_feature(&self) -> bool;

    //------ REST API ----
    fn limit_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>>;

    fn market_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>>;
    fn dry_market_order(
        &self,
        market_config: &MarketConfig,
        create_time: MicroSec,
        order_id: &str,
        client_order_id: &str,
        side: OrderSide,
        size: Decimal,
        transaction_id: &str,
    ) -> Vec<Order>;
    fn cancel_order(&self, market_config: &MarketConfig, order_id: &str) -> anyhow::Result<Order>;
    fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>>;
    fn get_account(&self, market_config: &MarketConfig) -> anyhow::Result<AccountPair>;
}

pub trait OrderInterfaceImpl<T>
where
    T: RestApi,
{
    fn get_restapi(&self) -> &T;

    fn set_enable_order_feature(&mut self, enable_order: bool);
    fn get_enable_order_feature(&self) -> bool;

    async fn make_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let order_side = OrderSide::from(side);

        let api = self.get_restapi();
        api.new_order(
            &market_config,
            order_side,
            price,
            size,
            order_type,
            client_order_id,
        )
        .await
    }

    //------ REST API ----
    async fn limit_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        check_if_enable_order!(self);
        let price = market_config.round_price(price)?;
        let size = market_config.round_size(size)?;

        self.make_order(
            &market_config,
            side,
            price,
            size,
            OrderType::Limit,
            client_order_id,
        )
        .await
    }

    async fn market_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        check_if_enable_order!(self);
        let size = market_config.round_size(size)?;

        self.make_order(
            market_config,
            side,
            dec![0.0],
            size,
            OrderType::Market,
            client_order_id,
        )
        .await
    }

    async fn cancel_order(
        &self,
        market_config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order> {
        check_if_enable_order!(self);

        let api = self.get_restapi();

        api.cancel_order(market_config, order_id)
            .await
            .with_context(|| {
                format!(
                    "Error in cancel_order: {:?} {:?}",
                    &market_config, &order_id
                )
            })
    }

    async fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        let api = self.get_restapi();

        api.open_orders(market_config).await
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        let api = self.get_restapi();

        api.get_account().await
    }

    async fn async_start_user_stream(&mut self) -> anyhow::Result<()>;
}

pub trait MarketInterface {
    // --- GET CONFIG INFO ----
    fn get_production(&self) -> bool;
    fn get_config(&self) -> MarketConfig;

    fn get_market_config(&self) -> MarketConfig;

    fn set_broadcast_message(&mut self, broadcast_message: bool);
    fn get_broadcast_message(&self) -> bool;

    // --- DB ---->>
    fn drop_table(&mut self) -> anyhow::Result<()>;
    fn get_cache_duration(&self) -> MicroSec;
    fn reset_cache_duration(&mut self);
    fn stop_db_thread(&mut self);
    fn cache_all_data(&mut self);
    fn get_archive_info(&self) -> (MicroSec, MicroSec);

    fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame>;
    fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame>;
    fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame>;
    fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> anyhow::Result<PyDataFrame>;
    fn info(&mut self) -> String;
    fn get_board_json(&self, size: usize) -> anyhow::Result<String>;
    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)>;
    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)>;
    fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)>;
    fn get_running(&self) -> bool;
    fn vacuum(&self);
    fn get_file_name(&self) -> String; // get db file path
                                       //<<----------------- DB

    fn _repr_html_(&self) -> String;

    fn download(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        archive_only: bool,
        low_priority: bool,
    ) -> i64;
    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64>;
    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64>;
    fn expire_unfix_data(&mut self) -> anyhow::Result<()>;

    fn start_market_stream(&mut self);

    fn open_realtime_channel(&mut self) -> anyhow::Result<MarketStream>;
    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<MarketStream>;
}

pub trait MarketImpl<T>
where
    T: RestApi,
{
    // --- GET CONFIG INFO ----
    fn get_restapi(&self) -> &T;
    fn get_config(&self) -> MarketConfig;

    fn get_db(&self) -> Arc<Mutex<TradeDataFrame>>;

    fn get_history_web_base_url(&self) -> String;

    async fn async_start_market_stream(&mut self) -> anyhow::Result<()>;

    fn db_start_up_rec(&self) -> PyResult<Py<PyAny>> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        let trade = lock.db_start_up_rec();
        Python::with_gil(|py| {
            if let Some(trade) = trade {
                Ok(trade.into_py(py))
            } else {
                Ok(Python::None(py))
            }
        })
    }

    fn latest_db_rec(&self, search_before: MicroSec) -> anyhow::Result<Trade> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        let trade = lock.latest_db_rec(search_before);

        if trade.is_some() {
            return Ok(trade.unwrap());
        }

        Err(anyhow!(
            "no record from {:?}({:?})",
            time_string(search_before),
            search_before
        ))
    }

    fn cache_all_data(&mut self) -> anyhow::Result<()> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.update_cache_all()
    }

    fn get_archive_info(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        let start_time = lock.get_archive_start_time();
        let end_time = lock.get_archive_end_time();

        Ok((start_time, end_time))
    }

    fn get_db_info(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        let db = self.get_db();
        let lock = db.lock().unwrap();

        let start_time = lock.get_db_start_time(0);
        let end_time = lock.get_db_end_time(0);

        Ok((start_time, end_time))
    }

    fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        let mut df = lock.fetch_cache_df(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;

        Ok(PyDataFrame(df))
    }

    fn select_db_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        let mut df = lock.fetch_db_df(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;

        Ok(PyDataFrame(df))
    }

    fn select_archive_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        let mut df = lock.fetch_archive_df(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;

        Ok(PyDataFrame(df))
    }

    fn select_cache_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        let mut df = lock.select_cache_df(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;

        Ok(PyDataFrame(df))
    }

    fn select_cache_ohlcv_df(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        let mut df = lock.select_cache_ohlcv_df(start_time, end_time)?;
        convert_timems_to_datetime(&mut df)?;

        Ok(PyDataFrame(df))
    }

    fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.py_ohlcvv_polars(start_time, end_time, window_sec)
    }

    fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.py_ohlcv_polars(start_time, end_time, window_sec)
    }

    fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.py_vap(start_time, end_time, price_unit)
    }

    fn start_time(&mut self) -> MicroSec {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock.start_time()
    }

    fn end_time(&mut self) -> MicroSec {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.end_time()
    }

    fn info(&mut self) -> String {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.info()
    }

    fn _repr_html_(&self) -> String {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock._repr_html_()
    }

    fn get_order_book(&self) -> Arc<RwLock<OrderBook>>;

    async fn async_get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        let orderbook = self.get_order_book();

        let (mut bids, mut asks) = {
            let lock = orderbook.read().unwrap();
            lock.get_board()?
        };

        if bids.shape().0 == 0 || asks.shape().0 == 0 {
            return Ok((PyDataFrame(bids), PyDataFrame(asks)));
        }

        let bids_edge: f64 = bids
            .column(KEY::price)
            .unwrap()
            .max()
            .unwrap()
            .unwrap_or(0.0);
        let asks_edge: f64 = asks
            .column(KEY::price)
            .unwrap()
            .min()
            .unwrap()
            .unwrap_or(0.0);

        if asks_edge < bids_edge || bids_edge == 0.0 || asks_edge == 0.0 {
            log::warn!("bids_edge({}) < asks_edge({})", bids_edge, asks_edge);

            self.async_refresh_order_book().await?;

            let orderbook = self.get_order_book();

            (bids, asks) = {
                let lock = orderbook.read().unwrap();

                lock.get_board()
            }
            .with_context(|| "Error in get_board")?;
        }

        return Ok((PyDataFrame(bids), PyDataFrame(asks)));
    }

    fn get_board_json(&self, size: usize) -> anyhow::Result<String> {
        let orderbook = self.get_order_book();

        let json = {
            let lock = orderbook.read();
            if lock.is_err() {
                return Err(anyhow!("Error get lock in get_board_json {:?}", lock));
            }
            let lock = lock.unwrap();
            lock.get_json(size)?
        };

        Ok(json)
    }

    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        let orderbook = self.get_order_book();

        let (bids, asks) = {
            let lock = orderbook.read().unwrap();
            lock.get_board_vec()?
        };

        Ok((bids, asks))
    }

    async fn async_get_edge_price(&mut self) -> anyhow::Result<(Decimal, Decimal)> {
        let orderbook = self.get_order_book();

        let mut edge_price = {
            let lock = orderbook.read().unwrap();
            lock.get_edge_price()
        };

        if edge_price.is_err() {
            self.async_refresh_order_book().await?;
            let lock = orderbook.read().unwrap();
            edge_price = lock.get_edge_price();
        }

        Ok(edge_price.unwrap())
    }

    fn open_db_channel(&mut self) -> anyhow::Result<Sender<Vec<Trade>>> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        lock.open_channel()
    }

    async fn async_download_recent_trades(
        &self,
        market_config: &MarketConfig,
    ) -> anyhow::Result<Vec<Trade>> {
        self.get_restapi().get_recent_trades(market_config).await
    }

    /// open back test channel
    /// returns:
    ///     actual date to start
    ///     actual date to end.
    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<(MicroSec, MicroSec, MarketStream)> {
        let (sender, market_stream) = MarketStream::open();

        let mut archive = {
            let db = self.get_db();
            let trade_dataframe = db.lock().unwrap();
            trade_dataframe.get_archive()
        };

        let dates = archive.select_dates(time_from, time_to)?;

        let actual_start = dates[0];
        let actual_end = dates[dates.len() - 1];

        std::thread::spawn(move || {
            let result = archive.foreach(time_from, time_to, &mut |trade| {
                let message: MarketMessage = trade.into();
                sender.send(message)?;

                Ok(())
            });

            if result.is_err() {
                log::error!("Error in select: {:?}", result.err().unwrap());
            }
        });

        return Ok((actual_start, actual_end, market_stream));
    }

    async fn async_download<U>(
        &mut self,
        ndays: i64,
        connect_ws: bool,
        force: bool,
        force_archive: bool,
        force_recent: bool,
        verbose: bool,
    ) -> anyhow::Result<()>
    where
        U: WebSocketClient + 'static,
    {
        log::debug!("download ndays={:?}, connect_ws={:?}, force={:?}, force_archive={:?}, force_recent={:?}, verbose={:?}",
                ndays, connect_ws, force, force_archive, force_recent, verbose
        );
        let force_recent = if force { true } else { force_recent };

        self.async_download_realtime::<U>(connect_ws, force_recent, verbose)
            .await?;

        let force_archive = if force { true } else { force_archive };
        self.async_download_archive(ndays, force_archive, verbose)
            .await?;

        Ok(())
    }

    async fn async_download_archive(
        &self,
        ndays: i64,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        let db = self.get_db();
        let api = self.get_restapi();
        let lock = db.lock();

        if lock.is_err() {
            log::error!("db get lock failure ");
            return Err(anyhow!("db get lock error"));
        }

        let mut lock = lock.unwrap();

        let count = lock.download_archive(api, ndays, force, verbose).await?;
        let archive_end = lock.get_archive_end_time();

        // delete old data from db.
        if archive_end != 0 {
            let expire =
                TradeDb::expire_control_message(0, archive_end + 1, true, "download archive");

            log::debug!("expire: {:?}", expire);

            let tx = lock.open_channel()?;
            tx.send(expire)?;
        }

        Ok(count)
    }

    async fn async_download_latest(&mut self, verbose: bool) -> anyhow::Result<(i64, i64)> {
        if verbose {
            println!("async_download_lastest");
            flush_log();
        }

        let api = self.get_restapi();
        let config = self.get_config().clone();

        let mut trades = api.get_recent_trades(&config).await?;
        trades.sort_by(|t1, t2| t1.time.cmp(&t2.time));
        let rec = trades.len() as i64;

        if rec == 0 {
            return Err(anyhow!("No data "));
        }

        trades[0].status = LogStatus::UnFixStart;

        if verbose {
            println!("from rec: {:?}", trades[0].__str__());
            println!("to   rec: {:?}", trades[(rec as usize) - 1].__str__());
            println!("rec: {}", rec);
            flush_log();
        }
        let tx = self.open_db_channel()?;

        let start_time = trades[0].time;
        let end_time = trades[(rec - 1) as usize].time;

        let expire_message =
            TradeDb::expire_control_message(start_time, end_time, false, "before download_latest");

        tx.send(expire_message)?;

        tx.send(trades)?;

        Ok((start_time, end_time))
    }

    async fn async_download_realtime<U>(
        &mut self,
        connect_ws: bool,
        force: bool,
        verbose: bool,
    ) -> anyhow::Result<()>
    where
        U: WebSocketClient + 'static,
    {
        if connect_ws {
            if verbose {
                println!("connect ws");
            }
            self.async_start_market_stream().await?;
            log::info!("start public ws");
        }

        let (start_time, _end_time) = self.async_download_latest(verbose).await?;
        log::info!(
            "download recent {:?}->{:?} [force={:?}]",
            time_string(start_time),
            time_string(_end_time),
            force
        );

        let rec = self.latest_db_rec(start_time);

        let range_from = if force {
            log::info!("force download");
            0 // from begining
        } else if rec.is_err() {
            log::info!("repave all");
            0 // from begining
        } else {
            let t = rec.unwrap().time;
            log::info!("download from {:?}", time_string(t));

            t
        };

        self.async_download_range_virtual(range_from, start_time, verbose)
            .await?;

        Ok(())
    }

    fn calc_db_time(
        &self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<(MicroSec, MicroSec)> {
        let (_start_t, archive_end) = self.get_archive_info()?;

        let archive_end = if archive_end == 0 {
            TradeDataFrame::archive_end_default()
        } else {
            archive_end
        };

        let time_from = if time_from < archive_end {
            archive_end
        } else {
            time_from
        };

        let time_to = if time_to == 0 { NOW() } else { time_to };

        Ok((time_from, time_to))
    }

    async fn async_download_range_virtual(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        if verbose {
            println!(
                "download_range_virtual from={}({}) to={}({})",
                time_from,
                time_string(time_from),
                time_to,
                time_string(time_to)
            )
        }

        let tx = self.open_db_channel()?;
        let api = self.get_restapi();

        let (time_from, time_to) = self.calc_db_time(time_from, time_to)?;

        let time_to = FLOOR_SEC(time_to, api.klines_width());
        let expire_to = time_to + api.klines_width() * MICRO_SECOND;

        let expire_message = TradeDb::expire_control_message(
            time_from,
            expire_to,
            true,
            "before download_range_virtual",
        );

        tx.send(expire_message)?;

        let mut trade_page = RestPage::New;
        let mut rec = 0;

        loop {
            let (klines, page) = api
                .get_klines(&self.get_config(), time_from, time_to, &trade_page)
                .await?;

            let l = klines.len();

            if l == 0 {
                break;
            }

            let trades: Vec<Trade> = convert_klines_to_trades(klines, api.klines_width());

            if verbose {
                println!(
                    "download_range (loop) {}({}) {}({}) {}[rec]",
                    time_string(time_from),
                    time_from,
                    time_string(time_to),
                    time_to,
                    l
                );
            }
            let l = trades.len();

            rec += l as i64;
            tx.send(trades)?;

            if page == RestPage::Done {
                break;
            }
            trade_page = page;
        }

        Ok(rec)
    }

    async fn async_download_range(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        if verbose {
            println!(
                "download_range from={}({}) to={}({})",
                time_from,
                time_string(time_from),
                time_to,
                time_string(time_to)
            )
        }
        let (time_from, time_to) = self.calc_db_time(time_from, time_to)?;

        let expire_message =
            TradeDb::expire_control_message(time_from, time_to, true, "before download_gap");

        let tx = self.open_db_channel()?;
        tx.send(expire_message)?;

        let api = self.get_restapi();

        let mut trade_page = RestPage::New;
        let mut rec = 0;

        loop {
            let (trades, page) = api
                .get_trades(&self.get_config(), time_from, time_to, &trade_page)
                .await?;

            let l = trades.len();

            if l == 0 {
                break;
            }

            let start_time = trades[0].time;
            let end_time = trades[l - 1].time;

            if verbose {
                println!(
                    "download_range (loop) {}({}) {}({}) {}[rec]",
                    time_string(start_time),
                    start_time,
                    time_string(end_time),
                    end_time,
                    l
                );
            }

            rec += l as i64;
            tx.send(trades)?;

            if verbose {
                println!("rec: {}", rec);
                flush_log();
            }

            if page == RestPage::Done {
                break;
            }
            trade_page = page;
        }

        Ok(rec)
    }

    async fn async_refresh_order_book(&mut self) -> anyhow::Result<()> {
        let api = self.get_restapi();
        let config = self.get_config();
        let board = api.get_board_snapshot(&config).await?;

        let book = self.get_order_book();
        let mut lock = book.write().unwrap();
        lock.update(&board);

        Ok(())
    }
}

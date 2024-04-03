// Copyright(c) 2024. yasstake. All rights reserved.

use crossbeam_channel::Sender;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::MarketMessage;

use rust_decimal_macros::dec;
use std::sync::{Arc, Mutex, RwLock};

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
        flush_log, time_string, AccountPair, MarketConfig, MarketStream, MicroSec, Order,
        OrderSide, OrderType, ServerConfig, Trade, DAYS, NOW,
    },
    db::{df::KEY, sqlite::TradeTable},
};

macro_rules! check_if_enable_order {
    ($s: expr) => {
        if !$s.get_enable_order_feature() {
            log::error!("Order feature is disabled.");
            anyhow::bail!("Order feature is disabled, you can enable exchange property 'enable_order_with_my_own_risk' to True");
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

pub trait OrderInterfaceImpl<T, U>
where
    T: RestApi<U>,
    U: ServerConfig,
{
    fn set_enable_order_feature(&mut self, enable_order: bool);
    fn get_enable_order_feature(&self) -> bool;

    fn get_server_config(&self) -> &U;

    fn price_dp(&self, market_config: &MarketConfig, price: Decimal) -> Decimal {
        let price_scale = market_config.price_scale;
        let price_dp = price.round_dp(price_scale);
        price_dp
    }

    fn size_dp(&self, market_config: &MarketConfig, size: Decimal) -> Decimal {
        let size_scale = market_config.size_scale;
        let size_dp = size.round_dp(size_scale);
        size_dp
    }

    fn order_side(side: &str) -> OrderSide {
        OrderSide::from(side)
    }

    async fn make_order(
        &self,
        market_config: &MarketConfig,
        side: &str,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let price = self.price_dp(&market_config, price);
        let size = self.size_dp(&market_config, size);
        let order_side = Self::order_side(side);

        T::new_order(
            &self.get_server_config(),
            &market_config,
            order_side,
            price,
            size,
            order_type,
            client_order_id,
        )
        .await
        .with_context(|| {
            format!(
                "Error in make_order: {:?} {:?} {:?} {:?} {:?} {:?}",
                &market_config, &side, &price, &size, &order_type, &client_order_id
            )
        })
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

        T::cancel_order(&self.get_server_config(), &market_config, order_id)
            .await
            .with_context(|| {
                format!(
                    "Error in cancel_order: {:?} {:?}",
                    &market_config, &order_id
                )
            })
    }

    async fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        T::open_orders(&self.get_server_config(), market_config)
            .await
            .with_context(|| format!("Error in get_open_orders: {:?}", &market_config))
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        T::get_account(&self.get_server_config())
            .await
            .with_context(|| format!("Error in get_account"))
    }

    async fn async_start_user_stream(&mut self) -> anyhow::Result<()>;
}

pub trait MarketInterface {
    // --- GET CONFIG INFO ----
    fn get_production(&self) -> bool;

    fn get_config(&self) -> MarketConfig;
    fn get_exchange_name(&self) -> String;
    fn get_trade_category(&self) -> String;
    fn get_trade_symbol(&self) -> String;

    fn get_market_config(&self) -> MarketConfig;

    fn set_broadcast_message(&mut self, broadcast_message: bool);
    fn get_broadcast_message(&self) -> bool;

    // --- DB ---->>
    fn drop_table(&mut self) -> anyhow::Result<()>;
    fn get_cache_duration(&self) -> MicroSec;
    fn reset_cache_duration(&mut self);
    fn stop_db_thread(&mut self);
    fn cache_all_data(&mut self);
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

pub trait MarketImpl<T, U>
where
    T: RestApi<U>,
    U: ServerConfig,
{
    fn get_server_config(&self) -> U;
    // --- GET CONFIG INFO ----
    fn get_config(&self) -> MarketConfig;
    fn get_exchange_name(&self) -> String;
    fn get_trade_category(&self) -> String;
    fn get_trade_symbol(&self) -> String;

    /// Download historical log from REST API
    fn download_latest(&mut self, verbose: bool) -> anyhow::Result<i64>;

    fn download_archives(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64>;

    /// Download historical log from REST API, between the latest data and the latest FIX data.
    /// If the latest FIX data is not found, generate psudo data from klines.
    fn download_gap(&mut self, force: bool, verbose: bool) -> anyhow::Result<i64>;

    fn get_db(&self) -> Arc<Mutex<TradeTable>>;

    fn get_history_web_base_url(&self) -> String;

    /// Check if database is valid at the date
    fn validate_db_by_date(&mut self, date: MicroSec) -> anyhow::Result<bool> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        lock.validate_by_date(date)
    }

    fn find_latest_gap(&self, force: bool) -> anyhow::Result<(MicroSec, MicroSec)> {
        log::debug!("[start] find_latest_gap");
        let start_time = NOW() - DAYS(2);

        let db = self.get_db();

        let (fix_time, unfix_time) = {
            let mut lock = db.lock().unwrap();
            let fix_time = lock.latest_fix_time(start_time, force)?;
            if fix_time == 0 {
                return Err(anyhow!("No data found"));
            }

            let unfix_time = lock.first_unfix_time(fix_time)?;

            (fix_time, unfix_time)
        };

        Ok((fix_time, unfix_time))
    }

    fn find_latest_gap_trade(&self, force: bool) -> anyhow::Result<(Option<Trade>, Option<Trade>)> {
        log::debug!("[start] find_latest_gap_trade force={}", force);
        let start_time = NOW() - DAYS(2);

        let db = self.get_db();

        let mut lock = db.lock().unwrap();
        let latest_trade = lock.latest_fix_trade(start_time, force)?;

        if latest_trade.is_none() {
            log::debug!("No data found");
            return Ok((None, None));
        }

        let latest_trade_id = latest_trade.as_ref().unwrap().id.clone();
        let fix_time = latest_trade.as_ref().unwrap().time;

        let first_unfix_trade = lock.first_unfix_trade(fix_time)?;

        if first_unfix_trade.is_some() {
            let first_unfix_trade_id = first_unfix_trade.as_ref().unwrap().id.clone();

            if first_unfix_trade_id == latest_trade_id {
                log::debug!("first_unfix_trade_id <= latest_trade_id");
                return Ok((None, None));
            }
        }

        Ok((latest_trade, first_unfix_trade))
    }

    // --- DB ----
    fn drop_table(&mut self) -> anyhow::Result<()> {
        let db = self.get_db();
        let lock = db.lock().unwrap();

        lock.drop_table()?;

        Ok(())
    }

    fn get_cache_duration(&self) -> MicroSec {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock.get_cache_duration()
    }

    fn reset_cache_duration(&mut self) {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.reset_cache_duration();
    }

    fn cache_all_data(&mut self) -> anyhow::Result<()> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.update_cache_all()
    }

    fn select_trades(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<PyDataFrame> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.py_select_trades_polars(start_time, end_time)
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

    fn info(&mut self) -> String {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock.info()
    }

    fn get_file_name(&self) -> String {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock.get_file_name()
    }

    fn get_running(&self) -> bool {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock.is_running()
    }

    fn vacuum(&self) {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        if lock.vacuum().is_err() {
            log::error!("Error in vacuum");
        }
    }

    fn _repr_html_(&self) -> String {
        let db = self.get_db();
        let lock = db.lock().unwrap();
        lock._repr_html_()
    }

    /// Order book
    ///
    ///
    fn get_order_book(&self) -> Arc<RwLock<OrderBook>>;

    fn reflesh_order_book(&mut self) -> anyhow::Result<()>;

    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
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

            self.reflesh_order_book()?;

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

    fn get_edge_price(&mut self) -> anyhow::Result<(Decimal, Decimal)> {
        let orderbook = self.get_order_book();

        let mut edge_price = {
            let lock = orderbook.read().unwrap();
            lock.get_edge_price()
        };

        if edge_price.is_err() {
            self.reflesh_order_book();
            let lock = orderbook.read().unwrap();
            edge_price = lock.get_edge_price();
        }

        Ok(edge_price.unwrap())
    }

    async fn start_db_thread(&mut self) -> Sender<Vec<Trade>> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        lock.start_thread().await
    }

    async fn async_download_recent_trades(
        &self,
        market_config: &MarketConfig,
    ) -> anyhow::Result<Vec<Trade>> {
        T::get_recent_trades(&self.get_server_config(), market_config).await
    }

    async fn async_expire_unfix_data(&mut self, force: bool) -> anyhow::Result<()> {
        let db = self.get_db();
        let now = NOW();

        let expire_message = {
            let mut lock = db.lock().unwrap();
            lock.make_expire_control_message(now, force)
        };

        if expire_message.is_err() {
            return Ok(());
        }

        let expire_message = expire_message.unwrap();

        let tx = self.start_db_thread().await;
        let r = tx.send(expire_message);
        if r.is_err() {
            log::error!("Error in tx.send: {:?}", r.err().unwrap());
        }

        Ok(())
    }

    /// Download latest data from REST API
    // fn download_latest(&mut self, verbose: bool) -> i64;
    fn start_market_stream(&mut self) -> anyhow::Result<()>;

    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<MarketStream> {
        let (sender, market_stream) = MarketStream::open();

        let db = self.get_db();

        std::thread::spawn(move || {
            let mut table_db = db.lock().unwrap();

            let result = table_db.select(time_from, time_to, |trade| {
                let message: MarketMessage = trade.into();
                sender.send(message)?;

                Ok(())
            });

            if result.is_err() {
                log::error!("Error in select: {:?}", result.err().unwrap());
            }
        });

        return Ok(market_stream);
    }

    /*------------   async ----------------*/
    async fn async_get_latest_archive_date(&self) -> anyhow::Result<MicroSec> {
        let server_config = self.get_server_config();
        let config = self.get_config().clone();

        let r = T::latest_archive_date(&server_config, &config).await;
        r
    }

    async fn async_download_archive(
        &self,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        low_priority: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        T::download_archive(
            &self.get_server_config(),
            &self.get_config(),
            tx,
            date,
            low_priority,
            verbose,
        )
        .await
        .with_context(|| format!("Error in download_archive: {:?}", date))
    }

    /// Download historical data archive and store to database.
    async fn async_download_archives(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        low_priority: bool,
    ) -> anyhow::Result<i64> {
        log::info!("log download: {} days", ndays);
        if verbose {
            println!("log download: {} days", ndays);
            flush_log();
        }

        let latest_date = match self.async_get_latest_archive_date().await {
            Ok(timestamp) => timestamp,
            Err(_) => NOW() - DAYS(2),
        };

        log::info!("archive latest_date: {}", time_string(latest_date));
        if verbose {
            println!("archive latest_date: {}", time_string(latest_date));
            flush_log();
        }

        let mut download_rec: i64 = 0;

        let tx = self.start_db_thread().await;

        for i in 0..ndays {
            let date = latest_date - i * DAYS(1);

            if !force && self.validate_db_by_date(date)? {
                log::info!("{} is valid", time_string(date));

                if verbose {
                    println!("{} skip download", time_string(date));
                    flush_log();
                }
                continue;
            }

            match self
                .async_download_archive(&tx, date, low_priority, verbose)
                .await
            {
                Ok(rec) => {
                    log::info!("downloaded: {}", download_rec);
                    download_rec += rec;
                }
                Err(e) => {
                    log::error!("Error in download_log: {:?}", e);
                    if verbose {
                        println!("Error in download_log: {:?}", e);
                    }
                }
            }
        }

        Ok(download_rec)
    }

    async fn async_download_latest(&mut self, verbose: bool) -> anyhow::Result<i64> {
        if verbose {
            println!("async_download_lastest");
            flush_log();
        }

        let server_config = self.get_server_config();
        let config = self.get_config().clone();

        let trades = T::get_recent_trades(&server_config, &config).await?;
        let rec = trades.len() as i64;

        log::debug!("rec: {}", rec);

        if rec == 0 {
            return Ok(0);
        }

        if verbose {
            println!("from rec: {:?}", trades[0].__str__());
            println!("to   rec: {:?}", trades[(rec as usize) - 1].__str__());
            println!("rec: {}", rec);
            flush_log();
        }
        let tx = self.start_db_thread().await;

        let start_time = trades.iter().map(|trade| trade.time).min();

        if let Some(start_time) = start_time {
            let expire_control = self
                .get_db()
                .lock()
                .unwrap()
                .make_expire_control_message(start_time, false);

            if expire_control.is_err() {
                println!("make_expire_control_message {:?}", expire_control.err());
            } else {
                let expire_control = expire_control.unwrap();                
                if verbose {
                    println!("expire control from {:?}", expire_control[0].__str__());
                    println!("expire control to   {:?}", expire_control[1].__str__());
                }

                tx.send(expire_control)?;
            }
        }

        tx.send(trades)?;

        Ok(rec)
    }
}

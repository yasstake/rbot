use std::sync::Arc;
use crossbeam_channel::Sender;
use rbot_lib::common::BLOCK_ON;
use rbot_lib::db::db_full_path;
use rust_decimal_macros::dec;

use pyo3::{PyErr, PyResult};
use pyo3_polars::PyDataFrame;
use rbot_lib::common::BoardItem;
use rbot_lib::common::OrderBook;
use rbot_lib::net::RestApi;
use rust_decimal::Decimal;
use tokio::sync::{Mutex, RwLock};

use rbot_lib::{
    common::{
        flush_log, time_string, AccountStatus, MarketConfig, MarketStream,
        MicroSec, Order, OrderSide, OrderStatus, OrderType, ServerConfig, Trade, DAYS,
        NOW,
    },
    db::{df::KEY, sqlite::TradeTable},
    net::UdpSender,
};

pub trait MarketInterface {
    // --- GET CONFIG INFO ----
    fn get_config(&self) -> MarketConfig;
    fn get_exchange_name(&self) -> String;
    fn get_trade_category(&self) -> String;
    fn get_trade_symbol(&self) -> String;

    fn set_broadcast_message(&mut self, broadcast_message: bool);
    fn get_broadcast_message(&self) -> bool;

    // --- DB ---->>
    fn drop_table(&mut self) -> PyResult<()>;
    fn get_cache_duration(&self) -> MicroSec;
    fn reset_cache_duration(&mut self);
    fn stop_db_thread(&mut self);
    fn cache_all_data(&mut self);
    fn select_trades(&mut self, start_time: MicroSec, end_time: MicroSec) -> PyResult<PyDataFrame>;
    fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame>;
    fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame>;
    fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> PyResult<PyDataFrame>;
    fn info(&mut self) -> String;
    fn get_board_json(&self, size: usize) -> PyResult<String>;
    fn get_board(&mut self) -> PyResult<(PyDataFrame, PyDataFrame)>;
    fn get_board_vec(&self) -> PyResult<(Vec<BoardItem>, Vec<BoardItem>)>;
    fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)>;
    fn get_file_name(&self) -> String;
    fn get_market_config(&self) -> MarketConfig;
    fn get_running(&self) -> bool;
    fn vacuum(&self);
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
    fn download_latest(&mut self, verbose: bool) -> i64;
    fn start_market_stream(&mut self);
    fn start_user_stream(&mut self);
    // TODO: change signature to open_realtime_channel
    fn get_channel(&mut self) -> MarketStream;
    fn open_backtest_channel(&mut self, time_from: MicroSec, time_to: MicroSec) -> MarketStream;

    //------ REST API ----
    fn limit_order(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>>;

    fn market_order(
        &self,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>>;
    fn dry_market_order(
        &self,
        create_time: MicroSec,
        order_id: &str,
        client_order_id: &str,
        side: OrderSide,
        size: Decimal,
        transaction_id: &str,
    ) -> Vec<Order>;
    fn cancel_order(&self, order_id: &str) -> PyResult<Order>;
    fn cancel_all_orders(&self) -> PyResult<Vec<Order>>;
    fn get_order_status(&self) -> PyResult<Vec<OrderStatus>>;
    fn get_open_orders(&self) -> PyResult<Vec<Order>>;
    fn get_trade_list(&self) -> PyResult<Vec<OrderStatus>>;
    fn get_account(&self) -> PyResult<AccountStatus>;
    fn get_recent_trades(&self) -> Vec<Trade>;
}

macro_rules! block_on_result {
    ($f: expr) => {{
        match BLOCK_ON(async { $f.await }) {
            Ok(r) => Ok(r),
            Err(e) => {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error in block_on_result: {:?}",
                    e
                )));
            }
        }
    }};
}

macro_rules! check_if_enable_order {
    ($s: expr) => {
        if !$s.get_enable_order_feature() {
            log::error!("Order feature is disabled.");
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Order feature is disabled.",
            ));
        }
    };
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

    fn set_broadcast_message(&mut self, broadcast_message: bool);
    fn get_broadcast_message(&self) -> bool;

    fn download_latest(&mut self, verbose: bool) -> i64;

    fn set_enable_order_feature(&self, enable_order: bool);
    fn get_enable_order_feature(&self) -> bool;

    fn price_dp(&self, price: Decimal) -> Decimal {
        let price_scale = self.get_config().price_scale;
        let price_dp = price.round_dp(price_scale);
        price_dp
    }

    fn size_dp(&self, size: Decimal) -> Decimal {
        let size_scale = self.get_config().size_scale;
        let size_dp = size.round_dp(size_scale);
        size_dp
    }

    fn order_side(side: &str) -> OrderSide {
        OrderSide::from(side)
    }

    fn make_order(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        let price = self.price_dp(price);
        let size = self.size_dp(size);
        let order_side = Self::order_side(side);

        let result = BLOCK_ON(async {
            T::new_order(
                &self.get_server_config(),
                &self.get_market_config(),
                order_side,
                price,
                size,
                order_type,
                client_order_id,
            )
            .await
        });

        if result.is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "Error in make_order: {:?}",
                result
            )));
        }

        Ok(result.unwrap())
    }

    //------ REST API ----
    fn limit_order(
        &self,
        side: &str,
        price: Decimal,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        check_if_enable_order!(self);
        self.make_order(side, price, size, OrderType::Limit, client_order_id)
    }

    fn market_order(
        &self,
        side: &str,
        size: Decimal,
        client_order_id: Option<&str>,
    ) -> PyResult<Vec<Order>> {
        check_if_enable_order!(self);
        self.make_order(side, dec![0.0], size, OrderType::Market, client_order_id)
    }

    fn cancel_order(&self, order_id: &str) -> PyResult<Order> {
        check_if_enable_order!(self);
        block_on_result!(T::cancel_order(
            &self.get_server_config(),
            &self.get_config(),
            order_id
        ))
    }

    fn get_open_orders(&self) -> PyResult<Vec<Order>> {
        block_on_result!(T::open_orders(
            &self.get_server_config(),
            &self.get_config()
        ))
    }

    fn get_account(&self) -> PyResult<AccountStatus> {
        block_on_result!(T::get_account(
            &self.get_server_config(),
            &self.get_config()
        ))
    }

    fn get_recent_trades(&self) -> PyResult<Vec<Trade>> {
        block_on_result!(T::get_recent_trades(
            &self.get_server_config(),
            &self.get_config()
        ))
    }

    fn get_db(&self) -> Arc<Mutex<TradeTable>>;

    fn get_history_web_base_url(&self) -> String;

    fn open_udp(&mut self) -> Arc<Mutex<UdpSender>> {
        Arc::new(Mutex::new(UdpSender::open(
            &self.get_exchange_name(),
            &self.get_trade_category(),
            &self.get_trade_symbol(),
        )))
    }

    fn make_db_path(
        exchange_name: &str,
        trade_category: &str,
        trade_symbol: &str,
        db_base_dir: &str,
    ) -> String {
        let db_path = db_full_path(&exchange_name, trade_category, trade_symbol, db_base_dir);

        return db_path.to_str().unwrap().to_string();
    }

    fn get_latest_archive_date(&self) -> Result<MicroSec, String> {
        let result = 
            T::latest_archive_date(&self.get_server_config(), &self.get_config());

        if result.is_err() {
            return Err(result.unwrap_err());
        }

        Ok(result.unwrap())
    }

    /// Check if database is valid at the date
    fn validate_db_by_date(&mut self, date: MicroSec) -> bool {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;

            lock.validate_by_date(date)
        })
    }

    /*
        async fn _start_market_stream(&mut self) {
            // if thread is working, do nothing.
            if self.public_handler.is_some() {
                println!("market stream is already running.");
                return;
            }

            // delete unstable data
            let db_channel = self.db.start_thread();

            // if the latest data is overrap with REST minute, do not delete data.
            let trades = self.get_recent_trades();
            let l = trades.len();
            if l != 0 {
                let rest_start_time = trades[l - 1].time;
                let last_time = self.db.end_time(rest_start_time);
                if last_time.is_err() {
                    let now = NOW();

                    log::debug!(
                        "db has gap, so delete after FIX data now={:?} / db_end={:?}",
                        time_string(now),
                        time_string(rest_start_time)
                    );

                    let delete_message = self.db.connection.make_expire_control_message(now);

                    log::debug!("delete_message: {:?}", delete_message);

                    db_channel.send(delete_message).unwrap();
                } else {
                    log::debug!(
                        "db has no gap, so continue to receive data {}",
                        time_string(rest_start_time)
                    );
                }

                db_channel.send(trades).unwrap();
            }

            let agent_channel = self.agent_channel.clone();
            let ws_channel = self.public_ws._open_channel().await;

            self.public_ws
                ._connect(|message| {
                    let m = serde_json::from_str::<BybitWsMessage>(&message);

                    if m.is_err() {
                        log::warn!("Error in serde_json::from_str: {:?}", message);
                        println!("ERR: {:?}", message);
                    }

                    let m = m.unwrap();

                    return m.into();
                })
                .await;

            let board = self.board.clone();
            let db_channel_for_after = db_channel.clone();

            let udp_sender = self.udp_sender.clone();

            let handler = tokio::task::spawn(async move {
                loop {
                    let message = ws_channel.recv();
                    let message = message.unwrap();

                    if message.trade.len() != 0 {
                        log::debug!("Trade: {:?}", message.trade);
                        let r = db_channel.send(message.trade.clone());

                        if r.is_err() {
                            log::error!("Error in db_channel.send: {:?}", r);
                        }
                    }

                    if message.orderbook.is_some() {
                        let orderbook = message.orderbook.clone().unwrap();
                        let mut b = board.write().unwrap();
                        b.update(&orderbook);
                        drop(b);
                    }

                    let messages = message.extract();

                    // update board

                    // send message to agent
                    for m in messages {
                        if m.trade.is_some() {
                            // broadcast only trade message
                            if udp_sender.is_some() {
                                let sender = udp_sender.as_ref().unwrap().as_ref();
                                sender.lock().unwrap().send_market_message(&m);
                            }

                            let mut ch = agent_channel.write().unwrap();
                            let r = ch.send(m.clone());
                            drop(ch);

                            if r.is_err() {
                                log::error!("Error in db_channel.send: {:?}", r);
                            }
                        }
                    }
                }
            });

            self.public_handler = Some(handler);

            // update recent trade
            // wait for channel open
            sleep(Duration::from_millis(100)); // TODO: fix to wait for channel open
            let trade = self.get_recent_trades();
            db_channel_for_after.send(trade).unwrap();

            // TODO: store recent trade timestamp.

            log::info!("start_market_stream");
        }
    */
    // --- DB ----
    fn drop_table(&mut self) -> PyResult<()> {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;

            if lock.drop_table().is_err() {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                    "Error in drop_table",
                ));
            }

            Ok(())
        })
    }

    fn get_cache_duration(&self) -> MicroSec {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            lock.get_cache_duration()
        })
    }

    fn reset_cache_duration(&mut self) {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.reset_cache_duration();
        })
    }

    fn stop_db_thread(&mut self) {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.stop_thread()
        })
    }

    fn cache_all_data(&mut self) {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.update_cache_all();
        })
    }

    fn select_trades(&mut self, start_time: MicroSec, end_time: MicroSec) -> PyResult<PyDataFrame> {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.py_select_trades_polars(start_time, end_time)
        })
    }

    fn ohlcvv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.py_ohlcvv_polars(start_time, end_time, window_sec)
        })
    }

    fn ohlcv(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        window_sec: i64,
    ) -> PyResult<PyDataFrame> {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.py_ohlcv_polars(start_time, end_time, window_sec)
        })
    }

    fn vap(
        &mut self,
        start_time: MicroSec,
        end_time: MicroSec,
        price_unit: i64,
    ) -> PyResult<PyDataFrame> {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;
            lock.py_vap(start_time, end_time, price_unit)
        })
    }

    fn info(&mut self) -> String {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            lock.info()
        })
    }

    fn get_file_name(&self) -> String {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            lock.get_file_name()
        })
    }

    fn get_running(&self) -> bool {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            lock.is_running()
        })
    }

    fn vacuum(&self) {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            if lock.vacuum().is_err() {
                log::error!("Error in vacuum");
            }
        })
    }

    fn _repr_html_(&self) -> String {
        BLOCK_ON(async {
            let db = self.get_db();
            let lock = db.lock().await;
            lock._repr_html_()
        })
    }

    /// Order book
    ///
    ///
    fn get_order_book(&self) -> Arc<RwLock<OrderBook>>;

    fn reflesh_order_book(&mut self);

    fn get_board(&mut self) -> PyResult<(PyDataFrame, PyDataFrame)> {
        BLOCK_ON(async {
            let orderbook = self.get_order_book();
            let lock = orderbook.read().await;

            let r = lock.get_board();
            drop(lock);
            if r.is_err() {
                return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error in get_board: {:?}",
                    r
                )));
            }

            let (mut bids, mut asks) = r.unwrap();

            if bids.is_empty() || asks.is_empty() {
                return Ok((PyDataFrame(bids), PyDataFrame(asks)));
            }

            let bids_edge: f64 = bids.column(KEY::price).unwrap().max().unwrap();
            let asks_edge: f64 = asks.column(KEY::price).unwrap().min().unwrap();

            if asks_edge < bids_edge {
                log::warn!("bids_edge({}) < asks_edge({})", bids_edge, asks_edge);

                self.reflesh_order_book();

                let orderbook = self.get_order_book();
                let lock = orderbook.read().await;

                let r = lock.get_board();

                (bids, asks) = r.unwrap();
                drop(lock)
            }

            return Ok((PyDataFrame(bids), PyDataFrame(asks)));
        })
    }

    fn get_board_json(&self, size: usize) -> PyResult<String> {
        BLOCK_ON(async {
            let orderbook = self.get_order_book();
            let lock = orderbook.read().await;

            let result = lock.get_json(size);
            drop(lock);

            match result {
                Ok(json) => Ok(json),
                Err(e) => Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                    "Error in get_board_json: {:?}",
                    e
                ))),
            }
        })
    }

    fn get_board_vec(&self) -> PyResult<(Vec<BoardItem>, Vec<BoardItem>)> {
        BLOCK_ON(async {
            let orderbook = self.get_order_book();
            let lock = orderbook.read().await;
            let (bids, asks) = lock.get_board_vec().unwrap();

            Ok((bids, asks))
        })
    }

    fn get_edge_price(&self) -> PyResult<(Decimal, Decimal)> {
        BLOCK_ON(async {
            let orderbook = self.get_order_book();
            let lock = orderbook.read().await;

            Ok(lock.get_edge_price())
        })
    }

    fn get_market_config(&self) -> MarketConfig;

    fn start_db_thread(&mut self) -> Sender<Vec<Trade>> {
        BLOCK_ON(async {
            let db = self.get_db();
            let mut lock = db.lock().await;

            lock.start_thread()
        })
    }

    /// Download historical data archive and store to database.
    fn download(
        &mut self,
        ndays: i64,
        force: bool,
        verbose: bool,
        archive_only: bool,
        low_priority: bool,
    ) -> i64 {
        log::info!("log download: {} days", ndays);
        if verbose {
            println!("log download: {} days", ndays);
            flush_log();
        }

        let latest_date = match self.get_latest_archive_date() {
            Ok(timestamp) => timestamp,
            Err(_) => NOW() - DAYS(2),
        };

        log::info!("archive latest_date: {}", time_string(latest_date));
        if verbose {
            println!("archive latest_date: {}", time_string(latest_date));
            flush_log();
        }

        let mut download_rec: i64 = 0;

        let tx = self.start_db_thread();

        for i in 0..ndays {
            let date = latest_date - i * DAYS(1);

            if !force && self.validate_db_by_date(date) {
                log::info!("{} is valid", time_string(date));

                if verbose {
                    println!("{} skip download", time_string(date));
                    flush_log();
                }
                continue;
            }

            match self.download_archive(&tx, date, low_priority, verbose) {
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

        if !archive_only {
            let rec = self.download_latest(verbose);
            download_rec += rec;
        }
        // let expire_message = self.db.connection.make_expire_control_message(now);
        // tx.send(expire_message).unwrap();

        download_rec
    }

    fn download_archive(
        &self,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        low_priority: bool,
        verbose: bool,
    ) -> Result<i64, String> {
        let result = BLOCK_ON(async {
            let rec = T::download_archive(
                &self.get_server_config(),
                &self.get_config(),
                tx,
                date,
                low_priority,
                verbose,
            )
            .await;

            if rec.is_err() {
                return Err(rec.unwrap_err());
            }

            Ok(rec.unwrap())
        });

        if result.is_err() {
            return Err(result.unwrap_err());
        }

        Ok(result.unwrap())
    }

    /// Download latest data from REST API
    // fn download_latest(&mut self, verbose: bool) -> i64;
    fn start_market_stream(&mut self);
    fn start_user_stream(&mut self);
    fn get_channel(&mut self) -> MarketStream;
    fn open_backtest_channel(&mut self, time_from: MicroSec, time_to: MicroSec) -> MarketStream;
}

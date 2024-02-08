use crossbeam_channel::Sender;
use rbot_lib::common::BLOCK_ON;
use rbot_lib::db::db_full_path;
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
// use anyhow::Result;

use rbot_lib::{
    common::{
        flush_log, time_string, AccountStatus, MarketConfig, MarketStream, MicroSec, Order,
        OrderSide, OrderType, ServerConfig, Trade, DAYS, NOW,
    },
    db::{df::KEY, sqlite::TradeTable},
    net::UdpSender,
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
    fn get_account(&self, market_config: &MarketConfig) -> anyhow::Result<AccountStatus>;
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

    fn make_order(
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

        BLOCK_ON(async {
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
        })
    }

    //------ REST API ----
    fn limit_order(
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
    }

    fn market_order(
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
    }

    fn cancel_order(&self, market_config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        check_if_enable_order!(self);
        BLOCK_ON(async {
            T::cancel_order(&self.get_server_config(), &market_config, order_id)
                .await
                .with_context(|| {
                    format!(
                        "Error in cancel_order: {:?} {:?}",
                        &market_config, &order_id
                    )
                })
        })
    }

    fn get_open_orders(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        BLOCK_ON(async {
            T::open_orders(&self.get_server_config(), market_config)
                .await
                .with_context(|| format!("Error in get_open_orders: {:?}", &market_config))
        })
    }

    fn get_account(&self, market_config: &MarketConfig) -> anyhow::Result<AccountStatus> {
        BLOCK_ON(async {
            T::get_account(&self.get_server_config(), market_config)
                .await
                .with_context(|| format!("Error in get_account: {:?}", &market_config))
        })
    }

    fn start_user_stream(&mut self);
}

pub trait MarketInterface {
    // --- GET CONFIG INFO ----
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

    /// Download historical log from REST API, between the latest data and the latest FIX data.
    /// If the latest FIX data is not found, generate psudo data from klines.
    fn download_gap(&mut self, verbose: bool) -> anyhow::Result<i64>;

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

    fn get_latest_archive_date(&self) -> anyhow::Result<MicroSec> {
        T::latest_archive_date(&self.get_server_config(), &self.get_config())
    }

    /// Check if database is valid at the date
    fn validate_db_by_date(&mut self, date: MicroSec) -> anyhow::Result<bool> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        lock.validate_by_date(date)
    }

    fn find_latest_gap(&self) -> anyhow::Result<(MicroSec, MicroSec)> {
        log::debug!("[start] find_latest_gap");
        let start_time = NOW() - DAYS(2);

        let db = self.get_db();
        let mut lock = db.lock().unwrap();

        let fix_time = lock.latest_fix_time(start_time)?;
        let unfix_time = lock.first_unfix_time(fix_time)?;
        drop(lock);

        if fix_time == 0 {
            return Err(anyhow!("No data found"));
        }
        log::debug!(
            "latest FIX time: {} / first unfix time: {}",
            time_string(fix_time),
            time_string(unfix_time));

        Ok((fix_time, unfix_time))
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

    fn stop_db_thread(&mut self) {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        lock.stop_thread()
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

    fn reflesh_order_book(&mut self);

    fn get_board(&mut self) -> anyhow::Result<(PyDataFrame, PyDataFrame)> {
        let orderbook = self.get_order_book();
        let lock = orderbook.read().unwrap();

        let (mut bids, mut asks) = lock.get_board()?;

        if bids.shape().0 == 0 || asks.shape().0 == 0 {
            return Ok((PyDataFrame(bids), PyDataFrame(asks)));
        }

        let bids_edge: f64 = bids.column(KEY::price).unwrap().max().unwrap();
        let asks_edge: f64 = asks.column(KEY::price).unwrap().min().unwrap();

        if asks_edge < bids_edge {
            log::warn!("bids_edge({}) < asks_edge({})", bids_edge, asks_edge);

            self.reflesh_order_book();

            let orderbook = self.get_order_book();
            let lock = orderbook.read().unwrap();

            let r = lock.get_board();

            (bids, asks) = r.unwrap();
            drop(lock)
        }

        return Ok((PyDataFrame(bids), PyDataFrame(asks)));
    }

    fn get_board_json(&self, size: usize) -> anyhow::Result<String> {
        let orderbook = self.get_order_book();
        let lock = orderbook.read().unwrap();

        let json = lock.get_json(size)?;
        drop(lock);

        Ok(json)
    }

    fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        let orderbook = self.get_order_book();
        let lock = orderbook.read().unwrap();
        let (bids, asks) = lock.get_board_vec()?;

        Ok((bids, asks))
    }

    fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)> {
        let orderbook = self.get_order_book();
        let lock = orderbook.read().unwrap();

        Ok(lock.get_edge_price())
    }

    fn get_market_config(&self) -> MarketConfig;

    fn start_db_thread(&mut self) -> Sender<Vec<Trade>> {
        let db = self.get_db();
        let mut lock = db.lock().unwrap();
        BLOCK_ON(async { lock.start_thread() })
    }

    /// Download historical data archive and store to database.
    fn download_archives(
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

            if !force && self.validate_db_by_date(date)? {
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

        Ok(download_rec)
    }

    fn download_archive(
        &self,
        tx: &Sender<Vec<Trade>>,
        date: MicroSec,
        low_priority: bool,
        verbose: bool,
    ) -> anyhow::Result<i64> {
        BLOCK_ON(async {
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
        })
    }

    fn download_recent_trades(&self, market_config: &MarketConfig) -> anyhow::Result<Vec<Trade>> {
        BLOCK_ON(async { T::get_recent_trades(&self.get_server_config(), market_config).await })
    }

    fn expire_unfix_data(&mut self) -> anyhow::Result<()> {
        let db = self.get_db();
        let now = NOW();

        let mut lock = db.lock().unwrap();
        let expire_message = lock.make_expire_control_message(now)?;
        drop(lock);

        let tx = self.start_db_thread();
        let r = tx.send(expire_message);
        if r.is_err() {
            log::error!("Error in tx.send: {:?}", r.err().unwrap());
        }

        Ok(())
    }

    /// Download latest data from REST API
    // fn download_latest(&mut self, verbose: bool) -> i64;
    fn start_market_stream(&mut self) -> anyhow::Result<()>;
    // fn start_user_stream(&mut self); -> move to OrderInterface

    fn open_realtime_channel(&mut self) -> anyhow::Result<MarketStream>;
    fn open_backtest_channel(
        &mut self,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<MarketStream>;
}

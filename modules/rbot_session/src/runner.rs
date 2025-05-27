// Copyright(c) 2022-2024. yasstake. All rights reserved.

use crossbeam_channel::Receiver;
use pyo3::{
    pyclass, pymethods,
    types::{IntoPyDict, PyAnyMethods},
    Bound, Py, PyAny, PyErr, Python, PyResult,
};
use rust_decimal::{prelude::ToPrimitive, Decimal};

use super::{has_method, ExecuteMode, Session};

use rbot_lib::{
    common::{
        calc_class, date_time_string, flush_log, format_number, get_agent_message, microsec_to_sec,
         time_string, AccountCoins, MarketConfig, MarketMessage, MarketStream, MicroSec, Order, PyRunningBar, 
         Trade, FLOOR_SEC, MARKET_HUB, MICRO_SECOND, NOW, SEC
    },
    net::{UdpReceiver, UdpSender},
};

use rbot_server::start_board_server;

#[pyclass]
#[derive(Debug, Clone)]
pub struct Runner {
    has_on_init: bool,
    has_on_clock: bool,
    has_on_tick: bool,
    has_on_update: bool,

    has_account_update: bool,
    #[pyo3(get)]
    start_timestamp: i64,
    execute_time: i64,
    verbose: bool,

    print_interval: MicroSec,
    #[pyo3(get)]
    last_timestamp: MicroSec,
    current_clock: MicroSec,
    loop_count: i64,

    on_clock_count: i64,
    on_tick_count: i64,
    on_update_count: i64,
    on_account_update_count: i64,
    last_print_tick_time: MicroSec,
    last_print_loop_count: i64,
    last_print_real_time: MicroSec,

    backtest_start_time: MicroSec,
    backtest_end_time: MicroSec,

    execute_mode: ExecuteMode,
    agent_id: String,

    config: MarketConfig,
    exchange_name: String,
    category: String,
    symbol: String,
}

#[pymethods]
impl Runner {
    #[new]
    pub fn new() -> Self {
        Self {
            has_on_init: false,
            has_on_tick: false,
            has_on_clock: false,
            has_on_update: false,
            has_account_update: false,
            start_timestamp: 0,
            execute_time: -1, // -1 means infinite loop
            print_interval: SEC(5),
            execute_mode: ExecuteMode::BackTest,
            last_timestamp: 0,
            current_clock: 0,
            loop_count: 0,
            on_clock_count: 0,
            on_tick_count: 0,
            on_update_count: 0,
            on_account_update_count: 0,
            verbose: false,
            last_print_tick_time: 0,
            last_print_loop_count: 0,
            last_print_real_time: 0,

            backtest_start_time: 0,
            backtest_end_time: 0,

            agent_id: "".to_string(),
            config: MarketConfig::default(),
            exchange_name: "".to_string(),
            category: "".to_string(),
            symbol: "".to_string(),
        }
    }

    pub fn reset_count(&mut self) {
        self.on_clock_count = 0;
        self.on_tick_count = 0;
        self.on_update_count = 0;
        self.on_account_update_count = 0;

        self.start_timestamp = 0;
        self.last_timestamp = 0;
        self.current_clock = 0;
        self.loop_count = 0;

        self.last_print_tick_time = 0;
        self.last_print_loop_count = 0;
        self.last_print_real_time = 0;
    }

    #[pyo3(signature = (*, exchange, market, agent, start_time=0, end_time=0, execute_time=0, verbose=false, log_memory=true, log_file=None))]
    pub fn back_test(
        &mut self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        agent: &Bound<PyAny>,
        start_time: MicroSec,
        end_time: MicroSec,
        execute_time: i64,
        verbose: bool,
        log_memory: bool,
        log_file: Option<String>,
    ) -> anyhow::Result<Py<Session>> {
        self.execute_time = execute_time;
        self.print_interval = SEC(60 * 60);
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::BackTest;

        self.update_market_info(market)?;
        self.update_agent_info(agent)?;

        let (start_time, end_time, receiver) =
            Self::open_backtest_receiver(market, start_time, end_time)?;

        self.backtest_start_time = start_time;
        self.backtest_end_time = end_time;

        if verbose {
            self.print_archive_info(market);
        }

        // let bar = ProgressBar::new()
        let self_ref = self;

        self_ref.run(
            exchange,
            market,
            &receiver,
            agent,
            false,
            log_memory,
            log_file,
            &mut |_, _remain_time| {},
        )
    }

    #[pyo3(signature = (*, exchange, market, agent, log_memory=false, execute_time=0, verbose=false, log_file=None, client=false, no_download=false))]
    pub fn dry_run(
        &mut self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        agent: &Bound<PyAny>,
        log_memory: bool,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
        client: bool,
        no_download: bool,
    ) -> anyhow::Result<Py<Session>> {
        self.execute_time = execute_time;
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::Dry;

        self.update_market_info(&market)?;
        self.update_agent_info(agent)?;

        let exchange_name = self.exchange_name.clone();
        let category = self.category.clone();
        let symbol = self.symbol.clone();
        let agent_id = self.agent_id.clone();

        if client {
            let receiver =
                UdpReceiver::open_channel(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(
                exchange,
                market,
                &receiver,
                agent,
                client,
                log_memory,
                log_file,
                &mut |_, _| {},
            )
        } else {
            self.prepare_data(exchange, &market, no_download)?;

            let receiver = MARKET_HUB.subscribe(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(
                exchange,
                market,
                &receiver,
                agent,
                client,
                log_memory,
                log_file,
                &mut |_, _| {},
            )
        }
    }

    #[pyo3(signature = (*,exchange,  market, agent, log_memory=false, execute_time=0, verbose=false, log_file=None, client=false, no_download=false))]
    pub fn real_run(
        &mut self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        agent: &Bound<PyAny>,
        log_memory: bool,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
        client: bool,
        no_download: bool,
    ) -> anyhow::Result<Py<Session>> {
        self.update_market_info(&market)?;
        self.update_agent_info(agent)?;

        self.execute_time = execute_time;
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::Real;

        let exchange_name = self.exchange_name.clone();
        let category = self.category.clone();
        let symbol = self.symbol.clone();
        let agent_id = self.agent_id.clone();

        if client {
            let receiver =
                UdpReceiver::open_channel(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(
                exchange,
                market,
                &receiver,
                agent,
                client,
                log_memory,
                log_file,
                &mut |_s, _rt| {},
            )
        } else {
            self.prepare_data(exchange, market, no_download)?;
            let receiver = MARKET_HUB.subscribe(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(
                exchange,
                market,
                &receiver,
                agent,
                client,
                log_memory,
                log_file,
                &mut |_s, _rt| {},
            )
        }
    }

    pub fn start_proxy(&mut self) -> anyhow::Result<()> {
        self.execute_mode = ExecuteMode::Real;

        let receiver = MARKET_HUB.subscribe_all()?;
        let sender = UdpSender::open();

        start_board_server()?;

        loop {
            let message = receiver.recv()?;

            if sender.send_message(&message).is_err() {
                log::warn!("Failed to send message: {:?}", message);
            }
        }
    }
}

const MAX_WARMUP_STEPS: i64 = 500;

impl Runner {
    pub fn agent_id(&self) -> String {
        "".to_string()
    }

    pub fn print_archive_info(&self, market: &Bound<PyAny>) {
        let info = self.archive_status(market);

        println!("ARCHIVE has data [{}]", info);
    }

    pub fn prepare_data(
        &self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        no_download: bool,
    ) -> anyhow::Result<()> {
        // prepare market data
        // 1. start market & user stream
        // 2. download market data
        Python::with_gil(|py| {
            if self.execute_mode == ExecuteMode::Real || self.execute_mode == ExecuteMode::Dry {
                market.call_method0("open_market_stream")?;

                if self.verbose {
                    println!("--- open market stream ---");
                    flush_log();
                }
            }

            if self.execute_mode == ExecuteMode::Real {
                exchange.call_method0("open_user_stream")?;

                if self.verbose {
                    println!("--- open user stream ---");
                    flush_log();
                }
            }

            if (!no_download) && self.execute_mode == ExecuteMode::Real
                || self.execute_mode == ExecuteMode::Dry
            {
                if self.verbose {
                    println!("--- start download log data ---");
                    flush_log();
                }

                let kwargs =
                    vec![("connect_ws", true), ("verbose", self.verbose)];

                let kwargs = kwargs.into_py_dict(py)?;
                market.call_method("download", (5, ), Some(&kwargs))?;
                log::debug!("download_latest is done");
            }

            Ok::<(), anyhow::Error>(()) // Add type annotation for Result
        })?;

        if self.verbose {
            self.print_archive_info(market);
        }

        Ok(())
    }

    pub fn update_market_info(&mut self, market: &Bound<PyAny>) -> Result<(), PyErr> {
        let market_config = market.getattr("config")?;
        let market_config = market_config.extract::<MarketConfig>()?;

        self.config = market_config.clone();
        self.exchange_name = market_config.exchange_name;
        self.category = market_config.trade_category;
        self.symbol = market_config.trade_symbol;

        Ok(())
    }

    pub fn update_agent_info(&mut self, agent: &Bound<PyAny>) -> Result<(), PyErr> {
        self.has_on_init = has_method(agent, "on_init");
        self.has_on_clock = has_method(agent, "on_clock");
        self.has_on_tick = has_method(agent, "on_tick");
        self.has_on_update = has_method(agent, "on_update");
        self.has_account_update = has_method(agent, "on_account_update");

        if (!self.has_on_init)
            && (!self.has_on_clock)
            && (!self.has_on_tick)
            && (!self.has_on_update)
            && (!self.has_account_update)
        {
            log::error!("Agent has no method to call. Please implement at least one of on_init, on_clock, on_tick, on_update, on_account_update");
            return Err(PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(
                "Agent has no method to call. Please implement at least one of on_init, on_clock, on_tick, on_update, on_account_update",
            ));
        }

        if self.verbose {
            println!(
                "has_on_init:        {}",
                if self.has_on_init { "YES" } else { " no  " }
            );
            println!(
                "has_on_clock:       {}",
                if self.has_on_clock { "YES" } else { " no  " }
            );
            println!(
                "has_on_tick:        {}",
                if self.has_on_tick { "YES" } else { " no  " }
            );
            println!(
                "has_on_update:      {}",
                if self.has_on_update { "YES" } else { " no  " }
            );
            println!(
                "has_account_update: {}",
                if self.has_account_update {
                    "YES"
                } else {
                    " no  "
                }
            );
            flush_log();
        }

        let agent_class = agent.getattr("__class__").unwrap();
        let agent_name = agent_class.getattr("__name__").unwrap();
        let agent_name = agent_name.extract::<String>().unwrap();
        log::info!("Agent name: {}", agent_name);

        self.agent_id = format!("{}", agent_name);

        Ok(())
    }

    fn create_session(
        &self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        client_mode: bool,
        log_memory: bool,
        log_file: Option<String>,
    ) -> Py<Session> {
        Python::with_gil(|py| {
            let session_name = self.agent_id.clone();

            let mut session = Session::new(
                exchange,
                market,
                self.execute_mode.clone(),
                client_mode,
                Some(&session_name),
                log_memory,
            );

            if log_file.is_some() {
                let log_file = log_file.unwrap();

                if session.open_log(&log_file).is_err() {
                    log::error!("Failed to open log file: {}", &log_file);
                    println!("Failed to open log file: {}", &log_file);
                };
            } else if !log_memory {
                log::warn!(
                    "log_memory and log_file are both false. Please set one of them to true."
                );
                println!("WARNING: log_memory and log_file are both false. Please set one of them to true.");
            }

            Py::new(py, session).unwrap()
        })
    }

    pub fn execute_message_update_session(
        &mut self,
        py_session: &Py<Session>,
        message: &MarketMessage,
    ) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let mut session = py_session.borrow_mut(py);
            session.on_message(&message);
            self.last_timestamp = session.get_timestamp();

            Ok(())
        })
    }

    pub fn execute_message(
        &mut self,
        py_session: &Py<Session>,
        agent: &Bound<PyAny>,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let result = self.on_message(&py, agent, &py_session, &message, interval_sec);
            if result.is_err() {
                return Err(result.unwrap_err());
            }

            Ok(())
        })?;

        Ok(())
    }

    pub fn run<F>(
        &mut self,
        exchange: &Bound<PyAny>,
        market: &Bound<PyAny>,
        //        receiver: impl Stream<Item = anyhow::Result<MarketMessage>>,
        receiver: &Receiver<MarketMessage>,
        agent: &Bound<PyAny>,
        client_mode: bool,
        log_memory: bool,
        log_file: Option<String>,
        print_progress: &mut F,
    ) -> anyhow::Result<Py<Session>>
    where
        F: FnMut(&Py<Session>, i64),
    {
        self.start_timestamp = 0;

        let object = exchange.as_borrowed();
        let exchange_status = object.getattr("production").unwrap();
        let production: bool = exchange_status.extract().unwrap();

        let duration = microsec_to_sec(self.backtest_end_time - self.backtest_start_time);
        //let mut bar= RunningBar::new(duration);
        let mut bar = PyRunningBar::new();

        if self.verbose {
            if self.execute_mode == ExecuteMode::BackTest {
                bar.init(duration, true, true, true);
            }
            else {
                bar.init(duration, false, true, true);
            }


            if production {
                bar.print("========= CONNECT PRODUCTION NET  ==========");
            } else {
                bar.print("--------- connect test net  ----------------");
            }

            match self.execute_mode {
                ExecuteMode::Real => println!("************   REAL MODE   ****************"),
                ExecuteMode::Dry => println!("------------   dry run mode   -------------"),
                ExecuteMode::BackTest => println!("///////////    backtest mode   ////////////"),
            }

            bar.print(&format!("market: {}, ", self.exchange_name));
            bar.print(&format!("agent_id: {}, ", self.agent_id));
            bar.print(&format!("log_memory: {}, ", log_memory));
            bar.print(&format!("duration: {}[sec], ", self.execute_time));

            if let Some(file) = log_file.clone() {
                bar.print(&format!("logfile: {}", file));
            }
            if client_mode {
                bar.print("Client mode");
            }

            match self.execute_mode {
                ExecuteMode::Real => {
                    bar.print("************      START     ****************");
                }
                ExecuteMode::Dry => {
                    bar.print("------------      START        -------------");
                }
                ExecuteMode::BackTest => {
                    let days = microsec_to_sec(self.backtest_end_time - self.backtest_start_time)
                        / 24        // days
                        / 60        // hour
                        / 60; // min
                    println!(
                        "backtest from={} to={}[{}days]",
                        date_time_string(self.backtest_start_time),
                        date_time_string(self.backtest_end_time),
                        days
                    );
                    bar.print("///////////       START        ////////////");
                }
            }

            flush_log();
        }

        // TODO: retrive session id.
        let py_session = self.create_session(exchange, market, client_mode, log_memory, log_file);

        self.call_agent_on_init(&agent, &py_session)?;
        let interval_sec = self.get_clock_interval(&py_session)?;

        // warm up loop
        let mut warm_up_step: i64 = 1;
        while let Ok(message) = receiver.recv() {
            self.execute_message_update_session(&py_session, &message)?;

            log::debug!("warm up loop {:?}:{:?}", warm_up_step, message);

            if self.is_session_initialized(&py_session) {
                break;
            }

            if MAX_WARMUP_STEPS <= warm_up_step {
                break;
            }

            warm_up_step += 1;
        }

        // main loop
        let mut remain_time: i64 = 0;
        let loop_start_time = NOW();

        while let Ok(message) = receiver.recv() {
            //------- MAIN LOOP ---------
            self.execute_message(&py_session, agent, &message, interval_sec)?;
            self.loop_count += 1;

            //-------print status etc.
            // break if the running time exceeceds the loop_duration
            if self.start_timestamp == 0 {
                self.start_timestamp = self.last_timestamp;
                log::debug!("start_timestamp: {}", time_string(self.start_timestamp));
            } else if 0 < self.execute_time {
                remain_time = self.start_timestamp + SEC(self.execute_time) - self.last_timestamp;

                if remain_time < 0 {
                    log::debug!("remain_time out break");
                    break;
                }
            }

            if self.print_interval < self.last_timestamp - self.last_print_tick_time
                || self.last_print_tick_time == 0
            {
                if self.verbose {
                    // progress message
                    print_progress(&py_session, remain_time);
                    let progress = self.progress_string(remain_time);
                    bar.message(&progress);

                    if self.execute_mode == ExecuteMode::BackTest {
                        let sec_processed =
                            microsec_to_sec(self.last_timestamp - self.start_timestamp);
                        bar.set_progress(sec_processed);
                    }

                    // profit message
                    let profit = self.get_profit(&py_session);

                    if self.last_timestamp != 0 {
                        let execute_duration_sec =
                            (self.last_timestamp - self.start_timestamp) / MICRO_SECOND;

                        if 60 < execute_duration_sec {
                            bar.profit(&calc_class(
                                &self.config,
                                profit.to_f64().unwrap(),
                                execute_duration_sec / 60,
                            ));
                        }
                    }

                    // other info
                    let (limit_buy_count, limit_sell_count, market_buy_count, market_sell_count) =
                        self.get_session_info(&py_session);

                    let order_string = format!("{:>6}[Limit/Buy]  {:>6}[Limit/Sell]  {:>6}[Market/Buy]  {:>6}[Market/Sell]",
                            format_number(limit_buy_count), 
                            format_number(limit_sell_count),
                            format_number(market_buy_count), 
                            format_number(market_sell_count));

                    bar.order(&order_string);

                    for line in get_agent_message() {
                        bar.print(&line);
                    }
                }
                self.last_print_tick_time = self.last_timestamp;
            }
        }
        for line in get_agent_message() {
            bar.print(&line);
        }

        self.print_run_result(loop_start_time);

        Ok(py_session)
    }

    fn get_profit(&self, py_session: &Py<Session>) -> Decimal {
        let profit = Python::with_gil(|py| {
            let profit = py_session.getattr(py, "total_profit").unwrap();
            let profit: Decimal = profit.extract(py).unwrap();
            profit
        });

        profit
    }

    fn get_session_info(&self, py_session: &Py<Session>) -> (i64, i64, i64, i64) {
        let result = Python::with_gil(|py| {
            let count = py_session.getattr(py, "limit_buy_count").unwrap();
            let limit_buy_count: i64 = count.extract(py).unwrap();

            let count = py_session.getattr(py, "limit_sell_count").unwrap();
            let limit_sell_count: i64 = count.extract(py).unwrap();

            let count = py_session.getattr(py, "market_buy_count").unwrap();
            let market_buy_count: i64 = count.extract(py).unwrap();

            let count = py_session.getattr(py, "market_sell_count").unwrap();
            let market_sell_count: i64 = count.extract(py).unwrap();

            //let message = py_session.getattr(py, "message").unwrap();
            //let message: Vec<String> = message.extract(py).unwrap();

            (
                limit_buy_count,
                limit_sell_count,
                market_buy_count,
                market_sell_count,
            )
        });

        result
    }

    fn print_run_result(&self, loop_start_time: i64) {
        if self.verbose {
            println!(
                "\n--- end run --- {} ({}[rec])",
                time_string(self.last_timestamp),
                self.loop_count
            );

            println!("Done running agent");

            let process_duration = self.last_timestamp - self.start_timestamp;
            println!(
                "Process duration: {} [Days] ({} sec)",
                process_duration / SEC(60 * 60 * 24),
                process_duration / 1_000_000
            );

            let elapsed = (NOW() - loop_start_time) / 1_000_000;
            println!("Elapsed time: {} sec", elapsed);
            println!("Loop count: {}", self.loop_count);
            if 0 < elapsed {
                println!("Loop per sec: {}", self.loop_count / elapsed);
            }
            println!("on_tick count: {}", self.on_tick_count);
            println!("on_clock count: {}", self.on_clock_count);
            println!("on_update count: {}", self.on_update_count);
            println!("on_account_update count: {}", self.on_account_update_count);
        }
    }

    fn progress_string(&self, remain_time: MicroSec) -> String {
        let time = format!(
            "[{:<.19}]  {:>10}[rec]",
            time_string(self.last_timestamp),
            format_number(self.loop_count),
        );

        let on_tick = if self.on_tick_count != 0 {
            format!("  {:>8}[tk]", format_number(self.on_tick_count))
        } else {
            ",  **** [tk]".to_string()
        };

        let on_clock = if self.on_clock_count != 0 {
            format!("  {:>8}[cl]", format_number(self.on_clock_count))
        } else {
            ",  **** [cl]".to_string()
        };

        let on_update = if self.on_update_count != 0 {
            format!("  {:>6}[up]", format_number(self.on_update_count))
        } else {
            ",  **** [up]".to_string()
        };

        let remain_time = if 0 < remain_time {
            format!("  remain={:>4}[S]", remain_time / 1_000_000)
        } else {
            "".to_string()
        };

        format!(
            "{}{}{}{}{}",
            time, on_tick, on_clock, on_update, remain_time
        )
    }

    fn archive_status(&self, market: &Bound<PyAny>) -> String {
        let obj = market.getattr("archive_info").unwrap();
        let (start_time, end_time): (MicroSec, MicroSec) = obj.extract().unwrap();

        let s = format!(
            "[{} => {}]",
            date_time_string(start_time),
            date_time_string(end_time),
        );

        s
    }

    pub fn on_message(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> anyhow::Result<()> {
        let _config = self.config.clone();

        // on clockはSession更新前に呼ぶ
        // こうすることでsession.curent_timestampより先の値でon_clockが呼ばれる.
        // これは、on_clockが呼ばれた時点で、ohlcの更新が終わっていることを保証するため.
        if self.has_on_clock && interval_sec != 0 {
            if let MarketMessage::Trade(trade) = message {
                let new_clock = FLOOR_SEC(trade.time, interval_sec);

                if self.current_clock == 0 {
                    self.current_clock = new_clock;
                } else if self.current_clock < new_clock {
                    self.current_clock = new_clock;

                    self.call_agent_on_clock(py, agent, py_session, new_clock)?;
                }
            }
        }

        // on_clockの後にsessionを更新する。
        let mut session = py_session.borrow_mut(*py);
        let new_orders = session.on_message(&message);
        self.last_timestamp = session.get_timestamp();
        drop(session);

        match message {
            MarketMessage::Trade(trade) => {
                if self.has_on_tick {
                    self.last_timestamp = trade.time;
                    self.call_agent_on_tick(py, agent, py_session, trade)?;
                }
            }
            MarketMessage::Order(order) => {
                if self.has_on_update {
                    self.call_agent_on_update(py, agent, py_session, order)?;
                }
            }
            MarketMessage::Account(account) => {
                // IN Real run, account message is from user stream.
                // AccountUpdateはFilledかPartiallyFilledのみ発生。
                if self.has_account_update {
                    self.call_agent_on_account_update(py, agent, py_session, &account)?;
                }
            }
            _ => {
                log::warn!("Invalid message type: {:?}", message);
            }
        }

        // otherwise, account message is created from session for simulation.
        if new_orders.len() != 0
            && (self.execute_mode == ExecuteMode::BackTest || self.execute_mode == ExecuteMode::Dry)
        {
            let mut account_change = false;

            if self.has_on_update {
                for order in &new_orders {
                    if self.update_psudo_account_by_order(py, py_session, order) {
                        account_change = true;
                    }

                    self.call_agent_on_update(py, agent, py_session, order)?;
                }
            }

            if account_change && self.has_account_update {
                self.call_agent_on_account_update_backtest(py, agent, py_session)?;
            }
        }

        Ok(())
    }

    pub fn update_psudo_account_by_order(
        &mut self,
        py: &Python,
        py_session: &Py<Session>,
        order: &Order,
    ) -> bool {
        let mut session = py_session.borrow_mut(*py);

        session.update_psudo_account_by_order(order)
    }

    fn get_clock_interval(self: &mut Self, py_session: &Py<Session>) -> Result<i64, PyErr> {
        Python::with_gil(|py| {
            let session = py_session.borrow_mut(py);
            let interval_sec = session.get_clock_interval_sec();

            Ok(interval_sec)
        })
    }

    fn is_session_initialized(&self, py_session: &Py<Session>) -> bool {
        Python::with_gil(|py| {
            let session = py_session.borrow_mut(py);
            session.is_initialized()
        })
    }

    fn call_agent_on_init(
        self: &mut Self,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
    ) -> anyhow::Result<()> {
        if !self.has_on_init {
            return Ok(());
        }

        let result = 
        Python::with_gil(|py| -> PyResult<()> {
            let session = py_session.borrow_mut(py);
            agent.call_method1("on_init", (session,))?;
            Ok(())
        });
    
        if result.is_ok() {
            return Ok(());
        }
         
        Err(anyhow::Error::new(result.unwrap_err()))
    }

    fn call_agent_on_tick(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
        trade: &Trade,
    ) -> Result<(), PyErr> {
        let session = py_session.borrow_mut(*py);
        let price = trade.price.to_f64().unwrap();
        let size = trade.size.to_f64().unwrap();

        agent.call_method1("on_tick", (session, trade.order_side, price, size))?;
        self.on_tick_count += 1;
        Ok(())
    }

    fn call_agent_on_update(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
        order: &Order,
    ) -> Result<(), PyErr> {
        let session = py_session.borrow_mut(*py);
        let py_order = Py::new(*py, order.clone()).unwrap();

        agent.call_method1("on_update", (session, py_order))?;
        self.on_update_count += 1;

        Ok(())
    }

    fn call_agent_on_clock(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
        clock: MicroSec,
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);

        session.set_current_clock(self.current_clock);

        agent.call_method1("on_clock", (session, clock))?;
        self.on_clock_count += 1;

        Ok(())
    }

    fn call_agent_on_account_update(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
        account: &AccountCoins,
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);

        let account_pair = account.extract_pair(&self.config);

        if session.log_account(&account_pair).is_err() {
            log::error!("call_agent_on_account_update: log_account failed");
        }

        log::debug!("call_agent_on_account_update: {:?}", &account_pair);
        let py_account = Py::new(*py, account.clone()).unwrap();

        agent.call_method1("on_account_update", (session, py_account))?;
        self.on_account_update_count += 1;

        Ok(())
    }

    /// When backtest, the account infomation is from session.
    fn call_agent_on_account_update_backtest(
        self: &mut Self,
        py: &Python,
        agent: &Bound<PyAny>,
        py_session: &Py<Session>,
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);
        let account = session.get_account();

        let account_pair = account.extract_pair(&self.config);

        if session.log_account(&account_pair).is_err() {
            log::error!("call_agent_on_account_update_dummy: log_account failed");
        }
        log::debug!("call_agent_on_account_update_dummy: {:?}", &account);

        let py_account = Py::new(*py, account).unwrap();

        agent.call_method1("on_account_update", (session, py_account))?;
        self.on_account_update_count += 1;

        Ok(())
    }

    pub fn open_backtest_receiver(
        market: &Bound<PyAny>,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<(MicroSec, MicroSec, Receiver<MarketMessage>)> {
        let stream = market.call_method1("open_backtest_channel", (time_from, time_to))?;
        let (start_time, end_time, stream) =
            stream.extract::<(MicroSec, MicroSec, MarketStream)>()?;

        Ok((start_time, end_time, stream.reciver))
    }
}


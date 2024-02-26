// Copyright(c) 2022-2023. yasstake. All rights reserved.

use crossbeam_channel::Receiver;
use pyo3::{pyclass, pymethods, types::IntoPyDict, Py, PyAny, PyErr, PyObject, Python};
use rust_decimal::prelude::ToPrimitive;

use super::{has_method, ExecuteMode, Session};
use async_stream::stream;
use futures::{Stream, StreamExt};

use rbot_lib::{
    common::{
        flush_log, time_string, AccountPair, MarketConfig, MarketMessage, MarketStream, MicroSec,
        Order, Trade, FLOOR_SEC, MARKET_HUB, NOW, SEC,
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

    #[pyo3(signature = (*, exchange, market, agent, start_time=0, end_time=0, execute_time=0, verbose=false, log_file=None))]
    pub fn back_test(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        agent: &PyAny,
        start_time: MicroSec,
        end_time: MicroSec,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
    ) -> anyhow::Result<Py<Session>> {
        self.update_market_info(&market)?;
        self.update_agent_info(agent)?;

        self.execute_time = execute_time;
        self.print_interval = SEC(60 * 60);
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::BackTest;

        let receiver = Self::open_backtest_receiver(&market, start_time, end_time)?;
        self.run(exchange, market, &receiver, agent, false, true, log_file)
    }

    #[pyo3(signature = (*, exchange, market, agent, log_memory=false, execute_time=0, verbose=false, log_file=None, client=false))]
    pub fn dry_run(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        agent: &PyAny,
        log_memory: bool,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
        client: bool,
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

            self.run(exchange, market, &receiver, agent, client, log_memory, log_file)
        } else {
            self.prepare_data(&exchange, &market)?;
            let receiver = MARKET_HUB.subscribe(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(exchange, market, &receiver, agent, client, log_memory, log_file)
        }
    }

    /*
    #[pyo3(signature = (*,exchange,  market, agent, log_memory=false, execute_time=0, verbose=false, log_file=None, client=false))]
    pub fn real_run_bak(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        agent: &PyAny,
        log_memory: bool,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
        client: bool,
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
            //            let rt = tokio::runtime::Runtime::new().unwrap();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let mut udp = UdpReceiver::open();

                let receiver = udp
                    .receive_stream(&exchange_name, &category, &symbol, &agent_id)
                    .await;

                self.async_run(exchange, market, receiver, agent, log_memory, log_file)
                    .await
            })
        } else {
            self.prepare_data(&exchange, &market)?;

            //            let rt = tokio::runtime::Runtime::new().unwrap();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                let receiver = MARKET_HUB
                    .subscribe_stream(&exchange_name, &category, &symbol, &agent_id)
                    .await;

                self.async_run(exchange, market, receiver, agent, log_memory, log_file)
                    .await
            })
        }
    }
    */

    #[pyo3(signature = (*,exchange,  market, agent, log_memory=false, execute_time=0, verbose=false, log_file=None, client=false))]
    pub fn real_run(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        agent: &PyAny,
        log_memory: bool,
        execute_time: i64,
        verbose: bool,
        log_file: Option<String>,
        client: bool,
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
            if self.verbose {
                println!("--- open udp receiver ---");
            }
            let receiver =
                UdpReceiver::open_channel(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(exchange, market, &receiver, agent, client, log_memory, log_file)
        } else {
            if self.verbose {
                println!("--- open market hub receiver ---");
            }
            self.prepare_data(&exchange, &market)?;
            let receiver = MARKET_HUB.subscribe(&exchange_name, &category, &symbol, &agent_id)?;

            self.run(exchange, market, &receiver, agent, client, log_memory, log_file)
        }
    }

    pub fn start_proxy(&mut self) -> anyhow::Result<()> {
        self.execute_mode = ExecuteMode::Real;

        let receiver = MARKET_HUB.subscribe_all()?;
        let sender = UdpSender::open();


        start_board_server()?;
        log::debug!("start board proxy");

        loop {
            let message = receiver.recv()?;

            if sender.send_message(&message).is_err() {
                log::warn!("Failed to send message: {:?}", message);
            }
        }
    }
}

const WARMUP_STEPS: i64 = 10;

impl Runner {
    pub fn agent_id(&self) -> String {
        "".to_string()
    }

    pub fn prepare_data(&self, exchange: &PyObject, market: &PyObject) -> anyhow::Result<()> {
        // prepare market data
        // 1. start market & user stream
        // 2. download market data
        Python::with_gil(|py| {
            if self.execute_mode == ExecuteMode::Real || self.execute_mode == ExecuteMode::Dry {
                market.call_method0(py, "start_market_stream")?;

                if self.verbose {
                    println!("--- start market stream ---");
                    flush_log();
                }
            }

            if self.execute_mode == ExecuteMode::Real {
                exchange.call_method0(py, "start_user_stream")?;

                if self.verbose {
                    println!("--- start user stream ---");
                    flush_log();
                }
            }

            if self.execute_mode == ExecuteMode::Real || self.execute_mode == ExecuteMode::Dry {
                if self.verbose {
                    println!("--- start download log data ---");
                    flush_log();
                }

                let kwargs = if self.verbose {
                    vec![("verbose", true)]
                } else {
                    vec![("verbose", false)]
                };
                let kwargs = kwargs.into_py_dict(py);

                market.call_method(py, "download_latest", (), Some(kwargs))?;
                log::debug!("download_latest is done");
                market.call_method(py, "download_gap", (), Some(kwargs))?;
                log::debug!("download_gap is done");

                let kwargs = vec![("ndays", 1)];
                market.call_method(py, "download_archive", (), Some(kwargs.into_py_dict(py)))?;
                log::debug!("download_archive is done");
            }

            Ok::<(), anyhow::Error>(()) // Add type annotation for Result
        })?;

        Ok(())
    }

    pub fn update_market_info(&mut self, market: &PyObject) -> Result<(), PyErr> {
        let r = Python::with_gil(|py| {
            let config = market.getattr(py, "market_config").unwrap();

            let exchange_name = market.getattr(py, "exchange_name").unwrap();
            let category = market.getattr(py, "trade_category").unwrap();
            let symbol = market.getattr(py, "trade_symbol").unwrap();

            self.config = config.extract::<MarketConfig>(py).unwrap();
            self.exchange_name = exchange_name.extract::<String>(py).unwrap();
            self.category = category.extract::<String>(py).unwrap();
            self.symbol = symbol.extract::<String>(py).unwrap();

            Ok(())
        });

        r
    }

    pub fn update_agent_info(&mut self, agent: &PyAny) -> Result<(), PyErr> {
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

        let time = NOW() / 1_000_000;

        self.agent_id = format!("{}{}", agent_name, Runner::int_to_base64(time));

        Ok(())
    }

    fn create_session(
        &self,
        exchange: PyObject,
        market: PyObject,
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
                false,
            );

            if log_file.is_some() {
                let log_file = log_file.unwrap();

                if session.open_log(&log_file).is_err() {
                    log::error!("Failed to open log file: {}", &log_file);
                    println!("Failed to open log file: {}", &log_file);
                };
            } else if !log_memory {
                log::error!(
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
        agent: &PyAny,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> anyhow::Result<()> {
        Python::with_gil(|py| {
            let result = self.on_message(&py, agent, &py_session, &message, interval_sec);
            if result.is_err() {
                return Err(result.unwrap_err());
            }
            self.loop_count += 1;

            Ok(())
        })?;

        Ok(())
    }

    pub async fn async_run(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        receiver: impl Stream<Item = anyhow::Result<MarketMessage>>,
        // receiver: &Receiver<MarketMessage>,
        agent: &PyAny,
        client_mode: bool,
        log_memory: bool,
        log_file: Option<String>,
    ) -> anyhow::Result<Py<Session>> {
        if self.verbose {
            print!("--- run {:?} mode --- ", self.execute_mode);
            print!("market: {}, ", self.exchange_name);
            print!("agent_id: {}, ", self.agent_id);
            print!("log_memory: {}, ", log_memory);
            println!("duration: {}[sec], ", self.execute_time);

            flush_log();
        }

        let py_session = self.create_session(exchange, market, client_mode, log_memory, log_file);

        self.call_agent_on_init(&agent, &py_session)?;
        let interval_sec = self.get_clock_interval(&py_session)?;

        let mut rec = Box::pin(receiver);

        // warm up loop
        let mut warm_up_loop: i64 = WARMUP_STEPS;
        while let Some(message) = rec.next().await {
            let message = message?;
            self.execute_message_update_session(&py_session, &message)?;

            if let MarketMessage::Trade(_trade) = &message {
                warm_up_loop = warm_up_loop - 1;
                if warm_up_loop <= 0 {
                    break;
                }
            }
        }

        let mut remain_time: i64 = 0;

        let loop_start_time = NOW();
        while let Some(message) = rec.next().await {
            let message = message?;
            self.execute_message(&py_session, agent, &message, interval_sec)?;
            self.loop_count += 1;

            // break if the running time exceeceds the loop_duration
            if self.start_timestamp == 0 {
                self.start_timestamp = self.last_timestamp;
                if self.verbose {
                    println!("start_timestamp: {}", time_string(self.start_timestamp));
                }
            } else if 0 < self.execute_time {
                remain_time = self.start_timestamp + SEC(self.execute_time) - self.last_timestamp;

                if remain_time < 0 {
                    log::debug!("remain_time out break");
                    break;
                }
            }

            // print progress if verbose flag is set.
            if self.verbose {
                self.print_progress(remain_time);
            }
        }
        self.print_run_result(loop_start_time);

        Ok(py_session)
    }

    pub fn run(
        &mut self,
        exchange: PyObject,
        market: PyObject,
        //        receiver: impl Stream<Item = anyhow::Result<MarketMessage>>,
        receiver: &Receiver<MarketMessage>,
        agent: &PyAny,
        client_mode: bool,
        log_memory: bool,
        log_file: Option<String>,
    ) -> anyhow::Result<Py<Session>> {
        if self.verbose {
            print!("--- run {:?} mode --- ", self.execute_mode);
            print!("market: {}, ", self.exchange_name);
            print!("agent_id: {}, ", self.agent_id);
            print!("log_memory: {}, ", log_memory);
            println!("duration: {}[sec], ", self.execute_time);

            flush_log();
        }

        let py_session = self.create_session(exchange, market, client_mode, log_memory, log_file);

        self.call_agent_on_init(&agent, &py_session)?;
        let interval_sec = self.get_clock_interval(&py_session)?;

        if self.verbose {
            println!("start warm up");
            flush_log();
        }
        // warm up loop
        let mut warm_up_loop: i64 = WARMUP_STEPS;
        while let Ok(message) = receiver.recv() {
            log::debug!("warm up loop: {:?}", message);
            self.execute_message_update_session(&py_session, &message)?;

            if let MarketMessage::Trade(_trade) = &message {
                warm_up_loop = warm_up_loop - 1;
                if warm_up_loop <= 0 {
                    break;
                }
            }
        }

        if self.verbose {
            println!("start receiver tick");
            flush_log();
        }

        let mut remain_time: i64 = 0;

        let loop_start_time = NOW();
        while let Ok(message) = receiver.recv() {
            self.execute_message(&py_session, agent, &message, interval_sec)?;
            self.loop_count += 1;

            // break if the running time exceeceds the loop_duration
            if self.start_timestamp == 0 {
                self.start_timestamp = self.last_timestamp;
                if self.verbose {
                    println!("start_timestamp: {}", time_string(self.start_timestamp));
                }
            } else if 0 < self.execute_time {
                remain_time = self.start_timestamp + SEC(self.execute_time) - self.last_timestamp;

                if remain_time < 0 {
                    log::debug!("remain_time out break");
                    break;
                }
            }

            // print progress if verbose flag is set.
            if self.verbose {
                self.print_progress(remain_time);
            }
        }
        self.print_run_result(loop_start_time);

        Ok(py_session)
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
    /*
        pub fn run(
            &mut self,
            exchange: PyObject,
            market: PyObject,
            receiver: &impl Stream<Item = anyhow::Result<MarketMessage>>,
            // receiver: &Receiver<MarketMessage>,
            agent: &PyAny,
            log_memory: bool,
            log_file: Option<String>,
        ) -> Result<Py<Session>, PyErr> {
            if self.verbose {
                print!("--- run {:?} mode --- ", self.execute_mode);
                print!("market: {}, ", self.exchange_name);
                print!("agent_id: {}, ", self.agent_id);
                print!("log_memory: {}, ", log_memory);
                println!("duration: {}[sec], ", self.execute_time);

                flush_log();
            }

            let result = Python::with_gil(|py| {
                if self.verbose {
                    println!("--- run loop ---");
                    flush_log();
                }

                let session_name = self.agent_id.clone();
                let mut session = Session::new(
                    exchange,
                    market,
                    self.execute_mode.clone(),
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
                    log::error!(
                        "log_memory and log_file are both false. Please set one of them to true."
                    );
                    println!("WARNING: log_memory and log_file are both false. Please set one of them to true.");
                }

                let py_session = Py::new(py, session).unwrap();
                let mut warm_up_loop: i64 = WARMUP_STEPS;

                if self.has_on_init {
                    self.call_agent_on_init_py(&py, agent, &py_session)?;
                }

                let interval_sec = self.get_clock_interval(&py, &py_session)?;

                if self.has_on_clock && interval_sec == 0 {
                    print!("*** WARNING: on_clock is set but clock_interval is 0. (on_clock will not be called ***");
                }

                self.reset_count();
                let loop_start_time = NOW();

                let mut remain_time: i64 = 0;

                loop {
                    let message = receiver.recv();

                    if message.is_err() {
                        log::info!("Data stream is closed {:?}", message);
                        break;
                    }

                    let message = message.unwrap();
                    if self.execute_mode == ExecuteMode::BackTest {
                        if let MarketMessage::Trade(trade) = &message {
                            if trade.status == LogStatus::UnFix {
                                log::info!("UnFix trade is found. Stop running.");
                                break;
                            }
                        }
                    }

                    if 0 < warm_up_loop {
                        // in warm up loop, we don't call agent, just update session
                        let mut session = py_session.borrow_mut(py);
                        session.on_message(&message);
                        self.last_timestamp = session.get_timestamp();

                        if let MarketMessage::Trade(_trade) = &message {
                            warm_up_loop = warm_up_loop - 1;
                        }

                        if self.verbose {
                            print!("\rwarm up loop: {}", warm_up_loop)
                        }

                        drop(session);

                        continue;
                    }

                    // after warm up loop, we call agent(in the method session is updated)
                    self.loop_count += 1;

                    let r = self.on_message(&py, agent, &py_session, &message, interval_sec);
                    if r.is_err() {
                        return Err(r.unwrap_err());
                    }

                    // break if the running time exceeceds the loop_duration
                    if self.start_timestamp == 0 {
                        self.start_timestamp = self.last_timestamp;
                        if self.verbose {
                            println!("start_timestamp: {}", time_string(self.start_timestamp));
                        }
                    } else if 0 < self.execute_time {
                        remain_time =
                            self.start_timestamp + SEC(self.execute_time) - self.last_timestamp;

                        if remain_time < 0 {
                            log::debug!("remain_time out break");
                            break;
                        }
                    }

                    // print progress if verbose flag is set.
                    if self.verbose {
                        self.print_progress(remain_time);
                    }
                }

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

                Ok(py_session)
            });

            result
        }
    */
    /*
    pub fn execute_message(
        &mut self,
        py  : &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        interval_sec: i64,
        message: MarketMessage,
        warm_up: bool,
    ) -> anyhow::Result<()> {
        if self.execute_mode == ExecuteMode::BackTest {
            if let MarketMessage::Trade(trade) = &message {
                if trade.status == LogStatus::UnFix {
                    log::info!("UnFix trade is found. Stop running.");
                    return Err(anyhow::anyhow!("UnFix trade is found. Stop running."));
                }
            }
        }

        if warm_up {
            // in warm up loop, we don't call agent, just update session
            let mut session = py_session.borrow_mut(py);
            session.on_message(&message);
            self.last_timestamp = session.get_timestamp();

            if let MarketMessage::Trade(_trade) = &message {
                warm_up_loop = warm_up_loop - 1;
            }

            if self.verbose {
                print!("\rwarm up loop: {}", warm_up_loop)
            }

            drop(session);

            continue;
        }

        // after warm up loop, we call agent(in the method session is updated)
        self.loop_count += 1;

        let r = self.on_message(&py, agent, &py_session, &message, interval_sec);
        if r.is_err() {
            return Err(r.unwrap_err());
        }

        // break if the running time exceeceds the loop_duration
        if self.start_timestamp == 0 {
            self.start_timestamp = self.last_timestamp;
            if self.verbose {
                println!("start_timestamp: {}", time_string(self.start_timestamp));
            }
        } else if 0 < self.execute_time {
            remain_time =
                self.start_timestamp + SEC(self.execute_time) - self.last_timestamp;

            if remain_time < 0 {
                log::debug!("remain_time out break");
                break;
            }
        }

        // print progress if verbose flag is set.
        if self.verbose {
            self.print_progress(remain_time);
        }


    }
    */

    pub fn print_progress(&mut self, remain_time: i64) -> bool {
        if self.last_timestamp - self.last_print_tick_time < self.print_interval
            && self.last_print_tick_time != 0
        {
            return false;
        }

        let mode = match self.execute_mode {
            ExecuteMode::Dry => "[Dry run ]",
            ExecuteMode::Real => "[Real run]",
            ExecuteMode::BackTest => "[BackTest]",
        };

        print!(
            "\r{}{:<.19}, {:>6}[rec]",
            mode,
            time_string(self.last_timestamp),
            self.loop_count,
        );

        if self.on_tick_count != 0 {
            print!(", {:>6}[tk]", self.on_tick_count,);
        }

        if self.on_clock_count != 0 {
            print!(", {:>6}[cl]", self.on_clock_count,);
        }

        if self.on_update_count != 0 {
            print!(", {:>6}[up]", self.on_update_count,);
        }

        if 0 < remain_time {
            print!(", {:>4}[ETA]", remain_time / 1_000_000);
        }

        if self.execute_mode == ExecuteMode::BackTest {
            let count = self.loop_count - self.last_print_loop_count;
            self.last_print_loop_count = self.loop_count;

            let now = NOW();
            let real_elapsed_time = now - self.last_print_real_time;
            self.last_print_real_time = now;

            let rec_per_sec = ((count * 1_000_000) as f64) / real_elapsed_time as f64; // in sec
            let rec_per_sec = rec_per_sec as i64;

            let tick_elapsed_time = self.last_timestamp - self.last_print_tick_time;
            let speed = tick_elapsed_time / real_elapsed_time;

            print!(", {:>7}[rec/s]({:>5} X)", rec_per_sec, speed,);
        }

        flush_log();

        self.last_print_tick_time = self.last_timestamp;

        return true;
    }

    pub fn on_message(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> anyhow::Result<()> {
        let config = self.config.clone();

        // on clockはSession更新前に呼ぶ
        // こうすることでsession.curent_timestampより先の値でon_clockが呼ばれる.
        // これは、on_clockが呼ばれた時点で、ohlcの更新が終わっていることを保証するため.
        if self.has_on_clock && interval_sec != 0 {
            if let MarketMessage::Trade(trade) = message {
                let new_clock = FLOOR_SEC(trade.time, interval_sec);

                if self.current_clock < new_clock {
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
                    let account_pair = account.extract_pair(&config);

                    self.call_agent_on_account_update(py, agent, py_session, &account_pair)?;
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
                self.call_agent_on_account_update_dummy(py, agent, py_session)?;
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

    /*
    fn get_clock_interval_py(
        self: &mut Self,
        py: &Python,
        py_session: &Py<Session>,
    ) -> Result<i64, PyErr> {
        let session = py_session.borrow_mut(*py);
        let interval_sec = session.get_clock_interval_sec();

        Ok(interval_sec)
    }
    */

    fn get_clock_interval(self: &mut Self, py_session: &Py<Session>) -> Result<i64, PyErr> {
        Python::with_gil(|py| {
            let session = py_session.borrow_mut(py);
            let interval_sec = session.get_clock_interval_sec();

            Ok(interval_sec)
        })
    }

    /*
    fn call_agent_on_init_py(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
    ) -> Result<(), PyErr> {
        let session = py_session.borrow_mut(*py);

        agent.call_method1("on_init", (session,))?;

        Ok(())
    }
    */

    fn call_agent_on_init(
        self: &mut Self,
        agent: &PyAny,
        py_session: &Py<Session>,
    ) -> anyhow::Result<()> {
        if self.verbose {
            println!("--- call on_init ---");
        }
        if !self.has_on_init {
            return Ok(());
        }

        Python::with_gil(|py| {
            let session = py_session.borrow_mut(py);
            agent.call_method1("on_init", (session,))
        })?;

        Ok(())
    }

    fn call_agent_on_tick(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
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
        agent: &PyAny,
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
        agent: &PyAny,
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
        agent: &PyAny,
        py_session: &Py<Session>,
        account: &AccountPair,
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);
        session.set_real_account(account);
        if session.log_account(account).is_err() {
            log::error!("call_agent_on_account_update: log_account failed");
        }

        log::debug!("call_agent_on_account_update: {:?}", &account);
        let py_account = Py::new(*py, account.clone()).unwrap();

        agent.call_method1("on_account_update", (session, py_account))?;
        self.on_account_update_count += 1;

        Ok(())
    }

    /// When backtest, the account infomation is from session.
    fn call_agent_on_account_update_dummy(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);
        let account = session.get_account();
        if session.log_account(&account).is_err() {
            log::error!("call_agent_on_account_update_dummy: log_account failed");
        }
        log::debug!("call_agent_on_account_update_dummy: {:?}", &account);

        let py_account = Py::new(*py, account).unwrap();

        agent.call_method1("on_account_update", (session, py_account))?;
        self.on_account_update_count += 1;

        Ok(())
    }

    pub fn open_backtest_receiver(
        market: &PyObject,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> anyhow::Result<Receiver<MarketMessage>> {
        let market_stream = Python::with_gil(|py| {
            let stream = market.call_method1(py, "open_backtest_channel", (time_from, time_to))?;
            let stream = stream.extract::<MarketStream>(py)?;

            Ok::<MarketStream, anyhow::Error>(stream)
        })?;

        Ok(market_stream.reciver)
    }

    pub async fn open_backtest_stream(
        market: &PyObject,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> impl Stream<Item = anyhow::Result<MarketMessage>> {
        let market_stream = Python::with_gil(|py| {
            let stream = market.call_method1(py, "open_backtest_channel", (time_from, time_to));

            if stream.is_ok() {
                let stream = stream.unwrap();
                let stream = stream.extract::<MarketStream>(py).unwrap();
                stream
            } else {
                let err = stream.unwrap_err();
                log::error!("Error in open_backtest_channel: {:?}", err);
                panic!("Error in open_backtest_channel: {:?}", err);
            }
        });

        let market_stream = market_stream.reciver;

        stream! {
            loop {
                let message = market_stream.recv()?;

                yield Ok(message);
            }
        }
    }

    // base 64 generated by co pilot!!
    pub fn int_to_base64(num: i64) -> String {
        let mut num = num;
        let mut result = String::new();

        loop {
            let r = num % 64;
            num = num / 64;

            let c = match r {
                0..=25 => (r + 65) as u8 as char,
                26..=51 => (r + 71) as u8 as char,
                52..=61 => (r - 4) as u8 as char,
                62 => '+',
                63 => '/',
                _ => panic!("Invalid number"),
            };

            result.push(c);

            if num == 0 {
                break;
            }
        }

        result
    }
}

#[cfg(test)]
mod runner_test {
    #[test]
    fn test_int_to_base64() {
        let b64 = super::Runner::int_to_base64(0);
        println!("b64: {}", b64);

        let b64 = super::Runner::int_to_base64(1000001);
        println!("b64: {}", b64);
    }
}

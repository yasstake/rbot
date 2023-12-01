// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::io::{stdout, Write};

use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyObject, Python};
use rust_decimal::prelude::ToPrimitive;

use crate::common::{
    time_string, AccountStatus, MarketMessage, MarketStream, MicroSec, Order, Trade, FLOOR_SEC,
    NOW, SEC,
};
use crossbeam_channel::Receiver;

use super::{has_method, ExecuteMode, Session};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Runner {
    has_on_clock: bool,
    has_on_tick: bool,
    has_on_update: bool,
    has_account_update: bool,
    start_timestamp: i64,
    loop_duration: i64,
    verbose: bool,

    print_interval: MicroSec,
    current_timestamp: MicroSec,
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
}

#[pymethods]
impl Runner {
    #[new]
    pub fn new() -> Self {
        Self {
            has_on_tick: false,
            has_on_clock: false,
            has_on_update: false,
            has_account_update: false,
            start_timestamp: 0,
            loop_duration: -1, // -1 means infinite loop
            print_interval: SEC(10),
            execute_mode: ExecuteMode::BackTest,
            current_timestamp: 0,
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
        }
    }

    pub fn reset_count(&mut self) {
        self.on_clock_count = 0;
        self.on_tick_count = 0;
        self.on_update_count = 0;
        self.on_account_update_count = 0;

        self.start_timestamp = 0;
        self.current_timestamp = 0;
        self.current_clock = 0;
        self.loop_count = 0;

        self.last_print_tick_time = 0;
        self.last_print_loop_count = 0;
        self.last_print_real_time = 0;
    }

    pub fn update_agent_info(&mut self, agent: &PyAny) {
        self.has_on_clock = has_method(agent, "on_clock");
        self.has_on_tick = has_method(agent, "on_tick");
        self.has_on_update = has_method(agent, "on_update");
        self.has_account_update = has_method(agent, "on_account_update");

        if self.verbose {
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
        }
    }

    #[pyo3(signature = (market, agent, interval_sec, start_time=0, end_time=0, loop_duration=0, verbose=false))]
    pub fn back_test(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
        start_time: MicroSec,
        end_time: MicroSec,
        loop_duration: i64,
        verbose: bool,
    ) -> Result<Py<Session>, PyErr> {
        self.update_agent_info(agent);
        let stream = Self::open_backtest_stream(&market, start_time, end_time);
        let reciever = stream.reciver;
        self.loop_duration = loop_duration;
        self.print_interval = SEC(60 * 60);
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::BackTest;

        self.run(market, &reciever, agent, interval_sec, true)
    }

    #[pyo3(signature = (market, agent, interval_sec, log_memory=false, loop_duration=0, verbose=false))]
    pub fn dry_run(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
        log_memory: bool,
        loop_duration: i64,
        verbose: bool,
    ) -> Result<Py<Session>, PyErr> {
        self.update_agent_info(agent);
        Python::with_gil(|py| {
            market.call_method0(py, "start_market_stream").unwrap();
        });

        let stream = Self::get_market_stream(&market);
        let reciever = stream.reciver;

        self.loop_duration = loop_duration;
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::Dry;

        self.run(market, &reciever, agent, interval_sec, log_memory)
    }

    #[pyo3(signature = (market, agent, interval_sec, log_memory=false, loop_duration=0, verbose=false))]
    pub fn real_run(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
        log_memory: bool,
        loop_duration: i64,
        verbose: bool,
    ) -> Result<Py<Session>, PyErr> {
        self.update_agent_info(agent);
        let stream = Self::get_market_stream(&market);
        let reciever = stream.reciver;

        Python::with_gil(|py| {
            market.call_method0(py, "start_market_stream").unwrap();
            market.call_method0(py, "start_user_stream").unwrap();
        });
        self.loop_duration = loop_duration;
        self.verbose = verbose;
        self.execute_mode = ExecuteMode::Real;

        self.run(market, &reciever, agent, interval_sec, log_memory)
    }
}

const WARMUP_STEPS: i64 = 10;

impl Runner {
    pub fn run(
        &mut self,
        market: PyObject,
        receiver: &Receiver<MarketMessage>,
        agent: &PyAny,
        interval_sec: i64,
        log_memory: bool,
    ) -> Result<Py<Session>, PyErr> {
        let result = Python::with_gil(|py| {
            if self.verbose {
                println!("--- start run ---{:?} mode ---", self.execute_mode);
            }

            let mut session = Session::new(market, self.execute_mode.clone(), None, log_memory);

            if !log_memory {
                // TODO: ERROR handling
                session.open_log(&session.session_name.clone()).unwrap();
            }

            let py_session = Py::new(py, session).unwrap();
            let mut warm_up_loop: i64 = WARMUP_STEPS;

            self.reset_count();

            let loop_start_time = NOW();

            loop {
                let message = receiver.recv();

                if message.is_err() {
                    log::info!("Data stream is closed {:?}", message);
                    break;
                }

                let message = message.unwrap();

                if 0 < warm_up_loop {
                    // in warm up loop, we don't call agent, just update session
                    let mut session = py_session.borrow_mut(py);
                    session.on_message(&message);
                    self.current_timestamp = session.get_current_timestamp();

                    if message.trade.is_some() {
                        warm_up_loop = warm_up_loop - 1;
                    }

                    if self.verbose {
                        print!("\rwarm up loop: {}", warm_up_loop)
                    }

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
                    self.start_timestamp = self.current_timestamp;
                    if self.verbose {
                        println!("start_timestamp: {}", time_string(self.start_timestamp));
                    }
                } else if 0 < self.loop_duration
                    && SEC(self.loop_duration) < self.current_timestamp - self.start_timestamp
                {
                    break;
                }

                // print progress if verbose flag is set.
                if self.verbose {
                    self.print_progress();
                }
            }

            if self.verbose {
                println!(
                    "\n--- end run --- {} ({}[rec])",
                    time_string(self.current_timestamp),
                    self.loop_count
                );

                println!("Done running agent");

                let process_duration = self.current_timestamp - self.start_timestamp;
                println!(
                    "Process duration: {} [Days] ({} sec)",
                    process_duration / SEC(60 * 60 * 24),
                    process_duration / 1_000_000
                );

                let elapsed = (NOW() - loop_start_time) / 1_000_000;
                println!("Elapsed time: {} sec", elapsed);
                println!("Loop count: {}", self.loop_count);
                println!("Loop per sec: {}", self.loop_count / elapsed);
            }

            Ok(py_session)
        });

        result
    }

    pub fn print_progress(&mut self) -> bool {
        if self.last_print_tick_time == 0 {
            self.last_print_tick_time = self.current_timestamp;
            return false;
        }

        if self.print_interval < self.current_timestamp - self.last_print_tick_time {
            let mode = match self.execute_mode {
                ExecuteMode::Dry => "[Dry run ]",
                ExecuteMode::Real => "[Real run]",
                ExecuteMode::BackTest => "[BackTest]",
            };

            print!(
                "\r{}{:<.19}, {:>6}[rec], ({:>6}[tick], {:>4}[clock], {:>3}[update])",
                mode,
                time_string(self.current_timestamp),
                self.loop_count,
                self.on_tick_count,
                self.on_clock_count,
                self.on_update_count,
            );
            let _ = stdout().flush();

            if self.execute_mode == ExecuteMode::BackTest {
                let count = self.loop_count - self.last_print_loop_count;
                self.last_print_loop_count = self.loop_count;

                let now = NOW();
                let real_elapsed_time = now - self.last_print_real_time;
                self.last_print_real_time = now;

                let rec_per_sec = ((count * 1_000_000) as f64) / real_elapsed_time as f64; // in sec
                let rec_per_sec = rec_per_sec as i64;

                let tick_elapsed_time = self.current_timestamp - self.last_print_tick_time;
                let speed = tick_elapsed_time / real_elapsed_time;

                print!(", {:>7}[rec/s]({:>8} X)\n", rec_per_sec, speed,);
            }

            self.last_print_tick_time = self.current_timestamp;

            return true;
        }

        false
    }

    pub fn on_message(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> Result<(), PyErr> {
        // on clockはSession更新前に呼ぶ
        // こうすることでsession.curent_timestampより先の値でon_clockが呼ばれる.
        // これは、on_clockが呼ばれた時点で、ohlcの更新が終わっていることを保証するため.
        if self.has_on_clock && message.trade.is_some() {
            let trade = message.trade.as_ref().unwrap();
            let new_clock = FLOOR_SEC(trade.time, interval_sec);

            if self.current_clock < new_clock {
                self.current_clock = new_clock;

                self.call_agent_on_clock(py, agent, py_session, new_clock)?;
            }
        }

        // on_clockの後にsessionを更新する。
        let mut session = py_session.borrow_mut(*py);
        session.on_message(&message);
        self.current_timestamp = session.get_current_timestamp();
        drop(session);

        if self.has_account_update && message.account.is_some() {
            let account = message.account.as_ref().unwrap();
            self.call_agent_on_account_update(py, agent, py_session, account)?;
        }

        if self.has_on_tick && message.trade.is_some() {
            let trade = message.trade.as_ref().unwrap();
            self.current_timestamp = trade.time;
            self.call_agent_on_tick(py, agent, py_session, trade)?;
        }

        if self.has_on_update && message.order.is_some() {
            let order = message.order.as_ref().unwrap();
            self.call_agent_on_update(py, agent, py_session, order)?;
        }

        Ok(())
    }

    /*
    fn on_clock(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        interval_sec: i64,
    ) -> Result<(), PyErr> {
        let trade = message.trade.as_ref().unwrap();

        let new_clock = FLOOR_SEC(trade.time, interval_sec);

        if self.current_clock < new_clock {
            self.current_clock = new_clock;
            let session = py_session.borrow_mut(*py);
            agent.call_method1("on_clock", (session, self.current_clock))?;
        }

        Ok(())
    }
    */
    /*
        pub fn dummy_on_message(
            self: &mut Self,
            py: &Python,
            agent: &PyAny,
            py_session: &Py<Session>,
            message: &MarketMessage,
            window_sec: i64,
        ) -> Result<(), PyErr> {
            if self.has_on_clock && message.trade.is_some() {
                self.on_clock(py, agent, py_session, message, window_sec)?;
            }

            let mut session = py_session.borrow_mut(*py);
            let orders = session.on_message(&message);
            drop(session);

            log::debug!("dummy_on_message: {:?}, {:?}", message, orders);

            if message.account.is_some() {
                let session = py_session.borrow_mut(*py);

                let account = message.account.as_ref().unwrap();
                let py_account = Py::new(*py, account.clone()).unwrap();

                let result = agent.call_method1("on_account_update", (session, py_account));

                if result.is_err() {
                    return Err(result.unwrap_err());
                }
            }

            if self.has_on_tick && message.trade.is_some() {
                let trade = message.trade.as_ref().unwrap();

                let session = py_session.borrow_mut(*py);

                let price = trade.price.to_f64().unwrap();
                let size = trade.size.to_f64().unwrap();

                agent.call_method1("on_tick", (session, trade.order_side, price, size))?;
            }

            if self.has_on_update {
                for order in orders {
                    let py_order = Py::new(*py, order.clone()).unwrap();
                    let session = py_session.borrow_mut(*py);
                    agent.call_method1("on_update", (session, py_order))?;
                }
            }

            Ok(())
        }
    */
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
        let session = py_session.borrow_mut(*py);

        agent.call_method1("on_clock", (session, clock))?;
        self.on_clock_count += 1;

        Ok(())
    }

    fn call_agent_on_account_update(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        account: &AccountStatus,
    ) -> Result<(), PyErr> {
        let session = py_session.borrow_mut(*py);
        let py_account = Py::new(*py, account.clone()).unwrap();

        agent.call_method1("on_account_update", (session, py_account))?;
        self.on_account_update_count += 1;

        Ok(())
    }

    /// get market stream from Market object
    /// Every market object shold implement `channel` attribute
    /// TODO: retrun Error
    pub fn get_market_stream(market: &PyObject) -> MarketStream {
        Python::with_gil(|py| {
            let stream = market.getattr(py, "channel").unwrap();
            let stream = stream.extract::<MarketStream>(py).unwrap();

            stream
        })
    }

    /// TODO: return error
    pub fn open_backtest_stream(
        market: &PyObject,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> MarketStream {
        Python::with_gil(|py| {
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
        })
    }
}

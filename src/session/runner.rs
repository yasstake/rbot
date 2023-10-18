use std::borrow::BorrowMut;
use std::f32::consts::E;
use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyObject, Python};
use rusqlite::ffi::SQLITE_FCNTL_CKSM_FILE;
use rust_decimal::prelude::ToPrimitive;

use crate::common::{
    MarketConfig, MarketMessage, MarketStream, MicroSec, Order, Trade, FLOOR_SEC, SEC,
};
use crate::exchange::binance::config;

use crossbeam_channel::Receiver;

use super::{has_method, Session};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Runner {
    name: String,
    current_clock: MicroSec,
}

#[pymethods]
impl Runner {
    #[new]
    pub fn new() -> Self {
        Self {
            name: "Runner".to_string(),
            current_clock: 0,
        }
    }

    pub fn back_test(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
        time_from: MicroSec,
        time_to: MicroSec,
    ) -> Result<Py<Session>, PyErr> {
        let stream = Self::open_backtest_stream(&market, time_from, time_to);
        let reciever = stream.reciver;
        self.run(market, &reciever, agent, true, interval_sec, true)
    }

    pub fn dry_run(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
    ) -> Result<Py<Session>, PyErr> {
        Python::with_gil(|py| {
            market.call_method0(py, "start_market_stream").unwrap();
        });

        let stream = Self::get_market_stream(&market);
        let reciever = stream.reciver;
        self.run(market, &reciever, agent, true, interval_sec, false)
    }

    pub fn real_run(
        &mut self,
        market: PyObject,
        agent: &PyAny,
        interval_sec: i64,
        log_memory: bool,
    ) -> Result<Py<Session>, PyErr> {
        let stream = Self::get_market_stream(&market);
        let reciever = stream.reciver;

        Python::with_gil(|py| {
            market.call_method0(py, "start_market_stream").unwrap();
            market.call_method0(py, "start_user_stream").unwrap();
        });

        self.run(market, &reciever, agent, false, interval_sec, false)
    }
}

const WARMUP_STEPS: i64 = 100;

impl Runner {
    pub fn run(
        &mut self,
        market: PyObject,
        receiver: &Receiver<MarketMessage>,
        agent: &PyAny,
        dummy: bool,
        interval_sec: i64,
        log_memory: bool,
    ) -> Result<Py<Session>, PyErr> {
        let result = Python::with_gil(|py| {
            let config = market.getattr(py, "market_config").unwrap();
            let config = config.extract::<MarketConfig>(py).unwrap();

            let mut session = Session::new(market, dummy, &config, None, log_memory);

            if !log_memory {
                // TODO: ERROR handling
                session.open_log(&session.session_name.clone()).unwrap();
            }

            let py_session = Py::new(py, session).unwrap();

            // TODO: implment on_clock;
            let has_on_clock = has_method(agent, "on_clock");
            let has_on_tick = has_method(agent, "on_tick");
            let has_on_update = has_method(agent, "on_update");

            let mut warm_up_loop: i64 = WARMUP_STEPS;

            loop {
                let message = receiver.recv();

                if message.is_err() {
                    log::info!("Data stream is closed {:?}", message);
                    break;
                }

                let message = message.unwrap();

                if dummy {
                    let r = if 0 < warm_up_loop {
                        self.dummy_on_message(
                            &py,
                            agent,
                            &py_session,
                            &message,
                            false,
                            false,
                            false,
                            interval_sec,
                        )
                    } else {
                        self.dummy_on_message(
                            &py,
                            agent,
                            &py_session,
                            &message,
                            has_on_clock,
                            has_on_tick,
                            has_on_update,
                            interval_sec,
                        )
                    };

                    if r.is_err() {
                        return Err(r.unwrap_err());
                    }
                } else {
                    let r = if 0 < warm_up_loop {
                        self.on_message(
                            &py,
                            agent,
                            &py_session,
                            &message,
                            false,
                            false,
                            false,
                            interval_sec,
                        )
                    } else {
                        self.on_message(
                            &py,
                            agent,
                            &py_session,
                            &message,
                            has_on_clock,
                            has_on_tick,
                            has_on_update,
                            interval_sec,
                        )
                    };

                    if r.is_err() {
                        return Err(r.unwrap_err());
                    }
                }

                if message.trade.is_some() {
                    warm_up_loop = warm_up_loop - 1;                    
                }
            }

            Ok(py_session)
        });

        if result.is_err() {
            return Err(result.unwrap_err());
        } else {
            println!("Done running agent");
            Ok(result.unwrap())
        }
    }

    pub fn on_message(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        has_on_clock: bool,
        has_on_tick: bool,
        has_on_update: bool,
        interval_sec: i64,
    ) -> Result<(), PyErr> {
        if has_on_clock && message.trade.is_some() {
            self.on_clock(py, agent, py_session, message, interval_sec)?;
        }

        let mut session = py_session.borrow_mut(*py);
        session.on_message(&message);
        drop(session);

        log::debug!("on_message: {:?}", message);

        if message.account.is_some() {
            let session = py_session.borrow_mut(*py);

            let account = message.account.as_ref().unwrap();
            let py_account = Py::new(*py, account.clone()).unwrap();

            let result = agent.call_method1("on_account_update", (session, py_account));

            if result.is_err() {
                return Err(result.unwrap_err());
            }
        }

        if message.trade.is_some() {
            let trade = message.trade.as_ref().unwrap();

            let session = py_session.borrow_mut(*py);
            if has_on_tick {
                let price = trade.price.to_f64().unwrap();
                let size = trade.size.to_f64().unwrap();

                agent.call_method1("on_tick", (session, trade.order_side, price, size))?;
            }
        }

        if message.order.is_some() {
            let order = message.order.as_ref().unwrap();
            let py_order = Py::new(*py, order.clone()).unwrap();

            let session = py_session.borrow_mut(*py);

            if has_on_update {
                agent.call_method1("on_update", (session, py_order))?;
            }
        }

        Ok(())
    }

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

    pub fn dummy_on_message(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        has_on_clock: bool,
        has_on_tick: bool,
        has_on_update: bool,
        window_sec: i64,
    ) -> Result<(), PyErr> {
        if has_on_clock && message.trade.is_some() {
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

        if has_on_tick && message.trade.is_some() {
            let trade = message.trade.as_ref().unwrap();

            let session = py_session.borrow_mut(*py);

            let price = trade.price.to_f64().unwrap();
            let size = trade.size.to_f64().unwrap();

            agent.call_method1("on_tick", (session, trade.order_side, price, size))?;
        }

        if has_on_update {
            for order in orders {
                let py_order = Py::new(*py, order.clone()).unwrap();
                let session = py_session.borrow_mut(*py);
                agent.call_method1("on_update", (session, py_order))?;
            }
        }

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

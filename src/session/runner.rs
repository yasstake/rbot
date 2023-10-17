use std::borrow::BorrowMut;
use std::f32::consts::E;
use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyObject, Python};
use rusqlite::ffi::SQLITE_FCNTL_CKSM_FILE;
use rust_decimal::prelude::ToPrimitive;

use crate::common::{MarketConfig, MarketMessage, MarketStream, MicroSec, Order, Trade, FLOOR_SEC, SEC};
use crate::exchange::binance::config;

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

    pub fn back_test(&self) {
        // TODO: implement
        // repave
        // start db loop
    }

    pub fn dry_run(&mut self, market: PyObject, agent: &PyAny, interval_sec: i64) -> Result<(), PyErr> {
        // repave
        // start market stream
        self.run(market, agent, true, interval_sec)
    }

    pub fn real_run(&mut self, market: PyObject, agent: &PyAny, interval_sec: i64) -> Result<(), PyErr> {
        // repave
        // start market stream
        // start user stream
        self.run(market, agent, false, interval_sec)
    }

    pub fn run(&mut self, market: PyObject, agent: &PyAny, dummy: bool, interval_sec: i64) -> Result<(), PyErr> {
        let stream = Self::get_market_stream(&market);
        let stream = stream.reciver;

        let result = Python::with_gil(|py| {
            let config = market.getattr(py, "market_config").unwrap();
            let config = config.extract::<MarketConfig>(py).unwrap();
            // TODO: implement reflet session name based on agent name
            let mut session = Session::new(market, dummy, &config, None);
            session.open_log(&session.session_name.clone());
            let py_session = Py::new(py, session).unwrap();

            // TODO: implment on_clock;
            let has_on_clock = has_method(agent, "on_clock");
            let has_on_tick = has_method(agent, "on_tick");
            let has_on_update = has_method(agent, "on_update");

            loop {
                let message = stream.recv();

                if message.is_err() {
                    log::error!("Error in stream.recv: {:?}", message);
                    break;
                }

                let message = message.unwrap();

                if dummy {
                    let r = self.dummy_on_message(
                        &py,
                        agent,
                        &py_session,
                        &message,
                        has_on_clock,
                        has_on_tick,
                        has_on_update,
                        interval_sec
                    );

                    if r.is_err() {
                        return Err(r.unwrap_err());
                    }
                } else {
                    let r = self.on_message(
                        &py,
                        agent,
                        &py_session,
                        &message,
                        has_on_clock,
                        has_on_tick,
                        has_on_update,
                        interval_sec                        
                    );

                    if r.is_err() {
                        return Err(r.unwrap_err());
                    }
                }
            }
            Ok(())
        });

        if result.is_err() {
            return Err(result.unwrap_err());
        } else {
            println!("Done running agent");
            Ok(())
        }
    }
}

impl Runner {
    pub fn on_message(
        self: &mut Self,
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        has_on_clock: bool,
        has_on_tick: bool,
        has_on_update: bool,
        interval_sec: i64
    ) -> Result<(), PyErr> {

        if has_on_clock && message.trade.is_some() {
            self.on_clock(py, agent, py_session, message, interval_sec)?;
        }

        let mut session = py_session.borrow_mut(*py);
        session.on_message(&message);
        drop(session);

        if has_on_clock {
            // TODO: do something for clock intervall
        }

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
        interval_sec: i64
    ) -> Result<(), PyErr>
    {
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
        window_sec: i64
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
    pub fn get_market_stream(market: &PyObject) -> MarketStream {
        Python::with_gil(|py| {
            let stream = market.getattr(py, "channel").unwrap();
            let stream = stream.extract::<MarketStream>(py).unwrap();

            stream
        })
    }
}

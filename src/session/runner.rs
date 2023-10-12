use std::borrow::BorrowMut;
use std::f32::consts::E;
use std::sync::Arc;
use std::thread;

use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyObject, Python};
use rusqlite::ffi::SQLITE_FCNTL_CKSM_FILE;
use rust_decimal::prelude::ToPrimitive;

use crate::common::{MarketMessage, MarketStream, MicroSec, Order, Trade, MarketConfig};
use crate::exchange::binance::config;

use super::{has_method, Session};

#[pyclass]
#[derive(Debug, Clone)]
pub struct Runner {
    name: String,
}

#[pymethods]
impl Runner {
    #[new]
    pub fn new() -> Self {
        Self {
            name: "Runner".to_string(),
        }
    }

    pub fn back_test(&self) {
        // TODO: implement
    }

    pub fn dry_run(&self) {
        // TODO: implement
    }

    pub fn run(
        &mut self,
        market: PyObject,
        // py_session: PyObject,
        agent: &PyAny,
    ) -> Result<(), PyErr> {
        let stream = Self::get_market_stream(&market);
        let stream = stream.reciver;

        let result = Python::with_gil(|py| {
            let config = market.getattr(py, "market_config").unwrap();
            let config = config.extract::<MarketConfig>(py).unwrap();
            // TODO: implement reflet session name based on agent name
            let mut session = Session::new(market, false, &config, None);
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

                let r = Runner::on_message(
                    &py,
                    agent,
                    &py_session,
                    &message,
                    has_on_clock,
                    has_on_tick,
                    has_on_update,
                );

                if r.is_err() {
                    return Err(r.unwrap_err());
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
        py: &Python,
        agent: &PyAny,
        py_session: &Py<Session>,
        message: &MarketMessage,
        has_on_clock: bool,
        has_on_tick: bool,
        has_on_update: bool,
    ) -> Result<(), PyErr> {
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

                agent.call_method1(
                    "on_tick",
                    (session, trade.order_side, price, size)
                )?;
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

        if has_on_clock {
            // TODO: do something for clock intervall
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

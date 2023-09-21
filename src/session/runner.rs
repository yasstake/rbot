use std::borrow::BorrowMut;
use std::f32::consts::E;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyObject, Python};

use crate::common::{MarketMessage, MarketStream, MicroSec, Order, Trade};

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

    pub fn run(
        &mut self,
        market: PyObject,
        // py_session: PyObject,
        agent: &PyAny,
    ) -> Result<(), PyErr> {
        let stream = Self::get_market_stream(&market);
        let stream = stream.reciver;

        let result = Python::with_gil(|py| {
            let py_session = Py::new(py, Session::new(market, false)).unwrap();

            loop {
                let message = stream.recv();

                if message.is_err() {
                    log::error!("Error in stream.recv: {:?}", message);
                    break;
                }

                let message = message.unwrap();

                let r = Runner::on_message(&py, agent, &py_session, &message);

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
    ) -> Result<(), PyErr> {
        let mut session = py_session.borrow_mut(*py);

        session.on_message(&message);
        drop(session);

        if message.trade.is_some() {
            let session = py_session.borrow_mut(*py);

            let trade = message.trade.as_ref().unwrap();

            let result = agent.call_method1(
                "on_tick",
                (session, trade.order_side, trade.price, trade.size),
            );

            if result.is_err() {
                return Err(result.unwrap_err());
            }
        }
        
        if message.order.is_some() {
            let session = py_session.borrow_mut(*py);    

            let order = message.order.as_ref().unwrap();
            let py_order = Py::new(*py, order.clone()).unwrap();

            let result = agent.call_method1(
                "on_update",
                (session, py_order)
            );

            if result.is_err() {
                return Err(result.unwrap_err());
            }
        }
        
        Ok(())
    }

    pub fn get_market_stream(market: &PyObject) -> MarketStream {
        Python::with_gil(|py| {
            let stream = market.getattr(py, "channel").unwrap();
            let stream = stream.extract::<MarketStream>(py).unwrap();

            stream
        })
    }

    /*
    pub fn on_clock(agent: &PyAny, session: Session, time: MicroSec) -> Result<Session, PyErr> {
        let result = agent.call_method1("on_clock", (session, time));

        if result.is_err() {
            return Err(result.unwrap_err());
        }

    }

    pub fn on_update<'a>(agent: &'a PyAny, session: Session, order: Order) -> Result<Session, PyErr> {
        let result = agent.call_method1("on_update", (session, order));

        if result.is_err() {
            return Err(result.unwrap_err());
        }

        Ok(session)
    }
    */
}

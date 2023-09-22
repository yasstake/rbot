// Copyright(c) 2022. yasstake. All rights reserved.

use std::io::{stdout, Write};

use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyResult, Python};
use rusqlite::{params_from_iter};

use crate::{
    common::{
        order::{log_order_result, make_log_buffer, OrderResult, OrderSide, OrderStatus, Trade},
        time::{time_string, MicroSec, CEIL, FLOOR, MICRO_SECOND, NOW, HHMM},
    },
    db::{sqlite::TradeTableQuery},
    sim::session::DummySession, exchange::open_db,
};

#[pyclass(name = "_BackTester")]
pub struct BackTester {
    #[pyo3(get)]
    exchange_name: String,
    #[pyo3(get)]
    market_name: String,
    #[pyo3(get)]
    agent_on_tick: bool,
    #[pyo3(get)]
    agent_on_clock: bool,
    #[pyo3(get)]
    agent_on_update: bool,
    #[pyo3(get)]
    size_in_price_currency: bool,
    #[pyo3(get)]
    clock_interval: i64,
    #[pyo3(get, set)]
    maker_fee_rate: f64,
    #[pyo3(get)]
    last_run_start: MicroSec,
    #[pyo3(get)]
    last_run_end: MicroSec,
    #[pyo3(get)]
    last_run_record: MicroSec,
    last_clock: MicroSec,
    #[pyo3(get)]    
    on_tick_count: i64,
    #[pyo3(get)]    
    on_clock_count: i64,
    #[pyo3(get)]    
    on_update_count: i64,
}

#[pymethods]
impl BackTester {
    #[new]
    pub fn new(exchange_name: &str, market_type: &str, market_name: &str, size_in_price_currency: bool) -> Self {
        return BackTester {
            exchange_name: exchange_name.to_string(),
            market_name: market_name.to_string(),
            agent_on_tick: false,
            agent_on_clock: false,
            agent_on_update: false,
            size_in_price_currency,
            clock_interval: 0,
            maker_fee_rate: 0.01 * 0.1,
            last_run_start: 0,
            last_run_end: 0,
            last_run_record: 0,
            last_clock: 0,
            on_tick_count: 0,
            on_clock_count: 0,
            on_update_count: 0,
        };
    }

    #[pyo3(signature=(agent, from_time = 0, to_time = 0))]
    pub fn run(
        &mut self,
        agent: &PyAny,
        from_time: MicroSec,
        to_time: MicroSec,
    ) -> PyResult<Vec<OrderResult>> {
        self.agent_on_tick = self.has_want_event(agent, "on_tick");
        self.agent_on_clock = self.has_want_event(agent, "on_clock");
        self.agent_on_update = self.has_want_event(agent, "on_update");

        log::debug!("want on tick  {:?}", self.agent_on_tick);
        log::debug!("want on clock {:?}", self.agent_on_clock);
        log::debug!("want on event {:?}", self.agent_on_update);

        self.clock_interval = self.agent_clock_interval(agent);
        log::debug!("clock interval {:?}", self.clock_interval);

        let mut db = open_db(
            self.exchange_name.as_str(), 
            self.market_name.as_str(),
            self.market_name.as_str());
        
        db.reset_cache_duration();

        //let mut statement = db.select_all_statement();
        let (mut statement, param) = db.select_statement(from_time, to_time);

        let mut order_history: Vec<OrderResult> = make_log_buffer();

        let iter = statement
            .query_map(params_from_iter(param.iter()), |row| {
                let bs_str: String = row.get_unwrap(1);
                let bs = OrderSide::from_str_default(bs_str.as_str());

                Ok(Trade {
                    time: row.get_unwrap(0),
                    price: row.get_unwrap(2),
                    size: row.get_unwrap(3),
                    order_side: bs,
                    id: row.get_unwrap(4),
                })
            })
            .unwrap();

        let session = DummySession::new(
            self.exchange_name.as_str(),
            self.market_name.as_str(),
            self.size_in_price_currency,
        );

        let mut loop_start_hour = 0;
        let mut loop_count = 0;

        let mut last_print_hour = 0;
        let mut last_print_time = NOW();
        let mut average_speed = 0;
        let mut last_record_no: i64 = 0;
        let mut rec_per_sec = 0;
        const PRINT_INTERVAL: i64 = 60 * 60 * 6;
        let mut current_time: MicroSec = 0;

        return Python::with_gil(|py| {
            let py_session = Py::new(py, session).unwrap();

            for trade in iter {
                match trade {
                    Ok(t) => {
                        // use of unsafe block
                        // see. https://pyo3.rs/main/memory.html
                        let pool = unsafe {py.new_pool()};
                        let py = pool.python();

                        current_time = t.time;
                        // TODO: Implement skip first 100 ticks

                        // TODO: error handling
                        let result = self.process_trade(py, &agent, &t, &py_session, &mut order_history);

                        match result {
                            Ok(_) => {

                            },
                            Err(e) => {
                                return Err(e);
                            }
                        }

                        loop_count += 1;
                        let current_hour = FLOOR(current_time, PRINT_INTERVAL); // print every 6 hour
                        if last_print_hour != current_hour {
                            if loop_start_hour == 0 {
                                loop_start_hour = current_hour;
                                self.last_run_start = current_time;
                            }
                            let duration = NOW() - last_print_time;

                            average_speed =
                                ((PRINT_INTERVAL * MICRO_SECOND) / duration + average_speed * 4) / 5;

                            rec_per_sec = ((loop_count - last_record_no) * MICRO_SECOND / duration + rec_per_sec * 4) / 5;

                            print!("\rBack testing... {:<.16} /{:>4}[days] / rec={:>10} / on_tick={:>6} / on_clock={:>6} / on_update={:>4} /{:>8} xSpeed / {:>8}[rec/sec]", 
                                        time_string(t.time), (current_hour - loop_start_hour)/ HHMM(24, 0), 
                                        loop_count,
                                        self.on_tick_count, self.on_clock_count, self.on_update_count, average_speed, rec_per_sec);
                            let _ = stdout().flush();

                            last_print_time = NOW();
                            last_print_hour = current_hour;
                            last_record_no = loop_count;
                        }
                    }
                    Err(e) => {
                        log::warn!("err {}", e);
                        // TODO: Error handling
                        // return Err(e);
                        // return Err(PyError::from(e.to_string()));
                    }
                }
            }

            // self.last_run_end = session.current_timestamp;
            self.last_run_record = loop_count;
            self.last_run_end = current_time;

            return Ok(order_history);
        });
    }
}




impl BackTester {
    fn process_trade(
        &mut self,
        py: Python,
        agent: &PyAny,
        trade: &Trade,
        py_session: &Py<DummySession>,
        order_history: &mut Vec<OrderResult>,
    ) -> Result<(), PyErr> {

            if self.agent_on_clock {
                let current_clock = CEIL(trade.time, self.clock_interval);
                if current_clock != self.last_clock {
                    let mut session = py_session.borrow_mut(py);
                    session.current_timestamp = current_clock;
                    drop(session);

                    let result = self.clock(&py_session, agent, current_clock);
                    match result {
                        Ok(_) => {
                            self.on_clock_count += 1;
                        }
                        Err(e) => {
                            let trace = e.traceback(py);

                            if trace.is_some() {
                                log::error!("{:?}", trace);
                            }

                            return Err(e);
                        }
                    };
                    self.last_clock = current_clock;
                }
            }

            let mut tick_result: Vec<OrderResult> = vec![];

            let mut session = py_session.borrow_mut(py);
            session.process_trade(trade, &mut tick_result);
            drop(session);

            if self.agent_on_tick {
                let result = self.tick(&py_session, agent, trade);

                match result {
                    Ok(_) => {
                        self.on_tick_count += 1;
                    }
                    Err(e) => {
                        let trace = e.traceback(py);

                        if trace.is_some() {
                            log::error!("{:?}", trace);
                        }
                        return Err(e);
                    }
                }
            }

            for mut r in tick_result {
                r = self.calc_profit(r);

                if self.agent_on_update {
                    let result = self.update(&py_session, agent, r.update_time, &r);

                    match result {
                        Ok(_) => {
                            self.on_update_count += 1;
                        }
                        Err(e) => {
                            let trace = e.traceback(py);

                            if trace.is_some() {
                                log::error!("{:?}", trace);
                            }
                            return Err(e);
                        }
                    }
                }
                log_order_result(order_history, r);
            }

            return Ok(());

    }




    fn tick(
        &mut self,
        session: &Py<DummySession>,
        agent: &PyAny,
        trade: &Trade,
    ) -> Result<(), PyErr> {

        let result = agent.call_method1(
            "_on_tick",
            (
                trade.time,
                session,
                trade.order_side.to_string(),
                trade.price,
                trade.size,
            ),
        );

        match result {
            Ok(_ok) => {

            }
            Err(e) => {
                println!("call Agent.on_tick error {:?}", e);
                log::error!("Call on_tick Error {:?}", e);
                return Err(e);
            }
        }

        drop(trade);
        drop(agent);
        drop(session);

        return Ok(());
    }

    fn clock(
        &mut self,
        session: &Py<DummySession>,
        agent: &PyAny,
        clock: i64,
    ) -> Result<(), PyErr> {
        let result = agent.call_method1("_on_clock", (clock, session));
        match result {
            Ok(_ok) => {
                //
            }
            Err(e) => {
                println!("call Agent.on_clock error {:?}", e);
                log::error!("Call on_clock Error {:?}", e);
                return Err(e);
            }
        }
        
        return Ok(());
    }

    fn update(
        &mut self,
        session: &Py<DummySession>,
        agent: &PyAny,
        time: MicroSec,
        r: &OrderResult,
    ) -> Result<(), PyErr> {
        let result = agent.call_method1("_on_update", (time, session, r.clone()));

        match result {
            Ok(_ok) => {
                //
            }
            Err(e) => {
                println!("call Agent.on_update error {:?}", e);
                log::error!("Call on_update Error {:?}", e);
                return Err(e);
            }
        }

        return Ok(());
    }

    fn has_want_event(&self, agent: &PyAny, event_function_name: &str) -> bool {
        if agent.dir().contains(event_function_name).unwrap() {
            return true;
        }

        return false;
    }

    // TODO: エラー処理
    fn agent_clock_interval(&self, agent: &PyAny) -> i64 {
        let interval_sec_py = agent.call_method0("clock_interval").unwrap();
        let interval_sec = interval_sec_py.extract::<i64>().unwrap();

        return interval_sec;
    }

    // トータルだけ損益を計算する。
    // TODO: MakerとTakerでも両率を変更する。
    fn calc_profit(&self, mut order_result: OrderResult) -> OrderResult {
        if order_result.status == OrderStatus::OpenPosition
            || order_result.status == OrderStatus::ClosePosition
        {
            order_result.fee = order_result.order_foreign_size * self.maker_fee_rate;
            order_result.total_profit = order_result.profit - order_result.fee;

            match order_result.order_side {
                OrderSide::Buy => {
                    order_result.position_change = order_result.order_home_size;
                }
                OrderSide::Sell => {
                    order_result.position_change = -(order_result.order_home_size);
                }
                OrderSide::Unknown => {
                    log::error!("unknown status {:?}", order_result);
                }
            }
        }

        order_result
    }
}

#[cfg(test)]
mod back_testr_test {
    use super::*;

    #[test]
    fn test_session_copy() {
        pyo3::prepare_freethreaded_python();

        #[derive(Clone, Debug)]
        #[pyclass]
        struct S {
            pub a: i64,
        }
        
        let session = S { a: 1};

        Python::with_gil(|py| {
            println!("{:?}", session);
            let py_session = Py::new(py, session).unwrap();

            for i in 1..100000 {
            // println!("{:?}", py_session);
            let mut session = py_session.borrow_mut(py);
            //let mut session = py_session.extract::<S>(py).unwrap();
            session.a += 1;

            drop(session);

            let _py_session2 = py_session.borrow(py);
            if i % 1000 == 0 {
//                println!("{:?}", py_session2);
            }
            //            let session2 = py_session.extract::<S>(py).unwrap();
            //println!("{:?}", session2);

            //drop(py_session);

            //            println!("{:?}", session2);
            }
        });
    }
}

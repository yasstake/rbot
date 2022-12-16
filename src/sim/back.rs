use std::io::{stdout, Write};

use chrono::OutOfRangeError;
use polars::export::arrow::bitmap::or;
use pyo3::{pyclass, pymethods, Py, PyAny, PyErr, PyResult, Python};
use rusqlite::{params, params_from_iter};

use crate::{
    common::{
        order::{log_order_result, make_log_buffer, OrderResult, OrderSide, OrderStatus, Trade},
        time::{time_string, MicroSec, CEIL, FLOOR, MICRO_SECOND, NOW},
    },
    db::open_db,
    sim::session::DummySession,
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
    #[pyo3(get, set)]
    maker_fee_rate: f64,
    #[pyo3(get)]
    last_run_start: MicroSec,
    #[pyo3(get)]
    last_run_end: MicroSec,
    #[pyo3(get)]
    last_run_record: MicroSec,
}

#[pymethods]
impl BackTester {
    #[new]
    pub fn new(exchange_name: &str, market_name: &str, size_in_price_currency: bool) -> Self {
        return BackTester {
            exchange_name: exchange_name.to_string(),
            market_name: market_name.to_string(),
            agent_on_tick: false,
            agent_on_clock: false,
            agent_on_update: false,
            size_in_price_currency,
            maker_fee_rate: 0.01 * 0.1,
            last_run_start: 0,
            last_run_end: 0,
            last_run_record: 0,
        };
    }

    #[args(from_time = 0, to_time = 0)]
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

        let clock_interval = self.clock_interval(agent);
        log::debug!("clock interval {:?}", clock_interval);

        let mut db = open_db(self.exchange_name.as_str(), self.market_name.as_str());
        db.reset_cache_duration();

        //let mut statement = db.select_all_statement();
        let (mut statement, param) = db.select_statement(from_time, to_time);

        let mut order_history: Vec<OrderResult> = make_log_buffer();

        let r = Python::with_gil(|py| {
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

            let mut session = DummySession::new(
                self.exchange_name.as_str(),
                self.market_name.as_str(),
                self.size_in_price_currency,
            );
            let mut s = Py::new(py, session).unwrap();
            let mut last_clock: i64 = 0;

            // TODO: change hardcording its time.
            let mut skip_tick = 100;

            let mut loop_count = 0;
            let mut on_tick_count = 0;
            let mut on_clock_count = 0;
            let mut on_update_count = 0;

            let mut last_print_hour = 0;
            let mut last_print_time = NOW();
            let mut average_speed = 0;

            for trade in iter {
                match trade {
                    Ok(t) => {
                        if skip_tick == 0 {
                            loop_count += 1;

                            if self.agent_on_clock {
                                let current_clock = CEIL(t.time, clock_interval);
                                if current_clock != last_clock {
                                    session = s.extract::<DummySession>(py).unwrap();
                                    session.current_timestamp = current_clock;
                                    s = Py::new(py, session).unwrap();

                                    match self.clock(s, agent, current_clock) {
                                        Ok(session) => {
                                            on_clock_count += 1;
                                            s = session;
                                        }
                                        Err(e) => {
                                            let trace = e.traceback(py);

                                            if trace.is_some() {
                                                log::error!("{:?}", trace);
                                            }

                                            return Err(e);
                                        }
                                    };
                                    last_clock = current_clock;
                                }
                            }
                        } else {
                            log::debug!("skip {}/{}", time_string(t.time), skip_tick);
                            if skip_tick == 1 {
                                log::debug!("start {}/{}", time_string(t.time), t.time);
                                self.last_run_start = t.time;
                            }
                            skip_tick -= 1;
                        }

                        session = s.extract::<DummySession>(py).unwrap();

                        let mut tick_result: Vec<OrderResult> = vec![];

                        session.process_trade(&t, &mut tick_result);
                        s = Py::new(py, session).unwrap();

                        if self.agent_on_tick {
                            match self.tick(s, agent, &t) {
                                Ok(session) => {
                                    on_tick_count += 1;
                                    s = session;
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
                            // TODO calc fee and profit
                            r = self.calc_profit(r);

                            if self.agent_on_update {
                                match self.update(s, agent, r.update_time, r.clone()) {
                                    Ok(session) => {
                                        on_update_count += 1;
                                        s = session;
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
                            log_order_result(&mut order_history, r);
                        }

                        const PRINT_INTERVAL:i64 = 60*60*6;
                        let current_hour = FLOOR(t.time, PRINT_INTERVAL);  // print every 6 hour
                        if last_print_hour != current_hour {
                            let duration = NOW() - last_print_time;

                            average_speed = ((PRINT_INTERVAL * MICRO_SECOND)/duration + average_speed * 4)/ 5;

                            print!("\rBack testing... {:<.16} / rec={:>10} / on_tick={:>6} / on_clock={:>6} / on_update={:>4} /{:>8} xSpeed", 
                                    time_string(t.time), loop_count,
                                    on_tick_count, on_clock_count, on_update_count, average_speed);
                            let _ = stdout().flush();

                            last_print_time = NOW();                            
                            last_print_hour = current_hour;
                        }
                    }
                    Err(e) => {
                        log::warn!("err {}", e);
                        // return Err(e.to_string());
                    }
                }
            }
            session = s.extract::<DummySession>(py).unwrap();
            self.last_run_end = session.current_timestamp;

            self.last_run_record = loop_count;

            Ok(order_history)
        });

        return r;
    }
}

impl BackTester {
    fn tick(
        &mut self,
        session: Py<DummySession>,
        agent: &PyAny,
        trade: &Trade,
    ) -> Result<Py<DummySession>, PyErr> {
        let result = agent.call_method1(
            "_on_tick",
            (
                trade.time,
                &session,
                trade.order_side.to_string(),
                trade.price,
                trade.size,
            ),
        );
        match result {
            Ok(_ok) => {
                //
            }
            Err(e) => {
                println!("call Agent.on_tick error {:?}", e);
                log::error!("Call on_tick Error {:?}", e);
                return Err(e);
            }
        }

        return Ok(session);
    }

    fn clock(
        &mut self,
        session: Py<DummySession>,
        agent: &PyAny,
        clock: i64,
    ) -> Result<Py<DummySession>, PyErr> {
        let result = agent.call_method1("_on_clock", (clock, &session));
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

        return Ok(session);
    }

    fn update(
        &mut self,
        session: Py<DummySession>,
        agent: &PyAny,
        time: MicroSec,
        r: OrderResult,
    ) -> Result<Py<DummySession>, PyErr> {
        let result = agent.call_method1("_on_update", (time, &session, r));

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

        return Ok(session);
    }

    fn has_want_event(&self, agent: &PyAny, event_function_name: &str) -> bool {
        if agent.dir().contains(event_function_name).unwrap() {
            return true;
        }

        return false;
    }

    fn clock_interval(&self, agent: &PyAny) -> i64 {
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
    use pyo3::prelude::PyModule;

    #[test]
    fn test_create() {
        let _b = BackTester::new("FTX", "BTC-PERP", false);
    }

    #[test]
    fn test_run() {
        let b = &mut BackTester::new("BN", "BTCBUSD", false);

        Python::with_gil(|py| {
            let agent_class = PyModule::from_code(
                py,
                r#"
class Agent:
    def __init__():
        pass

    def on_tick(session, time, side, price, size):
        print(time, side, price, size)
"#,
                "agent.py",
                "agent",
            )
            .unwrap()
            .getattr("Agent")
            .unwrap();

            let agent = agent_class.call0().unwrap();

            b.run(agent, 0, 0);
        });
    }
}

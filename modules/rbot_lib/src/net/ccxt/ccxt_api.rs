use pyo3::{types::{PyAnyMethods as _, PyBool, PyModule}, Py, PyAny, Python};


const PY_CCXT: &str = include_str!("./ccxt_api.py");


pub fn create_ccxt_none() -> Py<PyAny> {
    let none = Python::with_gil(|py|{
        Python::None(py)
    });

    none
}

pub fn create_ccxt_handle(exchange: &str, key: &str, secret:&str, production: bool) -> Py<PyAny> {
    Python::with_gil(|py| {
        let py_module =
            PyModule::from_code_bound(py, PY_CCXT, "ccxt_api.py", "ccxt_api");

        if py_module.is_err() {
            log::error!("ccxt_api class create error")
        }

        let py_module = py_module.unwrap();

        let ccxt_api = py_module.getattr("CCXTApi").unwrap();

        let production = PyBool::new_bound(py, production);

        let params = (exchange, key, secret, production);
        
        let ccxt_api = ccxt_api.call1(params).unwrap();

        ccxt_api.into()
    })
}


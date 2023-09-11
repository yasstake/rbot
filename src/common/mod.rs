// Copyright(c) 2022. yasstake. All rights reserved.

use pyo3::{pyfunction, PyErr};
use log::{LevelFilter};
use simple_logger::SimpleLogger;

pub mod time;
pub mod order;

#[pyfunction]
/// Initializes the logger with a warning level filter.
pub fn init_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Warn).init();
}

#[pyfunction]
/// Initializes a debug logger with the `Debug` log level.
pub fn init_debug_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
}

/// Converts a `Result<T, String>` to a `Result<T, PyErr>` by mapping the `Err` variant to a `PyErr`.
pub fn convert_pyresult<T>(T: Result<T, String>) -> Result<T, PyErr> {
    match T {
        Ok(t) => Ok(t),
        Err(e) => Err(pyo3::exceptions::PyException::new_err(e)),
    }
}

#[cfg(test)]
mod test_common_mod {
    use super::*;
    #[test]
    fn test_init_log() {
        init_log();
        init_debug_log()
    }
}
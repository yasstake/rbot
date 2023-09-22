// Copyright(c) 2022. yasstake. All rights reserved.

use pyo3::{pyfunction, PyErr};
use log::{LevelFilter};
use simple_logger::SimpleLogger;



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
pub fn convert_pyresult<T1, T2>(r: Result<T1, String>) -> Result<T2, PyErr>
    where T2: From<T1>
{
    match r {
        Ok(t) => {
            let r:T2 = t.into();            
            Ok(r)
        },
        Err(e) => Err(pyo3::exceptions::PyException::new_err(e)),
    }
}

/*
trait Representable {
    fn __str__(&self) -> String
    where 
        Self: serde::Serialize,
    {
        serde_json::to_string(&self).unwrap()
    }

    fn __repr__(&self) -> String
    where 
        Self: serde::Serialize,
    {
        self.__str__()
    }
}
*/

/// implement macro for Representable
/// 
#[macro_export]
macro_rules! json_struct {
    ($name:ident { $($field:ident : $type:ty),* $(,)? }) => {
        #[derive(Debug, Serialize)]
        struct $name {
            $($field : $type),*
        }

        impl $name {
            fn __str__(&self) -> String {
                serde_json::to_string(&self).unwrap()
            }
        }
    };
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
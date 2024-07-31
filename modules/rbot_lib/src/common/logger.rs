// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use std::io::Write;
use std::sync::Once;
use pyo3::{pyfunction, PyErr};
use env_logger::Env; 

static INIT: Once = Once::new();

#[pyfunction]
/// Initializes the logger with a warning level filter.
pub fn init_log() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("warn")).init();
    });
}

#[pyfunction]
/// Initializes a debug logger with the `Debug` log level.
pub fn init_debug_log() {
    INIT.call_once(|| {
        env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();
    });
}

pub fn flush_log() {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
}


/// Converts a `Result<T, String>` to a `Result<T, PyErr>` by mapping the `Err` variant to a `PyErr`.
/// TODO: replace to anyhow
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

pub fn convert_pyresult_vec<T1, T2>(r: Result<Vec<T1>, String>) -> Result<Vec<T2>, PyErr>
    where T2: From<T1>
{
    let mut v: Vec<T2> = vec![];

    match r {
        Ok(items) => {
            for i in items {
                let r:T2 = i.into();            
                v.push(r);
            }
        }
        Err(e) => return Err(pyo3::exceptions::PyException::new_err(e)),
    }

    Ok(v)
}



//TODO: remove unused macro
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
        flush_log();
    }
}
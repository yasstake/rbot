// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use std::env::VarError;

use hmac::digest::{consts::False, typenum::NotEq};
use pyo3::{types::{PyAnyMethods, PyModule}, Python};

const RBOT_MULTICAST_ADDR: &str = "224.0.0.51";
const DEFAULT_MULTICAST_PORT: i64 = 3001;

/// Get the root directory of the rbot database.
pub fn env_rbot_db_root() -> Result<String, VarError> {
    std::env::var("RBOT_DB_ROOT")
}

/// Get the multicast address of the rbot.
pub fn env_rbot_multicast_addr() -> String {
    let addr = std::env::var("RBOT_MULTICAST_ADDR");
    if addr.is_err() {
        log::info!(
            "RBOT_MULTICAST_ADDR is not set, use default address {}.",
            RBOT_MULTICAST_ADDR
        );
        return RBOT_MULTICAST_ADDR.to_string();
    }

    addr.unwrap()
}

/// Get the multicast port of the rbot.
pub fn env_rbot_multicast_port() -> i64 {
    let port = std::env::var("RBOT_MULTICAST_PORT");
    if port.is_err() {
        log::info!(
            "RBOT_MULTICAST_PORT is not set, use default port {}.",
            DEFAULT_MULTICAST_PORT
        );
        return DEFAULT_MULTICAST_PORT;
    }
    let port = port.unwrap().parse::<i64>();
    if port.is_err() {
        log::warn!("RBOT_MULTICAST_PORT is not a number {}", port.unwrap_err());
        return DEFAULT_MULTICAST_PORT;
    }

    port.unwrap()
}

pub fn is_notebook() -> bool {
    Python::with_gil(|py| {
        let notebook = PyModule::from_code_bound(
            py,
            r#"
def is_notebook() -> bool:
    try:
        shell = get_ipython().__class__.__name__
        if shell == "ZMQInteractiveShell":
            return True
        else:
            return False

    except NameError:
        return False            
            "#,
            "notebook.py",
            "notebook",
        );

        if notebook.is_err() {
            log::debug!("pymodule import error");
            return false;
        }
        let notebook = notebook.unwrap();

        let is_note = notebook.call_method0("is_notebook");
        if is_note.is_err() {
            log::debug!("call python error");
            return false;
        }
        
        let r = is_note.unwrap().extract::<bool>();
        if r.is_err() {
            log::debug!("result extract error");
            return false;
        }

        log::debug!("all pass result can be trudted.");

        return r.unwrap();
    })
}



#[cfg(test)]
mod test_env {
    use arrow::compute::is_not_null;

    use crate::common::init_debug_log;

    use super::is_notebook;

    #[test]
    fn test_is_notebook() {
        init_debug_log();

        let note = is_notebook();

        if note {
            log::debug!("NOTEBOOK");
        }
        else {
            log::debug!("SHELL");
        }
    }
}

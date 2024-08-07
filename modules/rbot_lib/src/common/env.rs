// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABSOLUTELY NO WARRANTY.

use directories::UserDirs;
use env_file_reader::{self, read_file};
use std::env::{self, VarError};

use hmac::digest::{consts::False, typenum::NotEq};
use pyo3::{
    types::{PyAnyMethods, PyModule},
    Python,
};

use super::{SecretString, ServerConfig};

const RBOT_MULTICAST_ADDR: &str = "224.0.0.51";
const DEFAULT_MULTICAST_PORT: i64 = 3001;

/// Get the root directory of the rbot database.
pub fn env_rbot_db_root() -> Result<String, VarError> {
    std::env::var("RBOT_DB_ROOT")
}

const RBOT_ENV_DIR: &str = ".rusty-bot";
const API_KEY: &str = "API_KEY";
const API_SECRET: &str = "API_SECRET";

fn env_reader(exchange_name: &str, key: &str) -> String {
    if let Ok(v) = env::var(format!("{}_{}", exchange_name, key)) {
        v
    } else {
        "".to_string()
    }
}

fn test_extension(production: bool) -> String {
    if production {
        "".to_string()
    } else {
        "_TEST".to_string()
    }
}

fn dot_env_reader(exchange_name: &str, production: bool, key: &str) -> String {
    let user_dir = UserDirs::new().unwrap();
    let home_dir = user_dir.home_dir();

    let rbot_dir = home_dir.join(RBOT_ENV_DIR);

    let file_name = format!("{}{}.env", exchange_name, test_extension(production));
    let env_file = rbot_dir.join(file_name);

    // if not file exist. return env file
    if !env_file.exists() {
        return env_reader(&exchange_name, key);
    }

    let variables = read_file(env_file);

    if variables.is_err() {
        return "".to_string();
    }

    let variables = variables.unwrap();

    let value = variables.get(key);

    if value.is_none() {
        return "".to_string();
    }

    return value.unwrap().clone();
}

pub fn env_api_key(exchange_name: &str, production: bool) -> SecretString {
    let key = dot_env_reader(exchange_name, production, API_KEY);

    if key == "" {
        println!(
            "API KEY environment variable [{}_API_KEY{}] is not set",
            exchange_name,
            test_extension(production)
        );
        log::warn!(
            "API KEY environment variable [{}_API_KEY{}] is not set",
            exchange_name,
            test_extension(production)
        );
    }

    SecretString::new(&key)
}

pub fn env_api_secret(exchange_name: &str, production: bool) -> SecretString {
    let secret = &dot_env_reader(exchange_name, production, API_SECRET);

    if secret == "" {
        println!(
            "API SECRET environment variable [{}_API_SECRET{}] is not set",
            exchange_name,
            test_extension(production)
        );
        log::warn!(
            "API SECRET environment variable [{}_API_SECRET{}] is not set",
            exchange_name,
            test_extension(production)
        );
    }

    SecretString::new(&secret)
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
import sys

def is_notebook() -> bool:
    if 'google.colab' in sys.modules:
        return True               

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
        } else {
            log::debug!("SHELL");
        }
    }
}

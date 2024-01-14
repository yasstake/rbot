use std::env::{var, VarError};

pub fn env_rbot_db_root() -> Result<String, VarError> {
    std::env::var("RBOT_DB_ROOT")
}

const DEFAULT_PORT_NUM: i64 = 10001;

pub fn env_rbot_port_base() -> i64 {
    let port_base = std::env::var("RBOT_PORT_BASE");
    if port_base.is_err() {
        log::warn!("RBOT_PORT_BASE is not set");
        return DEFAULT_PORT_NUM;
    }
    let port_base = port_base.unwrap().parse::<i64>();
    if port_base.is_err() {
        log::warn!("RBOT_PORT_BASE is not a number {}", port_base.unwrap_err());
        return DEFAULT_PORT_NUM;
    }

    port_base.unwrap()
}

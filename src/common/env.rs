use std::env::VarError;

pub fn env_rbot_db_root() -> Result<String, VarError> {
    std::env::var("RBOT_DB_ROOT")
}

// TODO: High port を使う　49152～65535
const DEFAULT_PORT_NUM: i64 = 51000;

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

const RBOT_MULTICAST_ADDR: &str = "224.0.0.51";
pub fn env_rbot_multicast_addr() -> String {
    let addr = std::env::var("RBOT_MULTICAST_ADDR");
    if addr.is_err() {
        log::warn!("RBOT_MULTICAST_ADDR is not set");
        return RBOT_MULTICAST_ADDR.to_string();
    }

    addr.unwrap()
}

pub fn env_rbot_multicast_port() -> i64 {
    let port = std::env::var("RBOT_MULTICAST_PORT");
    if port.is_err() {
        log::warn!("RBOT_MULTICAST_PORT is not set");
        return 3001;
    }
    let port = port.unwrap().parse::<i64>();
    if port.is_err() {
        log::warn!("RBOT_MULTICAST_PORT is not a number {}", port.unwrap_err());
        return 3001;
    }

    port.unwrap()
}

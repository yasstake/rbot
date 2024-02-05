// Copyright(c) 2022-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use std::env::VarError;

pub fn env_rbot_db_root() -> Result<String, VarError> {
    std::env::var("RBOT_DB_ROOT")
}
const RBOT_MULTICAST_ADDR: &str = "224.0.0.51";
const DEFAULT_MULTICAST_PORT: i64 = 3001;

/*
pub fn env_rbot_port_base() -> i64 {
    let port_base = std::env::var("RBOT_PORT_BASE");
    if port_base.is_err() {
        log::info!("RBOT_PORT_BASE is not set, use default port {}.", DEFAULT_PORT_NUM);
        return DEFAULT_PORT_NUM;
    }
    let port_base = port_base.unwrap().parse::<i64>();
    if port_base.is_err() {
        log::warn!("RBOT_PORT_BASE is not a number {}", port_base.unwrap_err());
        return DEFAULT_PORT_NUM;
    }

    port_base.unwrap()
}
*/


pub fn env_rbot_multicast_addr() -> String {
    let addr = std::env::var("RBOT_MULTICAST_ADDR");
    if addr.is_err() {
        log::info!("RBOT_MULTICAST_ADDR is not set, use default address {}.", RBOT_MULTICAST_ADDR);
        return RBOT_MULTICAST_ADDR.to_string();
    }

    addr.unwrap()
}

pub fn env_rbot_multicast_port() -> i64 {
    let port = std::env::var("RBOT_MULTICAST_PORT");
    if port.is_err() {
        log::info!("RBOT_MULTICAST_PORT is not set, use default port {}.", DEFAULT_MULTICAST_PORT);
        return DEFAULT_MULTICAST_PORT;
    }
    let port = port.unwrap().parse::<i64>();
    if port.is_err() {
        log::warn!("RBOT_MULTICAST_PORT is not a number {}", port.unwrap_err());
        return DEFAULT_MULTICAST_PORT;
    }

    port.unwrap()
}
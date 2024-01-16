
pub mod udp;
pub mod rest;

pub use udp::*;
pub use rest::*;

use crate::env_rbot_port_base;

pub fn get_http_port() -> i64 {
    env_rbot_port_base()
}

pub fn get_udp_port() -> i64 {
    env_rbot_port_base() + 1
}

pub fn get_udp_source_port() -> i64 {
    env_rbot_port_base() + 2
}


    
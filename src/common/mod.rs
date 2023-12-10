mod time;
mod order;
mod ch;
mod logger;
mod config;
mod account;


use std::io::Write;

pub use time::*;
pub use order::*;
pub use ch::*;
pub use logger::*;
pub use order::*;
pub use config::*;
pub use account::*;

pub fn flush_log() {
    let _ = std::io::stdout().flush();
    let _ = std::io::stderr().flush();
}

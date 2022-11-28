
use pyo3::pyfunction;
use log::{LevelFilter};
use simple_logger::SimpleLogger;

pub mod time;
pub mod order;

#[pyfunction]
pub fn init_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Warn).init();
}

#[pyfunction]
pub fn init_debug_log() {
    let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
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
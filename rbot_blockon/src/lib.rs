
use std::future::Future;
use once_cell::sync::Lazy;
use tokio::time::{timeout, Duration};


pub static RUNTIME: Lazy<tokio::runtime::Runtime> =
    Lazy::new(|| tokio::runtime::Runtime::new().unwrap());


#[allow(non_snake_case)]
pub fn BLOCK_ON<F: Future>(f: F) -> F::Output {
    let result = RUNTIME.block_on(f);
    result
}

#[allow(non_snake_case)]
pub fn BLOCK_ON_TIMEOUT<F>(timeout_sec: u64, f: F) -> F::Output
where
    F: Future,
{
    log::debug!("BLOCK_ON_TIMEOUT: (timeout={})", timeout_sec);

    let result = RUNTIME.block_on(async {
        let duration = Duration::from_secs(timeout_sec);

        match timeout(duration, f).await {
            Ok(result) => result,
            Err(_) => panic!("Timeout"),
        }        
    });

    result
}


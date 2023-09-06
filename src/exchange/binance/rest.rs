use std::thread::sleep;
use std::time::Duration;

use std::sync::mpsc::Sender; 

use super::message::BinanceMessageId;
use super::message::BinanceRestBoard;
use super::message::BinanceTradeMessage;
use crate::common::order::Trade;
use crate::common::time::HHMM;
use crate::common::time::time_string;
use crate::common::time::MicroSec;
use crate::common::time::NOW;
use crate::db::df::KEY::id;
use crate::exchange::rest_get;

const SERVER: &str = "https://api.binance.com";

/// Sends a REST request to Binance to get the server time and returns it in microseconds.
///
/// # Example
///
/// ```
/// use rbot::exchange::binance::rest::server_time;
///
/// let time = server_time();
/// println!("Server time: {}", time);
/// ```
pub fn server_time() -> MicroSec {
    let path = "/api/v3/time";

    let result = rest_get(SERVER, path);

    match result {
        Ok(message) => {
            let v: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

            let server_time = v["serverTime"].as_u64().unwrap();

            return (server_time * 1_000) as MicroSec;
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return 0;
        }
    }
}

/// Processes recent trades for a given symbol and applies a function to the resulting vector of trades.
///
/// # Arguments
///
/// * `symbol` - A string slice that holds the symbol for which recent trades are to be processed.
/// * `f` - A closure that takes a vector of trades and returns a `Result<(), String>`.
///
/// # Returns
///
/// Returns a `Result` containing a tuple of `BinanceMessageId` and `MicroSec` if the operation is successful, otherwise returns an error message as a `String`.
fn process_recent_trade<F>(symbol: &str, f: &mut F) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let path = format!("/api/v3/trades?symbol={}&limit=1000", symbol);

    let result = rest_get(SERVER, path.as_str());

    match result {
        Ok(message) => {
            return process_binance_trade_message(message.as_str(), f);
        }
        Err(e) => Err(e),
    }
}

/// Processes a Binance trade message and returns the first trade ID and time.
///
/// # Arguments
///
/// * `message` - A string slice that holds the Binance trade message.
/// * `f` - A closure that takes a vector of trades and returns a Result object.
///
/// # Returns
///
/// A Result object containing a tuple of the first trade ID and time, or an error message if the message could not be parsed.
/// https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
/// # Examples
///
/// ```
/// use rbot::exchange::binance::rest::process_binance_trade_message;
///
/// let message = r#"[
///  {
///     "id": 28457,
///     "price": "4.00000100",
///     "qty": "12.00000000",
///     "quoteQty": "48.000012",
///     "time": 1499865549590,
///     "isBuyerMaker": true,
///     "isBestMatch": true
/// }
/// ]"#;
///
/// let result = process_binance_trade_message(message, |trades| {
///     // Do something with the trades
///    println!("trades: {:?}", trades);
///     Ok(())
/// });
///
/// assert!(result.is_ok());
/// ```
pub fn process_binance_trade_message<F>(
    message: &str,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let result = serde_json::from_str::<Vec<BinanceTradeMessage>>(message);

    match result {
        Ok(message) => {
            let mut trades: Vec<Trade> = vec![];
            let mut first_id: u64 = 0;
            let mut first_time: MicroSec = 0;

            for m in message {
                let t = m.to_trade();

                if m.id < first_id || first_id == 0 {
                    first_id = m.id;
                    first_time = t.time;
                }

                trades.push(t);
            }

            (*f)(trades)?;

            return Ok((first_id, first_time));
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e.to_string());
        }
    }
}

/// Processes historical trades for a given symbol and from a given trade ID.
///
/// # Arguments
///
/// * `symbol` - A string slice that holds the symbol for which to retrieve historical trades.
/// * `from_id` - An unsigned 64-bit integer that holds the trade ID from which to start retrieving historical trades.
/// * `f` - A closure that takes a vector of `Trade` structs and returns a `Result<(), String>`.
///
/// # Returns
///
/// A `Result` containing a tuple of `BinanceMessageId` and `MicroSec` structs on success, or an error message on failure.
///
/// # Example
///
/// ```
/// use rbot::exchange::binance::rest::process_old_trade;
///
/// let symbol = "BTCUSDT";
/// let from_id = 0;        // 0 means the latest trade
///
/// let result = process_old_trade(symbol, from_id, |trades| {
///     Ok(())
/// });
///
/// match result {
///     Ok((message_id, microsec)) => {
///        println!("message_id: {}, microsec: {}", message_id, microsec);
///     }
///     Err(e) => {
///         // Handle error
///     }
/// }
/// ```
pub fn process_old_trade<F>(
    symbol: &str,
    from_id: u64,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let mut path: String;

    if from_id == 0 {
        path = format!("/api/v3/historicalTrades?symbol={}&limit=1000", symbol);
    } else {
        path = format!(
            "/api/v3/historicalTrades?symbol={}&fromId={}&limit=1000",
            symbol, from_id
        );
    }

    let result = rest_get(SERVER, path.as_str());

    match result {
        Ok(message) => {
            return process_binance_trade_message(message.as_str(), f);
        }
        Err(e) => Err(e),
    }
}

const PAGE_SIZE: u64 = 1000;
const API_INTERVAL_LIMIT: i64 = 100 * 1000;

pub fn insert_trade_db(
    symbol: &str,
    from_time: MicroSec,
    tx: Sender<Vec<Trade>>
) -> Result<(BinanceMessageId, MicroSec), String> 
{
    return process_old_trade_from(symbol, from_time, &mut |trades| {
        tx.send(trades).unwrap();
        Ok(())
    });
}

pub fn process_old_trade_from<F>(
    symbol: &str,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String> 
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let now = server_time();
    
    let result = _process_old_trade_from(symbol, from_time, f);

    match result {
        Ok((trade_id, trade_time)) => {
            match _process_old_trade_from(symbol, now - HHMM(0, 1), f) {
                Ok(_) => {
                    return Ok((trade_id, trade_time));
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return Err(e);
                }
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}




fn _process_old_trade_from<F>(
    symbol: &str,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let mut from_id = 0;
    let mut count = 0;

    let start_time = server_time();

    loop {
        let mut start_timer = NOW();

        println!("from_id: {}", from_id);

        let result = process_old_trade(symbol, from_id, f);

        match result {
            Ok((trade_id, trade_time)) => {
                if trade_time < from_time {
                    println!(
                        "trade time {}({}) is exceed specified time {}({}).",
                        time_string(trade_time),
                        trade_time,
                        time_string(from_time),
                        from_time
                    );
                    return Ok((trade_id, trade_time));
                } else if (from_id != 0) && from_id != trade_id {
                    println!("data end fromID={} / trade_id={}", from_id, trade_id);
                    return Ok((trade_id, trade_time));
                }

                from_id = trade_id - PAGE_SIZE;
            }
            Err(e) => {
                println!("Error: {:?}", e);

                return Err(e);
            }
        }

        count += 1;
        log::debug!("{} ", count);

        let elapsed = NOW() - start_timer;

        if elapsed < API_INTERVAL_LIMIT {
            sleep(Duration::from_micros((API_INTERVAL_LIMIT - elapsed) as u64));
        }
    }
}

pub fn get_board_snapshot(symbol: &str) -> Result<BinanceRestBoard, String> {
    let path = format!("/api/v3/depth?symbol={}&limit=1000", symbol);

    let result = rest_get(SERVER, path.as_str());

    match result {
        Ok(message) => {
            let v: BinanceRestBoard = serde_json::from_str(message.as_str()).unwrap();
            Ok(v)
        }
        Err(e) => {
            log::error!("Error: {:?}", e);
            Err(e)
        }
    }
}

#[cfg(test)]
mod binance_rest_tests {
    use crate::common::time::{time_string, HHMM, NOW};

    use super::*;

    #[test]
    fn test_get_board() {
        let result = get_board_snapshot("BTCBUSD");

        let board = result.unwrap();
        println!("{:?}", board);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common::time::{time_string, HHMM},
        exchange::Trade,
    };

    #[test]
    fn test_process_binance_trade_message() {
        //        let message = r#"[{"id":1,"price":"0.001","qty":"100","time":123456789,"isBuyerMaker":true,"isBestMatch":true}]"#;
        let message = r#"
 [
  {
     "id": 28457,
     "price": "4.00000100",
     "qty": "12.00000000",
     "quoteQty": "48.000012",
     "time": 1499865549590,
     "isBuyerMaker": true,
     "isBestMatch": true
 }
 ]"#;

        let trade = process_binance_trade_message(message, &mut |message| {
            println!("message: {:?}", message);
            Ok(())
        });

        assert_eq!(trade.is_ok(), true);
    }

    #[test]
    fn test_process_recent_trade() {
        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        let result = process_recent_trade(symbol, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_process_old_trade() {
        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        let result = process_old_trade(symbol, 0, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_timing_rest_api() {
        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        let result = process_old_trade(symbol, 0, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));

        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        println!("-----");

        let result = process_recent_trade(symbol, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_server_time() {
        let time = server_time();
        println!("Server time: {} ({})", time_string(time), time);
    }

    #[test]
    fn test_process_old_trade_until() {
        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        let result = process_old_trade_from(symbol, NOW() - HHMM(1, 0), &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }
}

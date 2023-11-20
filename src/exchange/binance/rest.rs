use std::thread::sleep;
use std::time::Duration;

// use crossbeam_channel::Receiver;
use crossbeam_channel::Sender;

use super::message::BinanceAccountInformation;
use super::message::BinanceCancelOrderResponse;
use super::message::BinanceMessageId;
use super::message::BinanceOrderResponse;
use super::message::BinanceOrderStatus;
use super::message::BinanceRestBoard;
use super::message::BinanceTradeMessage;
use super::BinanceConfig;
use crate::common::time_string;
use crate::common::MicroSec;
use crate::common::OrderSide;
use crate::common::Trade;
use crate::common::HHMM;
use crate::common::NOW;
use crate::exchange::rest_delete;
use crate::exchange::rest_get;
use crate::exchange::rest_post;
use crate::exchange::rest_put;

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
pub fn process_recent_trade<F>(
    config: &BinanceConfig,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let path = format!("/api/v3/trades?symbol={}&limit=1000", config.trade_symbol);

    let result = rest_get(&config.rest_endpoint, path.as_str(), vec![], None, None);

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
    config: &BinanceConfig,
    from_id: u64,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let path: String;

    if from_id == 0 {
        path = format!(
            "/api/v3/historicalTrades?symbol={}&limit=1000",
            config.trade_symbol
        );
    } else {
        path = format!(
            "/api/v3/historicalTrades?symbol={}&fromId={}&limit=1000",
            config.trade_symbol, from_id
        );
    }

    let result = rest_get(&config.rest_endpoint, path.as_str(), vec![], None, None);

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
    config: &BinanceConfig,
    start_time: MicroSec,
    tx: Sender<Vec<Trade>>,
) -> Result<(BinanceMessageId, MicroSec), String> {
    return process_old_trade_from(config, start_time, &mut |trades| {
        tx.send(trades).unwrap();
        Ok(())
    });
}

pub fn process_old_trade_from<F>(
    config: &BinanceConfig,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let now = server_time(config)?;

    let result = _process_old_trade_from(&config, from_time, f);

    match result {
        Ok((trade_id, trade_time)) => match _process_old_trade_from(&config, now - HHMM(0, 1), f) {
            Ok(_) => {
                return Ok((trade_id, trade_time));
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e);
            }
        },
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

fn _process_old_trade_from<F>(
    config: &BinanceConfig,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let mut from_id = 0;
    let mut count = 0;

    // TODO: check implementation
    let start_time = server_time(config);

    loop {
        let mut start_timer = NOW();

        log::debug!("from_id: {}", from_id);

        let result = process_old_trade(&config, from_id, f);

        match result {
            Ok((trade_id, trade_time)) => {
                if trade_time < from_time {
                    log::debug!(
                        "trade time {}({}) is exceed specified time {}({}).",
                        time_string(trade_time),
                        trade_time,
                        time_string(from_time),
                        from_time
                    );
                    return Ok((trade_id, trade_time));
                } else if (from_id != 0) && from_id != trade_id {
                    log::debug!("data end fromID={} / trade_id={}", from_id, trade_id);
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

pub fn get_board_snapshot(config: &BinanceConfig) -> Result<BinanceRestBoard, String> {
    let path = format!("/api/v3/depth?symbol={}&limit=1000", config.trade_symbol);

    let result = rest_get(&config.rest_endpoint, path.as_str(), vec![], None, None);

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

use chrono::format::format;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde_json::Value;
use sha2::Sha256;

/// Parses the result of a Binance API call and returns a `serde_json::Value` or an error message.
///
/// # Arguments
///
/// * `result` - A `Result` containing either the API response as a `String` or an error message as a `String`.
///
/// # Returns
///
/// * `Ok(serde_json::Value)` - If the API response was successfully parsed into a `serde_json::Value`.
/// * `Err(String)` - If there was an error parsing the API response or if the response contained an error message.
pub fn parse_binance_result(result: Result<String, String>) -> Result<serde_json::Value, String> {
    if result.is_err() {
        let err_message = format!("ERROR: {}", result.err().unwrap());
        return Err(err_message);
    }

    let message = result.unwrap();

    let v = serde_json::from_str::<Value>(message.as_str());

    if v.is_err() {
        let err_message = format!("{}:\n{}", v.err().unwrap(), message);
        return Err(err_message);
    }

    let v = v.unwrap();

    let code = v.get("code");
    if code.is_some() {
        let code = code.unwrap().as_i64().unwrap();
        let msg = v.get("msg").unwrap().as_str().unwrap();

        let err_message = format!("{}: {}\n{}", code, msg, message);
        return Err(err_message);
    }

    Ok(v)
}

pub fn binance_get_key(
    config: &BinanceConfig,
    path: &str,
    query: Option<&str>,
) -> Result<Value, String> {
    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));
    let result = rest_get(&config.rest_endpoint, path, headers, query, None);

    return parse_binance_result(result);
}

pub fn binance_get_sign(
    config: &BinanceConfig,
    path: &str,
    query: Option<&str>,
) -> Result<Value, String> {
    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let q = if query.is_some() {
        query.unwrap().to_string()
    } else {
        "".to_string()
    };

    let query = sign_with_timestamp(&config.api_secret, &q);
    let result = rest_get(&config.rest_endpoint, path, headers, Some(&query), None);

    log::debug!("AUTH GET: path{} / body: {} / {:?}", path, q, result);

    return parse_binance_result(result);
}

pub fn binance_put_key(config: &BinanceConfig, path: &str, body: &str) -> Result<Value, String> {
    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));
    let result = rest_put(&config.rest_endpoint, path, headers, body);

    return parse_binance_result(result);
}

pub fn binance_post_key(config: &BinanceConfig, path: &str, body: &str) -> Result<Value, String> {
    let url = format!("{}{}", config.rest_endpoint, path);

    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let result = rest_post(&config.rest_endpoint, path, headers, body);

    return parse_binance_result(result);
}

pub fn binance_post_sign(config: &BinanceConfig, path: &str, body: &str) -> Result<Value, String> {
    let url = format!("{}{}", config.rest_endpoint, path);

    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let body = sign_with_timestamp(&config.api_secret, &body.to_string());

    log::debug!("path{} / body: {}", path, body);
    let result = rest_post(&config.rest_endpoint, path, headers, &body);

    return parse_binance_result(result);
}

pub fn binance_delete_sign(
    config: &BinanceConfig,
    path: &str,
    body: &str,
) -> Result<Value, String> {
    let url = format!("{}{}", config.rest_endpoint, path);

    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let body = sign_with_timestamp(&config.api_secret, &body.to_string());

    log::debug!("path{} / body: {}", path, body);
    let result = rest_delete(&config.rest_endpoint, path, headers, &body);

    return parse_binance_result(result);
}

use hex;

fn sign_with_timestamp(secret_key: &String, mut message: &String) -> String {
    let time = (NOW() / 1_000) as u64;

    let message = format!("{}&recvWindow={}&timestamp={}", message, 6000, time);

    return sign(secret_key, &message);
}

fn sign(secret_key: &String, message: &String) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let mac = mac.finalize();
    let signature = hex::encode(mac.into_bytes());

    let message = format!("{}&signature={}", message, signature);

    message
}

/// Sends a REST request to Binance to get the server time and returns it in microseconds.
///
/// # Example
///
/// ```
/// use rbot::common::init_debug_log;
/// use rbot::exchange::binance::rest::server_time;
/// init_debug_log();
/// let config = rbot::exchange::binance::BinanceConfig::BTCBUSD();
///
/// let time = server_time(&config);
/// log::debug!("Server time: {}", time);
/// ```
pub fn server_time(config: &BinanceConfig) -> Result<MicroSec, String> {
    let path = "/api/v3/time";

    let result = rest_get(&config.rest_endpoint, path, vec![], None, None);

    match result {
        Ok(message) => {
            let v: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

            let st = v.get("serverTime");
            if st.is_some() {
                let time = st.unwrap().as_u64().unwrap();

                return Ok((time * 1_000) as MicroSec);
            } else {
                println!("serverTime is not found.");

                let err_message = format!("serverTime is not found {}", message);
                return Err(err_message);
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

fn order_side_string(side: OrderSide) -> String {
    if side == OrderSide::Buy {
        return "BUY".to_string();
    } else if side == OrderSide::Sell {
        return "SELL".to_string();
    } else {
        panic!("Unknown order side");
    }
}

fn parse_response<T>(result: Result<Value, String>) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    match result {
        Ok(v) => match serde_json::from_value::<T>(v) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e.to_string());
            }
        },
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}


/// Creates a new limit order on Binance exchange.
///
/// # Arguments
///
/// * `config` - A reference to a `BinanceConfig` object containing the API key and secret key.
/// * `side` - The order side, either `OrderSide::Buy` or `OrderSide::Sell`.
/// * `price` - The price at which to place the order.
/// * `size` - The size of the order.
/// * `cliend_order_id` - An optional client order ID to assign to the order.
///
/// # Returns
///
/// A `Result` containing a `BinanceOrderResponse` object if the order was successfully placed,
/// or an error message as a `String` if the order failed.
/// 
/// For SPOT:
/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
/// 
/// For MARGIN:
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade
pub fn new_limit_order(
    config: &BinanceConfig,
    side: OrderSide,
    price: Decimal,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BinanceOrderResponse, String> {
    let path = "/api/v3/order";
    let side = order_side_string(side);
    let mut body = format!(
        "symbol={}&side={}&type=LIMIT&timeInForce=GTC&quantity={}&price={}",
        config.trade_symbol, side, size, price 
    );

    if cliend_order_id.is_some() {
        let cliend_order_id = cliend_order_id.unwrap();
        body = format!("{}&newClientOrderId={}", body, cliend_order_id);
    }

    parse_response::<BinanceOrderResponse>(binance_post_sign(&config, path, body.as_str()))
}


/// Creates a new market order on Binance exchange.
///
/// # Arguments
///
/// * `config` - A reference to a `BinanceConfig` struct containing the API key and secret key.
/// * `side` - The order side, either `OrderSide::Buy` or `OrderSide::Sell`.
/// * `size` - The size of the order.
/// * `cliend_order_id` - An optional client order ID.
///
/// # Returns
///
/// A `Result` containing a `BinanceOrderResponse` struct if successful, or an error message `String` if unsuccessful.
///
/// For SPOT:
/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
/// 
/// For MARGIN:
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade
pub fn new_market_order(
    config: &BinanceConfig,
    side: OrderSide,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BinanceOrderResponse, String> {
    let path = "/api/v3/order";
    let side = order_side_string(side);
    let mut body = format!(
        "symbol={}&side={}&type=MARKET&quantity={}",
        config.trade_symbol, side, size
    );

    if cliend_order_id.is_some() {
        let cliend_order_id = cliend_order_id.unwrap();
        body = format!("{}&newClientOrderId={}", body, cliend_order_id);
    }

    parse_response::<BinanceOrderResponse>(binance_post_sign(&config, path, body.as_str()))
}

// https://binance-docs.github.io/apidocs/spot/en/#query-order-user_data
/*
pub fn alter_order(
    config: &BinanceConfig,
    order_id: &str,
    price: Decimal,
    size: Decimal,
) -> Result<BinanceOrderResponse, String> {
}
*/

/// https://binance-docs.github.io/apidocs/spot/en/#cancel-all-open-orders-on-a-symbol-trade
pub fn cancel_order(
    config: &BinanceConfig,
    order_id: &str,
) -> Result<BinanceCancelOrderResponse, String> {
    let path = "/api/v3/order";
    let body = format!("symbol={}&orderId={}", config.trade_symbol, order_id);

    parse_response::<BinanceCancelOrderResponse>(binance_delete_sign(&config, path, body.as_str()))
}

pub fn cancell_all_orders(
    config: &BinanceConfig,
) -> Result<Vec<BinanceCancelOrderResponse>, String> {
    let path = "/api/v3/openOrders";
    let body = format!("symbol={}", config.trade_symbol);

    parse_response::<Vec<BinanceCancelOrderResponse>>(binance_delete_sign(
        &config,
        path,
        body.as_str(),
    ))
}


/// Get Balance from user account
/// for SPOT
/// https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data/// 
/// 
/// for MARGIN
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-balance-user_data
pub fn get_balance(config: &BinanceConfig) -> Result<BinanceAccountInformation, String> {
    let path = "/api/v3/account";

    parse_response::<BinanceAccountInformation>(binance_get_sign(&config, path, None))
}

pub fn create_listen_key(config: &BinanceConfig) -> Result<String, String> {
    let message = binance_post_key(&config, "/api/v3/userDataStream", "").unwrap();

    if message.get("listenKey").is_some() {
        let listen_key = message.get("listenKey").unwrap().as_str().unwrap();
        return Ok(listen_key.to_string());
    } else {
        let err_message = format!("listenKey is not found {}", message);
        return Err(err_message);
    }
}

pub fn extend_listen_key(config: &BinanceConfig, key: &str) -> Result<(), String> {
    let path = format!("/api/v3/userDataStream?listenKey={}", key);
    let message = binance_put_key(&config, path.as_str(), "");

    match message {
        Ok(_) => {
            return Ok(());
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

pub fn order_status(config: &BinanceConfig) -> Result<Vec<BinanceOrderStatus>, String> {
    let path = "/api/v3/allOrders";
    let query = format!("symbol={}&limit=1000", config.trade_symbol);

    parse_response::<Vec<BinanceOrderStatus>>(binance_get_sign(&config, path, Some(query.as_str())))
}

use crate::exchange::binance::message::BinanceListOrdersResponse;

pub fn open_orders(config: &BinanceConfig) -> Result<Vec<BinanceOrderStatus>, String> {
    let path = "/api/v3/openOrders";
    let query = format!("symbol={}", config.trade_symbol);

    parse_response::<Vec<BinanceOrderStatus>>(binance_get_sign(&config, path, Some(query.as_str())))
}

pub fn trade_list(config: &BinanceConfig) -> Result<Vec<BinanceListOrdersResponse>, String> {
    let path = "/api/v3/myTrades";
    let query = format!("symbol={}&limit=1000", config.trade_symbol);

    parse_response::<Vec<BinanceListOrdersResponse>>(binance_get_sign(
        &config,
        path,
        Some(query.as_str()),
    ))
}


#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;

    use super::*;
    use crate::common::{init_debug_log, time_string, HHMM, Order};

    use crate::common::init_log;

    #[test]
    fn test_trade_list() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");
        let result = trade_list(&config);

        println!("result: {:?}", result);
    }

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
        let config = BinanceConfig::BTCUSDT();
        let symbol = config.trade_symbol;
        let mut latest_time = 0;
        let config = BinanceConfig::BTCUSDT();

        let result = process_recent_trade(&config, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_process_old_trade() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = process_old_trade(&config, 0, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_timing_rest_api() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = process_old_trade(&config, 0, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));

        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        println!("-----");

        let result = process_recent_trade(&config, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_process_old_trade_until() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = process_old_trade_from(&config, NOW() - HHMM(1, 0), &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_server_time() {
        let config = BinanceConfig::BTCUSDT();
        let time = server_time(&config).unwrap();
        println!("Server time: {} ({})", time_string(time), time);
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#signed-trade-user_data-and-margin-endpoint-security
    /// test sample from above url
    #[test]
    fn test_sign() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let message = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";

        let signature = sign(&secret.to_string(), &message.to_string());
        //

        println!("signature: {}", signature);
        assert_eq!(signature, "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559&signature=c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
    }

    #[test]
    fn test_clock_diff() {
        let config = BinanceConfig::BTCUSDT();

        let time = server_time(&config).unwrap();
        let local_time = NOW();
        println!(
            "Server time: {} ({})  / local time {} ({})",
            time_string(time),
            time,
            time_string(local_time),
            local_time
        );
    }

    #[test]
    fn test_new_limit_order() {
        //        let config = BinanceConfig::BTCBUSD();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        let result = new_limit_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(24_000.0).unwrap(),
            Decimal::from_f64(0.001).unwrap(),
            Some(&"LimitOrder-test"),
        );
        println!("result: {:?}", result.unwrap());
    }

    #[test]
    fn test_new_market_order() {
        init_debug_log();

        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let result = new_market_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(0.001).unwrap(),
            Some(&"rest-test"),
        );

        println!("result: {:?}", result.unwrap());
        println!("");
    }

    #[test]
    fn test_cancel_order() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let order_id = "444627";

        let result = cancel_order(&config, order_id);

        match result {
            Ok(r) => {
                println!("{:?}", r);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    fn test_binance_cancel_all_orders() {
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        let result = cancell_all_orders(&config);

        match result {
            Ok(r) => {
                println!("{:?}", r);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    fn test_get_balance() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let result = get_balance(&config);
        println!("result: {:?}", result);
    }

    #[test]
    fn test_create_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);
    }

    #[test]
    fn test_extend_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);

        let result = extend_listen_key(&config, key.as_str());
        println!("result: {:?}", result);
    }

    #[test]
    fn test_order_status() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let message = order_status(&config).unwrap();
        println!("message: {:?}", message);
    }

    #[test]
    fn test_open_orders() {
        init_debug_log();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        let message = open_orders(&config).unwrap();
        println!("message: {:?}", message);

        for i in message {
            let order: Order = i.into();
            println!("i: {:?}", order);
        }
    }

    #[test]
    fn test_order_list() {
        init_debug_log();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        let message = trade_list(&config).unwrap();
        println!("message: {:?}", message);
    }

    #[test]
    fn test_sport_order_e2e() {
        init_debug_log();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        // display balance
        let balance = get_balance(&config).unwrap();
        println!("{:?}", balance);


        // make limit order
        let order = new_limit_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(24_000.0).unwrap(),
            Decimal::from_f64(0.001).unwrap(),
            Some(&"LimitOrder-test"),
        ).unwrap();

        // cancel

        let orders: Vec<Order> = order.into();

        if orders.len() == 1 {
            let order = &orders[0];
            println!("order: {:?}", order);

            let result = cancel_order(&config, &order.order_id);
            println!("result: {:?}", result);
        }

    }
}

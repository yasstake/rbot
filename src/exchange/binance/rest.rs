use std::thread::sleep;
use std::time::Duration;

use std::sync::mpsc::Sender; 

use super::BinanceConfig;
use super::message::BinanceAccountInformation;
use super::message::BinanceCancelOrderResponse;
use super::message::BinanceMessageId;
use super::message::BinanceOrderResponse;
use super::message::BinanceOrderStatus;
use super::message::BinanceRestBoard;
use super::message::BinanceTradeMessage;
use crate::common::order::OrderSide;
use crate::common::order::Trade;
use crate::common::time::HHMM;
use crate::common::time::time_string;
use crate::common::time::MicroSec;
use crate::common::time::NOW;
use crate::db::df::KEY::id;
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
fn process_recent_trade<F>(config: &BinanceConfig, f: &mut F) -> Result<(BinanceMessageId, MicroSec), String>
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
    let mut path: String;

    if from_id == 0 {
        path = format!("/api/v3/historicalTrades?symbol={}&limit=1000", config.trade_symbol);
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
    from_time: MicroSec,
    tx: Sender<Vec<Trade>>
) -> Result<(BinanceMessageId, MicroSec), String> 
{
    return process_old_trade_from(config, from_time, &mut |trades| {
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
        Ok((trade_id, trade_time)) => {
            match _process_old_trade_from(&config, now - HHMM(0, 1), f) {
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
    config: &BinanceConfig,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let mut from_id = 0;
    let mut count = 0;

    let start_time = server_time(config);

    loop {
        let mut start_timer = NOW();

        println!("from_id: {}", from_id);

        let result = process_old_trade(&config, from_id, f);

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
        return Err(err_message)
    }

    Ok(v)
}

pub fn binance_get_key(config: &BinanceConfig, path: &str, query: Option<&str>) -> Result<Value, String> {
    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));
    let result = rest_get(&config.rest_endpoint, path, headers, query, None);

    return parse_binance_result(result);    
}

pub fn binance_get_sign(config: &BinanceConfig, path: &str, query: Option<&str>) -> Result<Value, String> {
    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let q = if query.is_some() {
        query.unwrap().to_string()
    }
    else {
        "".to_string()
    };

    let query = sign_with_timestamp(&config.api_secret, &q);
    let result = rest_get(&config.rest_endpoint, path, headers, Some(&query), None);

    println!("path{} / body: {} / {:?}", path, q, result);

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

    println!("path{} / body: {}", path, body);
    let result = rest_post(&config.rest_endpoint, path, headers, &body);

    return parse_binance_result(result);    
}

pub fn binance_delete_sign(config: &BinanceConfig, path: &str, body: &str) -> Result<Value, String> {
    let url = format!("{}{}", config.rest_endpoint, path);

    let mut headers = vec![];
    headers.push(("X-MBX-APIKEY", config.api_key.as_str()));

    let body = sign_with_timestamp(&config.api_secret, &body.to_string());

    println!("path{} / body: {}", path, body);
    let result = rest_delete(&config.rest_endpoint, path, headers, &body);

    return parse_binance_result(result);    
}


use hex;

fn sign_with_timestamp(secret_key: &String, mut message: &String) -> String {
    let time = (NOW()/1_000) as u64;

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
        Ok(v) => {
            match serde_json::from_value::<T>(v) {
                Ok(v) => {
                    return Ok(v);
                }
                Err(e) => {
                    println!("Error: {:?}", e);
                    return Err(e.to_string());
                }
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade

pub fn new_limit_order(config: &BinanceConfig, side: OrderSide, price: Decimal, size: Decimal) -> Result<BinanceOrderResponse, String>{
    let path = "/api/v3/order";    
    let side = order_side_string(side);
    let body = format!("symbol={}&side={}&type=LIMIT&timeInForce=GTC&quantity={}&price={}", config.trade_symbol, side, size, price);

    parse_response::< BinanceOrderResponse>(binance_post_sign(&config, path, body.as_str()))  
   
}

/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
pub fn new_market_order(config: &BinanceConfig, side: OrderSide, size: Decimal) -> Result<BinanceOrderResponse, String>{
    let path = "/api/v3/order";    
    let side = order_side_string(side);
    let body = format!("symbol={}&side={}&type=MARKET&quantity={}", config.trade_symbol, side, size);

    parse_response::< BinanceOrderResponse>(binance_post_sign(&config, path, body.as_str()))  
   
}

/// https://binance-docs.github.io/apidocs/spot/en/#cancel-all-open-orders-on-a-symbol-trade
pub fn cancel_order(config: &BinanceConfig, order_id: String) -> Result<BinanceCancelOrderResponse, String> {
    let path = "/api/v3/order";    
    let body = format!("symbol={}&orderId={}", config.trade_symbol, order_id);

    parse_response::<BinanceCancelOrderResponse>(binance_delete_sign(&config, path, body.as_str()))
}


/// https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data
/// Get Balance from user account

pub fn get_balance(config: &BinanceConfig) -> Result<BinanceAccountInformation, String> {
    let path = "/api/v3/account";  

    parse_response::<BinanceAccountInformation>(binance_get_sign(&config, path, None))
}

pub fn create_listen_key(config:& BinanceConfig) -> Result<String, String> {
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

#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;

    use super::*;
    use crate::{
        common::time::{time_string, HHMM},
    };

    use crate::common::init_log;


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
        let config = BinanceConfig::BTCBUSD();        
        let symbol = config.trade_symbol;
        let mut latest_time = 0;
        let config = BinanceConfig::BTCBUSD();

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
        let config = BinanceConfig::BTCBUSD();        

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
        let config = BinanceConfig::BTCBUSD();        

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
        let config = BinanceConfig::BTCBUSD();        

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
        let config = BinanceConfig::BTCBUSD();        
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
        let config = BinanceConfig::BTCBUSD();     

        let time = server_time(&config).unwrap();
        let local_time = NOW();
        println!("Server time: {} ({})  / local time {} ({})", time_string(time), time, time_string(local_time), local_time);
    }

    #[test]
    fn test_new_limit_order() {
//        let config = BinanceConfig::BTCBUSD();     
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let result = new_limit_order(&config, OrderSide::Buy, Decimal::from_f64(25_000.0).unwrap(), 
            Decimal::from_f64(0.001).unwrap());
            println!("result: {:?}", result.unwrap());
    }

    #[test]
    fn test_new_market_order() {
//        let config = BinanceConfig::BTCBUSD();     
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let result = new_market_order(&config, OrderSide::Buy,
            Decimal::from_f64(0.001).unwrap());
        
        println!("result: {:?}", result.unwrap());
    }

    #[test]
    fn test_cancel_order() {
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let order_id = "444627";

        let result = cancel_order(&config, order_id.to_string());
        println!("result: {:?}", result.unwrap());        
    }
    
    #[test]
    fn test_get_balance() {
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let result = get_balance(&config);
        println!("result: {:?}", result);
    }

    #[test]
    fn test_create_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);
    }

    #[test]
    fn test_extend_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);

        let result = extend_listen_key(&config, key.as_str());
        println!("result: {:?}", result);
    }

    #[test]
    fn test_order_status() {
        let config = BinanceConfig::TESTSPOT("BTCBUSD".to_string());

        let message = order_status(&config).unwrap();
        println!("message: {:?}", message);
    }

}

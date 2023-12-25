// Copyright(c) 2022-2023. yasstake. All rights reserved.

use serde_json::from_str;

use rust_decimal::Decimal;


use crate::common::LogStatus;
use crate::common::MicroSec;
use crate::common::OrderSide;
use crate::common::Trade;
use crate::common::NOW;
use crate::common::flush_log;
use crate::common::time_string;
use crate::exchange::rest_delete;
use crate::exchange::rest_get;
use crate::exchange::rest_post;
use crate::exchange::rest_put;



use super::config::BybitConfig;
use super::message::BybitAccountInformation;
use super::message::BybitCancelOrderResponse;
use super::message::BybitKlinesResponse;
use super::message::BybitOrderResponse;
use super::message::BybitOrderStatus;
use super::message::BybitRestBoard;
use super::message::BybitRestResponse;
use super::message::BybitTradeResponse;

pub fn bybit_rest_get(config: &BybitConfig, path: &str, params: &str) -> Result<BybitRestResponse, String> {
    let server = config.rest_endpoint.clone();

    let query = format!("{}?{}", path, params);

    let result = 
    rest_get(&server, &query, vec![], None, None);

    match result {
        Ok(result) => {
            let result = from_str::<BybitRestResponse>(&result);

            if result.is_ok() {
                let result = result.unwrap();

                if result.return_code != 0 {
                    return Err(result.return_message);
                }
                return Ok(result);
            } else {
                let result = result.unwrap_err();
                return Err(result.to_string());
            }
        },
        Err(err) => {
            return Err(err.to_string());
        }
    }
}



/// https://bybit-exchange.github.io/docs/v5/market/orderbook

pub fn get_board_snapshot(config: &BybitConfig) -> Result<BybitRestBoard, String> {
    let path = "/v5/market/orderbook";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        200);
    
    let r = bybit_rest_get(config, path, &params);

    if r.is_err() {
        let r = r.unwrap_err();
        return Err(r);
    }

    let message = r.unwrap().body;

    let result = serde_json::from_value::<BybitRestBoard>(message);

    if result.is_ok() {
        let result = result.unwrap();
        return Ok(result);
    } else {
        let result = result.unwrap_err();
        return Err(result.to_string());
    }
}

pub fn get_trade_history(config: &BybitConfig) -> Result<BybitTradeResponse, String> {
    let path = "/v5/market/recent-trade";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        1000);
    
    let r = bybit_rest_get(config, path, &params);

    if r.is_err() {
        let r = r.unwrap_err();
        return Err(r);
    }

    let message = r.unwrap().body;

    let result = serde_json::from_value::<BybitTradeResponse>(message);

    if result.is_ok() {
        let result = result.unwrap();
        return Ok(result);
    } else {
        let result = result.unwrap_err();
        return Err(result.to_string());
    }
}

/// https://bybit-exchange.github.io/docs/v5/market/kline
/// 
fn get_trade_kline(config: &BybitConfig) -> Result<BybitKlinesResponse, String> {
    let path = "/v5/market/kline";

    let params = format!("category={}&symbol={}&interval=1&limit={}", // 1 min
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        1000);
    
    let r = bybit_rest_get(&config, path, &params);

    if r.is_err() {
        let r = r.unwrap_err();
        return Err(r);
    }

    let message = r.unwrap().body;

    let result = serde_json::from_value::<BybitKlinesResponse>(message);

    if result.is_ok() {
        let result = result.unwrap();
        return Ok(result);
    } else {
        let result = result.unwrap_err();
        return Err(result.to_string());
    }
}

pub fn new_limit_order(
    config: &BybitConfig,
    side: OrderSide,
    price: Decimal,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BybitOrderResponse, String> {
    /*
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
    */

    return Err("Not implemented".to_string());
}

pub fn new_market_order(
    config: &BybitConfig,
    side: OrderSide,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BybitOrderResponse, String> {
    return Err("Not implemented".to_string());
}

pub fn cancel_order(
    config: &BybitConfig,
    order_id: &str,
) -> Result<BybitCancelOrderResponse, String> {

    return Err("Not implemented".to_string());
}

pub fn cancell_all_orders(
    config: &BybitConfig
) -> Result<Vec<BybitCancelOrderResponse>, String> {

    return Err("Not implemented".to_string());
}

pub fn get_balance(config: &BybitConfig) -> Result<BybitAccountInformation, String> {
    return Err("Not implemented".to_string());
}


pub fn order_status(config: &BybitConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


pub fn open_orders(config: &BybitConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}

pub fn trade_list(config: &BybitConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


#[cfg(test)]
mod bybit_rest_test{
    use pyo3::ffi::Py_Initialize;

    use crate::exchange::bybit::{config::BybitConfig, message::BybitKlines};

    use super::get_board_snapshot;

    #[test]
    fn get_board_snapshot_test() {
        

        let config = BybitConfig::SPOT_BTCUSDT();

        let r = get_board_snapshot(&config).unwrap();

        print!("{:?}", r);
    }

    #[test]
    fn get_trade_history_test() {
        let config = BybitConfig::SPOT_BTCUSDT();

        let r = super::get_trade_history(&config).unwrap();

        print!("{:?}", r);
    }

    #[test]
    fn get_trade_kline_test() {
        let config = BybitConfig::SPOT_BTCUSDT();

        let r = super::get_trade_kline(&config).unwrap();

        print!("{:?}", r);

        let r: BybitKlines = r.into();
        
        print!("{:?}", r);
    }
}
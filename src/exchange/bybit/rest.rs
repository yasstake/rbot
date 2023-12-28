// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::f32::consts::E;

use serde_json::from_str;

use rust_decimal::Decimal;


use crate::common::LogStatus;
use crate::common::MarketConfig;
use crate::common::MicroSec;
use crate::common::OrderSide;
use crate::common::Trade;
use crate::common::NOW;
use crate::common::flush_log;
use crate::common::msec_to_microsec;
use crate::common::time_string;
use crate::db::sqlite::TradeTable;
use crate::exchange::rest_delete;
use crate::exchange::rest_get;
use crate::exchange::rest_post;
use crate::exchange::rest_put;


use super::message::BybitAccountInformation;
use super::message::BybitCancelOrderResponse;
use super::message::BybitKlines;
use super::message::BybitKlinesResponse;
use super::message::BybitOrderResponse;
use super::message::BybitOrderStatus;
use super::message::BybitRestBoard;
use super::message::BybitRestResponse;
use super::message::BybitTimestamp;
use super::message::BybitTradeResponse;

pub fn bybit_rest_get(server: &str, path: &str, params: &str) -> Result<BybitRestResponse, String> {
    let query = format!("{}?{}", path, params);

    let result = 
    rest_get(server, &query, vec![], None, None);

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

pub fn get_board_snapshot(server: &str, config: &MarketConfig) -> Result<BybitRestBoard, String> {
    let path = "/v5/market/orderbook";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        200);
    
    let r = bybit_rest_get(server, path, &params);

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

pub fn get_recent_trade(server: &str, config: &MarketConfig) -> Result<BybitTradeResponse, String> {
    let path = "/v5/market/recent-trade";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        1000);
    
    let r = bybit_rest_get(server, path, &params);

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

pub fn get_trade_kline(server: &str, config: &MarketConfig, start_time: MicroSec, end_time: MicroSec) -> Result<BybitKlines, String> {
    let mut klines = BybitKlines::new();

    let mut start_time = TradeTable::ohlcv_start(start_time) / 1_000;
    let end_time = TradeTable::ohlcv_end(end_time) / 1_000 - 1;

    loop {
        let r = get_trade_kline_raw(server, config, start_time, end_time);

        log::debug!("get_trade_kline_from({:?}) -> {:?}", start_time, r);

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let mut r = r.unwrap();

        let klines_len = r.klines.len();

        if klines_len == 0 {
            log::debug!{"End of data"};
            break;
        }

        start_time = r.klines[0].timestamp + 60 * 1_000;    // increase 60[sec] = 1 min

        r.append(&klines);
        klines = r;

        if end_time <= start_time {
            break;
        }
    }

    Ok(klines)
}


/// https://bybit-exchange.github.io/docs/v5/market/kline
/// 
fn get_trade_kline_raw(server: &str, config: &MarketConfig, start_time: MicroSec, end_time: MicroSec) -> Result<BybitKlines, String> {
    if end_time <= start_time {
        return Err("end_time <= start_time".to_string());
    }

    let path = "/v5/market/kline";

    let params = format!("category={}&symbol={}&interval=1&start={}&end={}&limit={}", // 1 min
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        start_time,
        end_time,
        1000);
    
    let r = bybit_rest_get(server, path, &params);

    if r.is_err() {
        let r = r.unwrap_err();
        return Err(r);
    }

    let message = r.unwrap().body;

    let result = serde_json::from_value::<BybitKlinesResponse>(message);

    if result.is_ok() {
        let result = result.unwrap();
        return Ok(result.into());
    } else {
        let result = result.unwrap_err();
        return Err(result.to_string());
    }
}

pub fn new_limit_order(
    server: &str,
    config: &MarketConfig,
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
    server: &str,
    config: &MarketConfig,
    side: OrderSide,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BybitOrderResponse, String> {
    return Err("Not implemented".to_string());
}

pub fn cancel_order(
    server: &str, 
    config: &MarketConfig,
    order_id: &str,
) -> Result<BybitCancelOrderResponse, String> {

    return Err("Not implemented".to_string());
}

pub fn cancell_all_orders(
    server: &str,
    config: &MarketConfig
) -> Result<Vec<BybitCancelOrderResponse>, String> {

    return Err("Not implemented".to_string());
}

pub fn get_balance(
    server: &str,
    config: &MarketConfig) -> Result<BybitAccountInformation, String> {
    return Err("Not implemented".to_string());
}


pub fn order_status(
    server: &str,
    config: &MarketConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


pub fn open_orders(
    server: &str,
    config: &MarketConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}

pub fn trade_list(
    server: &str,
    config: &MarketConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


#[cfg(test)]
mod bybit_rest_test{
    use std::{thread::sleep, time::Duration};

    use pyo3::ffi::Py_Initialize;

    use crate::{exchange::bybit::{message::BybitKlines, config::{BybitServerConfig, BybitConfig}}, common::{NOW, time_string, HHMM, init_debug_log}, db::sqlite::TradeTable};

    use super::get_board_snapshot;

    #[test]
    fn get_board_snapshot_test() {
        let server_config = BybitServerConfig::new(false);    
        let config = BybitConfig::SPOT_BTCUSDT();

        let r = get_board_snapshot(&server_config.rest_server, &config).unwrap();

        print!("{:?}", r);
    }

    #[test]
    fn get_trade_history_test() {
        let server_config = BybitServerConfig::new(false);    
        //let config = BybitConfig::SPOT_BTCUSDT();
        let config = BybitConfig::BTCUSDT();        
        let r = super::get_recent_trade(&server_config.rest_server, &config).unwrap();

        println!("{:?}", time_string(r.trades[0].time * 1000));
        let l = r.trades.len();
        println!("{:?} / rec={}", time_string(r.trades[l-1].time * 1000), l);        

        print!("{:?}", r);
    }

    #[test]
    fn get_trade_kline_test() {
        let server_config = BybitServerConfig::new(false);    
        let config = BybitConfig::SPOT_BTCUSDT();

        let start_time = (NOW() - HHMM(0,2));
        let now = NOW();

        let r = super::get_trade_kline_raw(&server_config.rest_server, &config, start_time/1000, now/1000).unwrap();

        println!("{:?}({:?})", time_string(now), now);
        println!("{:?}", r.__str__());

        let start_time = TradeTable::ohlcv_start(start_time);
        let now = TradeTable::ohlcv_start(now);

        let r = super::get_trade_kline_raw(&server_config.rest_server, &config, start_time/1000, now/1000).unwrap();

        println!("{:?}({:?})", time_string(now), now);
        println!("{:?}", r.__str__());

        let now = now - 1;

        let r = super::get_trade_kline_raw(&server_config.rest_server, &config, start_time/1000, now/1000).unwrap();

        println!("{:?}({:?})", time_string(now), now);
        println!("{:?}", r.__str__());

    }

    #[test]
    fn get_trade_kline_from_test() {
        let server_config = BybitServerConfig::new(false);    
        let config = BybitConfig::SPOT_BTCUSDT();
        
        init_debug_log();

        let now = NOW();
        let start_time = now- HHMM(24,10);

        let r = super::get_trade_kline(&server_config.rest_server, &config, start_time, now).unwrap();

        println!("{:?}", r.__str__());
    }

    #[test]
    fn get_trade_kline_to_df() {
        let server_config = BybitServerConfig::new(false);    
        let config = BybitConfig::SPOT_BTCUSDT();
        
        init_debug_log();

        let now = NOW();
        let start_time = now- HHMM(0, 10);

        let r = super::get_trade_kline(&server_config.rest_server, &config, start_time, now).unwrap();

        let df = r.to_dataframe();

        println!("{:?}", df);
    }
}
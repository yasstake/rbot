// Copyright(c) 2022-2023. yasstake. All rights reserved.

use std::thread::sleep;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::common::LogStatus;
use crate::common::MicroSec;
use crate::common::OrderSide;
use crate::common::Trade;
use crate::common::NOW;
use crate::common::flush_log;
use crate::common::time_string;
use crate::exchange::SkeltonConfig;
use crate::exchange::rest_delete;
use crate::exchange::rest_get;
use crate::exchange::rest_post;
use crate::exchange::rest_put;

use super::config::BybitConfig;
use super::message::BybitAccountInformation;
use super::message::BybitCancelOrderResponse;
use super::message::BybitOrderResponse;
use super::message::BybitOrderStatus;
use super::message::BybitRestBoard;

/// https://bybit-exchange.github.io/docs/v5/market/orderbook

pub fn get_board_snapshot(config: &BybitConfig) -> Result<BybitRestBoard, String> {


    return Err("Not implemented".to_string());
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

pub fn new_limit_order(
    config: &SkeltonConfig,
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
    config: &SkeltonConfig,
    side: OrderSide,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BybitOrderResponse, String> {
    return Err("Not implemented".to_string());
}

pub fn cancel_order(
    config: &SkeltonConfig,
    order_id: &str,
) -> Result<BybitCancelOrderResponse, String> {

    return Err("Not implemented".to_string());
}

pub fn cancell_all_orders(
    config: &SkeltonConfig
) -> Result<Vec<BybitCancelOrderResponse>, String> {

    return Err("Not implemented".to_string());
}

pub fn get_balance(config: &SkeltonConfig) -> Result<BybitAccountInformation, String> {
    return Err("Not implemented".to_string());
}


pub fn order_status(config: &SkeltonConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


pub fn open_orders(config: &SkeltonConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}

pub fn trade_list(config: &SkeltonConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}


#[cfg(test)]
mod bybit_rest_test{
    use crate::exchange::bybit::config::BybitConfig;

    use super::get_board_snapshot;

    #[test]
    fn get_board_snapshot_test() {
        let config = BybitConfig::SPOT_BTCUSDT();

        get_board_snapshot(&config);
    }


}
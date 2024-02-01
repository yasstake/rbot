// Copyright(c) 2022-2024. yasstake. All rights reserved.
#![allow(unused_variables)]
#![allow(dead_code)]

use rust_decimal::Decimal;
use rust_decimal_macros::dec;

use crate::common::AccountStatus;
use crate::common::MarketConfig;
use crate::common::Order;
use crate::common::OrderSide;
use crate::common::OrderType;
use crate::exchange::bitflyer::message::BitflyerExecutionResponse;
use crate::exchange::rest_get;

use super::config::BitflyerServerConfig;



/// https://bybit-exchange.github.io/docs/v5/market/orderbook
/*
pub fn get_board_snapshot(server: &str, config: &MarketConfig) -> Result<BitflyerRestBoard, String> {

    let path = "/v5/market/orderbook";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        200);

    let r = bitflyer_rest_get(server, path, &params);

    if r.is_err() {
        let r = r.unwrap_err();
        return Err(r);
    }

    let message = r.unwrap().body;

    let result = serde_json::from_value::<BitflyerRestBoard>(message);

    if result.is_ok() {
        let result = result.unwrap();
        return Ok(result);
    } else {
        let result = result.unwrap_err();
        return Err(result.to_string());
    }

    Err("Not implemented".to_string())
}
    */

pub fn get_recent_trade(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
) -> Result<Vec<BitflyerExecutionResponse>, String> {
    let path = "/v1/executions";

    let query = format!(
        "{}?product_code={}&count={}",
        path, config.trade_symbol, 1000
    );

    let result = rest_get(&server.rest_server, &query, vec![], None, None);

    println!("result={:?}", result);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let message = result.unwrap();

    let orders = serde_json::from_str::<Vec<BitflyerExecutionResponse>>(&message);

    if orders.is_err() {
        let orders = orders.unwrap_err();
        return Err(orders.to_string());
    }

    Ok(orders.unwrap())

    /*
    let path = "/v5/market/recent-trade";

    let params = format!("category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        1000);

    let r = bitflyer_rest_get(server, path, &params);

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
    */
}

/*
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

    Err("Not implemented".to_string())
}

    */

/*
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

    let r = bitflyer_rest_get(server, path, &params);

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

    Err("Not implemented".to_string())
}

    */

pub fn new_limit_order(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
    side: OrderSide,
    price: Decimal,
    size: Decimal,
    client_order_id: Option<&str>,
) -> Result<Order, String> {
    new_order(
        server,
        config,
        side,
        price,
        size,
        OrderType::Limit,
        client_order_id,
    )
}

pub fn new_market_order(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
    side: OrderSide,
    size: Decimal,
    client_order_id: Option<&str>,
) -> Result<Order, String> {
    new_order(
        server,
        config,
        side,
        dec![0.0],
        size,
        OrderType::Market,
        client_order_id,
    )
}

/// create new limit order
/// https://bybit-exchange.github.io/docs/v5/order/create-order
#[allow(dead_code)]
#[allow(unused_variables)]
pub fn new_order(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
    side: OrderSide,
    price: Decimal, // when order_type is Market, this value is ignored.
    size: Decimal,
    order_type: OrderType,
    client_order_id: Option<&str>,
) -> Result<Order, String> {
    /*
    let category = config.trade_category.clone();
    let symbol = config.trade_symbol.clone();

    let price = if order_type == OrderType::Market {
        None
    } else {
        Some(price)
    };

    let order = BybitOrderRequest {
        category: category,
        symbol: config.trade_symbol.clone(),
        side: side.to_string(),
        order_type: order_type.to_string(),
        qty: size,
        order_link_id: client_order_id,
        price: price,
    };

    log::debug!("order={:?}", order);

    let order_json = serde_json::to_string(&order).unwrap();
    log::debug!("order_json={}", order_json);

    let path = "/v5/order/create";

    let result = bitflyer_post_sign(&server, path, &order_json);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let result = result.unwrap();

    let response = serde_json::from_value::<BybitOrderRestResponse>(result.body);
    if response.is_err() {
        let response = response.unwrap_err();
        return Err(response.to_string());
    }
    let r = response.unwrap();

    let is_maker = if order_type == OrderType::Limit {
        true
    } else {
        false
    };

    let order = Order {
        symbol: symbol,
        create_time: msec_to_microsec(result.time),
        status: OrderStatus::New,
        order_id: r.order_id,
        client_order_id: r.order_link_id,
        order_side: side,
        order_type: order_type,
        order_price: if order_type == OrderType::Market {
            dec![0.0]
        } else {
            price.unwrap()
        },
        order_size: size,
        remain_size: size,
        transaction_id: "".to_string(),
        update_time: msec_to_microsec(result.time),
        execute_price: dec![0.0],
        execute_size: dec![0.0],
        quote_vol: dec![0.0],
        commission: dec![0.0],
        commission_asset: "".to_string(),
        is_maker: is_maker,
        message: "".to_string(),
        commission_home: dec![0.0],
        commission_foreign:dec![0.0],
        home_change: dec![0.0],
        foreign_change: dec![0.0],
        free_home_change: dec![0.0],
        free_foreign_change: dec![0.0],
        lock_home_change: dec![0.0],
        lock_foreign_change: dec![0.0],
        log_id: 0,
    };

    return Ok(order);
    */
    Err("Not implemented".to_string())
}

#[allow(unused_variables)]
pub fn cancel_order(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
    order_id: &str,
) -> Result<Order, String> {
    /*
    let message = CancelOrderMessage {
        category: config.trade_category.clone(),
        symbol: config.trade_symbol.clone(),
        order_id: order_id.to_string(),
    };

    let message_json = serde_json::to_string(&message).unwrap();
    let path = "/v5/order/cancel";
    let result = bitflyer_post_sign(&server, path, &message_json);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let result = result.unwrap();

    let response = serde_json::from_value::<BybitOrderRestResponse>(result.body);
    if response.is_err() {
        let response = response.unwrap_err();
        return Err(response.to_string());
    }
    let r = response.unwrap();

    let order = Order {
        symbol: "".to_string(),
        create_time: msec_to_microsec(result.time),
        status: OrderStatus::Canceled,
        order_id: r.order_id,
        client_order_id: r.order_link_id,
        order_side: OrderSide::Unknown,
        order_type: OrderType::Limit,
        order_price: dec![0.0],
        order_size: dec![0.0],
        remain_size: dec![0.0],
        transaction_id: "".to_string(),
        update_time: msec_to_microsec(result.time),
        execute_price: dec![0.0],
        execute_size: dec![0.0],
        quote_vol: dec![0.0],
        commission: dec![0.0],
        commission_asset: "".to_string(),
        is_maker: true,
        message: "".to_string(),
        commission_home: dec![0.0],
        commission_foreign:dec![0.0],
        home_change: dec![0.0],
        foreign_change: dec![0.0],
        free_home_change: dec![0.0],
        free_foreign_change: dec![0.0],
        lock_home_change: dec![0.0],
        lock_foreign_change: dec![0.0],
        log_id: 0,
    };

    return Ok(order);
    */
    Err("Not implemented".to_string())
}

pub fn cancell_all_orders(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
) -> Result<Vec<Order>, String> {
    /*
    let message = CancelAllMessage {
        category: config.trade_category.clone(),
        symbol: config.trade_symbol.clone(),
    };

    let message_json = serde_json::to_string(&message).unwrap();
    let path = "/v5/order/cancel-all";
    let result = bitflyer_post_sign(&server, path, &message_json);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let result = result.unwrap();

    let response = serde_json::from_value::<BybitMultiOrderRestResponse>(result.body);

    if response.is_err() {
        let response = response.unwrap_err();
        return Err(response.to_string());
    }

    let response = response.unwrap();

    let mut orders: Vec<Order> = vec![];

    for r in response.list {
        println!("r={:?}", r);

        let order = Order {
            symbol: "".to_string(),
            create_time: msec_to_microsec(result.time),
            status: OrderStatus::Canceled,
            order_id: r.order_id,
            client_order_id: r.order_link_id,
            order_side: OrderSide::Unknown,
            order_type: OrderType::Limit,
            order_price: dec![0.0],
            order_size: dec![0.0],
            remain_size: dec![0.0],
            transaction_id: "".to_string(),
            update_time: msec_to_microsec(result.time),
            execute_price: dec![0.0],
            execute_size: dec![0.0],
            quote_vol: dec![0.0],
            commission: dec![0.0],
            commission_asset: "".to_string(),
            is_maker: true,
            message: "".to_string(),
            commission_home: dec![0.0],
            commission_foreign:dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],
            log_id: 0,
        };
        orders.push(order);

    }

    return Ok(orders);
    */
    Err("Not implemented".to_string())
}

pub fn get_balance(server: &str, config: &MarketConfig) -> Result<AccountStatus, String> {
    return Err("Not implemented".to_string());
}

pub fn order_status(server: &str, config: &MarketConfig) -> Result<Vec<Order>, String> {
    return Err("Not implemented".to_string());
}

pub fn open_orders(
    server: &BitflyerServerConfig,
    config: &MarketConfig,
) -> Result<Vec<Order>, String> {
    /*

    let query_string = format!("category={}&symbol={}&limit=50", config.trade_category, config.trade_symbol);

    let path = "/v5/order/realtime";

    let result = bitflyer_get_sign(&server, path, &query_string);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let result = result.unwrap();

    if result.body.is_null() {
        return Ok(vec![]);
    }

    let mut orders: Vec<Order> = vec![];

    println!("result.body={:?}", result.body);

    let response = serde_json::from_value::<BybitMultiOrderStatus>(result.body);

    if response.is_err() {
        let response = response.unwrap_err();
        return Err(response.to_string());
    }

    let orders: Vec<Order> = response.unwrap().into();

    Ok(orders)
    */
    Err("Not implemented".to_string())
}

pub fn trade_list(server: &str, config: &MarketConfig) -> Result<Vec<Order>, String> {
    return Err("Not implemented".to_string());
}

#[cfg(test)]
mod test_restapi {
    use super::*;
    use crate::exchange::bitflyer::{config::BitflyerServerConfig, rest::get_recent_trade};

    #[test]
    fn test_recent_trade() {
        let server = BitflyerServerConfig::new();
        let config = MarketConfig::new_bitflyer("FX", "JPY", "BTC", 0, 0);

        println!("market_config={:?}", config);

        let r = get_recent_trade(&server, &config);

        println!("r={:?}", r);
    }
}

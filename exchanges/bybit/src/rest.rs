// Copyright(c) 2022-2023. yasstake. All rights reserved.

use csv::StringRecord;
use rbot_lib::common::time_string;
use rbot_lib::common::BoardTransfer;
use rbot_lib::common::Kline;
use rbot_lib::common::SEC;
use rbot_lib::db::TradeTable;
use rust_decimal_macros::dec;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::from_str;
use serde_json::Value;

use rust_decimal::Decimal;

use rbot_lib::common::{
    hmac_sign, msec_to_microsec, BoardItem, MarketConfig, MicroSec, Order, OrderSide, OrderStatus,
    OrderType, ServerConfig, Trade, NOW,
};

use rbot_lib::net::{rest_get, rest_post, RestApi};

use crate::message::microsec_to_bybit_timestamp;

use super::config::BybitServerConfig;
use super::message::BybitAccountInformation;
use super::message::BybitKlines;
use super::message::BybitKlinesResponse;
use super::message::BybitMultiOrderStatus;
use super::message::BybitOrderStatus;
use super::message::BybitRestBoard;
use super::message::BybitRestResponse;
use super::message::BybitTradeResponse;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BybitOrderRequest<'a> {
    pub category: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub qty: Decimal,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "orderLinkId")]
    pub order_link_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price: Option<Decimal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BybitOrderRestResponse {
    #[serde(rename = "orderId")]
    pub order_id: String,
    #[serde(rename = "orderLinkId")]
    pub order_link_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BybitMultiOrderRestResponse {
    pub list: Vec<BybitOrderRestResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CancelOrderMessage {
    category: String,
    symbol: String,
    #[serde(rename = "orderId")]
    order_id: String,
}

pub struct BybitRestApi {}

impl RestApi<BybitServerConfig> for BybitRestApi {
    async fn get_board_snapshot(
        server: &BybitServerConfig,
        config: &MarketConfig,
    ) -> Result<BoardTransfer, String> {
        let path = "/v5/market/orderbook";

        let params = format!(
            "category={}&symbol={}&limit={}",
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            config.board_depth
        );

        let r = Self::rest_get(&server, path, &params).await;

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let message = r.unwrap().body;

        let result = serde_json::from_value::<BybitRestBoard>(message);

        if result.is_ok() {
            let result = result.unwrap();
            return Ok(result.into());
        } else {
            let result = result.unwrap_err();
            return Err(result.to_string());
        }
    }

    async fn get_recent_trades(
        server: &BybitServerConfig,
        config: &MarketConfig,
    ) -> Result<Vec<Trade>, String> {
        let path = "/v5/market/recent-trade";

        let params = format!(
            "category={}&symbol={}&limit={}",
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            1000
        );

        let r = Self::rest_get(server, path, &params).await;

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let message = r.unwrap().body;

        let result = serde_json::from_value::<BybitTradeResponse>(message);

        if result.is_ok() {
            let result = result.unwrap();
            return Ok(result.into());
        } else {
            let result = result.unwrap_err();
            return Err(result.to_string());
        }
    }

    async fn get_trade_klines(
        server: &BybitServerConfig,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> Result<Vec<Kline>, String> {


        let mut klines: Vec<Kline> = vec![];

        let start_time = TradeTable::ohlcv_start(start_time);
        let mut end_time = TradeTable::ohlcv_end(end_time) - 1;

        loop {
            log::debug!("get_trade_kline_from: {:?}({:?}) -> {:?}({:?})", time_string(start_time), start_time, time_string(end_time), end_time);                        
            let r = Self::try_get_trade_klines(server, config, start_time, end_time).await;

            if r.is_err() {
                let r = r.unwrap_err();
                return Err(r);
            }

            let mut r = r.unwrap();

            let klines_len = r.len();

            if klines_len == 0 {
                log::debug! {"End of data"};
                break;
            }

            end_time = r[klines_len -1].timestamp -1;     // must be execute before append()
            log::debug!("start_time={:?} / end_time={:?} /({:?})rec", time_string(r[0].timestamp), time_string(r[klines_len -1].timestamp), klines_len);

            klines.append(&mut r);

            if end_time <= start_time {
                break;
            }
        }

        Ok(klines)
    }

    async fn new_order(
        server: &BybitServerConfig,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> Result<Vec<Order>, String> {
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

        let result = Self::post_sign(&server, path, &order_json).await;

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

        // in bybit only order id is valid when order is created.
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
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],
            log_id: 0,
        };

        return Ok(vec![order]);
    }

    async fn cancel_order(
        server: &BybitServerConfig,
        config: &MarketConfig,
        order_id: &str,
    ) -> Result<Order, String> {
        let message = CancelOrderMessage {
            category: config.trade_category.clone(),
            symbol: config.trade_symbol.clone(),
            order_id: order_id.to_string(),
        };

        let message_json = serde_json::to_string(&message).unwrap();
        let path = "/v5/order/cancel";
        let result = Self::post_sign(&server, path, &message_json).await;

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
            commission_foreign: dec![0.0],
            home_change: dec![0.0],
            foreign_change: dec![0.0],
            free_home_change: dec![0.0],
            free_foreign_change: dec![0.0],
            lock_home_change: dec![0.0],
            lock_foreign_change: dec![0.0],
            log_id: 0,
        };

        return Ok(order);
    }

    // TODO: implement paging.
    async fn open_orders(
        server: &BybitServerConfig,
        config: &MarketConfig,
    ) -> Result<Vec<Order>, String> {
        let query_string = format!(
            "category={}&symbol={}&limit=50",
            config.trade_category, config.trade_symbol
        );

        let path = "/v5/order/realtime";

        let result = Self::get_sign(&server, path, &query_string).await;

        if result.is_err() {
            let result = result.unwrap_err();
            return Err(result);
        }

        let result = result.unwrap();

        if result.body.is_null() {
            return Ok(vec![]);
        }

        println!("result.body={:?}", result.body);

        let response = serde_json::from_value::<BybitMultiOrderStatus>(result.body);

        if response.is_err() {
            let response = response.unwrap_err();
            return Err(response.to_string());
        }

        let orders: Vec<Order> = response.unwrap().into();

        Ok(orders)
    }

    async fn get_account(
        server: &BybitServerConfig,
        config: &MarketConfig,
    ) -> Result<rbot_lib::common::AccountStatus, String> {
        todo!()
    }

    fn format_historical_data_url(
        history_web_base: &str,
        category: &str,
        symbol: &str,
        yyyy: i64,
        mm: i64,
        dd: i64,
    ) -> String {
        todo!()
    }

    fn rec_to_trade(rec: &StringRecord) -> Trade {
        todo!()
    }

    fn archive_has_header() -> bool {
        todo!()
    }

}

impl BybitRestApi {
    async fn rest_get(
        server: &BybitServerConfig,
        path: &str,
        params: &str,
    ) -> Result<BybitRestResponse, String> {
        let query = format!("{}?{}", path, params);

        let result = rest_get(&server.get_rest_server(), &query, vec![], None, None).await;

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
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }

    pub async fn get_sign(
        server: &BybitServerConfig,
        path: &str,
        query_string: &str,
    ) -> Result<BybitRestResponse, String> {
        let timestamp = format!("{}", NOW() / 1_000);
        let api_key = server.get_api_key();
        let recv_window = "5000";

        let param_to_sign = format!("{}{}{}{}", timestamp, api_key, recv_window, query_string);
        let sign = hmac_sign(&server.get_api_secret(), &param_to_sign);

        let mut headers: Vec<(&str, &str)> = vec![];
        let api_key = server.get_api_key();

        headers.push(("X-BAPI-SIGN", &sign));
        headers.push(("X-BAPI-API-KEY", &api_key));
        headers.push(("X-BAPI-TIMESTAMP", &timestamp));
        headers.push(("X-BAPI-RECV-WINDOW", recv_window));

        let result = rest_get(&server.rest_server, path, headers, Some(query_string), None).await;

        Self::parse_rest_result(result)
    }

    async fn post_sign(
        server: &BybitServerConfig,
        path: &str,
        body: &str,
    ) -> Result<BybitRestResponse, String> {
        let timestamp = format!("{}", NOW() / 1_000);
        let api_key = server.get_api_key();
        let recv_window = "5000";

        let param_to_sign = format!("{}{}{}{}", timestamp, api_key, recv_window, body);
        let sign = hmac_sign(&server.get_api_secret(), &param_to_sign);

        let mut headers: Vec<(&str, &str)> = vec![];
        let api_key = server.get_api_key();

        headers.push(("X-BAPI-SIGN", &sign));
        headers.push(("X-BAPI-API-KEY", &api_key));
        headers.push(("X-BAPI-TIMESTAMP", &timestamp));
        headers.push(("X-BAPI-RECV-WINDOW", recv_window));
        headers.push(("Content-Type", "application/json"));

        let result = rest_post(&server.get_rest_server(), path, headers, &body).await;

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
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }

    fn parse_rest_result(result: Result<String, String>) -> Result<BybitRestResponse, String> {
        match result {
            Ok(result) => {
                if result == "" {
                    let response = BybitRestResponse {
                        return_code: 0,
                        return_message: "Ok".to_string(),
                        return_ext_info: Value::Null,
                        time: NOW() / 1_000,
                        body: Value::Null,
                    };
                    return Ok(response);
                }

                log::debug!("rest response: {}", result);

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
            }
            Err(err) => {
                return Err(err.to_string());
            }
        }
    }

    async fn try_get_trade_klines(
        server: &BybitServerConfig,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> Result<Vec<Kline>, String> {
        if end_time <= start_time {
            return Err("end_time <= start_time".to_string());
        }

        let path = "/v5/market/kline";

        let params = format!(
            "category={}&symbol={}&interval={}&start={}&end={}&limit={}", // 1 min
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            1,      // interval 1 min.
            microsec_to_bybit_timestamp(start_time),
            microsec_to_bybit_timestamp(end_time),
            1000 // max records.
        );

        let r = Self::rest_get(server, path, &params).await;

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let message = r.unwrap().body;

        let result = serde_json::from_value::<BybitKlinesResponse>(message);

        if result.is_err() {
            let result = result.unwrap_err();
            return Err(result.to_string());
        }

        let result = result.unwrap();
        return Ok(result.into());
    }
}

/*
fn bybit_rest_get(server: &str, path: &str, params: &str) -> Result<BybitRestResponse, String> {
    let query = format!("{}?{}", path, params);

    let result = rest_get(server, &query, vec![], None, None);

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
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}

fn bybit_post_sign(
    server: &BybitServerConfig,
    path: &str,
    body: &str,
) -> Result<BybitRestResponse, String> {
    let timestamp = format!("{}", NOW() / 1_000);
    let api_key = server.get_api_key();
    let recv_window = "5000";

    let param_to_sign = format!("{}{}{}{}", timestamp, api_key, recv_window, body);
    let sign = hmac_sign(&server.get_api_secret(), &param_to_sign);

    let api_key = server.get_api_key();

    let mut headers: Vec<(&str, &str)> = vec![];
    headers.push(("X-BAPI-SIGN", &sign));
    headers.push(("X-BAPI-API-KEY", &api_key));
    headers.push(("X-BAPI-TIMESTAMP", &timestamp));
    headers.push(("X-BAPI-RECV-WINDOW", recv_window));
    headers.push(("Content-Type", "application/json"));

    let result = rest_post(&server.rest_server, path, headers, &body);

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
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}

pub fn bybit_get_sign(
    server: &BybitServerConfig,
    path: &str,
    query_string: &str,
) -> Result<BybitRestResponse, String> {
    let timestamp = format!("{}", NOW() / 1_000);
    let api_key = server.get_api_key().clone();
    let recv_window = "5000";

    let param_to_sign = format!("{}{}{}{}", timestamp, api_key, recv_window, query_string);
    let sign = hmac_sign(&server.get_api_secret(), &param_to_sign);

    let api_key = server.get_api_key();

    let mut headers: Vec<(&str, &str)> = vec![];
    headers.push(("X-BAPI-SIGN", &sign));
    headers.push(("X-BAPI-API-KEY", &api_key));
    headers.push(("X-BAPI-TIMESTAMP", &timestamp));
    headers.push(("X-BAPI-RECV-WINDOW", recv_window));

    let result = rest_get(&server.rest_server, path, headers, Some(query_string), None);

    parse_rest_result(result)
}

fn parse_rest_result(result: Result<String, String>) -> Result<BybitRestResponse, String> {
    match result {
        Ok(result) => {
            if result == "" {
                let response = BybitRestResponse {
                    return_code: 0,
                    return_message: "Ok".to_string(),
                    return_ext_info: Value::Null,
                    time: NOW() / 1_000,
                    body: Value::Null,
                };
                return Ok(response);
            }

            log::debug!("rest response: {}", result);

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
        }
        Err(err) => {
            return Err(err.to_string());
        }
    }
}

/// https://bybit-exchange.github.io/docs/v5/market/orderbook

pub fn get_board_snapshot(server: &str, config: &MarketConfig) -> Result<BybitRestBoard, String> {
    let path = "/v5/market/orderbook";

    let params = format!(
        "category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        200
    );

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

    let params = format!(
        "category={}&symbol={}&limit={}",
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        1000
    );

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

pub fn get_trade_kline(
    server: &str,
    config: &MarketConfig,
    start_time: MicroSec,
    end_time: MicroSec,
) -> Result<BybitKlines, String> {
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
            log::debug! {"End of data"};
            break;
        }

        start_time = r.klines[0].timestamp + 60 * 1_000; // increase 60[sec] = 1 min

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
fn get_trade_kline_raw(
    server: &str,
    config: &MarketConfig,
    start_time: MicroSec,
    end_time: MicroSec,
) -> Result<BybitKlines, String> {
    if end_time <= start_time {
        return Err("end_time <= start_time".to_string());
    }

    let path = "/v5/market/kline";

    let params = format!(
        "category={}&symbol={}&interval=1&start={}&end={}&limit={}", // 1 min
        config.trade_category.as_str(),
        config.trade_symbol.as_str(),
        start_time,
        end_time,
        1000
    );

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
    server: &BybitServerConfig,
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
    server: &BybitServerConfig,
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
pub fn new_order(
    server: &BybitServerConfig,
    config: &MarketConfig,
    side: OrderSide,
    price: Decimal, // when order_type is Market, this value is ignored.
    size: Decimal,
    order_type: OrderType,
    client_order_id: Option<&str>,
) -> Result<Order, String> {
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

    let result = bybit_post_sign(&server, path, &order_json);

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
        commission_foreign: dec![0.0],
        home_change: dec![0.0],
        foreign_change: dec![0.0],
        free_home_change: dec![0.0],
        free_foreign_change: dec![0.0],
        lock_home_change: dec![0.0],
        lock_foreign_change: dec![0.0],
        log_id: 0,
    };

    return Ok(order);
}


pub fn cancel_order(
    server: &BybitServerConfig,
    config: &MarketConfig,
    order_id: &str,
) -> Result<Order, String> {
    let message = CancelOrderMessage {
        category: config.trade_category.clone(),
        symbol: config.trade_symbol.clone(),
        order_id: order_id.to_string(),
    };

    let message_json = serde_json::to_string(&message).unwrap();
    let path = "/v5/order/cancel";
    let result = bybit_post_sign(&server, path, &message_json);

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
        commission_foreign: dec![0.0],
        home_change: dec![0.0],
        foreign_change: dec![0.0],
        free_home_change: dec![0.0],
        free_foreign_change: dec![0.0],
        lock_home_change: dec![0.0],
        lock_foreign_change: dec![0.0],
        log_id: 0,
    };

    return Ok(order);
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CancelAllMessage {
    category: String,
    symbol: String,
}

pub fn cancell_all_orders(
    server: &BybitServerConfig,
    config: &MarketConfig,
) -> Result<Vec<Order>, String> {
    let message = CancelAllMessage {
        category: config.trade_category.clone(),
        symbol: config.trade_symbol.clone(),
    };

    let message_json = serde_json::to_string(&message).unwrap();
    let path = "/v5/order/cancel-all";
    let result = bybit_post_sign(&server, path, &message_json);

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
            commission_foreign: dec![0.0],
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
}

pub fn get_balance(server: &str, config: &MarketConfig) -> Result<BybitAccountInformation, String> {
    return Err("Not implemented".to_string());
}

pub fn order_status(server: &str, config: &MarketConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OpenOrderRequest {
    category: String,
    symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "orderId")]
    order_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<String>,
}

pub fn open_orders(
    server: &BybitServerConfig,
    config: &MarketConfig,
) -> Result<Vec<Order>, String> {
    let query_string = format!(
        "category={}&symbol={}&limit=50",
        config.trade_category, config.trade_symbol
    );

    let path = "/v5/order/realtime";

    let result = bybit_get_sign(&server, path, &query_string);

    if result.is_err() {
        let result = result.unwrap_err();
        return Err(result);
    }

    let result = result.unwrap();

    if result.body.is_null() {
        return Ok(vec![]);
    }

    let orders: Vec<Order> = vec![];

    println!("result.body={:?}", result.body);

    let response = serde_json::from_value::<BybitMultiOrderStatus>(result.body);

    if response.is_err() {
        let response = response.unwrap_err();
        return Err(response.to_string());
    }

    let orders: Vec<Order> = response.unwrap().into();

    Ok(orders)
}

pub fn trade_list(server: &str, config: &MarketConfig) -> Result<Vec<BybitOrderStatus>, String> {
    return Err("Not implemented".to_string());
}

*/

#[cfg(test)]
#[allow(non_snake_case)]
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused_variables)]
mod bybit_rest_test {
    use super::*;
    use std::{thread::sleep, time::Duration};

    use crate::config::BybitConfig;
    use pyo3::ffi::Py_Initialize;
    use rbot_lib::common::{init_debug_log, time_string, HHMM};
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_rest_get() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::rest_get(&server_config, "/v2/public/time", "").await;

        assert!(r.is_ok());
        println!("rest_get (time): {:?}", r.unwrap());
    }

    #[tokio::test]
    async fn test_get_sign() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::get_sign(&server_config, "/v2/public/time", "").await;
        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn get_board_snapshot_test_trait() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::get_board_snapshot(&server_config, &config).await;
        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_recent_trades() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::get_recent_trades(&server_config, &config).await;
        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_trade_kline() {
        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let now = NOW();
        let start_time = now - HHMM(24, 0);

        let r = BybitRestApi::get_trade_klines(&server_config, &config, start_time, now).await;
        assert!(r.is_ok());

        let r= r.unwrap();
        let l = r.len();
        println!("{:?}-{:?}  {:?} [rec]", time_string(r[0].timestamp), time_string(r[l-1].timestamp), l);
    }

    #[tokio::test]
    async fn test_new_limit_order() {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::new_order(
            &server_config,
            &config,
            OrderSide::Buy,
            dec![40000.0],
            dec![0.001],
            OrderType::Limit,
            None,
        )
        .await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_new_market_order() {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::new_order(
            &server_config,
            &config,
            OrderSide::Sell,
            dec![0.0],
            dec![0.001],
            OrderType::Market,
            None,
        )
        .await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_cancel_order() {
        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::new_order(
            &server_config,
            &config,
            OrderSide::Buy,
            dec![40000.0],
            dec![0.001],
            OrderType::Limit,
            None,
        )
        .await;

        assert!(r.is_ok());
        println!("{:?}", r);

        let r = BybitRestApi::cancel_order(&server_config, &config, &r.unwrap()[0].order_id).await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_open_orders() {
        init_debug_log();

        let server_config = BybitServerConfig::new(true);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::open_orders(&server_config, &config).await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }
}



/*
    #[test]
    fn get_trade_history_test() {
        let server_config = BybitServerConfig::new(false);
        //let config = BybitConfig::SPOT_BTCUSDT();
        let config = BybitConfig::BTCUSDT();
        let r = super::get_recent_trade(&server_config.rest_server, &config).unwrap();

        println!("{:?}", time_string(r.trades[0].time * 1000));
        let l = r.trades.len();
        println!("{:?} / rec={}", time_string(r.trades[l - 1].time * 1000), l);

        print!("{:?}", r);
    }
}

*/

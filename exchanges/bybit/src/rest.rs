#![allow(unused)]
// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::convert;
use std::fmt::format;

use csv::StringRecord;
use polars::export::num::FromPrimitive;
use rbot_lib::common::time_string;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::AccountPair;
use rbot_lib::common::BoardTransfer;
use rbot_lib::common::Kline;
use rbot_lib::common::LogStatus;
use rbot_lib::db::TradeTable;
use rust_decimal_macros::dec;
use serde_derive::Deserialize;
use serde_derive::Serialize;
use serde_json::from_str;
use serde_json::Value;

use rust_decimal::Decimal;

use anyhow::anyhow;
use anyhow::ensure;
#[allow(unused_imports)]
use anyhow::Context;
use anyhow::Result;

use rbot_lib::common::{
    hmac_sign, msec_to_microsec, MarketConfig, MicroSec, Order, OrderSide, OrderStatus, OrderType,
    ServerConfig, Trade, NOW,
};

use rbot_lib::net::{rest_get, rest_post, RestApi};

use crate::message::convert_coin_to_account_status;
use crate::message::microsec_to_bybit_timestamp;
use crate::message::BybitAccountCoin;
use crate::message::BybitAccountResponse;
use crate::message::BybitAccountStatus;

use super::config::BybitServerConfig;
use super::message::BybitKlinesResponse;
use super::message::BybitMultiOrderStatus;
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
    ) -> anyhow::Result<BoardTransfer> {
        let path = "/v5/market/orderbook";

        let params = format!(
            "category={}&symbol={}&limit={}",
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            config.board_depth
        );

        let r = Self::get(&server, path, &params)
            .await
            .with_context(|| {
                format!(
                    "get_board_snapshot: server={:?} / path={:?} / params={:?}",
                    server, path, params
                )
            })?;

        let message = r.body;

        let result = serde_json::from_value::<BybitRestBoard>(message)
            .with_context(|| format!("parse error in get_board_snapshot"))?;

        Ok(result.into())
    }

    async fn get_recent_trades(
        server: &BybitServerConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<Vec<Trade>> {
        let path = "/v5/market/recent-trade";

        let params = format!(
            "category={}&symbol={}&limit={}",
            &config.trade_category,
            &config.trade_symbol,
            1000 // max records.
        );


        let r = Self::get(server, path, &params)
            .await
            .with_context(|| {
                format!(
                    "get_recent_trades: server={:?} / path={:?} / params={:?}",
                    server, path, params
                )
            })?;

        let result = serde_json::from_value::<BybitTradeResponse>(r.body)
            .with_context(|| format!("parse error in get_recent_trades"))?;

        Ok(result.into())


//        Err(anyhow!("not implemented"))
    }

    async fn get_trade_klines(
        server: &BybitServerConfig,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<Vec<Kline>> {
        let mut klines_buf: Vec<Kline> = vec![];

        let start_time = TradeTable::ohlcv_start(start_time);

        let mut end_time = if end_time == 0 {
            TradeTable::ohlcv_end(NOW()) - 1
        } else {
            TradeTable::ohlcv_end(end_time) - 1
        };

        loop {
            log::debug!(
                "get_trade_kline_from: {:?}({:?}) -> {:?}({:?})",
                time_string(start_time),
                start_time,
                time_string(end_time),
                end_time
            );
            let mut klines = Self::try_get_trade_klines(server, config, start_time, end_time).await
            .with_context(|| {
                format!("get_trade_klines: start_time={:?} / end_time={:?}", start_time, end_time)
            })?;

            let klines_len = klines.len();
            if klines_len == 0 {
                log::debug! {"End of data"};
                break;
            }

            end_time = klines[klines_len - 1].timestamp - 1; // must be execute before append()
            log::debug!(
                "start_time={:?} / end_time={:?} /({:?})rec",
                time_string(klines[0].timestamp),
                time_string(klines[klines_len - 1].timestamp),
                klines_len
            );

            klines_buf.append(&mut klines);

            if (end_time != 0) && (end_time <= start_time) {
                log::debug!("end fetching data: end_time {}({}) <= start_time {}({})", time_string(end_time), end_time, time_string(start_time), start_time);
                break;
            }
        }

        Ok(klines_buf)
    }

    async fn new_order(
        server: &BybitServerConfig,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let category = config.trade_category.clone();
        let symbol = config.trade_symbol.clone();

        let price = if order_type == OrderType::Market {
            None
        } else {
            Some(price)
        };

        let order = BybitOrderRequest {
            category: category.clone(),
            symbol: symbol.clone(),
            side: side.to_string(),
            order_type: order_type.to_string(),
            qty: size,
            order_link_id: client_order_id,
            price: price,
        };

        let order_json = serde_json::to_string(&order)?;
        log::debug!("order_json={}", order_json);

        let path = "/v5/order/create";

        let result = Self::post_sign(&server, path, &order_json)
            .await
            .with_context(|| {
                format!(
                    "new_order: server={:?} / path={:?} / order_json={:?}",
                    server, path, order_json
                )
            })?;

        let r = serde_json::from_value::<BybitOrderRestResponse>(result.body)
            .with_context(|| format!("parse error in new_order "))?;

        let is_maker = order_type.is_maker();

        // in bybit only order id is valid when order is created.
        let mut order = Order {
            category: category,
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
            open_position: dec![0.0],
            close_position: dec![0.0],
            position: dec![0.0],
            profit: dec![0.0],
            fee: dec![0.0],
            total_profit: dec![0.0],
        };

        order.update_balance(config);

        return Ok(vec![order]);
    }

    async fn cancel_order(
        server: &BybitServerConfig,
        config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order> {
        let category = config.trade_category.clone();
        let message = CancelOrderMessage {
            category: category.clone(),
            symbol: config.trade_symbol.clone(),
            order_id: order_id.to_string(),
        };

        let message_json = serde_json::to_string(&message)?;
        let path = "/v5/order/cancel";
        let result = Self::post_sign(&server, path, &message_json)
            .await
            .with_context(|| {
                format!(
                    "cancel_order: server={:?} / path={:?} / message_json={:?}",
                    server, path, message_json
                )
            })?;

        let r = serde_json::from_value::<BybitOrderRestResponse>(result.body)?;

        let mut order = Order {
            category,
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
            open_position: dec![0.0],
            close_position: dec![0.0],
            position: dec![0.0],
            profit: dec![0.0],
            fee: dec![0.0],
            total_profit: dec![0.0],
        };

        order.update_balance(config);

        return Ok(order);
    }

    // TODO: implement paging.
    async fn open_orders(
        server: &BybitServerConfig,
config: &MarketConfig,
    ) -> anyhow::Result<Vec<Order>> {
        let query_string = format!(
            "category={}&symbol={}&limit=50",
            config.trade_category, config.trade_symbol
        );

        let path = "/v5/order/realtime";

        let result = Self::get_sign(&server, path, &query_string)
            .await
            .with_context(|| {
                format!(
                    "open_orders: server={:?} / path={:?} / query_string={:?}",
                    server, path, query_string
                )
            })?;

        log::debug!("result.body={:?}", result.body);
        if result.body.is_null() {
            return Ok(vec![]);
        }

        let response = serde_json::from_value::<BybitMultiOrderStatus>(result.body)
            .with_context(|| format!("order status parse error"))?;

        let mut orders: Vec<Order> = response.into();

        for o in orders.iter_mut() {
            o.update_balance(config);
        }

        Ok(orders)
    }

    async fn get_account(
        server: &BybitServerConfig
    ) -> anyhow::Result<AccountCoins> {

        let path = "/v5/account/wallet-balance";
        // TODO: implement otherthn accountType=UNIFIED(e.g.inverse)
        let query_string = format!("accountType=UNIFIED");
        //let query_string = format!("accountType=UNIFIED");

        let response = Self::get_sign(&server, path, &query_string)
            .await
            .with_context(|| format!("get_account error: {}/{}/{}", &server.get_rest_server(), path, &query_string))?;

        ensure!(response.is_success(), format!("return_code = {}, msg={}", response.is_success(), response.return_message));

        let account_status = serde_json::from_value::<BybitAccountResponse>(response.body)?;
        let coins: AccountCoins = account_status.into();

        Ok(coins)
    }

    fn format_historical_data_url(
        history_web_base: &str,
        category: &str,
        symbol: &str,
        yyyy: i64,
        mm: i64,
        dd: i64,
    ) -> String {
        format!(
            "{}/trading/{}/{}{:04}-{:02}-{:02}.csv.gz",
            history_web_base, symbol, symbol, yyyy, mm, dd
        )
    }

    /// timestamp,      symbol,side,size,price,  tickDirection,trdMatchID,                          grossValue,  homeNotional,foreignNotional
    /// 1620086396.8268,BTCUSDT,Buy,0.02,57199.5,ZeroMinusTick,224061a0-e105-508c-9696-b53ab4b5bb03,114399000000.0,0.02,1143.99    

    fn rec_to_trade(rec: &StringRecord) -> Trade {
        let timestamp = rec
            .get(0)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default()
            * 1_000_000.0;

        let timestamp = timestamp as MicroSec;

        let order_side = match rec.get(2).unwrap_or_default() {
            "Buy" => OrderSide::Buy,
            "Sell" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

        let id = rec.get(6).unwrap_or_default().to_string();

        let price = rec
            .get(4)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let price: Decimal = Decimal::from_f64(price).unwrap();

        let size = rec
            .get(3)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let size = Decimal::from_f64(size).unwrap();

        let trade = Trade::new(
            timestamp,
            order_side,
            price,
            size,
            LogStatus::FixArchiveBlock,
            &id,
        );

        return trade;
    }

    fn archive_has_header() -> bool {
        true
    }
}

use tracing::instrument;    

impl BybitRestApi {
    async fn get(
        server: &BybitServerConfig,
        path: &str,
        params: &str,
    ) -> anyhow::Result<BybitRestResponse> {
        let query = format!("{}?{}", path, params);

        let response = rest_get(&server.get_rest_server(), &query, vec![], None, None)
            .await
            .with_context(|| format!("rest_get error: {}/{}", &server.get_rest_server(), &query))?;

        Self::parse_rest_response(response)
    }

    pub async fn get_sign(
        server: &BybitServerConfig,
        path: &str,
        query_string: &str,
    ) -> anyhow::Result<BybitRestResponse> {
        let timestamp = format!("{}", NOW() / 1_000);
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();
        let recv_window = "5000";

        let param_to_sign = format!("{}{}{}{}", timestamp, api_key.clone(), recv_window, query_string);
        let sign = hmac_sign(&api_secret, &param_to_sign);

        let mut headers: Vec<(&str, &str)> = vec![];

        headers.push(("X-BAPI-SIGN", &sign));
        headers.push(("X-BAPI-API-KEY", &api_key));
        headers.push(("X-BAPI-TIMESTAMP", &timestamp));
        headers.push(("X-BAPI-RECV-WINDOW", recv_window));

        let result = rest_get(&server.rest_server, path, headers, Some(query_string), None)
            .await
            .with_context(|| format!("get_sign error: {}/{}/{}", &server.rest_server, path, query_string))?;

        Self::parse_rest_response(result)
    }

    async fn post_sign(
        server: &BybitServerConfig,
        path: &str,
        body: &str,
    ) -> anyhow::Result<BybitRestResponse> {
        let timestamp = format!("{}", NOW() / 1_000);
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();
        let recv_window = "5000";

        let param_to_sign = format!("{}{}{}{}", timestamp, api_key.clone(), recv_window, body);
        let sign = hmac_sign(&api_secret, &param_to_sign);

        let mut headers: Vec<(&str, &str)> = vec![];

        headers.push(("X-BAPI-SIGN", &sign));
        headers.push(("X-BAPI-API-KEY", &api_key));
        headers.push(("X-BAPI-TIMESTAMP", &timestamp));
        headers.push(("X-BAPI-RECV-WINDOW", recv_window));
        headers.push(("Content-Type", "application/json"));

        let response = rest_post(&server.get_rest_server(), path, headers, &body)
            .await
            .with_context(|| format!("post_sign error {}/{}", server.get_rest_server(), path))?;

        Self::parse_rest_response(response)
    }

    fn parse_rest_response(response: String) -> anyhow::Result<BybitRestResponse> {
        if response == "" {
            log::warn!("empty response");
            let response = BybitRestResponse {
                return_code: 0,
                return_message: "Ok".to_string(),
                return_ext_info: Value::Null,
                time: NOW() / 1_000,
                body: Value::Null,
            };
            return Ok(response);
        }

        log::debug!("rest response: {}", response);

        let result = from_str::<BybitRestResponse>(&response)
            .with_context(|| format!("parse error in parse_rest_response: {:?}", response))?;

        ensure!(
            result.is_success(),
            format!("parse rest response error = {}", result.return_message)
        );

        return Ok(result);
    }

    async fn try_get_trade_klines(
        server: &BybitServerConfig,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
    ) -> anyhow::Result<Vec<Kline>> {
        if start_time == 0 || (end_time == 0) {
            return Err(anyhow!(
                "end_time({}) or start_time({}) is zero",
                end_time,
                start_time
            ));
        }

        let path = "/v5/market/kline";

        let params = format!(
            "category={}&symbol={}&interval={}&start={}&end={}&limit={}", // 1 min
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            1, // interval 1 min.
            microsec_to_bybit_timestamp(start_time),
            microsec_to_bybit_timestamp(end_time),
            1000 // max records.
        );

        let r = Self::get(server, path, &params).await;

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let message = r.unwrap().body;

        let result = serde_json::from_value::<BybitKlinesResponse>(message)
            .with_context(|| format!("parse error in try_get_trade_klines"))?;

        return Ok(result.into());
    }
}


#[cfg(test)]
#[allow(non_snake_case)]
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused_variables)]
mod bybit_rest_test {
    use super::*;
    use std::{any, thread::sleep, time::Duration};
    use rbot_lib::common::{init_log};

    use crate::config::BybitConfig;
    use pyo3::ffi::Py_Initialize;
    use rbot_lib::common::{init_debug_log, time_string, HHMM};
    use rust_decimal_macros::dec;


    #[tokio::test]
    async fn get_board_snapshot_test()  -> anyhow::Result<()> {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::get_board_snapshot(&server_config, &config).await?;
        println!("{:?}", r);

        Ok(())
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

        let r = r.unwrap();
        let l = r.len();
        println!(
            "{:?}-{:?}  {:?} [rec]",
            time_string(r[0].timestamp),
            time_string(r[l - 1].timestamp),
            l
        );
    }

    #[tokio::test]
    async fn test_new_limit_order() {
        let server_config = BybitServerConfig::new(false);
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
        let server_config = BybitServerConfig::new(false);
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
        let server_config = BybitServerConfig::new(false);
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

        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::open_orders(&server_config, &config).await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_get_account() {
        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let r = BybitRestApi::get_account(&server_config).await;
        println!("{:?}", r);
        assert!(r.is_ok());
    }
}

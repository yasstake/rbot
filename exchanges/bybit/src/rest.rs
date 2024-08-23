#![allow(unused)]
// Copyright(c) 2022-2024. yasstake. All rights reserved.

use std::convert;
use std::fmt::format;

use chrono::Datelike as _;
use csv::StringRecord;
use polars::chunked_array::ops::ChunkCast as _;
use polars::datatypes::DataType;
use polars::export::num::FromPrimitive;
use polars::frame::DataFrame;
use polars::lazy::dsl::col;
use polars::lazy::dsl::lit;
use polars::lazy::frame::IntoLazy;
use polars::series::Series;
use rbot_lib::common::convert_klines_to_trades;
use rbot_lib::common::split_yyyymmdd;
use rbot_lib::common::time_string;
use rbot_lib::common::to_naive_datetime;
use rbot_lib::common::AccountCoins;
use rbot_lib::common::AccountPair;
use rbot_lib::common::BoardTransfer;
use rbot_lib::common::Kline;
use rbot_lib::common::LogStatus;
use rbot_lib::common::FLOOR_SEC;
use rbot_lib::db::ohlcv_end;
use rbot_lib::db::ohlcv_start;
use rbot_lib::db::TradeDataFrame;
use rbot_lib::db::KEY;
use rbot_lib::net::check_exist;
use rbot_lib::net::RestPage;
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
    ExchangeConfig, Trade, NOW,
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

pub struct BybitRestApi {
    server_config: ExchangeConfig,
}

impl BybitRestApi {
    pub fn new(server_config: &ExchangeConfig) -> Self {
        Self {
            server_config: server_config.clone(),
        }
    }
}

impl RestApi for BybitRestApi {
    async fn get_board_snapshot(&self, config: &MarketConfig) -> anyhow::Result<BoardTransfer> {
        let server = &self.server_config;

        let path = "/v5/market/orderbook";

        let params = format!(
            "category={}&symbol={}&limit={}",
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            config.board_depth
        );

        let r = Self::get(server, path, &params).await.with_context(|| {
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

    async fn get_recent_trades(&self, config: &MarketConfig) -> anyhow::Result<Vec<Trade>> {
        let server = &self.server_config;

        let path = "/v5/market/recent-trade";

        let params = format!(
            "category={}&symbol={}&limit={}",
            &config.trade_category,
            &config.trade_symbol,
            1000 // max records.
        );

        let r = Self::get(server, path, &params).await.with_context(|| {
            format!(
                "get_recent_trades: server={:?} / path={:?} / params={:?}",
                server, path, params
            )
        })?;

        let result = serde_json::from_value::<BybitTradeResponse>(r.body)
            .with_context(|| format!("parse error in get_recent_trades"))?;

        let mut trades: Vec<Trade> = result.into();

        Ok(trades)
    }

    async fn get_trades(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        _page: &RestPage,
    ) -> anyhow::Result<(Vec<Trade>, RestPage)> {
        Err(anyhow!("Bybit does not have get trade by range"))
    }

    async fn get_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage,
    ) -> anyhow::Result<(Vec<Kline>, RestPage)> {
        let start_time = FLOOR_SEC(start_time, self.klines_width());
        // 終わり時間は、TICKの範囲にふくまれていれば全体がかえってくる。
        let end_time = FLOOR_SEC(end_time, self.klines_width());

        println!("kline start_time {:?} / end_time {:?}", time_string(start_time), time_string(end_time));

        if start_time == end_time {
            return Ok((vec![], RestPage::Done));
        }

        if *page == RestPage::Done {
            return Err(anyhow!("call with RestPage::Done"));
        }

        if start_time == 0 || (end_time == 0) {
            return Err(anyhow!(
                "end_time({}) or start_time({}) is zero",
                end_time,
                start_time
            ));
        }

        let end_time = if let RestPage::Time(t) = page {
            t.clone() - 1           // 次のTick全体がかえってくるのをさける。
        }
        else {
            end_time
        };

        let path = "/v5/market/kline";

        let klines_width = self.klines_width() / 60;        // convert to min

        let params = format!(
            "category={}&symbol={}&interval={}&start={}&end={}&limit={}", // 1 min
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            klines_width, // interval 1 min.
            microsec_to_bybit_timestamp(start_time),
            microsec_to_bybit_timestamp(end_time),
            1000 // max records.
        );

        let r = Self::get(&self.server_config, path, &params).await;

        if r.is_err() {
            let r = r.unwrap_err();
            return Err(r);
        }

        let message = r.unwrap().body;

        let result = serde_json::from_value::<BybitKlinesResponse>(message)
            .with_context(|| format!("parse error in try_get_trade_klines"))?;

        let mut klines: Vec<Kline> = result.into();
        klines.reverse();

        let len = klines.len();

        let page = if len == 0 || klines[0].timestamp <= start_time {
            RestPage::Done
        }
        else {
            RestPage::Time((klines[0].timestamp))
        };
        
        return Ok((klines, page))
      }

    fn klines_width(&self) -> i64 {
        60
    }

    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let server = &self.server_config;

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

        let mut order = Order::default();

        order.category = category;
        order.symbol = symbol;
        order.create_time = msec_to_microsec(result.time);
        order.status = OrderStatus::New;
        order.order_id = r.order_id;
        order.client_order_id = r.order_link_id;
        order.order_side = side;
        order.order_type = order_type;
        order.order_price = if order_type == OrderType::Market {
            dec![0.0]
        } else {
            price.unwrap()
        };
        order.order_size = size;
        order.remain_size = size;
        order.update_time = msec_to_microsec(result.time);
        order.is_maker = is_maker;

        order.update_balance(&config);

        return Ok(vec![order]);
    }

    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        let server = &self.server_config;

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

        let mut order = Order::default();

        order.category = category;
        order.symbol = config.trade_symbol.clone();
        order.create_time = msec_to_microsec(result.time);
        order.status = OrderStatus::Canceled;
        order.order_id = r.order_id;
        order.client_order_id = r.order_link_id;
        order.order_side = OrderSide::Unknown;
        order.order_type = OrderType::Limit;
        order.update_time = msec_to_microsec(result.time);
        order.is_maker = true;

        order.update_balance(config);

        return Ok(order);
    }

    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        let server = &self.server_config;

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

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        let server = &self.server_config;

        let path = "/v5/account/wallet-balance";
        // TODO: implement otherthn accountType=UNIFIED(e.g.inverse)
        let query_string = format!("accountType=UNIFIED");
        //let query_string = format!("accountType=UNIFIED");

        let response = Self::get_sign(&server, path, &query_string)
            .await
            .with_context(|| {
                format!(
                    "get_account error: {}/{}/{}",
                    &server.get_rest_server(),
                    path,
                    &query_string
                )
            })?;

        ensure!(
            response.is_success(),
            format!(
                "return_code = {}, msg={}",
                response.is_success(),
                response.return_message
            )
        );

        let account_status = serde_json::from_value::<BybitAccountResponse>(response.body)?;
        let coins: AccountCoins = account_status.into();

        Ok(coins)
    }

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String {
        let web_base = self.server_config.get_historical_web_base();

        let (yyyy, mm, dd) = split_yyyymmdd(date);

        format!(
            "{}/trading/{}/{}{:04}-{:02}-{:02}.csv.gz",
            web_base, config.trade_symbol, config.trade_symbol, yyyy, mm, dd
        )
    }

    fn archive_has_header(&self) -> bool {
        true
    }

    async fn has_archive(&self, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool> {
        let url = self.history_web_url(config, date);

        let result = check_exist(&url).await?;

        Ok(result)
    }

    /// create DataFrame with columns;
    ///  KEY:time_stamp(Int64), KEY:order_side(Bool), KEY:price(f64), KEY:size(f64)
    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame> {
        let df = df.clone();

        let timestamp = df.column("timestamp")?.f64()? * 1_000_000.0;
        let timestamp = timestamp.cast(&DataType::Int64)?;

        let timestamp = timestamp.clone();
        let mut timestamp = Series::from(timestamp.clone());
        timestamp.rename(KEY::timestamp);

        let mut id = df.column("trdMatchID")?.clone();
        id.rename(KEY::id);

        let mut side = df.column("side")?.clone();
        side.rename(KEY::order_side);

        let mut price = df.column("price")?.clone();
        price.rename(KEY::price);

        let mut size = df.column("size")?.clone();
        size.rename(KEY::size);

        let df = DataFrame::new(vec![timestamp, side, price, size, id])?;


        Ok(df)
    }
}

impl BybitRestApi {
    async fn get(
        server: &ExchangeConfig,
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
        server: &ExchangeConfig,
        path: &str,
        query_string: &str,
    ) -> anyhow::Result<BybitRestResponse> {
        let timestamp = format!("{}", NOW() / 1_000);
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();
        let recv_window = "5000";

        let param_to_sign = format!(
            "{}{}{}{}",
            timestamp,
            api_key.clone(),
            recv_window,
            query_string
        );
        let sign = hmac_sign(&api_secret, &param_to_sign);

        let mut headers: Vec<(&str, &str)> = vec![];

        headers.push(("X-BAPI-SIGN", &sign));
        headers.push(("X-BAPI-API-KEY", &api_key));
        headers.push(("X-BAPI-TIMESTAMP", &timestamp));
        headers.push(("X-BAPI-RECV-WINDOW", recv_window));

        let result = rest_get(&server.get_rest_server(), path, headers, Some(query_string), None)
            .await
            .with_context(|| {
                format!(
                    "get_sign error: {}/{}/{}",
                    &server.get_rest_server(), path, query_string
                )
            })?;

        Self::parse_rest_response(result)
    }

    async fn post_sign(
        server: &ExchangeConfig,
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

        // log::debug!("rest response: {}", response);

        let result = from_str::<BybitRestResponse>(&response)
            .with_context(|| format!("parse error in parse_rest_response: {:?}", response))?;

        ensure!(
            result.is_success(),
            format!("parse rest response error = {}", result.return_message)
        );

        return Ok(result);
    }
}

#[cfg(test)]
#[allow(non_snake_case)]
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused_variables)]
mod bybit_rest_test {
    use super::*;
    use rbot_lib::common::{init_log, DAYS};
    use std::{any, thread::sleep, time::Duration};

    use crate::config::BybitConfig;
    use pyo3::ffi::Py_Initialize;
    use rbot_lib::common::{init_debug_log, time_string, HHMM};
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn get_board_snapshot_test() -> anyhow::Result<()> {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();

        let api = BybitRestApi::new(&server_config);

        let r = api.get_board_snapshot(&config).await?;
        println!("{:?}", r);

        Ok(())
    }

    #[tokio::test]
    async fn test_recent_trades() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let r = api.get_recent_trades(&config).await;
        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_trade_kline() -> anyhow::Result<()> {
        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let now = NOW();
        let start_time = now - DAYS(10);

        let (trades, page) = api
            .get_trades(&config, start_time, now, &RestPage::New)
            .await?;
        let l = trades.len();
        println!(
            "{:?}-{:?}  {:?} [rec]",
            time_string(trades[0].time),
            time_string(trades[l - 1].time),
            l
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_klines() -> anyhow::Result<()>{
        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let now = FLOOR_SEC(NOW(), 60) - HHMM(24, 10);
        let start_time = now - HHMM(24, 0);

        let (kline, page) = api.get_klines(&config, start_time, now, &RestPage::New).await?;
        println!("{:?} {:?}", kline.len(), page);
        println!("start = {:?} / actual = {:?}", time_string(start_time), time_string(kline[0].timestamp));
        println!("end = {:?} / actual = {:?}", time_string(now), time_string(kline[kline.len()-1].timestamp));
        println!("end rec = {:?}", kline[kline.len() -1]);

        println!("--");

        let (kline, page) = api.get_klines(&config, start_time, now + HHMM(0, 1), &RestPage::New).await?;
        println!("{:?} {:?}", kline.len(), page);
        println!("start = {:?} / actual = {:?}", time_string(start_time), time_string(kline[0].timestamp));
        println!("end = {:?} / actual = {:?}", time_string(now), time_string(kline[kline.len()-1].timestamp));
        println!("end rec-1 = {:?}", kline[kline.len() -2]);
        println!("end rec = {:?}", kline[kline.len() -1]);

        println!("start rec = {:?}", kline[0]);

        println!("--");

        let (kline, page) = api.get_klines(&config, start_time, now, &page).await?;

        println!("{:?}, {:?}", kline.len(), page);
        println!("start = {:?} / actual = {:?}", time_string(start_time), time_string(kline[0].timestamp));
        println!("end = {:?} / actual = {:?}", time_string(now), time_string(kline[kline.len()-1].timestamp));
        println!("end rec = {:?}", kline[kline.len() -1]);


        Ok(())
    }



    #[tokio::test]
    async fn test_new_limit_order() {
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let r = api
            .new_order(
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
        let api = BybitRestApi::new(&server_config);

        let r = api
            .new_order(
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
        let api = BybitRestApi::new(&server_config);

        let r = api
            .new_order(
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

        let r = api.cancel_order(&config, &r.unwrap()[0].order_id).await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_open_orders() {
        init_debug_log();

        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let r = api.open_orders(&config).await;

        assert!(r.is_ok());
        println!("{:?}", r);
    }

    #[tokio::test]
    async fn test_get_account() {
        init_debug_log();
        let server_config = BybitServerConfig::new(false);
        let config = BybitConfig::BTCUSDT();
        let api = BybitRestApi::new(&server_config);

        let r = api.get_account().await;
        println!("{:?}", r);
        assert!(r.is_ok());
    }
}

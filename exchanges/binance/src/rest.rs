// Copyright(c) 2022-2024. yasstake. All rights reserved.

use crate::{
    binance_order_status_vec_to_orders,
    BinanceAccountInformation, BinanceCancelOrderResponse, BinanceOrderResponse,
    BinanceOrderStatus, BinanceRestBoard, BinanceServerConfig, BinanceTradeMessage,
};

use anyhow::anyhow;
use csv::StringRecord;
use rbot_lib::{
    common::{
        flush_log, hmac_sign, AccountCoins, BoardTransfer, Kline, LogStatus, MarketConfig, MicroSec, Order, OrderSide, OrderType, ServerConfig, Trade, NOW
    },
    net::{rest_delete, rest_get, rest_post, rest_put, RestApi},
};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde_json::Value;

use anyhow::Context;

pub struct BinanceRestApi {}

impl RestApi<BinanceServerConfig> for BinanceRestApi {
    /// https://binance-docs.github.io/apidocs/spot/en/#order-book

    async fn get_board_snapshot(
        server: &BinanceServerConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<BoardTransfer> {
        let path = "/api/v3/depth";
        let params = format!("symbol={}&limit=1000", &config.trade_symbol);

        let message = Self::get(server, path, &params)
            .await
            .with_context(|| format!("get_board_snapshot error"))?;

        let board: BinanceRestBoard = serde_json::from_value(message)?;

        Ok(board.into())
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
    async fn get_recent_trades(
        server: &BinanceServerConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<Vec<Trade>> {
        log::debug!("get_recent_trades: {:?}", &config.trade_symbol);

        let path = "/api/v3/trades";
        let params = format!("symbol={}&limit=1000", &config.trade_symbol);

        let messasge = Self::get(server, path, &params)
            .await
            .with_context(|| format!("get_recent_trades error"))?;

        log::debug!("get_recent_trades: {:?}", messasge);

        let trades: Vec<BinanceTradeMessage> = serde_json::from_value(messasge)?;

        log::debug!("get_recent_trades: {:?}", trades.len());

        let mut result: Vec<Trade> = vec![];

        for t in trades {
            result.push(t.to_trade());

            log::debug!("trade: {:?}", t.to_trade());
        }

        Ok(result)
    }

    async fn get_trade_klines(
        _server: &BinanceServerConfig,
        _config: &MarketConfig,
        _start_time: MicroSec,
        _end_time: MicroSec,
    ) -> anyhow::Result<Vec<Kline>> {
        todo!();
    }

    async fn new_order(
        server: &BinanceServerConfig,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let path = "/api/v3/order";
        let side = Self::order_side_string(side);

        let order_type_str: &str = match order_type {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::Unknown => return Err(anyhow!("unknown order type")),
        };

        let mut body = format!(
            "symbol={}&side={}&type={}&quantity={}",
            config.trade_symbol, side, order_type_str, size
        );

        if order_type == OrderType::Limit {
            body = format!("{}&price={}&timeInForce=GTC", body, price);
        }

        if client_order_id.is_some() {
            let cliend_order_id = client_order_id.unwrap();
            body = format!("{}&newClientOrderId={}", body, cliend_order_id);
        }

        let message = Self::post_sign(&server, path, body.as_str())
            .await
            .with_context(|| format!("new_order error"))?;

        let order: BinanceOrderResponse = serde_json::from_value(message)?;

        let orders: Vec<Order> = order.to_order_vec(config);

        Ok(orders)
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#cancel-all-open-orders-on-a-symbol-trade
    async fn cancel_order(
        server: &BinanceServerConfig,
        config: &MarketConfig,
        order_id: &str,
    ) -> anyhow::Result<Order> {
        let path = "/api/v3/order";
        let body = format!("symbol={}&orderId={}", config.trade_symbol, order_id);

        let message = Self::delete_sign(&server, path, body.as_str())
            .await
            .with_context(|| format!("cancel_order error"))?;

        let response: BinanceCancelOrderResponse = serde_json::from_value(message)?;

        Ok(response.to_order(config))
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#current-open-orders-user_data
    async fn open_orders(
        server: &BinanceServerConfig,
        config: &MarketConfig,
    ) -> anyhow::Result<Vec<Order>> {
        let path = "/api/v3/openOrders";
        let query = format!("symbol={}", config.trade_symbol);

        let message = Self::get_sign(server, &path, Some(&query))
            .await
            .with_context(|| format!("open_orders error"))?;

        let orders: Vec<BinanceOrderStatus> = serde_json::from_value(message)?;

        let orders = binance_order_status_vec_to_orders(config, &orders);

        Ok(orders)
    }

    async fn get_account(server: &BinanceServerConfig) -> anyhow::Result<AccountCoins> {
        let path = "/api/v3/account";

        let message = Self::get_sign(server, path, None)
            .await
            .with_context(|| format!("get_account error"))?;

        let account: BinanceAccountInformation = serde_json::from_value(message)?;

        Ok(account.into_coins())
    }

    fn format_historical_data_url(
        history_web_base: &str,
        category: &str,
        symbol: &str,
        yyyy: i64,
        mm: i64,
        dd: i64,
    ) -> String {
        // https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip
        let category = category.to_lowercase();
        if category == "spot" {
            return format!(
                "{}/data/{}/daily/trades/{}/{}-trades-{:04}-{:02}-{:02}.zip",
                history_web_base, category, symbol, symbol, yyyy, mm, dd
            );
        } else {
            log::error!("Unknown category {}", category);
            return "".to_string();
        }
    }

    /// binance csv Formatter
    /// example;
    ///  630277243,16681.46000000,0.00298000,49.71075080,1668816000029,True,True
    ///  630277244,16681.43000000,0.00299000,49.87747570,1668816000053,True,True
    ///  630277245,16681.00000000,0.00299000,49.87619000,1668816000075,True,True    
    fn rec_to_trade(rec: &StringRecord) -> Trade {
        let id = rec.get(0).unwrap_or_default().to_string();
        let price = rec
            .get(1)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let price = Decimal::from_f64(price).unwrap_or_default();

        let size = rec
            .get(2)
            .unwrap_or_default()
            .parse::<f64>()
            .unwrap_or_default();

        let size = Decimal::from_f64(size).unwrap_or_default();

        let timestamp = rec
            .get(4)
            .unwrap_or_default()
            .parse::<MicroSec>()
            .unwrap_or_default()
            * 1_000;

        let is_buyer_make = rec.get(5).unwrap_or_default();
        let order_side = match is_buyer_make {
            "True" => OrderSide::Buy,
            "False" => OrderSide::Sell,
            _ => OrderSide::Unknown,
        };

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

    /// binance csv dose not have header.
    fn archive_has_header() -> bool {
        false
    }
}

impl BinanceRestApi {
    async fn get(server: &BinanceServerConfig, path: &str, params: &str) -> anyhow::Result<Value> {
        let query = format!("{}?{}", path, params);

        log::debug!("path{} / body: {}", path, query);
        flush_log();

        let response = rest_get(&server.get_rest_server(), &query, vec![], None, None)
            .await
            .with_context(|| format!("rest_get error: {}/{}", &server.get_rest_server(), &query))?;

        log::debug!("path{} / body: {}", path, response);

        Self::parse_binance_result(response)
    }

    async fn get_sign(
        server: &BinanceServerConfig,
        path: &str,
        params: Option<&str>,
    ) -> anyhow::Result<Value> {
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();

        let mut headers: Vec<(&str, &str)> = vec![];

        headers.push(("X-MBX-APIKEY", &api_key));

        let q = if params.is_some() {
            params.unwrap().to_string()
        } else {
            "".to_string()
        };

        let query = Self::sign_with_timestamp(&api_secret, &q);
        let message = rest_get(&server.get_rest_server(), path, headers, Some(&query), None)
            .await
            .with_context(|| {
                format!(
                    "binance_get_sign error {}/{}",
                    server.get_rest_server(),
                    path
                )
            })?;

        log::debug!("AUTH GET: path{} / body: {} / {:?}", path, q, message);

        Self::parse_binance_result(message)
    }

    async fn post_sign(
        server: &BinanceServerConfig,
        path: &str,
        body: &str,
    ) -> anyhow::Result<Value> {
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));

        let body = Self::sign_with_timestamp(&api_secret, body);

        log::debug!("path{} / body: {}", path, body);
        let message = rest_post(&server.get_rest_server(), path, headers, &body)
            .await
            .with_context(|| format!("post_sign error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(message)
    }

    fn sign_with_timestamp(secret_key: &str, message: &str) -> String {
        let time = (NOW() / 1_000) as u64;

        let message = format!("{}&recvWindow={}&timestamp={}", message, 6000, time);

        let sign = hmac_sign(secret_key, &message);

        return format!("{}&signature={}", message, sign);
    }

    async fn post_key(
        server: &BinanceServerConfig,
        path: &str,
        body: &str,
    ) -> anyhow::Result<Value> {
        let api_key = server.get_api_key().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));
        let result = rest_post(&server.get_rest_server(), path, headers, body)
            .await
            .with_context(|| format!("post_key error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(result)
    }

    async fn put_key(
        server: &BinanceServerConfig,
        path: &str,
        body: &str,
    ) -> anyhow::Result<Value> {
        let api_key = server.get_api_key().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));
        let result = rest_put(&server.get_rest_server(), path, headers, body)
            .await
            .with_context(|| format!("post_key error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(result)
    }

    pub async fn delete_sign(
        server: &BinanceServerConfig,
        path: &str,
        body: &str,
    ) -> anyhow::Result<Value> {
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));

        let body = Self::sign_with_timestamp(&api_secret, body);

        log::debug!("path{} / body: {}", path, body);
        let result = rest_delete(&server.get_rest_server(), path, headers, &body)
            .await
            .with_context(|| format!("delete_sign error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(result)
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

    pub fn parse_binance_result(message: String) -> anyhow::Result<Value> {
        let v = serde_json::from_str::<Value>(&message)
            .with_context(|| format!("json format error {:?}", message))?;

        let code = v.get("code");
        if code.is_some() {
            let code = code.unwrap().as_i64().unwrap();
            let msg = v.get("msg").unwrap().as_str().unwrap();

            let err_message = format!("{}: {}\n{}", code, msg, message);
            return Err(anyhow!(err_message));
        }

        Ok(v)
    }

    pub async fn create_listen_key(server: &BinanceServerConfig) -> anyhow::Result<String> {
        let message = Self::post_key(server, "/api/v3/userDataStream", "")
            .await
            .with_context(|| format!("create_listen_key error"))?;

        if message.get("listenKey").is_some() {
            let listen_key = message.get("listenKey").unwrap().as_str().unwrap();
            return Ok(listen_key.to_string());
        } else {
            let err_message = format!("listenKey is not found {}", message);
            return Err(anyhow!(err_message));
        }
    }

    pub async fn extend_listen_key(server: &BinanceServerConfig, key: &str) -> anyhow::Result<()> {
        let path = format!("/api/v3/userDataStream?listenKey={}", key);
        let _message = Self::put_key(server, path.as_str(), "")
            .await
            .with_context(|| format!("extend_listen_key error"))?;

        Ok(())
    }

    pub async fn get_historical_trades(
        server: &BinanceServerConfig,
        config: &MarketConfig,
        from_id: i64,
        from_time: MicroSec,
    ) -> anyhow::Result<Vec<Trade>> {
        let path = "/api/v3/historicalTrades";

        let params = if from_id == 0 {
            format!("symbol={}&limit=1000", config.trade_symbol)
        } else {
            format!(
                "symbol={}&fromId={}&limit=1000",
                config.trade_symbol, from_id
            )
        };

        let result = Self::get(&server, path, &params).await?;

        let binance_trades: Vec<BinanceTradeMessage> = serde_json::from_value(result)?;

        let mut trades: Vec<Trade> = vec![];

        for t in binance_trades {
            let mut trade = t.to_trade();

            if from_time != 0 && trade.time < from_time {
                continue;
            }

            trade.status = LogStatus::FixArchiveBlock;

            trades.push(trade);
        }

        Ok(trades)
    }
}

#[cfg(test)]
mod binance_api_test {
    use super::*;
    use crate::BinanceConfig;
    use rbot_lib::common::init_debug_log;
    use rust_decimal_macros::dec;
    
    #[tokio::test]
    async fn test_board_snapshot() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_board_snapshot(&server, &config).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_recent_trades() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_recent_trades(&server, &config).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_klines() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_trade_klines(&server, &config, 0, 0).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_new_order_limit() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::new_order(
            &server,
            &config,
            OrderSide::Buy,
            dec![50000],
            dec![0.001],
            OrderType::Limit,
            None,
        )
        .await;
        println!("result: {:?}", result);
    }


    #[tokio::test]
    async fn test_new_order_market() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        init_debug_log();

        let result = BinanceRestApi::new_order(
            &server,
            &config,
            OrderSide::Buy,
            dec![0],
            dec![0.001],
            OrderType::Market,
            None,
        )
        .await;
        println!("result: {:?}", result);
    }



    #[tokio::test]
    async fn test_open_orders() {
        init_debug_log();
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::open_orders(&server, &config).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_get_account() -> anyhow::Result<()> {
        init_debug_log();
        let server = BinanceServerConfig::new(false);

        let result = BinanceRestApi::get_account(&server).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_crate_listen_key() -> anyhow::Result<()> {
        init_debug_log();
        let server = BinanceServerConfig::new(false);

        let result = BinanceRestApi::create_listen_key(&server).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_historical_trades() {
        init_debug_log();
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_historical_trades(&server, &config, 0, 0).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_get_historical_trades_from_id_10000() {
        init_debug_log();
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_historical_trades(&server, &config, 10000, 0).await;
        println!("result: {:?}", result);
    }


}

// Copyright(c) 2022-2024. yasstake. All rights reserved.

use crate::{
    binance_order_status_vec_to_orders, BinanceAccountInformation, BinanceCancelOrderResponse,
    BinanceOrderResponse, BinanceOrderStatus, BinanceRestBoard, BinanceServerConfig,
    BinanceTradeMessage,
};

use anyhow::anyhow;
use polars::{chunked_array::{ops::{ChunkApply, ChunkCast as _}, ChunkedArray}, datatypes::DataType, frame::DataFrame, prelude::NamedFrom as _, series::{IntoSeries, Series}};
use rbot_lib::{
    common::{
        flush_log, hmac_sign, split_yyyymmdd, AccountCoins, BoardTransfer, Kline, LogStatus,
        MarketConfig, MicroSec, Order, OrderSide, OrderType, ExchangeConfig, Trade, NOW,
    }, db::KEY, net::{rest_delete, rest_get, rest_post, rest_put, RestApi, RestPage}
};
use rust_decimal::Decimal;
use serde_json::Value;

use anyhow::Context;

#[derive(Clone, Debug)]
pub struct BinanceRestApi {
    server_config: ExchangeConfig,
}

impl BinanceRestApi {
    pub fn new(server_config: &ExchangeConfig) -> Self {
        Self {
            server_config: server_config.clone(),
        }
    }
}

impl RestApi for BinanceRestApi {
    /// https://binance-docs.github.io/apidocs/spot/en/#order-book

    async fn get_board_snapshot(&self, config: &MarketConfig) -> anyhow::Result<BoardTransfer> {
        let path = "/api/v3/depth";
        let params = format!("symbol={}&limit=1000", &config.trade_symbol);

        let message = self
            .get(path, &params)
            .await
            .with_context(|| format!("get_board_snapshot error"))?;

        let board: BinanceRestBoard = serde_json::from_value(message)?;

        Ok(board.into())
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
    async fn get_recent_trades(&self, config: &MarketConfig) -> anyhow::Result<Vec<Trade>> {
        log::debug!("get_recent_trades: {:?}", &config.trade_symbol);

        let path = "/api/v3/trades";
        let params = format!("symbol={}&limit=1000", &config.trade_symbol);

        let messasge = self
            .get(path, &params)
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

    // 25 widghts => 6000 weights/min = 240 req per sec => 120 for safty. 8ms/Reqa
    async fn get_trades(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage,
    ) -> anyhow::Result<(Vec<Trade>, RestPage)> {
        log::debug!("get_recent_trades: {:?}", &config.trade_symbol);

        let path = "/api/v3/historicalTrades";

        let mut params = format!("symbol={}&limit=1000", &config.trade_symbol);

        if RestPage::New == *page {
            let trades = self.get_recent_trades(config).await?;

            let id: i64 = trades[0].id.parse()?;

            return Ok((trades, RestPage::Int(id)));
        } else if RestPage::Done == *page {
            return Err(anyhow!("called with RestPage::Done"));
        }

        let page_id: i64;

        if let RestPage::Int(id) = page {
            page_id = *id;

            if 1000 < page_id {
                params += &format!("&fromId={}", page_id - 1000);
            } else {
                params += &format!("&fromId={}", 0);
            }
        } else {
            log::error!("unknown page {:?}", page);

            return Err(anyhow!("unknown page{:?}", page));
        }

        let messasge = self
            .get(path, &params)
            .await
            .with_context(|| format!("get_trades error"))?;

        let trades: Vec<BinanceTradeMessage> = serde_json::from_value(messasge)?;

        let mut result: Vec<Trade> = vec![];

        if trades.len() == 0 {
            return Ok((vec![], RestPage::Done));
        }

        for t in trades {
            let trade: Trade = t.to_trade();

            if trade.time < start_time {
                log::debug!("trade: [{}]{:?}", start_time, trade);

                continue;
            }

            let id: i64 = trade.id.parse()?;
            if page_id <= id {
                continue;
            }

            result.push(trade);
        }

        if result.len() < 1000 {
            Ok((result, RestPage::Done))
        } else {
            let page = RestPage::Int(result[0].id.parse()?);
            Ok((result, page))
        }
    }

    async fn get_klines(
        &self,
        _config: &MarketConfig,
        _start_time: MicroSec,
        _end_time: MicroSec,
        page: &RestPage,
    ) -> anyhow::Result<(Vec<Kline>, RestPage)> {
        todo!();
    }

    fn klines_width(&self) -> i64 {
        todo!();
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

        let message = self
            .post_sign(path, body.as_str())
            .await
            .with_context(|| format!("new_order error"))?;

        let order: BinanceOrderResponse = serde_json::from_value(message)?;

        let orders: Vec<Order> = order.to_order_vec(config);

        Ok(orders)
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#cancel-all-open-orders-on-a-symbol-trade
    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        let path = "/api/v3/order";
        let body = format!("symbol={}&orderId={}", config.trade_symbol, order_id);

        let message = self
            .delete_sign(path, body.as_str())
            .await
            .with_context(|| format!("cancel_order error"))?;

        let response: BinanceCancelOrderResponse = serde_json::from_value(message)?;

        Ok(response.to_order(config))
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#current-open-orders-user_data
    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        let path = "/api/v3/openOrders";
        let query = format!("symbol={}", config.trade_symbol);

        let message = self
            .get_sign(&path, Some(&query))
            .await
            .with_context(|| format!("open_orders error"))?;

        let orders: Vec<BinanceOrderStatus> = serde_json::from_value(message)?;

        let orders = binance_order_status_vec_to_orders(config, &orders);

        Ok(orders)
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        let path = "/api/v3/account";

        let message = self
            .get_sign(path, None)
            .await
            .with_context(|| format!("get_account error"))?;

        let account: BinanceAccountInformation = serde_json::from_value(message)?;

        Ok(account.into_coins())
    }

    async fn has_archive(&self, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool> {
        todo!()
    }

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String {
        // https://data.binance.vision/data/spot/daily/trades/BTCBUSD/BTCBUSD-trades-2022-11-19.zip
        let category = config.trade_category.to_lowercase();

        let (yyyy, mm, dd) = split_yyyymmdd(date);

        // TODO: implement other than spot
        if category == "spot" {
            return format!(
                "{}/data/{}/daily/trades/{}/{}-trades-{:04}-{:02}-{:02}.zip",
                self.server_config.get_historical_web_base(),
                config.trade_category,
                config.trade_symbol,
                config.trade_symbol,
                yyyy,
                mm,
                dd
            );
        } else {
            log::error!("Unknown category {}", category);
            return "".to_string();
        }
    }

    fn archive_has_header(&self) -> bool {
        false
    }
    
    /// log_df format as below;
    ///     ID(0)      price(1)   size(2)                  timestamp[ms](4)  is_buyer(5)
    /// ┌────────────┬──────────┬──────────┬─────────────┬───────────────┬──────────┬──────────┐
    /// │ column_1   ┆ column_2 ┆ column_3 ┆ column_4    ┆ column_5      ┆ column_6 ┆ column_7 │
    /// │ ---        ┆ ---      ┆ ---      ┆ ---         ┆ ---           ┆ ---      ┆ ---      │
    /// │ i64        ┆ f64      ┆ f64      ┆ f64         ┆ i64           ┆ bool     ┆ bool     │
    /// ╞════════════╪══════════╪══════════╪═════════════╪═══════════════╪══════════╪══════════╡
    /// │ 3730692451 ┆ 56022.0  ┆ 0.005    ┆ 280.11      ┆ 1722988800052 ┆ true     ┆ true     │
    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame> {
        let _ = df;
        println!("{:?}", df);

        let df = df.clone();

        let timestamp = df.select_at_idx(4).unwrap().i64()? * 1_000;
        let timestamp = timestamp.cast(&DataType::Int64)?;

        let timestamp = timestamp.clone();
        let mut timestamp = Series::from(timestamp.clone());
        timestamp.rename(KEY::timestamp);

        let id_org = df.select_at_idx(0).unwrap();
        let id_org = id_org.cast(&DataType::String)?;        
        let mut id = Series::from(id_org.clone());
        id.rename(KEY::id);
        
        let side = df.select_at_idx(5).unwrap().clone();

        let side_vec: Vec<String> = side.bool()?.into_iter().map(|order_side| {
            match order_side {
                Some(true) => {
                    "Buy".to_string()
                },
                Some(false) => {
                    "Sell".to_string()
                }
                _ => {
                    log::error!("unknown side in log");
                    "Unknown".to_string()
                }
            } 
        }).collect();
        let side = Series::new(KEY::order_side, side_vec);

        let mut price = df.select_at_idx(1).unwrap().clone();
        price.rename(KEY::price);

        let mut size = df.select_at_idx(2).unwrap().clone();
        size.rename(KEY::size);

        let df = DataFrame::new(vec![timestamp, side, price, size, id])?;


        Ok(df)

    }

    /*
    fn format_historical_data_url(
    }

    fn line_to_trade(_line: &str) -> Trade {
        println!("NOT IMPLEMENTED line_to_trade");
        Trade::default()
    }

    fn convert_archive_line(_line: &str) -> String {
        println!("NOT IMPLEMENTED convert_archive_line");
        "".to_string()
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
    */
}

impl BinanceRestApi {
    async fn get(&self, path: &str, params: &str) -> anyhow::Result<Value> {
        let server = &self.server_config;
        let query = format!("{}?{}", path, params);

        log::debug!("path{} / body: {}", path, query);
        flush_log();

        let response = rest_get(&server.get_rest_server(), &query, vec![], None, None)
            .await
            .with_context(|| format!("rest_get error: {}/{}", &server.get_rest_server(), &query))?;

        log::debug!("path{} / body: {}", path, response);

        Self::parse_binance_result(response)
    }

    async fn get_sign(&self, path: &str, params: Option<&str>) -> anyhow::Result<Value> {
        let server = &self.server_config;
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

    async fn post_sign(&self, path: &str, body: &str) -> anyhow::Result<Value> {
        let server = &self.server_config;
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

    async fn post_key(&self, path: &str, body: &str) -> anyhow::Result<Value> {
        let server = &self.server_config;
        let api_key = server.get_api_key().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));
        let result = rest_post(&server.get_rest_server(), path, headers, body)
            .await
            .with_context(|| format!("post_key error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(result)
    }

    async fn put_key(&self, path: &str, body: &str) -> anyhow::Result<Value> {
        let server = &self.server_config;

        let api_key = server.get_api_key().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("X-MBX-APIKEY", &api_key));
        let result = rest_put(&server.get_rest_server(), path, headers, body)
            .await
            .with_context(|| format!("post_key error {}/{}", server.get_rest_server(), path))?;

        Self::parse_binance_result(result)
    }

    pub async fn delete_sign(&self, path: &str, body: &str) -> anyhow::Result<Value> {
        let server = &self.server_config;

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

    pub async fn create_listen_key(&self) -> anyhow::Result<String> {
        let message = self
            .post_key("/api/v3/userDataStream", "")
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

    pub async fn extend_listen_key(&self, key: &str) -> anyhow::Result<()> {
        let path = format!("/api/v3/userDataStream?listenKey={}", key);
        let _message = self
            .put_key(path.as_str(), "")
            .await
            .with_context(|| format!("extend_listen_key error"))?;

        Ok(())
    }

    pub fn make_connect_url(&self, key: &str) -> String {
        let server = &self.server_config;

        format!("{}/ws/{}", server.get_public_ws_server(), key)
    }

    pub async fn get_historical_trades(
        &self,
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

        let result = self.get(path, &params).await?;

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
    use rbot_lib::common::{init_debug_log, DAYS};
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_board_snapshot() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let result = api.get_board_snapshot(&config).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_recent_trades() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let result = api.get_recent_trades(&config).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_trades() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let (trades, page) = api.get_trades(&config, 0, 0, &RestPage::New).await?;
        println!("first: {:?}", trades[0]);
        println!("end t: {:?}", trades[trades.len() - 1]);

        let (trades, page) = api.get_trades(&config, 0, 0, &page).await?;
        println!("first: {:?}", trades[0]);
        println!("end t: {:?}", trades[trades.len() - 1]);

        Ok(())
    }

    #[tokio::test]
    async fn test_klines() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let result = api.get_klines(&config, 0, 0, &RestPage::New).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_new_order_limit() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let result = api
            .new_order(
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
        let api = BinanceRestApi::new(&server);

        init_debug_log();

        let result = api
            .new_order(
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
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        init_debug_log();

        let result = api.open_orders(&config).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_get_account() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let api = BinanceRestApi::new(&server);

        init_debug_log();
        let result = api.get_account().await?;
        println!("result: {:?}", result);

        Ok(())
    }

    /*
    #[tokio::test]
    async fn test_crate_listen_key() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        init_debug_log();

        let result = api.create_listen_key().await?;
        println!("result: {:?}", result);

        Ok(())
    }
    */
    #[tokio::test]
    async fn test_get_historical_trades() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let result = api.get_historical_trades(&config, 0, 0).await;
        println!("result: {:?}", result);
    }

    #[tokio::test]
    async fn test_get_historical_trades_from_id_10000() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        init_debug_log();

        let result = api.get_historical_trades(&config, 10000, 0).await;
        println!("result: {:?}", result);
    }

    #[test]
    fn test_historical_trade_url() {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();
        let api = BinanceRestApi::new(&server);

        let url = api.history_web_url(&config, NOW() - DAYS(2));
        println!("url={}", url);
    }
}

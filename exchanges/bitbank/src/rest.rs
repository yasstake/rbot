use std::{fs::File, io::BufReader, path::PathBuf, str::FromStr};
use serde_json::Value;
use tempfile::tempdir;

use polars::frame::DataFrame;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use rbot_lib::{
    common::{
        date_string, hmac_sign, split_yyyymmdd, AccountCoins, BoardItem, BoardTransfer, ExchangeConfig, Kline, MarketConfig, MicroSec, Order, OrderSide, OrderType, Trade, NOW
    },
    db::{df_to_parquet, log_download_tmp, TradeBuffer},
    net::{check_exist, rest_get, rest_post, RestApi, RestPage},
};

use crate::{BitbankRestResponse, BitbankTransactions};

use anyhow::{anyhow, Context as _};

pub const BITBANK_BOARD_DEPTH: u32 = 200;

pub struct BitbankRestApi {
    server_config: ExchangeConfig,
}

impl BitbankRestApi {
    pub fn new(server_config: &ExchangeConfig) -> Self {
        Self {
            server_config: server_config.clone(),
        }
    }
}

const ACCESS_TIME_WINDOW: i64 = 5000;

// TODO: impl
impl RestApi for BitbankRestApi {

    fn get_exchange(&self) -> ExchangeConfig {
        self.server_config.clone()
    }

    async fn get_board_snapshot(&self, config: &MarketConfig) -> anyhow::Result<BoardTransfer> {
        let server = &self.server_config;
        let path = format!("/{}/depth", config.trade_symbol);

        let api_path = server.get_public_api();
        let response = rest_get(&api_path, &path, vec![], None, None)
            .await
            .with_context(|| format!("get_board_snapshot error: {}/{}", api_path, path))?;

        println!("response: {}", response);

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)
            .with_context(|| format!("parse error in get_board_snapshot"))?;

        if rest_response.success == 0 {
            return Err(anyhow!(
                "get_board_snapshot error: {:?}",
                rest_response.data
            ));
        }

        let mut board: BoardTransfer = rest_response.into();
        board.snapshot = true;

        Ok(board)
    }

    async fn get_recent_trades(&self, config: &MarketConfig) -> anyhow::Result<Vec<Trade>> {
        let server = &self.server_config;
        let path = format!("/{}/transactions", config.trade_symbol);

        let response = rest_get(&server.get_public_api(), &path, vec![], None, None)
            .await
            .with_context(|| {
                format!(
                    "get_recent_trades error: {}/{}",
                    server.get_public_api(),
                    path
                )
            })?;

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)
            .with_context(|| format!("parse error in get_recent_trades"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("get_recent_trades error: {:?}", rest_response.data));
        }

        let trades: Vec<Trade> = rest_response.into();

        Ok(trades)
    }

    // TODO: impl
    async fn get_trades(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        _page: &RestPage,
    ) -> anyhow::Result<(Vec<Trade>, RestPage)> {
        // Bitbank doesn't support getting trades by time range
        // We can only get recent trades
        log::warn!("Bitbank does not support getting trades by time range");

        Ok((vec![], RestPage::Done))
    }

    async fn get_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        _end_time: MicroSec,
        _page: &RestPage,
    ) -> anyhow::Result<(Vec<Kline>, RestPage)> {
        let server = &self.server_config;
        let path = format!(
            "/{}/candlestick/1min/{}",
            config.trade_symbol,
            date_string(start_time)
        );

        let response = rest_get(&server.get_public_api(), &path, vec![], None, None)
            .await
            .with_context(|| format!("get_klines error: {}/{}", server.get_public_api(), path))?;

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)
            .with_context(|| format!("parse error in get_klines"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("get_klines error: {:?}", rest_response.data));
        }

        let klines: Vec<Kline> = rest_response.into();

        Ok((klines, RestPage::Done))
    }

    fn klines_width(&self) -> i64 {
        60 // 1 minute in seconds
    }

    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal,
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        let server = &self.server_config;
        let path = format!("/user/spot/order");

        let mut params = format!(
            "pair={}&side={}&amount={}",
            config.trade_symbol,
            side.to_string().to_lowercase(),
            size
        );

        if order_type == OrderType::Limit {
            params = format!("{}&price={}", params, price);
        }

        if let Some(id) = client_order_id {
            params = format!("{}&order_id={}", params, id);
        }

        let response = rest_post(&server.get_private_api(), &path, vec![], &params)
            .await
            .with_context(|| format!("new_order error: {}/{}", server.get_private_api(), path))?;

        let rest_response: BitbankRestResponse =
            serde_json::from_str(&response).with_context(|| format!("parse error in new_order"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("new_order error: {:?}", rest_response.data));
        }

        // TODO: Parse order response and convert to Order type
        // For now, return an empty vector since we don't have the order response type defined
        Ok(vec![])
    }

    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        let server = &self.server_config;
        let path = format!("/user/spot/cancel_order");

        let params = format!("pair={}&order_id={}", config.trade_symbol, order_id);

        let response = rest_post(&server.get_private_api(), &path, vec![], &params)
            .await
            .with_context(|| {
                format!("cancel_order error: {}/{}", server.get_private_api(), path)
            })?;

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)
            .with_context(|| format!("parse error in cancel_order"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("cancel_order error: {:?}", rest_response.data));
        }

        // TODO: Parse order response and convert to Order type
        // For now, return a default order since we don't have the order response type defined
        let mut order = Order::default();
        order.symbol = config.trade_symbol.clone();
        order.order_id = order_id.to_string();
        Ok(order)
    }

    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        let server = &self.server_config;
        let path = format!("/user/spot/active_orders");

        let params = format!("pair={}", config.trade_symbol);

        let response = rest_get(
            &server.get_private_api(),
            &path,
            vec![],
            Some(&params),
            None,
        )
        .await
        .with_context(|| format!("open_orders error: {}/{}", server.get_private_api(), path))?;

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)
            .with_context(|| format!("parse error in open_orders"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("open_orders error: {:?}", rest_response.data));
        }

        // TODO: Parse orders response and convert to Vec<Order>
        // For now, return an empty vector since we don't have the orders response type defined
        Ok(vec![])
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        let server = &self.server_config;
        let path = "/v1/user/assets";

        let response = self.get_sign(&path, None)
            .await
            .with_context(|| format!("get_account error: {}/{}", server.get_private_api(), path))?;

        let rest_response: BitbankRestResponse = serde_json::from_value(response)
            .with_context(|| format!("parse error in get_account"))?;

        if rest_response.success == 0 {
            return Err(anyhow!("get_account error: {:?}", rest_response.data));
        }

        // TODO: Parse account response and convert to AccountCoins
        // For now, return an empty AccountCoins since we don't have the account response type defined
        Ok(AccountCoins::default())
    }

    fn history_web_url(&self, config: &MarketConfig, date: MicroSec) -> String {
        let web_base = self.server_config.get_public_api();

        let (yyyy, mm, dd) = split_yyyymmdd(date);

        format!(
            "{}/{}/transactions/{:04}{:02}{:02}",
            web_base, config.trade_symbol, yyyy, mm, dd
        )
    }

    /*
    async fn has_web_archive(&self, config: &MarketConfig, date: MicroSec) -> anyhow::Result<bool> {
        let server = self.server_config.get_public_api();

        let (yyyy, mm, dd) = split_yyyymmdd(date);

        let path = format!(
            "/{}/transactions/{:04}{:02}{:02}",
            config.trade_symbol, yyyy, mm, dd
        );


        let response = rest_get(&server, &path, vec![], None, None).await?;

        let rest_response: BitbankRestResponse = serde_json::from_str(&response)?;

        if rest_response.success == 0 {
            return Err(anyhow!("archive get error {:?}", rest_response.data));
        }

        Ok(true)
    }
    */

    /// create DataFrame with columns;
    ///  KEY:time_stamp(Int64), KEY:order_side(Bool), KEY:price(f64), KEY:size(f64)
    fn logdf_to_archivedf(&self, df: &DataFrame) -> anyhow::Result<DataFrame> {
        /*
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
        */
        todo!()
    }

    async fn web_archive_to_parquet<F>(
        &self,
        config: &MarketConfig,
        parquet_file: &PathBuf,
        date: MicroSec,
        f: F,
    ) -> anyhow::Result<i64>
    where
        F: FnMut(i64, i64),
    {
        let url = self.history_web_url(config, date);

        let tmp_dir = tempdir().with_context(|| "create tmp dir error")?;

        let file_path = log_download_tmp(&url, tmp_dir.path(), f)
            .await
            .with_context(|| format!("log_download_tmp error {}->{:?}", url, tmp_dir))?;
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);
        let response: BitbankRestResponse = serde_json::from_reader(reader)?;

        if response.success == 0 {
            return Err(anyhow!("rest response error"));
        }

        let mut buffer = TradeBuffer::new();
        let trades: Vec<Trade> = response.into();

        for t in trades {
            buffer.push_trade(&t);
        }

        let mut df = buffer.to_dataframe();

        let rec = df_to_parquet(&mut df, &parquet_file)?;
        log::debug!("done {} [rec]", rec);

        return Ok(rec);
    }
}

impl BitbankRestApi {
    // https://github.com/bitbankinc/bitbank-api-docs/blob/master/rest-api_JP.md
    async fn get_sign(&self, path: &str, params: Option<&str>) -> anyhow::Result<Value> {
        let server = &self.server_config;
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("ACCESS-KEY", &api_key));

        let timestamp = NOW() / 1000;
        let now = timestamp.to_string();
        headers.push(("ACCESS-REQUEST-TIME", &now));

        let time_window = ACCESS_TIME_WINDOW.to_string();
        headers.push(("ACCESS-TIME-WINDOW", &time_window));

        let message = if let Some(p) = params {
            format!("{}{}{}", now, path, p)
        } else {
            format!("{}{}", now, path)
        };

        let signature = hmac_sign(&api_secret, &message);
        headers.push(("ACCESS-SIGNATURE", &signature));

        let response = rest_get(&server.get_private_api(), path, headers, params, None)
            .await
            .with_context(|| format!("get_sign error: {}/{}", server.get_private_api(), path))?;

        let v: Value = serde_json::from_str(&response)
            .with_context(|| format!("parse error in get_sign"))?;

        Ok(v)
    }

    async fn post_sign(&self, path: &str, params: Option<&str>) -> anyhow::Result<Value> {
        let server = &self.server_config;
        let api_key = server.get_api_key().extract();
        let api_secret = server.get_api_secret().extract();

        let mut headers: Vec<(&str, &str)> = vec![];
        headers.push(("ACCESS-KEY", &api_key));

        let timestamp = NOW() / 1000;
        let now = timestamp.to_string();
        headers.push(("REQUEST-TIME", &now));

        let time_window = ACCESS_TIME_WINDOW.to_string();
        headers.push(("ACCESS-TIME-WINDOW", &time_window));

        let message = if let Some(p) = params {
            format!("{}{}{}", now, path, p)
        } else {
            format!("{}{}", now, path)
        };

        let signature = hmac_sign(&api_secret, &message);
        headers.push(("ACCESS-SIGNATURE", &signature));

        let response = rest_get(&server.get_private_api(), path, headers, params, None)
            .await
            .with_context(|| format!("get_sign error: {}/{}", server.get_private_api(), path))?;

        let v: Value = serde_json::from_str(&response)
            .with_context(|| format!("parse error in get_sign"))?;

        Ok(v)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub sell: String,
    pub buy: String,
    pub high: String,
    pub low: String,
    pub last: String,
    pub vol: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Depth {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

#[cfg(test)]
mod bitbank_test {
    use std::{path::PathBuf, str::FromStr};

    use rbot_lib::{
        common::{init_debug_log, ExchangeConfig, MarketConfig, DAYS, NOW},
        net::{RestApi, RestPage},
    };

    use crate::BitbankRestApi;

    #[test]
    fn test_get_exchange_info() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        println!("{:?}", api.get_exchange());
        Ok(())
    }

    #[test]
    fn test_get_weburl() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let url = api.history_web_url(&config, NOW());

        println!("{:?}", url);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_board_snapshot() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let board = api.get_board_snapshot(&config).await?;

        assert!(!board.asks.is_empty(), "Asks should not be empty");
        assert!(!board.bids.is_empty(), "Bids should not be empty");

        println!("Board snapshot: {:?}", board);

        Ok(())
    }

    #[tokio::test]
    async fn test_has_archive() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let result = api.has_web_archive(&config, NOW()).await;
        println!("{:?}", result);

        let result = api.has_web_archive(&config, NOW() - DAYS(1)).await;
        println!("{:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_paquet() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let file = PathBuf::from_str("/tmp/test.parquet")?;

        let result = api
            .web_archive_to_parquet(&config, &file, NOW() - DAYS(1), |_f, _f2| {})
            .await;
        println!("{:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_recent_trades() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let trades = api.get_recent_trades(&config).await?;
        println!("{:?}", trades);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_klines() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let klines = api.get_klines(&config, NOW() - DAYS(1), NOW(), &RestPage::New).await?;
        println!("{:?}", klines);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_account() -> anyhow::Result<()> {
        init_debug_log();
        let server = ExchangeConfig::open("bitbank", true)?;
        let api = BitbankRestApi::new(&server);

        let account = api.get_account().await?;
        println!("{:?}", account);
        Ok(())
    }
}

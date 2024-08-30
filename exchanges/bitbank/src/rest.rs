
use std::{fs::File, io::BufReader, path::PathBuf};
use tempfile::tempdir;


use polars::frame::DataFrame;
use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use rbot_lib::{common::{split_yyyymmdd, AccountCoins, BoardTransfer, ExchangeConfig, Kline, MarketConfig, MicroSec, Order, OrderSide, OrderType, Trade}, db::{df_to_parquet, log_download_tmp, TradeBuffer}, net::{check_exist, rest_get, RestApi, RestPage}};

use crate::{BitbankRestResponse, BitbankTransactions};

use anyhow::{anyhow, Context as _};

const BITBANK_BOARD_DEPTH: u32 = 200;


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

 // TODO: impl
impl RestApi for BitbankRestApi {

    fn get_exchange(&self) -> ExchangeConfig {
        self.server_config.clone()
    }

    async fn get_board_snapshot(&self, config: &MarketConfig) -> anyhow::Result<BoardTransfer> {
        /*
        let server = &self.server_config;

        let path = "/v5/market/orderbook";

        let params = format!(
            "category={}&symbol={}&limit={}",
            config.trade_category.as_str(),
            config.trade_symbol.as_str(),
            BITBANK_BOARD_DEPTH
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
        */

        todo!()
    }

    // TODO: impl
    async fn get_recent_trades(&self, config: &MarketConfig) -> anyhow::Result<Vec<Trade>> {
        /*
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
        */

        todo!()
    }

    // TODO: impl
    async fn get_trades(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        _page: &RestPage,
    ) -> anyhow::Result<(Vec<Trade>, RestPage)> {
        /*
        Err(anyhow!("Bybit does not have get trade by range"))
        */
        todo!()
    }

    // TODO: impl
    async fn get_klines(
        &self,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
        page: &RestPage,
    ) -> anyhow::Result<(Vec<Kline>, RestPage)> {
        /*
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
        */
        todo!()
      }

      // TODO: impl
      fn klines_width(&self) -> i64 {
        60
    }

     // TODO: impl
    async fn new_order(
        &self,
        config: &MarketConfig,
        side: OrderSide,
        price: Decimal, // when order_type is Market, this value is ignored.
        size: Decimal,
        order_type: OrderType,
        client_order_id: Option<&str>,
    ) -> anyhow::Result<Vec<Order>> {
        /*
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
        */

        todo!()
    }

    async fn cancel_order(&self, config: &MarketConfig, order_id: &str) -> anyhow::Result<Order> {
        /*
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
        */
        todo!()
    }

    async fn open_orders(&self, config: &MarketConfig) -> anyhow::Result<Vec<Order>> {
        /*
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
        */
        todo!()
    }

    async fn get_account(&self) -> anyhow::Result<AccountCoins> {
        /* 
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
        */
        todo!()
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

        match response.data {
            crate::BitbankRestData::Transactions(transactions) => {
                for t in transactions {
                    buffer.push_trade(&t.into())
                }

                let mut df = buffer.to_dataframe();

                let rec = df_to_parquet(&mut df, &parquet_file)?;
                log::debug!("done {} [rec]", rec);
    
                return Ok(rec)
            }
        }

        //        Err(anyhow!("unknown type"))
    }


}

/*

impl BitbankRestApi {

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



*/






























/*

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: T,
}

pub struct BitbankApiClient {
    client: reqwest::Client,
}

impl BitbankApiClient {
    pub fn new() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Bitbank Rust API Client"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        BitbankApiClient { client }
    }

    pub async fn get_ticker(&self, pair: &str) -> Result<Ticker, String> {
        let url = format!("{}/{}{}", BASE_URL, pair, "/ticker");
        let response = self.client.get(&url).send().await;

        match response {
            Ok(res) => {
                let api_response: ApiResponse<Ticker> = res.json().await.unwrap();
                Ok(api_response.data)
            }
            Err(e) => Err(format!("Error: {}", e)),
        }
    }

    pub async fn get_depth(&self, pair: &str) -> Result<Depth, String> {
        let url = format!("{}/{}{}", BASE_URL, pair, "/depth");
        let response = self.client.get(&url).send().await;

        match response {
            Ok(res) => {
                let api_response: ApiResponse<Depth> = res.json().await.unwrap();
                Ok(api_response.data)
            }
            Err(e) => Err(format!("Error: {}", e)),
        }
    }
}
*/


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
mod bitbank_test{
    use std::{path::PathBuf, str::FromStr};

    use rbot_lib::{common::{ExchangeConfig, MarketConfig, DAYS, NOW}, net::RestApi};

    use crate::BitbankRestApi;

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
    async fn test_has_archive() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let result = api.has_web_archive(&config, NOW()).await ;
        println!("{:?}", result);

        let result = api.has_web_archive(&config, NOW() - DAYS(1)).await ;
        println!("{:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_download_paquet() -> anyhow::Result<()> {
        let server = ExchangeConfig::open("bitbank", true)?;
        let config = ExchangeConfig::open_exchange_market("bitbank", "BTC/JPY")?;
        let api = BitbankRestApi::new(&server);

        let file = PathBuf::from_str("/tmp/test.parquet")?;

        let result = api.web_archive_to_parquet(&config, &file, NOW() - DAYS(1), |_f, _f2| {}).await;
        println!("{:?}", result);

        Ok(())
    }
}
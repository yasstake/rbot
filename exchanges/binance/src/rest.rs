// Copyright(c) 2022-2024. yasstake. All rights reserved.

use crate::{binance_order_response_vec_to_orders, binance_order_status_vec_to_orders, BinanceAccountInformation, BinanceCancelOrderResponse, BinanceOrderResponse, BinanceOrderStatus, BinanceRestBoard, BinanceServerConfig, BinanceTradeMessage};

use anyhow::anyhow;
use csv::StringRecord;
use rbot_lib::{common::{hmac_sign, AccountCoins, BoardTransfer, Kline, LogStatus, MarketConfig, MicroSec, Order, OrderSide, OrderType, ServerConfig, Trade, NOW}, net::{rest_delete, rest_get, rest_post, rest_put, RestApi}};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use serde_json::Value;

use anyhow::Result;
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
        config: &rbot_lib::common::MarketConfig,
    ) -> anyhow::Result<Vec<Trade>> {
        let path = "/api/v3/trades";
        let params = format!("symbol={}&limit=1000", config.trade_symbol);

        let messasge = Self::get(&server, path, &params)
            .await
            .with_context(|| format!("get_recent_trades error"))?;

        let trades: Vec<BinanceTradeMessage> = serde_json::from_value(messasge)?;

        Ok(trades.into_iter().map(|t| t.to_trade()).collect())
    }

    async fn get_trade_klines(
        server: &BinanceServerConfig,
        config: &MarketConfig,
        start_time: MicroSec,
        end_time: MicroSec,
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
        //let type = "LIMIT";

        let order_type_str: &str = match order_type {
            OrderType::Limit => "LIMIT",
            OrderType::Market => "MARKET",
            OrderType::Unknown => return Err(anyhow!("unknown order type")),
        };

        let mut body = format!(
            "symbol={}&side={}&type={}&timeInForce=GTC&quantity={}",
            config.trade_symbol, side, order_type_str, size
        );

        if order_type == OrderType::Limit {
            body = format!("{}&price={}", body, price);
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

        let orders 
            = binance_order_status_vec_to_orders(config, &orders);

        Ok(orders)
    }

    async fn get_account(server: &BinanceServerConfig) -> anyhow::Result<AccountCoins> {
        let path = "/api/v3/account";

        let message = Self::get_sign(server, path, None)
            .await.with_context(||format!("get_account error"))?;

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
                "/data/{}/daily/trades/{}/{}-trades-{:04}-{:02}-{:02}.zip",
                category, symbol, symbol, yyyy, mm, dd
            );
        }
        else {
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

        let response = rest_get(&server.get_rest_server(), &query, vec![], None, None)
            .await
            .with_context(|| format!("rest_get error: {}/{}", &server.get_rest_server(), &query))?;

        Ok(Self::parse_binance_result(&response)?)
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

        Ok(Self::parse_binance_result(&message)?)
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

        Ok(Self::parse_binance_result(&message)?)
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

        Ok(Self::parse_binance_result(&result)?)
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

        Ok(Self::parse_binance_result(&result)?)
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

        Ok(Self::parse_binance_result(&result)?)
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

    pub fn parse_binance_result(message: &String) -> anyhow::Result<Value> {
        let v = serde_json::from_str::<Value>(message.as_str())
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
        let message = Self::post_key(server,  "/api/v3/userDataStream", "")
            .await.with_context(||format!("create_listen_key error"))?;
    
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
            .await.with_context(||format!("extend_listen_key error"))?;

        Ok(())
    }
    
}



/*
/*
/// Processes recent trades for a given symbol and applies a function to the resulting vector of trades.
///
/// # Arguments
///
/// * `symbol` - A string slice that holds the symbol for which recent trades are to be processed.
/// * `f` - A closure that takes a vector of trades and returns a `Result<(), String>`.
///
/// # Returns
///
/// Returns a `Result` containing a tuple of `BinanceMessageId` and `MicroSec` if the operation is successful, otherwise returns an error message as a `String`.
pub fn process_recent_trade<F>(
    config: &BinanceConfig,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let path = format!("/api/v3/trades?symbol={}&limit=1000", config.market_config.market_config.trade_symbol);

    let result = rest_get(&config.rest_endpoint, path.as_str(), vec![], None, None);

    match result {
        Ok(message) => {
            return process_binance_trade_message(message.as_str(), f);
        }
        Err(e) => Err(e),
    }
}
*/

/// Processes a Binance trade message and returns the first (trade ID and time) and last.
///
/// # Arguments
///
/// * `message` - A string slice that holds the Binance trade message.
/// * `f` - A closure that takes a vector of trades and returns a Result object.
///
/// # Returns
///
/// A Result object containing a tuple of the first trade ID and time, or an error message if the message could not be parsed.
/// https://binance-docs.github.io/apidocs/spot/en/#recent-trades-list
/// # Examples
///
/// ```
/// use rbot::exchange::binance::rest::process_binance_trade_message;
///
/// let message = r#"[
///  {
///     "id": 28457,
///     "price": "4.00000100",
///     "qty": "12.00000000",
///     "quoteQty": "48.000012",
///     "time": 1499865549590,
///     "isBuyerMaker": true,
///     "isBestMatch": true
/// }
/// ]"#;
///
/// let result = process_binance_trade_message(message, |trades| {
///     // Do something with the trades
///    println!("trades: {:?}", trades);
///     Ok(())
/// });
///
/// assert!(result.is_ok());
/// ```
pub fn process_binance_trade_message<F>(
    message: &str,
    f: &mut F,
) -> Result<
    (
        (BinanceMessageId, MicroSec),
        (BinanceMessageId, MicroSec),
        i64,
    ),
    String,
>
where
    F: FnMut(&Vec<Trade>) -> Result<(), String>,
{
    let result = serde_json::from_str::<Vec<BinanceTradeMessage>>(message);

    match result {
        Ok(message) => {
            let mut trades: Vec<Trade> = vec![];

            for m in message {
                let mut t = m.to_trade();
                t.status = LogStatus::FixRestApiBlock;
                trades.push(t);
            }

            let l = trades.len();

            if l == 0 {
                return Ok(((0, 0), (0, 0), 0));
            }

            let first_id = trades[0].id.parse::<BinanceMessageId>().unwrap();
            let first_time = trades[0].time;
            trades[0].status = LogStatus::FixRestApiStart;

            let last_id: BinanceMessageId = trades[l - 1].id.parse::<BinanceMessageId>().unwrap();
            let last_time: MicroSec = trades[l - 1].time;
            trades[l - 1].status = LogStatus::FixRestApiEnd;

            (*f)(&trades)?;

            return Ok(((first_id, first_time), (last_id, last_time), l as i64));
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(format!(
                "process binance trade message error {}",
                e.to_string()
            ));
        }
    }
}

/// Processes historical trades for a given symbol and from a given trade ID.
///
/// # Arguments
///
/// * `symbol` - A string slice that holds the symbol for which to retrieve historical trades.
/// * `from_id` - An unsigned 64-bit integer that holds the trade ID from which to start retrieving historical trades.
/// * `f` - A closure that takes a vector of `Trade` structs and returns a `Result<(), String>`.
///
/// # Returns
///
/// A `Result` containing a tuple of `BinanceMessageId` and `MicroSec` structs on success, or an error message on failure.
///
pub fn download_historical_trades<F>(
    config: &BinanceConfig,
    from_id: BinanceMessageId,
    f: &mut F,
) -> Result<
    (
        (BinanceMessageId, MicroSec),
        (BinanceMessageId, MicroSec),
        i64,
    ),
    String,
>
where
    F: FnMut(&Vec<Trade>) -> Result<(), String>,
{
    let path: String;

    path = format!(
        "/api/v3/historicalTrades?symbol={}&fromId={}&limit=1000",
        config.market_config.trade_symbol, from_id
    );

    let result = rest_get(&config.rest_endpoint, path.as_str(), vec![], None, None);

    match result {
        Ok(message) => {
            return process_binance_trade_message(message.as_str(), f);
        }
        Err(e) => Err(e),
    }
}

/*
pub fn insert_trade_db(
    config: &BinanceConfig,
    start_time: MicroSec,
    tx: Sender<Vec<Trade>>,
) -> Result<(BinanceMessageId, MicroSec), String> {
    return process_old_trade_from(config, start_time, &mut |trades| {
        tx.send(trades).unwrap();
        Ok(())
    });
}
*/

pub fn download_historical_trades_from_id<F>(
    config: &BinanceConfig,
    mut start_id: BinanceMessageId,
    verbose: bool,
    f: &mut F,
) -> Result<i64, String>
where
    F: FnMut(&Vec<Trade>) -> Result<(), String>,
{
    let duration = Duration::from_millis(100);

    let mut records: i64 = 0;

    loop {
        let ((s_id, s_time), (e_id, e_time), count) =
            download_historical_trades(config, start_id, f)?;
        log::debug!(
            "s_id: {} / s_time: {} / e_id: {} / e_time: {}",
            s_id,
            s_time,
            e_id,
            e_time
        );
        if s_id == e_id {
            break;
        }

        records += count;

        start_id = e_id + 1;

        if verbose {
            print!(
                "Download Historical API {} - {}\r",
                time_string(s_time),
                time_string(e_time)
            );
            flush_log();
        }

        sleep(duration);
    }

    Ok(records)
}

/*
pub fn process_old_trade_from<F>(
    config: &BinanceConfig,
    from_: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let now = server_time(config)?;

    let result = _process_old_trade_from(&config, from_time, f);

    match result {
        Ok((trade_id, trade_time)) => match _process_old_trade_from(&config, now - HHMM(0, 1), f) {
            Ok(_) => {
                return Ok((trade_id, trade_time));
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(e);
            }
        },
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}
*/
/*
fn _process_old_trade_from<F>(
    config: &BinanceConfig,
    from_time: MicroSec,
    f: &mut F,
) -> Result<(BinanceMessageId, MicroSec), String>
where
    F: FnMut(Vec<Trade>) -> Result<(), String>,
{
    let mut from_id = 0;
    let mut count = 0;

    // TODO: check implementation
    // let start_time = server_time(config);

    loop {
        let start_timer = NOW();

        log::debug!("from_id: {}", from_id);

        let result = download_historical_trades(&config, from_id, f);

        match result {
            Ok((trade_id, trade_time)) => {
                if trade_time < from_time {
                    log::debug!(
                        "trade time {}({}) is exceed specified time {}({}).",
                        time_string(trade_time),
                        trade_time,
                        time_string(from_time),
                        from_time
                    );
                    return Ok((trade_id, trade_time));
                } else if (from_id != 0) && from_id != trade_id {
                    log::debug!("data end fromID={} / trade_id={}", from_id, trade_id);
                    return Ok((trade_id, trade_time));
                }

                from_id = trade_id - PAGE_SIZE;
            }
            Err(e) => {
                println!("Error: {:?}", e);

                return Err(e);
            }
        }

        count += 1;
        log::debug!("{} ", count);

        let elapsed = NOW() - start_timer;

        if elapsed < API_INTERVAL_LIMIT {
            sleep(Duration::from_micros((API_INTERVAL_LIMIT - elapsed) as u64));
        }
    }
}
*/

use anyhow::Context;
use rbot_lib::{
    common::{
        hmac_sign, BoardTransfer, Kline, MarketConfig, MicroSec, Order, OrderSide, OrderType,
        ServerConfig, Trade, NOW,
    },
    net::{rest_delete, rest_get, rest_post, rest_put, RestApi},
};
use rust_decimal::Decimal;
use serde_json::Value;

/// Parses the result of a Binance API call and returns a `serde_json::Value` or an error message.
///
/// # Arguments
///
/// * `result` - A `Result` containing either the API response as a `String` or an error message as a `String`.
///
/// # Returns
///
/// * `Ok(serde_json::Value)` - If the API response was successfully parsed into a `serde_json::Value`.
/// * `Err(String)` - If there was an error parsing the API response or if the response contained an error message.

/// Sends a REST request to Binance to get the server time and returns it in microseconds.
///
/// # Example
///
/// ```
/// use rbot::common::init_debug_log;
/// use rbot::exchange::binance::rest::server_time;
/// init_debug_log();
/// let config = rbot::exchange::binance::BinanceConfig::BTCBUSD();
///
/// let time = server_time(&config);
/// log::debug!("Server time: {}", time);
/// ```
pub fn server_time(config: &BinanceConfig) -> Result<MicroSec, String> {
    let path = "/api/v3/time";

    let result = rest_get(&config.rest_endpoint, path, vec![], None, None);

    match result {
        Ok(message) => {
            let v: serde_json::Value = serde_json::from_str(message.as_str()).unwrap();

            let st = v.get("serverTime");
            if st.is_some() {
                let time = st.unwrap().as_u64().unwrap();

                return Ok((time * 1_000) as MicroSec);
            } else {
                println!("serverTime is not found.");

                let err_message = format!("serverTime is not found {}", message);
                return Err(err_message);
            }
        }
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

fn parse_response<T>(result: Result<Value, String>) -> Result<T, String>
where
    T: serde::de::DeserializeOwned,
{
    match result {
        Ok(value) => match serde_json::from_value::<T>(value) {
            Ok(v) => {
                return Ok(v);
            }
            Err(e) => {
                println!("Error: {:?}", e);
                return Err(format!("json parse error{}", e.to_string()));
            }
        },
        Err(e) => {
            println!("Error: {:?}", e);
            return Err(e);
        }
    }
}

/// Creates a new limit order on Binance exchange.
///
/// # Arguments
///
/// * `config` - A reference to a `BinanceConfig` object containing the API key and secret key.
/// * `side` - The order side, either `OrderSide::Buy` or `OrderSide::Sell`.
/// * `price` - The price at which to place the order.
/// * `size` - The size of the order.
/// * `cliend_order_id` - An optional client order ID to assign to the order.
///
/// # Returns
///
/// A `Result` containing a `BinanceOrderResponse` object if the order was successfully placed,
/// or an error message as a `String` if the order failed.
///
/// For SPOT:
/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
///
/// For MARGIN:
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade

/// Creates a new market order on Binance exchange.
///
/// # Arguments
///
/// * `config` - A reference to a `BinanceConfig` struct containing the API key and secret key.
/// * `side` - The order side, either `OrderSide::Buy` or `OrderSide::Sell`.
/// * `size` - The size of the order.
/// * `cliend_order_id` - An optional client order ID.
///
/// # Returns
///
/// A `Result` containing a `BinanceOrderResponse` struct if successful, or an error message `String` if unsuccessful.
///
/// For SPOT:
/// https://binance-docs.github.io/apidocs/spot/en/#new-order-trade
///
/// For MARGIN:
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-new-order-trade
pub fn new_market_order(
    config: &BinanceConfig,
    side: OrderSide,
    size: Decimal,
    cliend_order_id: Option<&str>,
) -> Result<BinanceOrderResponse, String> {
    let path = "/api/v3/order";
    let side = order_side_string(side);
    let mut body = format!(
        "symbol={}&side={}&type=MARKET&quantity={}",
        config.market_config.trade_symbol, side, size
    );

    if cliend_order_id.is_some() {
        let cliend_order_id = cliend_order_id.unwrap();
        body = format!("{}&newClientOrderId={}", body, cliend_order_id);
    }

    parse_response::<BinanceOrderResponse>(binance_post_sign(&config, path, body.as_str()))
}

// https://binance-docs.github.io/apidocs/spot/en/#query-order-user_data
/*
pub fn alter_order(
    config: &BinanceConfig,
    order_id: &str,
    price: Decimal,
    size: Decimal,
) -> Result<BinanceOrderResponse, String> {
}
*/

pub fn cancell_all_orders(
    config: &BinanceConfig,
) -> Result<Vec<BinanceCancelOrderResponse>, String> {
    let path = "/api/v3/openOrders";
    let body = format!("symbol={}", config.market_config.trade_symbol);

    parse_response::<Vec<BinanceCancelOrderResponse>>(binance_delete_sign(
        &config,
        path,
        body.as_str(),
    ))
}

/// Get Balance from user account
/// for SPOT
/// https://binance-docs.github.io/apidocs/spot/en/#account-information-user_data///
///
/// for MARGIN
/// https://binance-docs.github.io/apidocs/spot/en/#margin-account-balance-user_data
pub fn get_balance(config: &BinanceConfig) -> Result<BinanceAccountInformation, String> {
    let path = "/api/v3/account";

    parse_response::<BinanceAccountInformation>(binance_get_sign(&config, path, None))
}


pub fn order_status(config: &BinanceConfig) -> Result<Vec<BinanceOrderStatus>, String> {
    let path = "/api/v3/allOrders";
    let query = format!("symbol={}&limit=1000", config.market_config.trade_symbol);

    parse_response::<Vec<BinanceOrderStatus>>(binance_get_sign(&config, path, Some(query.as_str())))
}

use crate::exchange::binance::message::BinanceListOrdersResponse;

pub fn trade_list(config: &BinanceConfig) -> Result<Vec<BinanceListOrdersResponse>, String> {
    let path = "/api/v3/myTrades";
    let query = format!("symbol={}&limit=1000", config.market_config.trade_symbol);

    parse_response::<Vec<BinanceListOrdersResponse>>(binance_get_sign(
        &config,
        path,
        Some(query.as_str()),
    ))
}

#[allow(non_snake_case, unused_variables)]
#[cfg(test)]
mod tests {
    use rust_decimal::prelude::FromPrimitive;

    use super::*;
    use crate::common::{init_debug_log, time_string, Order};

    #[test]
    fn test_trade_list() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");
        let result = trade_list(&config);

        println!("result: {:?}", result);
    }

    #[test]
    fn test_process_binance_trade_message() {
        //        let message = r#"[{"id":1,"price":"0.001","qty":"100","time":123456789,"isBuyerMaker":true,"isBestMatch":true}]"#;
        let message = r#"
 [
  {
     "id": 28457,
     "price": "4.00000100",
     "qty": "12.00000000",
     "quoteQty": "48.000012",
     "time": 1499865549590,
     "isBuyerMaker": true,
     "isBestMatch": true
 }
 ]"#;

        let trade = process_binance_trade_message(message, &mut |message| {
            println!("message: {:?}", message);
            Ok(())
        });

        assert_eq!(trade.is_ok(), true);
    }

    /*
    #[test]
    fn test_process_recent_trade() {
        let config = BinanceConfig::BTCUSDT();
        let symbol = config.market_config.trade_symbol;
        let mut latest_time = 0;
        let config = BinanceConfig::BTCUSDT();

        let result = process_recent_trade(&config, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_process_old_trade() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = download_historical_trades(&config, 0, &mut |trades| {
            println!("trades: {:?}", trades);
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_timing_rest_api() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = download_historical_trades(&config, 0, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));

        let symbol = "BTCUSDT";
        let mut latest_time = 0;

        println!("-----");

        let result = process_recent_trade(&config, &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    #[test]
    fn test_process_old_trade_until() {
        let config = BinanceConfig::BTCUSDT();

        let mut latest_time = 0;

        let result = process_old_trade_from(&config, NOW() - HHMM(1, 0), &mut |trades| {
            latest_time = trades.last().unwrap().time;

            Ok(())
        });

        assert_eq!(result.is_ok(), true);
        let (first_id, first_time) = result.unwrap();
        println!(
            "NOW {}: first_id: {}, first_time: {}",
            time_string(server_time(&config).unwrap()),
            first_id,
            time_string(first_time)
        );
        println!("latest_time: {}", time_string(latest_time));
    }

    */

    #[test]
    fn test_server_time() {
        let config = BinanceConfig::BTCUSDT();
        let time = server_time(&config).unwrap();
        println!("Server time: {} ({})", time_string(time), time);
    }

    /// https://binance-docs.github.io/apidocs/spot/en/#signed-trade-user_data-and-margin-endpoint-security
    /// test sample from above url
    #[test]
    fn test_sign() {
        let secret = "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j";
        let message = "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559";

        let signature = hmac_sign(&secret.to_string(), &message.to_string());
        //

        println!("signature: {}", signature);
        assert_eq!(signature, "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&recvWindow=5000&timestamp=1499827319559&signature=c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
    }

    #[test]
    fn test_clock_diff() {
        let config = BinanceConfig::BTCUSDT();

        let time = server_time(&config).unwrap();
        let local_time = NOW();
        println!(
            "Server time: {} ({})  / local time {} ({})",
            time_string(time),
            time,
            time_string(local_time),
            local_time
        );
    }

    #[test]
    fn test_new_limit_order() {
        //        let config = BinanceConfig::BTCBUSD();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        let result = new_limit_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(24_000.0).unwrap(),
            Decimal::from_f64(0.001).unwrap(),
            Some(&"LimitOrder-test"),
        );
        println!("result: {:?}", result.unwrap());
    }

    #[test]
    fn test_new_market_order() {
        init_debug_log();

        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let result = new_market_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(0.001).unwrap(),
            Some(&"rest-test"),
        );

        println!("result: {:?}", result.unwrap());
        println!("");
    }

    #[test]
    fn test_cancel_order() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let order_id = "444627";

        let result = cancel_order(&config, order_id);

        match result {
            Ok(r) => {
                println!("{:?}", r);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    fn test_binance_cancel_all_orders() {
        let config = BinanceConfig::TEST_BTCUSDT();

        let result = cancell_all_orders(&config);

        match result {
            Ok(r) => {
                println!("{:?}", r);
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }

    #[test]
    fn test_get_balance() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let result = get_balance(&config);
        println!("result: {:?}", result);
    }

    #[test]
    fn test_create_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);
    }

    #[test]
    fn test_extend_listen_key() {
        let config = BinanceConfig::TESTSPOT("BTC", "BUSD");

        let key = create_listen_key(&config).unwrap();
        println!("key: {}", key);

        let result = extend_listen_key(&config, key.as_str());
        println!("result: {:?}", result);
    }

    #[test]
    fn test_order_status() {
        let config = BinanceConfig::TEST_BTCUSDT();

        let message = order_status(&config).unwrap();
        println!("message: {:?}", message);
    }

    #[test]
    fn test_open_orders() {
        init_debug_log();
        let config = BinanceConfig::TEST_BTCUSDT();

        let message = open_orders(&config).unwrap();
        println!("message: {:?}", message);

        for i in message {
            let order: Order = i.into();
            println!("i: {:?}", order);
        }
    }

    #[test]
    fn test_order_list() {
        init_debug_log();
        let config = BinanceConfig::TEST_BTCUSDT();

        let message = trade_list(&config).unwrap();
        println!("message: {:?}", message);
    }

    #[test]
    fn test_sport_order_e2e() {
        init_debug_log();
        let config = BinanceConfig::TESTSPOT("BTC", "USDT");

        // display balance
        let balance = get_balance(&config).unwrap();
        println!("{:?}", balance);

        // make limit order
        let order = new_limit_order(
            &config,
            OrderSide::Buy,
            Decimal::from_f64(24_000.0).unwrap(),
            Decimal::from_f64(0.001).unwrap(),
            Some(&"LimitOrder-test"),
        )
        .unwrap();

        // cancel

        let orders: Vec<Order> = order.into();

        if orders.len() == 1 {
            let order = &orders[0];
            println!("order: {:?}", order);

            let result = cancel_order(&config, &order.order_id);
            println!("result: {:?}", result);
        }
    }
}
*/

#[cfg(test)]
mod binance_api_test {
    use crate::BinanceConfig;
    use rbot_lib::common::init_debug_log;
    use rust_decimal_macros::dec;
    use tokio::*;
    use super::*;

    
    #[tokio::test]
    async fn test_board_snapshot() -> anyhow::Result<()> {
        let server = BinanceServerConfig::new(false);
        let config = BinanceConfig::BTCUSDT();

        let result = BinanceRestApi::get_board_snapshot(&server, &config).await?;
        println!("result: {:?}", result);

        Ok(())
    }

    #[tokio::test]
    async fn test_recent_trades() -> anyhow::Result<()>{
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
                &server, &config, OrderSide::Buy, 
                dec![50000], dec![0.001], 
                OrderType::Limit,                
                None).await;
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
    async fn test_get_account() -> anyhow::Result<()>{
        init_debug_log();
        let server = BinanceServerConfig::new(false);

        let result = BinanceRestApi::get_account(&server).await?;
        println!("result: {:?}", result);

        Ok(())
    }
}

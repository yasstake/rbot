// Copyright(c) 2023. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use std::{
    collections::HashMap,
    iter::FromIterator,
    sync::{Arc, Mutex},
};

use once_cell::sync::Lazy;
use polars::{
    prelude::{DataFrame, NamedFrom},
    series::Series,
};
use pyo3::{pyclass, pyfunction};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

use serde_derive::{Deserialize, Serialize};

use crate::common::MarketConfig;

use rmp_serde::to_vec;

use super::{
    order, string_to_decimal, MicroSec, Order, OrderSide, OrderStatus, OrderType, ServerConfig,
};

static ALL_BOARD: Lazy<Mutex<OrderBookList>> = Lazy::new(|| Mutex::new(OrderBookList::new()));

pub struct OrderBookList {
    books: HashMap<String, Arc<Mutex<OrderBookRaw>>>,
}

impl OrderBookList {
    pub fn make_path(config: &MarketConfig) -> String {
        Self::make_path_from_str(&config.exchange_name, &config.trade_category, &config.trade_symbol)
    }

    pub fn make_path_from_str(
        exchange_name: &str,
        trade_category: &str,
        trade_symbol: &str,
    ) -> String {
        format!("{}/{}/{}", exchange_name, trade_category, trade_symbol)
    }

    pub fn parse_path(path: &str) -> (String, String, String) {
        let v: Vec<&str> = path.split('/').collect();
        (v[0].to_string(), v[1].to_string(), v[2].to_string())
    }

    pub fn new() -> Self {
        OrderBookList {
            books: HashMap::new(),
        }
    }

    pub fn get(&self, key: &String) -> Option<OrderBook> {
        let board = self.books.get(key);

        if board.is_none() {
            return None;
        }

        let (exchange, category, symbol) = Self::parse_path(key);

        Some(OrderBook {
            exchage: exchange,
            category: category,
            symbol: symbol,
            board: board.unwrap().clone(),
        })
    }

    pub fn register(&mut self, path: &String, book: Arc<Mutex<OrderBookRaw>>) {
        self.books.insert(path.clone(), book);
    }

    pub fn unregister(&mut self, key: &String) {
        self.books.remove(key);
    }

    pub fn list(&self) -> Vec<String> {
        self.books.keys().map(|s| s.to_string()).collect()
    }
}

#[pyfunction]
pub fn get_orderbook_list() -> Vec<String> {
    ALL_BOARD.lock().unwrap().list()
}

#[pyfunction]
pub fn get_orderbook_json(path: &str, size: usize) -> anyhow::Result<String> {
    let board = ALL_BOARD
        .lock()
        .unwrap()
        .get(&path.to_string())
        .ok_or_else(|| anyhow::anyhow!("orderbook path=({})not found", path))?;

    board.get_json(size)
}

#[pyfunction]
pub fn get_orderbook_vec(path: &str) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
    let board = ALL_BOARD
        .lock()
        .unwrap()
        .get(&path.to_string())
        .ok_or_else(|| anyhow::anyhow!("orderbook path=({})not found", path))?;

    board.get_board_vec()
}


#[pyfunction]
pub fn get_orderbook_bin(path: &str) -> anyhow::Result<Vec<u8>> {
    let board = ALL_BOARD
        .lock()
        .unwrap()
        .get(&path.to_string())
        .ok_or_else(|| anyhow::anyhow!("orderbook path=({})not found", path))?;

    let transfer = board.get_board_trasnfer();

    Ok(transfer.to_vec())
}

#[pyfunction]
pub fn get_orderbook(path: &str) -> anyhow::Result<OrderBook> {
    ALL_BOARD
        .lock()
        .unwrap()
        .get(&path.to_string())
        .ok_or_else(|| anyhow::anyhow!("orderbook path=({})not found", path))
}

/*
pub fn get_orderbook_df(path: &str) -> anyhow::Result<(DataFrame, DataFrame)> {
    let board = ALL_BOARD
        .lock()
        .unwrap()
        .get(&path.to_string())
        .ok_or_else(|| anyhow::anyhow!("orderbook path=({})not found", path))?;

    board.get_board()
}
*/
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoardTransfer {
    pub last_update_time: MicroSec,
    pub first_update_id: u64,
    pub last_update_id: u64,
    pub bids: Vec<BoardItem>,
    pub asks: Vec<BoardItem>,
    pub snapshot: bool,
}

impl BoardTransfer {
    pub fn new() -> Self {
        BoardTransfer {
            last_update_time: 0,
            first_update_id: 0,
            last_update_id: 0,
            bids: vec![],
            asks: vec![],
            snapshot: false,
        }
    }

    pub fn from_orderbook(order_book: &OrderBookRaw) -> Self {
        BoardTransfer {
            first_update_id: order_book.first_update_id,
            last_update_time: order_book.last_update_time,
            last_update_id: order_book.last_update_id,
            bids: order_book.bids.get(),
            asks: order_book.asks.get(),
            snapshot: true
        }
    }

    pub fn insert_bid(&mut self, bid: &(Decimal, Decimal)) {
        self.bids.push(BoardItem::from_decimal(bid.0, bid.1));
    }

    pub fn insert_ask(&mut self, ask: &(Decimal, Decimal)) {
        self.asks.push(BoardItem::from_decimal(ask.0, ask.1));
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }

    pub fn to_vec(&self) -> Vec<u8> {
        to_vec(&self).unwrap()
    }

    pub fn from_vec(vec: Vec<u8>) -> Self {
        rmp_serde::from_slice(&vec).unwrap()
    }

    fn to_data_frame(board: &Vec<BoardItem>) -> DataFrame {
        let mut prices: Vec<f64> = vec![];
        let mut sizes: Vec<f64> = vec![];
        let mut cusum_col: Vec<f64> = vec![];
        let mut cusum: Decimal = dec![0.0];

        for item in board {
            prices.push(item.price.to_f64().unwrap());
            sizes.push(item.size.to_f64().unwrap());
            cusum += item.size;
            cusum_col.push(cusum.to_f64().unwrap());
        }

        let prices = Series::new("price", prices);
        let sizes = Series::new("size", sizes);
        let sum = Series::new("sum", cusum_col);

        let df = DataFrame::new(vec![prices, sizes, sum]).unwrap();

        df
    }

    pub fn to_dataframe(&mut self) -> anyhow::Result<(DataFrame, DataFrame)> {
        Ok((
            Self::to_data_frame(&self.bids),
            Self::to_data_frame(&self.asks),
        ))
    }
}

/// 板上の1行を表す。（価格＆数量）
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BoardItem {
    #[serde(deserialize_with = "string_to_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "string_to_decimal")]
    pub size: Decimal,
}

impl BoardItem {
    pub fn from_f64(price: f64, size: f64) -> Self {
        BoardItem {
            price: Decimal::from_f64(price).unwrap(),
            size: Decimal::from_f64(size).unwrap(),
        }
    }

    pub fn from_decimal(price: Decimal, size: Decimal) -> Self {
        BoardItem {
            price: price,
            size: size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Board {
    pub asc: bool,
    pub max_depth: u32,
    pub board: HashMap<Decimal, Decimal>,
}

impl Board {
    pub fn new(max_depth: u32, asc: bool) -> Self {
        Board {
            asc,
            max_depth: max_depth,
            board: HashMap::new(),
        }
    }

    pub fn set(&mut self, price: Decimal, size: Decimal) {
        if size == dec!(0.0) {
            self.board.remove(&price);
            return;
        }

        self.board.insert(price, size);
    }

    pub fn get_board(&self) -> &HashMap<Decimal, Decimal> {
        &self.board
    }

    /// Keyをソートして、Vecにして返す
    /// ascがtrueなら昇順、falseなら降順
    /// max_depthを超えたものは削除する.
    pub fn get(&self) -> Vec<BoardItem> {
        let mut vec: Vec<BoardItem> = Vec::from_iter(
            self.board
                .iter()
                .map(|(k, v)| BoardItem::from_decimal(*k, *v)),
        );

        if self.asc {
            vec.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
        } else {
            vec.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        }

        vec
    }

    pub fn clip_depth(&mut self) {
        let mut vec = self.get();

        if self.max_depth != 0 && self.max_depth < vec.len() as u32 {
            log::info!("board depth over. remove items.");
            let not_valid = vec.split_off(self.max_depth as usize);
            for item in not_valid {
                self.board.remove(&item.price);
            }
            self.board.shrink_to_fit();
        }
    }

    pub fn clear(&mut self) {
        self.board.clear();
    }

    pub fn to_dataframe(&mut self) -> anyhow::Result<DataFrame> {
        let board = self.get();

        let mut prices: Vec<f64> = vec![];
        let mut sizes: Vec<f64> = vec![];
        let mut cusum_col: Vec<f64> = vec![];
        let mut cusum: Decimal = dec![0.0];

        for item in board {
            prices.push(item.price.to_f64().unwrap());
            sizes.push(item.size.to_f64().unwrap());
            cusum += item.size;
            cusum_col.push(cusum.to_f64().unwrap());
        }

        let prices = Series::new("price", prices);
        let sizes = Series::new("size", sizes);
        let sum = Series::new("sum", cusum_col);

        let df = DataFrame::new(vec![prices, sizes, sum])?;

        Ok(df)
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderBookRaw {
    pub last_update_time: MicroSec,
    pub first_update_id: u64,    
    pub last_update_id: u64,
    pub bids: Board,
    pub asks: Board,
}

impl OrderBookRaw {
    pub fn new(max_depth: u32) -> Self {
        OrderBookRaw {
            first_update_id: 0,
            last_update_id: 0,
            last_update_time: 0,
            bids: Board::new(max_depth, false),
            asks: Board::new(max_depth, true),
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn get_asks_dataframe(&mut self) -> anyhow::Result<DataFrame> {
        self.asks.to_dataframe()
    }

    pub fn get_bids_dataframe(&mut self) -> anyhow::Result<DataFrame> {
        self.bids.to_dataframe()
    }

    pub fn get_edge_price(&mut self) -> anyhow::Result<(Decimal, Decimal)> {
        let bids = self.bids.get();
        let asks = self.asks.get();

        if bids.len() == 0 || asks.len() == 0 {
            return Err(anyhow::anyhow!("board has no data"));
        }

        let bid_price = bids.first().unwrap().price;
        let ask_price = asks.first().unwrap().price;

        return Ok((bid_price, ask_price));
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.asks.get()
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.bids.get()
    }

    pub fn update(&mut self, board_transfer: &BoardTransfer) {
        self.last_update_time = board_transfer.last_update_time;
        self.first_update_id = board_transfer.first_update_id;
        self.last_update_id = board_transfer.last_update_id;

        if board_transfer.snapshot {
            self.clear();
        }

        for item in &board_transfer.bids {
            self.bids.set(item.price, item.size);
        }

        for item in &board_transfer.asks {
            self.asks.set(item.price, item.size);
        }

        self.bids.clip_depth();
        self.asks.clip_depth();
    }
}

#[pyclass]
#[derive(Debug)]
pub struct OrderBook {
    exchage: String,
    category: String,
    symbol: String,
    board: Arc<Mutex<OrderBookRaw>>,
}

impl OrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        let exchange_name = config.exchange_name.clone();
        let category = config.trade_category.clone();
        let symbol = config.trade_symbol.clone();

        let path = OrderBookList::make_path(config);
        let board = Arc::new(Mutex::new(OrderBookRaw::new(config.board_depth)));

        ALL_BOARD.lock().unwrap().register(&path, board.clone());

        OrderBook {
            exchage: exchange_name,
            category: category,
            symbol: symbol,
            board: board,
        }
    }

    pub fn get_first_update_id(&self) -> u64 {
        self.board.lock().unwrap().first_update_id
    }

    pub fn get_last_update_time(&self) -> MicroSec {
        self.board.lock().unwrap().last_update_time
    }

    pub fn get_last_update_id(&self) -> u64 {
        self.board.lock().unwrap().last_update_id
    }

    pub fn from_bin(
        config: &MarketConfig,
        bin: Vec<u8>,
    ) -> anyhow::Result<Self> {
        let exchange_name = config.exchange_name.to_string();
        let category = config.trade_category.clone();
        let symbol = config.trade_symbol.clone();

        let board = Arc::new(Mutex::new(OrderBookRaw::new(config.board_depth)));

        {
            let mut board_lock = board.lock().unwrap();
            let transfer = BoardTransfer::from_vec(bin);
            board_lock.update(&transfer);
        }

        Ok(OrderBook {
            exchage: exchange_name,
            category: category,
            symbol: symbol,
            board: board,
        })
    }

    pub fn clear(&mut self) {
        self.board.lock().unwrap().clear();
    }

    pub fn get_board_trasnfer(&self) -> BoardTransfer {
        let board = self.board.lock().unwrap();
        BoardTransfer::from_orderbook(&board)
    }

    pub fn to_binary(&self) -> anyhow::Result<Vec<u8>> {
        let board_transfer = self.get_board_trasnfer();

        Ok(board_transfer.to_vec())
    }

    pub fn get_board_vec(&self) -> anyhow::Result<(Vec<BoardItem>, Vec<BoardItem>)> {
        let board = self.board.lock().unwrap();
        let bids = board.bids.get();
        let asks = board.asks.get();
        Ok((bids, asks))
    }

    pub fn get_board(&self) -> anyhow::Result<(DataFrame, DataFrame)> {
        let mut board = self.board.lock().unwrap();
        let bids = board.get_bids_dataframe()?;
        let asks = board.get_asks_dataframe()?;
        Ok((bids, asks))
    }

    pub fn get_json(&self, size: usize) -> anyhow::Result<String> {
        let board = self.board.lock().unwrap();
        let mut bids = board.bids.get();
        let mut asks = board.asks.get();

        if size != 0 {
            // if there is a size limit, cut off the data.
            bids = bids[0..size.min(bids.len())].to_vec();
            asks = asks[0..size.min(asks.len())].to_vec();
        }

        let json = serde_json::json!({
            "bids": bids,
            "asks": asks,
        });

        Ok(json.to_string())
    }

    pub fn get_edge_price(&self) -> anyhow::Result<(Decimal, Decimal)> {
        self.board.lock().unwrap().get_edge_price()
    }

    pub fn update(&mut self, board_transfer: &BoardTransfer) {
        self.board
            .lock()
            .unwrap()
            .update(board_transfer);
    }

    pub fn dry_market_order(
        &mut self,
        create_time: MicroSec,
        order_id: &str,
        client_order_id: &str,
        symbol: &str,
        side: OrderSide,
        size: Decimal,
        transaction_id: &str,
    ) -> anyhow::Result<Vec<Order>> {
        let board = self.board.lock().unwrap();

        let board = if side == OrderSide::Buy {
            board.asks.get()
        } else {
            board.bids.get()
        };

        let mut orders: Vec<Order> = vec![];
        let mut split_index = 0;

        let mut remain_size = size;

        // TODO: consume boards
        for item in board {
            if remain_size <= dec![0.0] {
                break;
            }

            let execute_size;
            let order_status;
            split_index += 1;

            if remain_size <= item.size {
                order_status = OrderStatus::Filled;
                execute_size = remain_size;
                remain_size = dec![0.0];
            } else {
                order_status = OrderStatus::PartiallyFilled;
                execute_size = item.size;
                remain_size -= item.size;
            }

            let mut order = Order::new(
                &self.category,
                symbol,
                create_time,
                &order_id,
                &client_order_id.to_string(),
                side,
                OrderType::Market,
                order_status,
                dec![0.0],
                size,
            );

            order.transaction_id = format!("{}-{}", transaction_id, split_index);
            order.update_time = create_time;
            order.is_maker = false;
            order.execute_price = item.price;
            order.execute_size = execute_size;
            order.remain_size = remain_size;
            order.quote_vol = order.execute_price * order.execute_size;

            orders.push(order);
        }

        if remain_size > dec![0.0] {
            log::error!("remain_size > 0.0: {:?}", remain_size);
        }

        Ok(orders)
    }
}

impl Drop for OrderBook {
    fn drop(&mut self) {
        let count = Arc::strong_count(&self.board);
        log::debug!("drop orderbook: {}", count);

        if count <= 1 {
            ALL_BOARD
                .lock()
                .unwrap()
                .unregister(&OrderBookList::make_path_from_str(
                    &self.exchage,
                    &self.category,
                    &self.symbol,
                ));
        }
    }
}

#[cfg(test)]
mod board_test {
    use super::*;

    #[test]
    fn test_board_set() {
        let mut config = MarketConfig::new("SPOT", "USD", "JPY", 2, 2, 1000);
        config.price_unit = dec!(0.5);

        let mut b = Board::new(0, true);

        b.set(dec!(10.0), dec!(1.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(1.5));
        println!("{:?}", b.get());

        b.set(dec!(11.5), dec!(2.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(0.0));
        println!("{:?}", b.get());

        let mut b = Board::new(0, false);

        println!("---------desc----------");

        b.set(dec!(10.0), dec!(1.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(1.5));
        println!("{:?}", b.get());

        b.set(dec!(11.5), dec!(2.0));
        println!("{:?}", b.get());

        b.set(dec!(9.0), dec!(0.0));
        println!("{:?}", b.get());

        println!("---------clear----------");
        b.clear();
        println!("{:?}", b.get());
    }

    #[test]
    fn serialize_board_transfer() {
        let mut config = MarketConfig::new("SPOT", "USD", "JPY", 2, 2, 1000);
        config.price_unit = dec!(0.5);

        let mut b = OrderBookRaw::new(0);

        let board_transfer = BoardTransfer{
            first_update_id: 0,
            last_update_time: 0,
            last_update_id: 0,
            bids: vec![
                BoardItem {
                    price: dec![10.0],
                    size: dec![0.01],
                },
                BoardItem {
                    price: dec![11.0],
                    size: dec![0.01],
                },
            ],
            asks: vec![
                BoardItem {
                    price: dec![10.0],
                    size: dec![0.01],
                },
                BoardItem {
                    price: dec![11.0],
                    size: dec![0.01],
                },
            ],
            snapshot: true
        };

        b.update(&board_transfer);

        let transfer = BoardTransfer::from_orderbook(&b);

        let json = transfer.to_json();
        println!("{} / {}", json, json.len());

        let vec = transfer.to_vec();
        println!("{:?} / {}", vec, vec.len());

        let t2 = BoardTransfer::from_vec(vec);
        println!("{:?}", t2);
    }
}

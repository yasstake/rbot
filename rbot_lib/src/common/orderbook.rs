// Copyright(c) 2023. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

use std::{collections::HashMap, iter::FromIterator, sync::Mutex};

use polars_core::{
    prelude::{DataFrame, NamedFrom},
    series::Series,
};
use pyo3::pyclass;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::Deserialize;
use serde_derive::Serialize;

use crate::common::MarketConfig;

use rmp_serde::to_vec;

use super::string_to_decimal;

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoardTransfer {
    pub bids: Vec<BoardItem>,
    pub asks: Vec<BoardItem>,
}

impl BoardTransfer {
    pub fn new() -> Self {
        BoardTransfer {
            bids: vec![],
            asks: vec![],
        }
    }

    pub fn from_orderbook(order_book: &OrderBookRaw) -> Self {
        BoardTransfer {
            bids: order_book.bids.get(),
            asks: order_book.asks.get(),
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

    pub fn to_dataframe(&mut self) -> Result<(DataFrame, DataFrame), ()> {
        Ok((
            Self::to_data_frame(&self.bids),
            Self::to_data_frame(&self.asks)
        ))
    }

}

/// 板上の1行を表す。（価格＆数量）
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq)]
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

    pub fn to_dataframe(&mut self) -> Result<DataFrame, ()> {
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

        let df = DataFrame::new(vec![prices, sizes, sum]).unwrap();

        Ok(df)
    }
}

#[pyclass]
#[derive(Debug, Clone, PartialEq)]
pub struct OrderBookRaw {
    pub snapshot: bool, // only use for message.
    pub bids: Board,
    pub asks: Board,
}

impl OrderBookRaw {
    pub fn new(max_depth: u32) -> Self {
        OrderBookRaw {
            snapshot: false,
            bids: Board::new(max_depth, false),
            asks: Board::new(max_depth, true),
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn get_asks_dataframe(&mut self) -> Result<DataFrame, ()> {
        self.asks.to_dataframe()
    }

    pub fn get_bids_dataframe(&mut self) -> Result<DataFrame, ()> {
        self.bids.to_dataframe()
    }

    pub fn get_edge_price(&mut self) -> (Decimal, Decimal) {
        let bids = self.bids.get();
        let asks = self.asks.get();

        let bid_price = bids.first().unwrap().price;
        let ask_price = asks.first().unwrap().price;

        return (bid_price, ask_price);
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.asks.get()
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.bids.get()
    }

    pub fn update(&mut self, bids_diff: &Vec<BoardItem>, asks_diff: &Vec<BoardItem>, force: bool) {
        if force {
            self.clear();
        }

        for item in bids_diff {
            self.bids.set(item.price, item.size);
        }

        for item in asks_diff {
            self.asks.set(item.price, item.size);
        }
    }

    /*
    pub fn clip_depth(&mut self) {
        self.bids.clip_depth();
        self.asks.clip_depth();
    }
    */
}

#[pyclass]
#[derive(Debug)]
pub struct OrderBook {
    board: Mutex<OrderBookRaw>,
}

impl OrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        OrderBook {
            board: Mutex::new(OrderBookRaw::new(config.board_depth)),
        }
    }

    pub fn clear(&mut self) {
        self.board.lock().unwrap().clear();
    }

    pub fn get_board_vec(&self) -> Result<(Vec<BoardItem>, Vec<BoardItem>), ()> {
        let board = self.board.lock().unwrap();
        let bids = board.bids.get();
        let asks = board.asks.get();
        Ok((bids, asks))
    }

    pub fn get_board(&self) -> Result<(DataFrame, DataFrame), ()> {
        let mut board = self.board.lock().unwrap();
        let bids = board.get_bids_dataframe()?;
        let asks = board.get_asks_dataframe()?;
        Ok((bids, asks))
    }

    pub fn get_json(&self, size: usize) -> Result<String, String> {
        let board = self.board.lock().unwrap();
        let bids = board.bids.get();
        let bids = bids[0..size.min(bids.len())].to_vec();

        let asks = board.asks.get();
        let asks = asks[0..size.min(asks.len())].to_vec();

        let json = serde_json::json!({
            "bids": bids,
            "asks": asks,
        });

        Ok(json.to_string())
    }

    pub fn get_edge_price(&self) -> (Decimal, Decimal) {
        self.board.lock().unwrap().get_edge_price()
    }

    pub fn update(&mut self, bids_diff: &Vec<BoardItem>, asks_diff: &Vec<BoardItem>, force: bool) {
        self.board
            .lock()
            .unwrap()
            .update(bids_diff, asks_diff, force);
    }

    /*
    pub fn clip_depth(&mut self) {
        let mut board = self.board.lock().unwrap();
        board.clip_depth();
        drop(board);
    }
    */
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

        b.update(&vec![
            BoardItem{price: dec![10.0], size:dec![0.01]},
            BoardItem{price: dec![11.0], size:dec![0.01]},
        ],
        &vec![
            BoardItem{price: dec![10.0], size:dec![0.01]},
            BoardItem{price: dec![11.0], size:dec![0.01]},
        ],false);

        let transfer = BoardTransfer::from_orderbook(&b);

        let json = transfer.to_json();
        println!("{} / {}", json, json.len());

        let vec = transfer.to_vec();
        println!("{:?} / {}", vec, vec.len());

        let t2 = BoardTransfer::from_vec(vec);
        println!("{:?}", t2);

    }
}

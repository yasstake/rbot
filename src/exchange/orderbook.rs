// Copyright(c) 2023. yasstake. All rights reserved.
// Abloultely no warranty.

use std::{collections::HashMap, sync::Mutex};


use polars_core::{
    prelude::{DataFrame, NamedFrom},
    series::Series,
};
use pyo3::pyclass;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{de, Deserialize, Deserializer};
use serde_derive::Serialize;

use crate::common::MarketConfig;

pub fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse f64")),
    }
}

pub fn string_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<f64>() {
        Ok(num) => Ok(Decimal::from_f64(num).unwrap()),
        Err(_) => Err(de::Error::custom("Failed to parse f64")),
    }
}

pub fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    match s.parse::<i64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom("Failed to parse i64")),
    }
}

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

#[derive(Debug)]
pub struct Board {
    asc: bool,
    max_depth: u32,
    board: HashMap<Decimal, Decimal>,
}

impl Board {
    pub fn new(config: &MarketConfig, asc: bool) -> Self {
        Board {
            asc,
            max_depth: config.board_depth,
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

    /// Keyをソートして、Vecにして返す
    /// ascがtrueなら昇順、falseなら降順
    /// max_depthを超えたものは削除する.
    pub fn get(&mut self) -> Vec<BoardItem> {
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

        if self.max_depth != 0 && self.max_depth < vec.len() as u32 {
            log::info!("board depth over. remove items.");
            let not_valid = vec.split_off(self.max_depth as usize);
            for item in not_valid {
                self.board.remove(&item.price);
            }
            self.board.shrink_to_fit();
        }

        vec
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

#[derive(Debug)]
pub struct OrderBookRaw {
    bids: Board,
    asks: Board,
}

impl OrderBookRaw {
    pub fn new(config: &MarketConfig) -> Self {
        OrderBookRaw {
            bids: Board::new(config, false),
            asks: Board::new(config, true),
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
}

#[derive(Debug)]
pub struct OrderBook {
    board: Mutex<OrderBookRaw>,
}

impl OrderBook {
    pub fn new(config: &MarketConfig) -> Self {
        OrderBook {
            board: Mutex::new(OrderBookRaw::new(config)),
        }
    }

    pub fn clear(&mut self) {
        self.board.lock().unwrap().clear();
    }

    pub fn get_board_vec(&self) -> Result<(Vec<BoardItem>, Vec<BoardItem>), ()> {
        let mut board = self.board.lock().unwrap();
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

    pub fn get_edge_price(&self) -> (Decimal, Decimal) {
        self.board.lock().unwrap().get_edge_price()
    }

    pub fn update(&mut self, bids_diff: &Vec<BoardItem>, asks_diff: &Vec<BoardItem>, force: bool) {
        self.board.lock().unwrap().update(bids_diff, asks_diff, force);
    }
}


#[test]
fn test_board_set() {
    let mut config = MarketConfig::new("USD", "JPY", 2, 2);
    config.price_unit = dec!(0.5);

    let mut b = Board::new(&config, true);

    b.set(dec!(10.0), dec!(1.0));
    println!("{:?}", b.get());

    b.set(dec!(9.0), dec!(1.5));
    println!("{:?}", b.get());

    b.set(dec!(11.5), dec!(2.0));
    println!("{:?}", b.get());

    b.set(dec!(9.0), dec!(0.0));
    println!("{:?}", b.get());

    let mut b = Board::new(&config, false);

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

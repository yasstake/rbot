use std::collections::HashMap;

use ndarray::Array2;
use numpy::{PyArray2, IntoPyArray};
use polars_core::{series::Series, prelude::{DataFrame, NamedFrom}};
use pyo3::{PyResult, Py, Python, pyclass};
use pyo3_polars::PyDataFrame;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserializer, de, Deserialize};
use serde_derive::{Deserialize, Serialize};

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
    step: Decimal,
    asc: bool,
    board: HashMap<Decimal, Decimal>,
}

impl Board {
    pub fn new(step: Decimal, asc: bool) -> Self {
        Board {
            step,
            asc,
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

    fn step(&self) -> Decimal {
        if self.asc {
            self.step
        } else {
            -self.step
        }
    }

    // Keyをソートして、Vecにして返す
    // ascがtrueなら昇順、falseなら降順
    // stepサイズごとで0の値も含めて返す
    // stepサイズが０のときは、stepサイズを無視して返す
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

    pub fn get_with_fill(&self) -> Vec<BoardItem> {
        let mut v: Vec<BoardItem> = vec![];
        let mut current_price = dec!(0.0);

        for item in self.get() {
            if current_price == dec!(0.0) {
                current_price = item.price;
            }

            while current_price != item.price {
                v.push(BoardItem::from_decimal(current_price, dec!(0.0)));
                current_price += self.step();
            }

            v.push(BoardItem::from_decimal(item.price, item.size));
            current_price += self.step();
        }

        v
    }

    pub fn get_array(&self) -> Array2<f64> {
        return Self::to_ndarray(&self.get());
    }

    pub fn clear(&mut self) {
        self.board.clear();
    }

    // convert to ndarray
    pub fn to_ndarray(board: &Vec<BoardItem>) -> Array2<f64> {
        let shape = (board.len(), 2);
        let mut array_vec: Vec<f64> = Vec::with_capacity(shape.0 * shape.1);

        for item in board {
            array_vec.push(item.price.to_f64().unwrap());
            array_vec.push(item.size.to_f64().unwrap());
        }

        let array: Array2<f64> = Array2::from_shape_vec(shape, array_vec).unwrap();

        array
    }

    pub fn to_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        let array = self.get_array();
        let r = Python::with_gil(|py| {
            let py_array2: &PyArray2<f64> = array.into_pyarray(py);
            py_array2.to_owned()
        });

        return Ok(r);
    }

    pub fn to_pydataframe(&self) -> PyResult<PyDataFrame> {
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
        let cusum = Series::new("cusum", cusum_col);

        let df = DataFrame::new(vec![prices, sizes, cusum]).unwrap();

        Ok(PyDataFrame(df))
    }
}

#[derive(Debug)]
struct Depth {
    bids: Board,
    asks: Board,
}

impl Depth {
    pub fn new(step: Decimal) -> Self {
        Depth {
            bids: Board::new(step, false),
            asks: Board::new(step, true),
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.asks.get()
    }

    pub fn get_asks_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.asks.to_pyarray()
    }

    pub fn get_asks_pydataframe(&self) -> PyResult<PyDataFrame> {
        self.asks.to_pydataframe()
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.bids.get()
    }

    pub fn get_bids_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.bids.to_pyarray()
    }

    pub fn get_bids_pydataframe(&self) -> PyResult<PyDataFrame> {
        self.bids.to_pydataframe()
    }

    pub fn set_bids(&mut self, price: Decimal, size: Decimal) {
        self.bids.set(price, size);
    }

    pub fn set_asks(&mut self, price: Decimal, size: Decimal) {
        self.asks.set(price, size);
    }
}

#[derive(Debug)]
pub struct OrderBook {
    symbol: String,
    depth: Depth,
}

impl OrderBook {
    pub fn new(symbol: String, step: Decimal) -> Self {
        OrderBook {
            symbol: symbol,
            depth: Depth::new(step),
        }
    }

    pub fn update(&mut self, bids_diff: &Vec<BoardItem>, asks_diff: &Vec<BoardItem>, force: bool) {
        if force {
            self.depth.clear();
        }

        for item in bids_diff {
            self.depth.set_bids(item.price, item.size);
        }

        for item in asks_diff {
            self.depth.set_asks(item.price, item.size);
        }
    }

    pub fn get_bids(&self) -> Vec<BoardItem> {
        self.depth.bids.get()
    }

    pub fn get_bids_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.depth.bids.to_pyarray()
    }

    pub fn get_bids_pydataframe(&self) -> PyResult<PyDataFrame> {
        self.depth.bids.to_pydataframe()
    }

    pub fn get_asks(&self) -> Vec<BoardItem> {
        self.depth.asks.get()
    }

    pub fn get_asks_pyarray(&self) -> PyResult<Py<PyArray2<f64>>> {
        self.depth.asks.to_pyarray()
    }

    pub fn get_asks_pydataframe(&self) -> PyResult<PyDataFrame> {
        self.depth.asks.to_pydataframe()
    }
    
}


#[test]
fn test_board_set() {
    let mut b = Board::new(dec!(0.5), true);

    b.set(dec!(10.0), dec!(1.0));
    println!("{:?}", b.get());

    b.set(dec!(9.0), dec!(1.5));
    println!("{:?}", b.get());

    b.set(dec!(11.5), dec!(2.0));
    println!("{:?}", b.get());

    b.set(dec!(9.0), dec!(0.0));
    println!("{:?}", b.get());

    let mut b = Board::new(dec!(0.5), false);

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
fn to_ndarray() {
    let mut b = Board::new(dec!(0.5), true);
    b.set(dec!(10.0), dec!(1.0));
    b.set(dec!(12.5), dec!(1.0));

    let array = b.get_array();
    println!("{:?}", array);
}

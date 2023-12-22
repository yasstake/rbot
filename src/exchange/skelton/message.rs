// Copyright(c) 2022-2023. yasstake. All rights reserved.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use pyo3::pyclass;
use rust_decimal::Decimal;


#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonRestBoard {
    pub last_update_id: i64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonOrderResponse {

}
#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonCancelOrderResponse {}

#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonAccountInformation {}

#[derive(Debug, Clone)]
#[pyclass]
pub struct SkeltonOrderStatus {}

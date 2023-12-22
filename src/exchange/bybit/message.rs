// Copyright(c) 2022-2023. yasstake. All rights reserved.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use pyo3::pyclass;
use rust_decimal::Decimal;


#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitRestBoard {
    pub last_update_id: i64,
    pub bids: Vec<(Decimal, Decimal)>,
    pub asks: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitOrderResponse {

}
#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitCancelOrderResponse {}

#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitAccountInformation {}

#[derive(Debug, Clone)]
#[pyclass]
pub struct BybitOrderStatus {}

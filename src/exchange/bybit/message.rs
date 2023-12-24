// Copyright(c) 2022-2023. yasstake. All rights reserved.
#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

use std::str::FromStr;

use pyo3::pyclass;
use rust_decimal::Decimal;
use serde_derive::{Serialize, Deserialize};
use serde_json::Value;


use crate::exchange::string_to_decimal;
use crate::exchange::string_to_i64;

type BybitTimestamp = i64;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitRestResponse {
    #[serde(rename = "retCode")]    
    pub return_code: i64,
    #[serde(rename = "retMsg")]
    pub return_message: String,
    #[serde(rename = "retExtInfo")]
    pub return_ext_info: Value,
    #[serde(rename = "time")]
    pub time: BybitTimestamp,
    #[serde(rename = "result")]
    pub body: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitRestBoard {
    #[serde(rename = "ts")]
    pub timestamp: BybitTimestamp,
    #[serde(rename = "u")]
    pub last_update_id: i64,
    #[serde(rename = "b")]
    pub bids: Vec<(Decimal, Decimal)>,
    #[serde(rename = "a")]
    pub asks: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKline {
    timestamp: BybitTimestamp,    
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKlines {
    category: String,
    symbol: String,
    pub klines: Vec<BybitKline>,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitKlinesResponse {
    #[serde(rename = "category")]
    category: String,
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "list")]
    //pub klines: Vec<(MicroSec, Decimal, Decimal, Decimal, Decimal, Decimal)>,
    pub klines: Vec<(String, String, String, String, String, String, String)>
}

impl Into<BybitKlines> for BybitKlinesResponse {
    fn into(self) -> BybitKlines {
        let mut klines = Vec::new();
        for kline in self.klines {
            let timestamp: BybitTimestamp = kline.0.parse().unwrap();
            let open = Decimal::from_str(&kline.1).unwrap();
            let high = Decimal::from_str(&kline.2).unwrap();
            let low = Decimal::from_str(&kline.3).unwrap();
            let close = Decimal::from_str(&kline.4).unwrap();
            let volume = Decimal::from_str(&kline.5).unwrap();
            // let turnover = Decimal::from_str(&kline.6).unwrap();            // ignore turnover

            let kline = BybitKline {
                timestamp: timestamp,
                open,
                high,
                low,
                close,
                volume,
            };

            klines.push(kline);
        }

        BybitKlines {
            category: self.category.clone(),
            symbol: self.symbol.clone(),
            klines,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitTrade {
    #[serde(rename = "execId")]
    pub exec_id: String,
    #[serde(rename = "symbol")]
    pub symbol: String,
    #[serde(rename = "price", deserialize_with = "string_to_decimal")]
    //pub price: Decimal,
    pub price: Decimal,    
    #[serde(rename = "size", deserialize_with = "string_to_decimal")]
    pub size: Decimal,
    #[serde(rename = "side")]
    pub side: String,
    #[serde(rename = "time", deserialize_with = "string_to_i64")]
    pub time: BybitTimestamp,
    #[serde(rename = "isBlockTrade")]
    pub is_block_trade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[pyclass]
pub struct BybitTradeResponse {
    pub category: String,
    #[serde(rename = "list")]
    pub trades: Vec<BybitTrade>,
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


#[cfg(test)]
mod bybit_message_test {
    use crate::exchange::bybit::message::{BybitRestResponse, BybitTradeResponse};

    use super::BybitRestBoard;

    #[test]
    fn test_bybit_rest_response() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"s":"BTCUSDT","a":[["29727.05","0.000069"],["29741.86","0.001922"],["29745","0.475333"],["29752.12","0.729903"],["29752.65","0.000553"]],"b":[["29714.9","0.0008"],["29636.2","0.001052"],["29636.1","0.001034"],["29635.9","0.001625"],["29620.9","0.002498"]],"ts":1703314210368,"u":1458765},"retExtInfo":{},"time":1703314210368}        
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        assert!(result.is_ok());

        println!("{:?}", result);
    }

    #[test]
    /// curl "https://api-testnet.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=200"
    fn test_binance_board_message() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"s":"BTCUSDT","a":[["29727.05","0.000069"],["29741.86","0.001922"],["29745","0.475333"],["29752.12","0.729903"],["29752.65","0.000553"]],"b":[["29714.9","0.0008"],["29636.2","0.001052"],["29636.1","0.001034"],["29635.9","0.001625"],["29620.9","0.002498"]],"ts":1703314210368,"u":1458765},"retExtInfo":{},"time":1703314210368}        
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        assert!(result.is_ok());

        let result = result.unwrap();

        let message = result.body;

        let result = serde_json::from_value::<BybitRestBoard>(message);

        assert!(result.is_ok());

        println!("{:?}", result);
    }

    /// curl "https://api-testnet.bybit.com/v5/market/recent-trade?category=spot&symbol=BTCUSDT&limit=1000"
    #[test]
    fn test_binance_recent_trade_message() {
        let message = r#"
        {"retCode":0,"retMsg":"OK","result":{"category":"spot","list":[{"execId":"2100000000055371969","symbol":"BTCUSDT","price":"29741.75","size":"0.002148","side":"Sell","time":"1703318475711","isBlockTrade":false},{"execId":"2100000000055371968","symbol":"BTCUSDT","price":"29741.76","size":"0.003827","side":"Sell","time":"1703318473308","isBlockTrade":false},{"execId":"2100000000055371967","symbol":"BTCUSDT","price":"29741.77","size":"0.001804","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371966","symbol":"BTCUSDT","price":"29741.77","size":"0.002185","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371965","symbol":"BTCUSDT","price":"29741.78","size":"0.00227","side":"Sell","time":"1703318471505","isBlockTrade":false}]},"retExtInfo":{},"time":1703318475875}
        "#;

        let result = serde_json::from_str::<BybitRestResponse>(&message);

        println!("{:?}", result);
        assert!(result.is_ok());

        let message = result.unwrap().body;

        let result = serde_json::from_value::<BybitTradeResponse>(message);
        println!("{:?}", result);
        assert!(result.is_ok());
    }

    #[test]
    fn test_binance_recent_trade_only() {
        let message = r#"
        {"category":"spot","list":[{"execId":"2100000000055371969","symbol":"BTCUSDT","price":"29741.75","size":"0.002148","side":"Sell","time":"1703318475711","isBlockTrade":false},{"execId":"2100000000055371968","symbol":"BTCUSDT","price":"29741.76","size":"0.003827","side":"Sell","time":"1703318473308","isBlockTrade":false},{"execId":"2100000000055371967","symbol":"BTCUSDT","price":"29741.77","size":"0.001804","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371966","symbol":"BTCUSDT","price":"29741.77","size":"0.002185","side":"Sell","time":"1703318473131","isBlockTrade":false},{"execId":"2100000000055371965","symbol":"BTCUSDT","price":"29741.78","size":"0.00227","side":"Sell","time":"1703318471505","isBlockTrade":false}]}
        "#;

        let result = serde_json::from_str::<BybitTradeResponse>(&message);
        println!("{:?}", result);
    }

}

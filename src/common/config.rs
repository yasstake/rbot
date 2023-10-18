use pyo3::{pyclass, pymethods};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde_derive::{Serialize, Deserialize};


#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FeeType {
    Home,
    Foreign,
    Both,
}
#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PriceType {
    Home,
    Foreign,
    Both,
}

#[pyclass]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketConfig {
    pub price_unit: Decimal,
    pub price_scale: u32,

    pub size_unit: Decimal,
    pub size_scale: u32,

    pub maker_fee: Decimal,
    pub taker_fee: Decimal,

    pub price_type: PriceType,
    pub fee_type: FeeType,

    pub home_currency: String,
    pub foreign_currency: String,

    pub market_order_price_slip: Decimal,
}


impl MarketConfig {
    pub fn new(
        home_currency: &str,
        foreign_currency: &str,
        price_scale: u32,
        size_scale: u32,
    ) -> Self {
        Self {
            price_unit: Decimal::new(1, price_scale),
            price_scale,
            size_unit: Decimal::new(1, size_scale),
            size_scale,
            maker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            taker_fee: dec![0.0], // dec![0.00_015],  // 0.015%
            price_type: PriceType::Foreign,
            fee_type: FeeType::Home,
            home_currency: home_currency.to_string(),
            foreign_currency: foreign_currency.to_string(),
            market_order_price_slip: dec![0.0],
        }
    }
}

#[pymethods]
impl MarketConfig {
    pub fn symbol(&self) -> String {
        return format!("{}{}", self.foreign_currency, self.home_currency);
    }
}

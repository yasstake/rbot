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
    #[pyo3(set)]
    pub price_unit: Decimal,
    #[pyo3(set)]    
    pub price_scale: u32,
    
    #[pyo3(set)]
    pub size_unit: Decimal,
    #[pyo3(set)]    
    pub size_scale: u32,

    #[pyo3(set)]
    pub maker_fee: Decimal,
    #[pyo3(set)]    
    pub taker_fee: Decimal,

    #[pyo3(set)]
    pub price_type: PriceType,
    #[pyo3(set)]    
    pub fee_type: FeeType,

    #[pyo3(set)]
    pub home_currency: String,
    #[pyo3(set)]    
    pub foreign_currency: String,
    
    #[pyo3(set)]
    pub market_order_price_slip: Decimal,

    #[pyo3(set)]
    pub board_depth: u32
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
            board_depth: 1000,
        }
    }
}

#[pymethods]
impl MarketConfig {
    pub fn symbol(&self) -> String {
        return format!("{}{}", self.foreign_currency, self.home_currency);
    }
}

use std::fs::File;

use apache_avro::{Codec, Schema, Writer, Reader, from_avro_datum};
use apache_avro::types::Record;

use crate::common::MicroSec;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
/// Represents a trade made on an exchange.
pub struct TradeAvro {
    /// The time the trade was executed, in microseconds since the epoch.
    pub time: i64,
    /// The side of the order that was filled.
    pub buy_side: bool,
    /// The price at which the trade was executed.
    pub price: f64,
    /// The size of the trade.
    pub size: f64,
    /// The unique identifier for the trade.
    pub id: String,
}

#[test]
fn save_load() -> anyhow::Result<()> {
    let trade_schema = Schema::parse_str(r#"
        "type": "record",
        "name": "TradeAvro",
        "fields": [
            {"name": "time", "type": "int"},
            {"name": "buy_side", "type": "bool"},
            {"name": "price", "type": "double"},
            {"name": "size", "type": "double"},
            {"name": "id", "type": "string"},
        ]
    "#)?;

    let mut writer = Writer::with_codec(&trade_schema, File::create("measurements.avro")?, Codec::Deflate);

    let rec1 = TradeAvro{
        time: 1,
        buy_side: true,
        price: 10.1,
        size: 20.1,
        id: "asdfasdfasdfasfd".to_string(),
    };

            

    Ok(())
}



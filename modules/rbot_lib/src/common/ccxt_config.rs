use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::{ExchangeConfig, FeeType, MarketConfig};

use anyhow::anyhow;

#[derive(Serialize, Deserialize, Debug)]
pub struct ExchangeJson {
    exchange_name: String,
    production: bool,
    public_api: String,
    private_api: String,
    historical_web_base: String,
    public_ws_server: String,
    private_ws_server: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarketJson {
    symbol: String,           //  "BTC/USDT",
    exchange_name: String,    //  "bybit",
    trade_category: String,   //: "spot",
    trade_symbol: String,     //  "BTCUSDT",
    home_currency: String,    //  "USDT",
    foreign_currency: String, //  "BTC",
    quote_currency: String,   // "USDT"
    settle_currency: String,  // "USDT"
    size_unit: f64,           //  1e-06,
    min_size: f64,            //  "0.000048",
    price_unit: f64,          //   0.01,
    maker_fee: f64,           //  0.001,
    taker_fee: f64,           //  0.001
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ExchangeConfigJson {
    exchange: String,
    production: ExchangeJson,
    testnet: Option<ExchangeJson>,
    markets: Vec<MarketJson>,
}

fn get_exchange_config(exchange_name: &str) -> anyhow::Result<ExchangeConfigJson> {
    let json_str = include_str!("./exchange.json");

    let exchanges = serde_json::from_str::<Vec<ExchangeConfigJson>>(&json_str)?;

    let exchange_name = exchange_name.to_lowercase();

    for exchange in exchanges {
        if exchange.exchange == exchange_name {
            return Ok(exchange);
        }
    }

    Err(anyhow!("now found echange {:?}", exchange_name))
}

pub fn get_server_config(exchange_name: &str, production: bool) -> anyhow::Result<ExchangeConfig> {
    let exchange_config = get_exchange_config(exchange_name)?;

    let exchange = if production {
        exchange_config.production
    } else {
        if let Some(config) = exchange_config.testnet {
            config
        } else {
            return Err(anyhow!("Exchange [] dose not have testnet"));
        }
    };

    Ok(ExchangeConfig::new(
        &exchange.exchange_name,
        exchange.production,
        &exchange.public_api,
        &exchange.private_api,
        &exchange.public_ws_server,
        &exchange.private_ws_server,
        &exchange.historical_web_base,
    ))
}

fn get_market_json(exchange_name: &str, symbol: &str) -> anyhow::Result<MarketJson> {
    let symbol = symbol.to_uppercase();

    let exchange_config = get_exchange_config(exchange_name)?;

    for market in exchange_config.markets {
        if market.symbol == symbol {
            return Ok(market);
        }
    }

    Err(anyhow!(
        "not found market ({}) in exchange({})",
        symbol,
        exchange_name
    ))
}


fn get_market_json_by_native_symbol(exchange_name: &str, category: &str, native_symbol: &str) -> anyhow::Result<MarketJson> {
    let native_symbol = native_symbol.to_uppercase();

    let exchange_config = get_exchange_config(exchange_name)?;

    for market in exchange_config.markets {
        if market.trade_category == category && market.trade_symbol == native_symbol {
            return Ok(market);
        }
    }

    Err(anyhow!(
        "not found market ({}) in exchange({})",
        native_symbol,
        exchange_name
    ))
}



pub fn get_market_config(exchange_name: &str, symbol: &str) -> anyhow::Result<MarketConfig> {
    let market = get_market_json(exchange_name, symbol)?;

    let fee_type = if market.settle_currency == market.foreign_currency {
        FeeType::Foreign
    } else {
        FeeType::Home
    };

    Ok(MarketConfig::new(
        &market.symbol,
        exchange_name, 
        &market.trade_category,
        &market.trade_symbol,
        &market.foreign_currency,
        &market.home_currency,
        &market.quote_currency,
        &market.settle_currency,
        market.price_unit,
        market.size_unit,
        market.min_size,
        market.maker_fee,
        market.taker_fee,
        fee_type,
    ))
}

pub fn get_market_config_by_native_symbol(exchange_name: &str, category: &str, native_symbol: &str) -> anyhow::Result<MarketConfig> {
    let market = get_market_json_by_native_symbol(exchange_name, category, native_symbol)?;

    let fee_type = if market.settle_currency == market.foreign_currency {
        FeeType::Foreign
    } else {
        FeeType::Home
    };

    Ok(MarketConfig::new(
        &market.symbol,
        exchange_name,
        &market.trade_category,
        &market.trade_symbol,
        &market.foreign_currency,
        &market.home_currency,
        &market.quote_currency,
        &market.settle_currency,
        market.price_unit,
        market.size_unit,
        market.min_size,
        market.maker_fee,
        market.taker_fee,
        fee_type,
    ))
}


pub fn list_exchange() -> anyhow::Result<Vec<String>> {
    let json_str = include_str!("./exchange.json");

    let exchanges = serde_json::from_str::<Vec<ExchangeConfigJson>>(&json_str)?;

    let mut exchange_list: Vec<String> = vec![];

    for exchange in exchanges {
        exchange_list.push(exchange.exchange)
    }

    return Ok(exchange_list);
}

pub fn list_symbols(exchange_name: &str) -> anyhow::Result<Vec<String>> {
    let exchange_config = get_exchange_config(exchange_name)?;

    let mut symbols: Vec<String> = vec![];

    for market in exchange_config.markets {
        symbols.push(market.symbol);
    }

    Ok(symbols)
}

#[test]
fn test_read_json() -> anyhow::Result<()> {
    let list = list_exchange()?;
    println!("{:?}", list);

    let symbols = list_symbols("Bybit")?;
    println!("{:?}", symbols);

    Ok(())
}

#[test]
fn test_get_market_config_by_native_symbol() -> anyhow::Result<()> {
    let market = get_market_config_by_native_symbol("Bybit", "spot", "  BTCUSDT")?;
    println!("{:?}", market);

    Ok(())
}

#[test]
fn test_get_market_config() -> anyhow::Result<()> {
    let market = get_market_config("Bybit",  "BTC/USDT")?;
    println!("{:?}", market);

    Ok(())
}
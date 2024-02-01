// Copyright(c) 2023-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

#![allow(dead_code)]

use std::io::Write;
use futures::Future;
use hmac::{Hmac, Mac};
use once_cell::sync::Lazy;
use polars_core::export::num::FromPrimitive;
use rust_decimal::Decimal;
use serde::{de, Deserialize, Deserializer, Serializer};
use sha2::Sha256;

pub static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| tokio::runtime::Runtime::new().unwrap());


#[allow(non_snake_case)]
pub fn BLOCK_ON<F: Future>(f: F) -> F::Output {
    let r = RUNTIME.block_on(f);
    return r;
}


pub fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    if s == "" {
        return Ok(0.0);
    }

    match s.parse::<f64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
    }
}

pub fn string_to_decimal<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s == "" {
        return Ok(Decimal::from_f64(0.0).unwrap());
    }

    match s.parse::<f64>() {
        Ok(num) => Ok(Decimal::from_f64(num).unwrap()),
        Err(_) => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
    }
}

pub fn string_to_i64<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if s == "" {
        return Ok(0);
    }

    match s.parse::<i64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(de::Error::custom(format!("Failed to parse i64 {}", s))),
    }
}

pub fn to_mask_string<S>(value: &String, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if value == "" {
        return serializer.serialize_str("--- NO KEY ---");
    }

    let mask = format!("{}*******************", value[0..2].to_string());
    serializer.serialize_str(&mask)
}

pub fn hmac_sign(secret_key: &String, message: &String) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let mac = mac.finalize();

    hex::encode(mac.into_bytes())
}


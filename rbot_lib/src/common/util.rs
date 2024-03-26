// Copyright(c) 2023-4. yasstake. All rights reserved.
// ABUSOLUTELY NO WARRANTY.

#![allow(dead_code)]


use hmac::{Hmac, Mac};
use once_cell::sync::Lazy;
use polars::export::num::FromPrimitive;
use rust_decimal::Decimal;
use serde::{de, Deserialize as _, Deserializer, Serialize, Serializer};
use serde_derive::Deserialize;
use serde_json::Value;
use sha2::Sha256;
use std::{fmt, io::Write, ops::Deref};
use anyhow::anyhow;

use super::env_rbot_db_root;



#[derive(Clone, Deserialize)]
pub struct SecretString {
    secret: String,
}

impl SecretString {
    pub fn new(s: &str) -> SecretString {
        SecretString {
            secret: s.to_string(),
        }
    }

    pub fn extract(&self) -> String {
        self.secret.clone()
    }
}

impl Deref for SecretString {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.secret
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "********")
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "********")
    }
}

impl Serialize for SecretString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("********")
    }
}


pub fn string_to_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let s = Value::deserialize(deserializer)?;

    match s {
        Value::String(s) => {
            if s == "" {
                return Ok(0.0);
            }

            match s.parse::<f64>() {
                Ok(num) => Ok(num),
                Err(_) => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
            }
        }
        Value::Number(n) => {
            if let Some(num) = n.as_f64() {
                return Ok(num);
            }
            return Err(de::Error::custom(format!("Failed to parse f64 {}", n)));
        }
        _ => Err(de::Error::custom(format!("Failed to parse f64 {}", s))),
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
    let s = Value::deserialize(deserializer)?;

    match s {
        Value::String(s) => {
            if s == "" {
                return Ok(0);
            }

            match s.parse::<i64>() {
                Ok(num) => Ok(num),
                Err(_) => Err(de::Error::custom(format!("Failed to parse i64 {}", s))),
            }
        }
        Value::Number(n) => {
            if let Some(num) = n.as_i64() {
                return Ok(num);
            }
            return Err(de::Error::custom(format!("Failed to parse i64 {}", n)));
        }
        _ => Err(de::Error::custom(format!("Failed to parse i64 {}", s))),
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

pub fn hmac_sign(secret_key: &str, message: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes())
        .expect("HMAC can take key of any size");
    mac.update(message.as_bytes());

    let mac = mac.finalize();

    hex::encode(mac.into_bytes())
}

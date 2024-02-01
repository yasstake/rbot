use reqwest::header::{HeaderMap, HeaderValue, USER_AGENT};
use serde::{Deserialize, Serialize};

const BASE_URL: &str = "https://public.bitbank.cc";

#[derive(Debug, Serialize, Deserialize)]
struct ApiResponse<T> {
    success: bool,
    data: T,
}

pub struct BitbankApiClient {
    client: reqwest::Client,
}

impl BitbankApiClient {
    pub fn new() -> Self {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("Bitbank Rust API Client"));

        let client = reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap();

        BitbankApiClient { client }
    }

    pub async fn get_ticker(&self, pair: &str) -> Result<Ticker, String> {
        let url = format!("{}/{}{}", BASE_URL, pair, "/ticker");
        let response = self.client.get(&url).send().await;

        match response {
            Ok(res) => {
                let api_response: ApiResponse<Ticker> = res.json().await.unwrap();
                Ok(api_response.data)
            }
            Err(e) => Err(format!("Error: {}", e)),
        }
    }

    pub async fn get_depth(&self, pair: &str) -> Result<Depth, String> {
        let url = format!("{}/{}{}", BASE_URL, pair, "/depth");
        let response = self.client.get(&url).send().await;

        match response {
            Ok(res) => {
                let api_response: ApiResponse<Depth> = res.json().await.unwrap();
                Ok(api_response.data)
            }
            Err(e) => Err(format!("Error: {}", e)),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Ticker {
    pub sell: String,
    pub buy: String,
    pub high: String,
    pub low: String,
    pub last: String,
    pub vol: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Depth {
    pub asks: Vec<[String; 2]>,
    pub bids: Vec<[String; 2]>,
}

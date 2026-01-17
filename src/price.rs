use anyhow::Result;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

// Custody addresses for different assets
const SOL_CUSTODY: &str = "7xS2gz2bTp3fwCC7knJvUWTEU9Tycczu6VhJYKgi1wdz";
const ETH_CUSTODY: &str = "AQCGyheWPLeo6Qp9WpYS9m3Qj479t7R636N9ey1rEjEn";
const BTC_CUSTODY: &str = "5Pv3gM9JrFFH883SWAhvJC9RPYmo8UNxuFtv5bMMALkm";

// Mint addresses for Jupiter API
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const ETH_MINT: &str = "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs";
const BTC_MINT: &str = "5XZw2LKTyrfvfiskJ78AMpackRjPcyCif1WhUsPDuVqQ"; // User provided

#[derive(Debug, Deserialize)]
struct JupiterPriceData {
    #[serde(rename = "usdPrice")]
    pub price: f64,
}

// Response can be a map directly or wrapped in "data"
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum JupiterResponse {
    Direct(HashMap<String, JupiterPriceData>),
    Wrapped { data: HashMap<String, JupiterPriceData> },
}

#[derive(Debug, Clone)]
pub enum Asset {
    SOL,
    ETH,
    BTC,
}

impl Asset {
    pub fn from_custody(custody: &str) -> Option<Self> {
        match custody {
            SOL_CUSTODY => Some(Asset::SOL),
            ETH_CUSTODY => Some(Asset::ETH),
            BTC_CUSTODY => Some(Asset::BTC),
            _ => None,
        }
    }
    
    pub fn symbol(&self) -> &'static str {
        match self {
            Asset::SOL => "SOL",
            Asset::ETH => "ETH",
            Asset::BTC => "BTC",
        }
    }
}

#[derive(Clone)]
pub struct PriceOracle {
    client: reqwest::Client,
    sol_price: Arc<Mutex<f64>>,
    eth_price: Arc<Mutex<f64>>,
    btc_price: Arc<Mutex<f64>>,
}

impl PriceOracle {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            sol_price: Arc::new(Mutex::new(0.0)),
            eth_price: Arc::new(Mutex::new(0.0)),
            btc_price: Arc::new(Mutex::new(0.0)),
        }
    }

    pub async fn fetch_all_prices(&self) -> Result<()> {
        let ids = format!("{},{},{}", SOL_MINT, ETH_MINT, BTC_MINT);
        let url = format!("https://api.jup.ag/price/v3?ids={}", ids);
        let api_key = std::env::var("JUPITER_API_KEY").unwrap_or_default(); // Optional if free tier doesn't require it, but safe practice.
        
        let response_result = self.client
            .get(&url)
            .header("x-api-key", api_key)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await;

        match response_result {
            Ok(resp) => {
                let json_text = resp.text().await?;
                // Parse as flexible structure
                let response: JupiterResponse = serde_json::from_str(&json_text)?;
                
                let data = match response {
                    JupiterResponse::Direct(map) => map,
                    JupiterResponse::Wrapped { data } => data,
                };
                
                if let Some(d) = data.get(SOL_MINT) {
                    *self.sol_price.lock().unwrap() = d.price;
                }
                
                if let Some(d) = data.get(ETH_MINT) {
                    *self.eth_price.lock().unwrap() = d.price;
                }
                
                if let Some(d) = data.get(BTC_MINT) {
                    *self.btc_price.lock().unwrap() = d.price;
                }
                Ok(())
            },
            Err(e) => {
                Err(e.into())
            }
        }
    }

    pub fn get_price(&self, asset: &Asset) -> f64 {
        match asset {
            Asset::SOL => *self.sol_price.lock().unwrap(),
            Asset::ETH => *self.eth_price.lock().unwrap(),
            Asset::BTC => *self.btc_price.lock().unwrap(),
        }
    }
    
    pub fn get_price_by_custody(&self, custody: &str) -> Option<f64> {
        Asset::from_custody(custody).map(|asset| self.get_price(&asset))
    }
    
    // Keep backward compatibility
    pub async fn get_sol_price(&self) -> f64 {
        *self.sol_price.lock().unwrap()
    }
}
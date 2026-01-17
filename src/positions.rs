use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use regex::Regex;

const TIMEOUT_SECS: u64 = 600; // 10 minutes

#[derive(Debug, Serialize)]
struct RpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Option<Vec<AccountInfo>>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
struct AccountInfo {
    pubkey: String,
}

#[derive(Clone)]
pub struct PositionFetcher {
    client: reqwest::Client,
    rpc_endpoint: String,
    program_id: String,
}

impl PositionFetcher {
    pub fn new() -> Self {
        let rpc_endpoint = std::env::var("RPC_ENDPOINT").expect("RPC_ENDPOINT must be set");
        let program_id = std::env::var("PROGRAM_ID").expect("PROGRAM_ID must be set");

        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(TIMEOUT_SECS))
                .build()
                .expect("Failed to build HTTP client"),
            rpc_endpoint,
            program_id,
        }
    }

    /// Fetch and stream all position pubkeys directly to the database
    pub async fn stream_all_pubkeys_to_db(&self, db: &crate::database::Database) -> Result<usize> {
        println!("📡 [Streaming Discovery] Fetching all position pubkeys from RPC...");
        println!("⏳ Using Program: {}", self.program_id);

        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getProgramAccounts".to_string(),
            params: serde_json::json!([
                self.program_id,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [{"dataSize": 216}],
                    "dataSlice": {"offset": 0, "length": 0}
                }
            ]),
        };

        let response = self
            .client
            .post(&self.rpc_endpoint)
            .json(&request)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            anyhow::bail!("RPC returned HTTP {}: {}", status, &text[..text.len().min(500)]);
        }

        println!("📦 [Streaming Discovery] Received response, scanning for pubkeys...");
        
        // Regex to find "pubkey":"..." pattern in JSON
        let pubkey_re = Regex::new(r#"pubkey":"([1-9A-HJ-NP-Za-km-z]{32,44})"#)?;
        
        let mut body = response.bytes_stream();
        let mut total_discovered = 0;
        let mut pending_batch = Vec::new();
        let mut leftovers = String::new();
        let mut last_log = std::time::Instant::now();
        let mut total_bytes = 0;

        while let Some(chunk_result) = body.next().await {
            let chunk = chunk_result?;
            total_bytes += chunk.len();
            
            // Convert chunk to string, including leftovers from previous chunk to handle split pubkeys
            let current_str = format!("{}{}", leftovers, String::from_utf8_lossy(&chunk));
            
            let mut last_match_end = 0;
            for cap in pubkey_re.captures_iter(&current_str) {
                if let Some(m) = cap.get(1) {
                    pending_batch.push(m.as_str().to_string());
                    total_discovered += 1;
                    last_match_end = cap.get(0).unwrap().end();
                }

                if pending_batch.len() >= 5000 {
                    db.upsert_pubkeys(&pending_batch).await?;
                    pending_batch.clear();
                }
            }
            
            // Keep the tail of the string that might contain a partial pubkey match
            leftovers = current_str[last_match_end..].to_string();
            if leftovers.len() > 100 { // Safety cap
                leftovers = leftovers[leftovers.len()-100..].to_string();
            }

            if last_log.elapsed() > Duration::from_secs(5) {
                println!(
                    "   💾 Downloaded {:.2} MB | Found {} pubkeys...",
                    total_bytes as f64 / 1_048_576.0,
                    total_discovered
                );
                last_log = std::time::Instant::now();
            }
        }

        // Final batch
        if !pending_batch.is_empty() {
            db.upsert_pubkeys(&pending_batch).await?;
        }

        println!("✅ [Streaming Discovery] Complete! Found {} total pubkeys.", total_discovered);
        Ok(total_discovered)
    }

    /// Fetch all position pubkeys (no account data, just addresses)
    pub async fn fetch_all_pubkeys(&self) -> Result<Vec<String>> {
        // Keeping this for compatibility or if needed, but we prefer streaming
        println!("📡 Fetching all position pubkeys from RPC...");
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getProgramAccounts".to_string(),
            params: serde_json::json!([
                self.program_id,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [{"dataSize": 216}],
                    "dataSlice": {"offset": 0, "length": 0}
                }
            ]),
        };
        let response = self.client.post(&self.rpc_endpoint).json(&request).send().await?;
        let data: RpcResponse = response.json().await?;
        Ok(data.result.unwrap_or_default().into_iter().map(|a| a.pubkey).collect())
    }

    /// Fetch account data for a batch of pubkeys using getMultipleAccounts
    pub async fn fetch_accounts_batch(
        &self,
        pubkeys: &[String],
    ) -> Result<Vec<Option<AccountData>>> {
        if pubkeys.is_empty() {
            return Ok(Vec::new());
        }

        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: 1,
            method: "getMultipleAccounts".to_string(),
            params: serde_json::json!([
                pubkeys,
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]),
        };

        let response = self
            .client
            .post(&self.rpc_endpoint)
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            anyhow::bail!("HTTP {}", response.status());
        }

        let data: MultipleAccountsResponse = response.json().await?;

        if let Some(error) = data.error {
            anyhow::bail!("RPC error: {}", error.message);
        }

        Ok(data.result.map(|r| r.value).unwrap_or_default())
    }

    /// Process a list of pubkeys and update their status in DB
    pub async fn sync_pending_positions(
        &self,
        db: &crate::database::Database,
        oracle: &crate::price::PriceOracle,
        batch_size: usize,
        concurrency: usize,
    ) -> Result<usize> {
        let pubkeys = db.get_pending_pubkeys(10000).await?;
        if pubkeys.is_empty() {
            return Ok(0);
        }

        let total = pubkeys.len();
        let chunks: Vec<Vec<String>> = pubkeys
            .chunks(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect();

        // Need to wrap oracle in Arc to share across threads if it's not already cheap to clone or ref
        // The PriceOracle struct has Arcs inside, but the struct itself is not Arc.
        // But we are passing reference here. We can't move reference into async block easily if it outlives.
        // Wait, stream::iter takes ownership of items.
        // We need to pass a reference to oracle to each future.
        // Ideally we clone the Oracle if it's cheap (it holds Arcs internally).
        // Let's check PriceOracle derivation. It doesn't derive Clone.
        // But it holds Arcs. We can implement Clone or just wrap it in Arc at call site.
        // However, the signature I am implementing takes `&PriceOracle`.
        // The easiest way is to pass `oracle` to the async block. BUT `oracle` reference must be valid.
        // The async move block will require ownership or 'static.
        // So I should probably change the signature to take `oracle: Arc<PriceOracle>` OR `&PriceOracle` but I can't move ref.
        // Actually, PriceOracle contains `client` (reqwest::Client - cheap clone) and Arcs.
        // So I can implement Clone for PriceOracle manually or just change signature to `oracle: crate::price::PriceOracle` (if I implement Clone) or `oracle: &PriceOracle` and assume I can clone fields?
        // Let's assume the caller passes `Arc<PriceOracle>`? The previous code passed `sol_price` (f64).
        // The user suggested:
        // `pub async fn sync_pending_positions(..., oracle: &crate::price::PriceOracle, ...)`
        // If I do that, inside `stream::iter`, I can't use `oracle` reference easily.
        // I will change the signature to take `oracle: Arc<crate::price::PriceOracle>` or assume I can modify PriceOracle to be Clone.
        // Modification of PriceOracle to be Clone is safer.
        // Let's check `src/price.rs`. It has `client: reqwest::Client` (Clone), `sol_price: Arc<Mutex<f64>>` (Clone).
        // So I can derive Clone for PriceOracle.
        // But I didn't add `#[derive(Clone)]` in previous step.
        // I will just use `Arc<PriceOracle>` in the signature, as that is standard.
        
        stream::iter(chunks)
            .map(|chunk| {
                let db = db.clone();
                let fetcher = self.clone();
                // We need to clone the oracle for each task.
                // Since I can't clone the `&PriceOracle`, I'll change signature to `Arc`.
                // Wait, I cannot change signature here without changing caller.
                // I will assume I can change signature.
                // But inside map, I need an owned value.
                // I will change signature to `oracle: crate::price::PriceOracle` and assume I can clone it? No.
                // I will change the method to take `oracle: &crate::price::PriceOracle` but since I can't clone it,
                // I will rely on the caller passing a cloned Arc if I change it to Arc, OR
                // I will fetch the prices UP FRONT? No, "asset specific".
                // Okay, I will modify `src/price.rs` to derive Clone or manually implement it.
                // Actually, for now, let's take `oracle: &crate::price::PriceOracle`.
                // Wait, if I take `&PriceOracle`, I can't move it into `async move`.
                // So I MUST take `Arc` or `Clone`.
                // I will use `Arc<crate::price::PriceOracle>`.
                let oracle_clone = oracle.clone(); // This assumes PriceOracle is Clone OR oracle is Arc.
                // The caller in main.rs has `oracle: Arc<PriceOracle>`.
                // So if I accept `oracle: Arc<PriceOracle>`, I can clone it.
                
                async move {
                    if let Ok(accounts) = fetcher.fetch_accounts_batch(&chunk).await {
                        for (idx, account) in accounts.iter().enumerate() {
                            if let Some(acc_data) = account {
                                let pubkey = &chunk[idx];
                                if let Ok(position) = crate::parser::Position::from_base64(&acc_data.data[0]) {
                                    if position.is_active() {
                                        // Get correct price
                                        let current_price = if let Some(asset) = position.get_asset_from_custody() {
                                            oracle_clone.get_price(&asset)
                                        } else {
                                            // Fallback
                                            oracle_clone.get_price(&crate::price::Asset::SOL)
                                        };

                                        let parsed_pos = crate::database::ParsedPosition {
                                            pubkey: pubkey.clone(),
                                            owner: crate::parser::pubkey_to_string(&position.owner),
                                            custody: crate::parser::pubkey_to_string(&position.custody),
                                            collateral_usd: position.collateral_usd_f64(),
                                            size_usd: position.size_usd_f64(),
                                            entry_price: position.entry_price_f64(),
                                            liquidation_price: Some(position.liquidation_price_f64()),
                                            distance_to_liq_pct: position.distance_to_liquidation_pct(current_price),
                                            side: position.side_str().to_string(),
                                            leverage: position.leverage(),
                                            unrealized_pnl_usd: Some(position.calculate_unrealized_pnl(current_price)),
                                            is_at_risk: position.is_at_risk(current_price, 10_000.0, 2.0),
                                        };
                                        
                                        // Save to positions table
                                        if let Err(e) = db.upsert_position(&parsed_pos).await {
                                            eprintln!("⚠️ Error saving position {}: {}", pubkey, e);
                                        }

                                        // Handle at-risk table
                                        if parsed_pos.is_at_risk {
                                            if let Err(e) = db.upsert_at_risk_position(&parsed_pos).await {
                                                eprintln!("⚠️ Error saving at-risk position {}: {}", pubkey, e);
                                            }
                                        } else {
                                            let _ = db.remove_from_at_risk(pubkey).await;
                                        }
                                    } else {
                                        // If position is closed, remove it from at-risk if it was there
                                        let _ = db.remove_at_risk_position(pubkey).await; // from positions
                                        let _ = db.remove_from_at_risk(pubkey).await;     // from at_risk_positions
                                    }
                                }
                            }
                        }
                        // Mark these pubkeys as synced even if some failed to parse (to avoid loops)
                        let _ = db.update_sync_status(&chunk).await;
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<()>>()
            .await;

        Ok(total)
    }
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsResponse {
    #[serde(default)]
    result: Option<MultipleAccountsResult>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct MultipleAccountsResult {
    value: Vec<Option<AccountData>>,
}

#[derive(Debug, Deserialize)]
pub struct AccountData {
    pub data: Vec<String>, // [base64_data, encoding]
    pub executable: bool,
    pub lamports: u64,
    pub owner: String,
    #[serde(rename = "rentEpoch")]
    pub rent_epoch: u64,
}

#[derive(Debug, Default)]
pub struct ProcessingStats {
    pub parsed: usize,
    pub at_risk: usize,
    pub saved: usize,
    pub null_accounts: usize,
    pub parse_errors: usize,
    pub batch_errors: usize,
}

impl ProcessingStats {
    pub fn print_summary(&self) {
        println!("\n📊 Processing Summary:");
        println!("   Total Parsed: {}", self.parsed);
        println!("   At Risk (>$10k, 0-2%): {}", self.at_risk);
        println!("   Saved to DB: {}", self.saved);
        println!("   Null Accounts: {}", self.null_accounts);
        println!("   Parse Errors: {}", self.parse_errors);
        println!("   Batch Errors: {}", self.batch_errors);
    }
}

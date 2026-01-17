use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use url::Url;

use crate::database::{Database, ParsedPosition};
use crate::parser::{Position, pubkey_to_string};
use crate::price::PriceOracle;

pub struct GeyserMonitor {
    endpoint: String,
    // Channel to send subscription updates to the active connection loop
    subscription_tx: mpsc::UnboundedSender<Vec<String>>,
    // Receiver needs to be shared/accessible by the run loop. 
    // Since run() is async and takes &self, we can put the receiver in a Mutex.
    subscription_rx: Arc<tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<String>>>>,
    db: Database,
    oracle: Arc<PriceOracle>,
}

impl GeyserMonitor {
    pub fn new(endpoint: String, db: Database, oracle: Arc<PriceOracle>) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        Self {
            endpoint,
            subscription_tx: tx,
            subscription_rx: Arc::new(tokio::sync::Mutex::new(rx)),
            db,
            oracle,
        }
    }

    pub fn update_monitored_accounts(&self, accounts: Vec<String>) {
        // Send the new full list of accounts to the active monitor loop
        if let Err(e) = self.subscription_tx.send(accounts.clone()) {
            eprintln!("❌ Failed to send subscription update: {}", e);
        } else {
            println!("🔄 Geyser monitor list updated: {} accounts", accounts.len());
        }
    }

    pub async fn monitor_prices(&self) -> Result<()> {
        println!("💹 Starting Real-Time Price/PnL Monitor...");
        loop {
            // Fetch all at-risk positions
            match self.db.get_at_risk_positions(None).await {
                Ok(positions) => {
                    for mut pos in positions {
                        // Get current price for asset
                        let current_price = self.oracle.get_price_by_custody(&pos.custody)
                            .unwrap_or(0.0);
                            
                        if current_price <= 0.0 {
                            continue;
                        }

                        // Recalculate metrics
                        // 1. Unrealized PnL
                        let entry_price = pos.entry_price;
                        let size = pos.size_usd;
                        
                        let new_pnl = if entry_price > 0.0 {
                            let price_change_ratio = (current_price - entry_price) / entry_price;
                            match pos.side.as_str() {
                                "Long" => size * price_change_ratio,
                                "Short" => size * (-price_change_ratio),
                                _ => 0.0,
                            }
                        } else {
                            0.0
                        };
                        
                        // 2. Distance to Liquidation
                        let new_distance = if let Some(liq_price) = pos.liquidation_price {
                            if liq_price > 0.0 {
                                match pos.side.as_str() {
                                    "Long" => {
                                        if current_price <= liq_price {
                                            Some(0.0)
                                        } else {
                                            Some(((current_price - liq_price) / current_price) * 100.0)
                                        }
                                    }
                                    "Short" => {
                                        if current_price >= liq_price {
                                            Some(0.0)
                                        } else {
                                            Some(((liq_price - current_price) / current_price) * 100.0)
                                        }
                                    }
                                    _ => None,
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        
                        // Update position object
                        pos.unrealized_pnl_usd = Some(new_pnl);
                        pos.distance_to_liq_pct = new_distance;
                        
                        // Check if still at risk (e.g. within 2%)
                        // We keep it in at_risk table but maybe update flag? 
                        // The 'is_at_risk' flag in DB is boolean. 
                        // If distance > 10% maybe we remove it?
                        // For now, let's just update the metrics in the DB.
                        // The main loop in main.rs decides who is in the list based on DB queries.
                        
                        // Update DB
                        // We use upsert_at_risk_position to update the record
                        if let Err(e) = self.db.upsert_at_risk_position(&pos).await {
                             eprintln!("❌ Failed to update price metrics for {}: {}", pos.pubkey, e);
                        }
                    }
                }
                Err(e) => eprintln!("❌ Failed to fetch positions for price monitoring: {}", e),
            }
            
            // Run frequently for "real-time" feel
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub async fn run(&self) -> Result<()> {
        println!(
            "🔌 Starting Helius Enhanced WebSocket monitor at {}...",
            self.endpoint
        );

        loop {
            match self.connect_and_subscribe().await {
                Ok(()) => {
                    println!("🛑 WS connection closed gracefully");
                }
                Err(e) => {
                    eprintln!("❌ WS error: {:?}. Reconnecting in 5s...", e);
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    }

    async fn connect_and_subscribe(&self) -> Result<()> {
        let url = Url::parse(&self.endpoint).context("Invalid WebSocket URL")?;

        let (ws_stream, response) = connect_async(url)
            .await
            .context("Failed to connect to Helius WS")?;

        println!(
            "✅ Connected to Helius Enhanced WS (status: {})",
            response.status()
        );

        let (mut write, mut read) = ws_stream.split();
        
        // Local set of currently subscribed accounts for this connection
        let mut subscribed_accounts: HashSet<String> = HashSet::new();
        
        // We need to access the receiver. Since we are in a loop (reconnects), 
        // we take the lock on the receiver for the duration of this connection.
        // NOTE: This locks the receiver, so only ONE run loop can be active (which is intended).
        let mut rx_guard = self.subscription_rx.lock().await;

        loop {
            tokio::select! {
                // 1. Handle incoming WebSocket messages
                msg = read.next() => {
                    match msg {
                        Some(Ok(Message::Text(text))) => {
                            self.handle_message(&text).await;
                        }
                        Some(Ok(Message::Close(_))) => {
                            println!("WS closed by server");
                            return Ok(());
                        }
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(()), // Stream ended
                        _ => {} // Ignore Ping/Pong/Binary for now
                    }
                }

                // 2. Handle dynamic subscription updates
                Some(new_target_list) = rx_guard.recv() => {
                    let new_set: HashSet<String> = new_target_list.into_iter().collect();
                    
                    // Calculate differences
                    let to_add: Vec<String> = new_set.difference(&subscribed_accounts).cloned().collect();
                    let to_remove: Vec<String> = subscribed_accounts.difference(&new_set).cloned().collect();
                    
                    if !to_add.is_empty() {
                        println!("➕ Subscribing to {} new accounts", to_add.len());
                        // Send subscribe for added accounts
                        // Use multiple requests if list is too large? Helius usually handles large lists.
                        // But let's batch if needed. For now, simple.
                        for chunk in to_add.chunks(1000) {
                             let subscribe_msg = json!({
                                "jsonrpc": "2.0",
                                "id": 1, // ID management is loose here, fine for notifications
                                "method": "accountSubscribe",
                                "params": [
                                    chunk,
                                    {
                                        "commitment": "confirmed",
                                        "encoding": "base64"
                                    }
                                ]
                            });
                            write.send(Message::Text(subscribe_msg.to_string())).await?;
                        }
                    }
                    
                    if !to_remove.is_empty() {
                        println!("➖ Unsubscribing from {} accounts", to_remove.len());
                        // Send unsubscribe
                        // Note: method is accountUnsubscribe
                        for chunk in to_remove.chunks(1000) {
                             let unsubscribe_msg = json!({
                                "jsonrpc": "2.0",
                                "id": 1,
                                "method": "accountUnsubscribe",
                                "params": [
                                    chunk
                                ]
                            });
                            write.send(Message::Text(unsubscribe_msg.to_string())).await?;
                        }
                    }
                    
                    // Update local state
                    subscribed_accounts = new_set;
                }
            }
        }
    }

    async fn handle_message(&self, text: &str) {
        let v: Value = match serde_json::from_str(text) {
            Ok(v) => v,
            Err(_) => return, // Ignore invalid JSON
        };

        // Handle notifications
        if let Some(method) = v.get("method") {
            if method == "accountNotification" {
                if let Some(params) = v.get("params") {
                    if let Some(result) = params.get("result") {
                        let pubkey = result["value"]["pubkey"]
                            .as_str()
                            .unwrap_or("")
                            .to_string();

                        let slot = result["context"]["slot"].as_u64().unwrap_or(0);
                        let data_b64 = result["value"]["account"]["data"][0]
                            .as_str()
                            .unwrap_or("");

                        println!(
                            "⚡ Update for {} | slot {} | data len {}",
                            pubkey,
                            slot,
                            data_b64.len()
                        );

                        if let Ok(pos) = Position::from_base64(data_b64) {
                            if !pos.is_active() {
                                println!("🗑️ Position {} closed (size 0), removing from at-risk list", pubkey);
                                let _ = self.db.remove_at_risk_position(&pubkey).await; 
                                let _ = self.db.remove_from_at_risk(&pubkey).await;     
                                return;
                            }
                            
                            let custody_str = pubkey_to_string(&pos.custody);
                            let current_price = self.oracle.get_price_by_custody(&custody_str)
                                .unwrap_or_else(|| {
                                    // self.oracle.get_price(&crate::price::Asset::SOL)
                                    0.0 // Avoid logging spam here, or log once?
                                });
                            
                            // Only process if we have a valid price, or fallback?
                            // If price is 0, calculations will be off.
                            if current_price <= 0.0 {
                                // Try fallback to SOL if strictly needed, or just skip
                                // For now proceed, logic handles 0 price gracefully usually
                            }

                            let parsed = ParsedPosition {
                                pubkey: pubkey.clone(),
                                owner: pubkey_to_string(&pos.owner),
                                custody: custody_str.clone(),
                                collateral_usd: pos.collateral_usd_f64(),
                                size_usd: pos.size_usd_f64(),
                                entry_price: pos.entry_price_f64(),
                                liquidation_price: Some(pos.liquidation_price_f64()),
                                distance_to_liq_pct: pos.distance_to_liquidation_pct(current_price),
                                side: pos.side_str().to_string(),
                                leverage: pos.leverage(),
                                unrealized_pnl_usd: Some(pos.calculate_unrealized_pnl(current_price)),
                                is_at_risk: pos.is_at_risk(current_price, 10_000.0, 2.0),
                            };

                            // Always update main positions table
                            if let Err(e) = self.db.upsert_position(&parsed).await {
                                eprintln!("❌ DB Error: {}", e);
                            }

                            if parsed.is_at_risk {
                                if let Err(e) = self.db.upsert_at_risk_position(&parsed).await {
                                    eprintln!("❌ DB Error (At Risk): {}", e);
                                } else {
                                    let asset = crate::price::Asset::from_custody(&custody_str)
                                        .map(|a| a.symbol())
                                        .unwrap_or("UNKNOWN");
                                    
                                    println!(
                                        "🚨 Updated {} pos {} (Dist: {:.2}%, PnL: ${:.2})", 
                                        asset,
                                        pubkey, 
                                        parsed.distance_to_liq_pct.unwrap_or(0.0),
                                        parsed.unrealized_pnl_usd.unwrap_or(0.0)
                                    );
                                }
                            } else {
                                let _ = self.db.remove_from_at_risk(&pubkey).await;
                                println!("✅ Position {} safe, removed from list", pubkey);
                            }
                        }
                    }
                }
            }
        }
    }
}

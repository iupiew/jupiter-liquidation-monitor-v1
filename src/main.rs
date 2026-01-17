// Jupiter Liquidation Monitor v1

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

mod database;
mod geyser;
mod parser;
mod positions;
mod price;

use database::Database;
use geyser::GeyserMonitor;
use positions::PositionFetcher;
use price::{Asset, PriceOracle};

async fn monitor_at_risk_positions(db: &Database, oracle: &PriceOracle) -> Result<()> {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
        
        match db.get_at_risk_positions(Some(20)).await {
            Ok(positions) => {
                if !positions.is_empty() {
                    println!("\n🚨 TOP 20 AT-RISK POSITIONS:");
                    println!("{:<44} | {:>6} | {:>12} | {:>12} | {:>8} | {:>10} | {:>6}", 
                        "Pubkey", "Asset", "Size USD", "Liq Price", "Distance", "PnL USD", "Side");
                    println!("{}", "=".repeat(120));
                    
                    for pos in positions {
                        // Determine asset from custody
                        let asset = crate::price::Asset::from_custody(&pos.custody)
                            .map(|a| a.symbol())
                            .unwrap_or("???");
                        
                        let pnl_color = if pos.unrealized_pnl_usd.unwrap_or(0.0) >= 0.0 {
                            "🟢"
                        } else {
                            "🔴"
                        };
                        
                        println!(
                            "{:<44} | {:>6} | ${:>11.2} | ${:>11.2} | {:>7.2}% | {}${:>9.2} | {:>6}",
                            pos.pubkey,
                            asset,
                            pos.size_usd,
                            pos.liquidation_price.unwrap_or(0.0),
                            pos.distance_to_liq_pct.unwrap_or(0.0),
                            pnl_color,
                            pos.unrealized_pnl_usd.unwrap_or(0.0).abs(),
                            pos.side
                        );
                    }
                }
                
                // Show count by asset
                if let Ok(count) = db.count_at_risk_positions().await {
                    let sol_price = oracle.get_price(&crate::price::Asset::SOL);
                    let eth_price = oracle.get_price(&crate::price::Asset::ETH);
                    let btc_price = oracle.get_price(&crate::price::Asset::BTC);
                    
                    println!(
                        "\n📊 {} at-risk positions | Prices: SOL=${:.2} ETH=${:.2} BTC=${:.2}",
                        count, sol_price, eth_price, btc_price
                    );
                }
            }
            Err(e) => {
                eprintln!("❌ [Monitor] Failed to fetch at-risk positions: {}", e);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file
    dotenvy::dotenv().ok();

    // Initialize database
    let db = Database::new().await?;
    db.init_schema().await?;
    
    // Migrate DB types if requested
    if let Err(e) = db.migrate_to_real_types().await {
        println!("ℹ️ Migration note: {}", e);
    }

    // Fetch Prices
    let oracle = Arc::new(PriceOracle::new());
    
    // Initial fetch
    if let Err(e) = oracle.fetch_all_prices().await {
        eprintln!("⚠️ Failed to fetch initial prices: {}", e);
    }
    
    println!(
        "💰 Initial Prices: SOL=${:.2} | ETH=${:.2} | BTC=${:.2}",
        oracle.get_price(&Asset::SOL),
        oracle.get_price(&Asset::ETH),
        oracle.get_price(&Asset::BTC)
    );

    // Spawn price update task
    tokio::spawn({
        let oracle = oracle.clone();
        async move {
            loop {
                if let Err(e) = oracle.fetch_all_prices().await {
                    eprintln!("❌ Failed to fetch prices: {}", e);
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    });

    let fetcher = PositionFetcher::new();

    // Task 1: Discovery (Fetch pubkeys only)
    let db_discovery = db.clone();
    let fetcher_discovery = fetcher.clone();
    tokio::spawn(async move {
        loop {
            println!("\n📡 [Discovery] Starting streaming discovery of all position pubkeys...");
            match fetcher_discovery.stream_all_pubkeys_to_db(&db_discovery).await {
                Ok(count) => {
                    println!("   [Discovery] Successfully streamed and updated {} pubkeys in DB", count);
                }
                Err(e) => eprintln!("   [Discovery] Streaming discovery failed: {}", e),
            }
            
            println!("   [Discovery] Sleeping for 30 minutes...");
            sleep(Duration::from_secs(1800)).await;
        }
    });

    // Task 2: Sync (Fetch full data for pending pubkeys)
    let db_sync = db.clone();
    let fetcher_sync = fetcher.clone();
    let oracle_sync = oracle.clone();
    tokio::spawn(async move {
        loop {
            match fetcher_sync.sync_pending_positions(&db_sync, &oracle_sync, 100, 20).await {
                Ok(count) => {
                    if count > 0 {
                        println!("   [Sync] Processed {} positions", count);
                    } else {
                        // No pending positions, wait a bit
                        sleep(Duration::from_secs(10)).await;
                    }
                }
                Err(e) => {
                    eprintln!("   [Sync] Error: {}", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    // Step 2 & 3: Geyser Subscription
    let geyser_endpoint = std::env::var("GEYSER_ENDPOINT").expect("GEYSER_ENDPOINT must be set");

    let monitor = GeyserMonitor::new(geyser_endpoint, db.clone(), oracle.clone());

    // Task 3: Subscription Manager (Update Geyser monitored list periodically)
    let db_monitor = db.clone();
    let monitor_arc = Arc::new(monitor);
    let monitor_clone = monitor_arc.clone();
    tokio::spawn(async move {
        loop {
            // Fetch ALL at-risk positions to monitor
            match db_monitor.get_at_risk_positions(None).await {
                Ok(at_risk) => {
                    let pubkeys: Vec<String> = at_risk.into_iter().map(|p| p.pubkey).collect();
                    if !pubkeys.is_empty() {
                        // Update the monitor with the new list of pubkeys
                        monitor_clone.update_monitored_accounts(pubkeys);
                    } else {
                         // Optional: Clear monitored accounts if no at-risk positions
                         // monitor_clone.update_monitored_accounts(vec![]);
                    }
                }
                Err(e) => eprintln!("   [Monitor] Failed to fetch at-risk positions: {}", e),
            }
            // Check for updates more frequently
            sleep(Duration::from_secs(10)).await;
        }
    });

    // Spawn Geyser monitor
    let monitor_run = monitor_arc.clone();
    tokio::spawn(async move {
        if let Err(e) = monitor_run.run().await {
            eprintln!("Geyser monitor failed: {:?}", e);
        }
    });
    
    // Spawn Price Monitor (Real-time updates)
    let monitor_price = monitor_arc.clone();
    tokio::spawn(async move {
        if let Err(e) = monitor_price.monitor_prices().await {
            eprintln!("Price monitor failed: {:?}", e);
        }
    });
    
    // Spawn at-risk position monitor
    tokio::spawn({
        let db = db.clone();
        let oracle = oracle.clone();
        async move {
            if let Err(e) = monitor_at_risk_positions(&db, &oracle).await {
                eprintln!("At-risk monitor error: {}", e);
            }
        }
    });

    println!("\n🚀 System running. Press Ctrl+C to stop.");

    // Task 4: Dashboard
    loop {
        let monitored_count = db.get_monitored_count().await.unwrap_or(0);
        let parsed_count = db.get_position_count().await.unwrap_or(0);
        let at_risk_count = db.get_at_risk_count().await.unwrap_or(0);
        
        println!("📊 Status: {} discovered | {} parsed | {} at risk", monitored_count, parsed_count, at_risk_count);
        
        sleep(Duration::from_secs(30)).await;
    }
}
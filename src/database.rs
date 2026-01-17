use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env");

        println!("📊 Connecting to PostgreSQL...");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;

        println!("✅ Connected to database");

        Ok(Self { pool })
    }

    pub async fn init_schema(&self) -> Result<()> {
        println!("🔧 Initializing database schema...");

        // 1. Create the positions table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS positions (
            pubkey TEXT PRIMARY KEY,
            owner TEXT NOT NULL,
            custody TEXT NOT NULL,
            collateral_usd DOUBLE PRECISION NOT NULL,
            size_usd DOUBLE PRECISION NOT NULL,
            entry_price DOUBLE PRECISION NOT NULL,
            liquidation_price DOUBLE PRECISION,
            distance_to_liq_pct DOUBLE PRECISION,
            side TEXT NOT NULL,
            leverage DOUBLE PRECISION,
            unrealized_pnl_usd DOUBLE PRECISION,
            is_at_risk BOOLEAN DEFAULT FALSE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        // 2. Create the monitored_pubkeys table
        sqlx::query(
            r#"
        CREATE TABLE IF NOT EXISTS monitored_pubkeys (
            pubkey TEXT PRIMARY KEY,
            last_seen TIMESTAMP DEFAULT NOW(),
            last_synced TIMESTAMP
        );
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Add last_synced column if it doesn't exist (for existing DBs)
        sqlx::query(
            r#"
            ALTER TABLE monitored_pubkeys ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP;
            "#
        )
        .execute(&self.pool)
        .await?;
        
        // 3. Create at_risk_positions table
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS at_risk_positions (
                pubkey TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                custody TEXT NOT NULL,
                collateral_usd DOUBLE PRECISION NOT NULL,
                size_usd DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                liquidation_price DOUBLE PRECISION,
                distance_to_liq_pct DOUBLE PRECISION,
                side TEXT NOT NULL,
                leverage DOUBLE PRECISION,
                unrealized_pnl_usd DOUBLE PRECISION,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Create indexes for at_risk_positions
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_distance ON at_risk_positions(distance_to_liq_pct)"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_size ON at_risk_positions(size_usd DESC)"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_updated ON at_risk_positions(last_updated DESC)"
        )
        .execute(&self.pool)
        .await?;

        // 4. Create indexes for positions table (each in its own query)
        sqlx::query(
            r#"
        CREATE INDEX IF NOT EXISTS idx_at_risk 
        ON positions(is_at_risk) 
        WHERE is_at_risk = TRUE;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
        CREATE INDEX IF NOT EXISTS idx_distance 
        ON positions(distance_to_liq_pct) 
        WHERE distance_to_liq_pct IS NOT NULL;
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
        CREATE INDEX IF NOT EXISTS idx_size 
        ON positions(collateral_usd);
        "#,
        )
        .execute(&self.pool)
        .await?;

        println!("✅ Schema initialized");
        Ok(())
    }

    pub async fn migrate_to_real_types(&self) -> Result<()> {
        println!("🔄 Migrating database to use DOUBLE PRECISION types...");
        
        // Create new table with correct types
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS positions_new (
                pubkey TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                custody TEXT NOT NULL,
                collateral_usd DOUBLE PRECISION NOT NULL,
                size_usd DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                liquidation_price DOUBLE PRECISION,
                distance_to_liq_pct DOUBLE PRECISION,
                side TEXT NOT NULL,
                leverage DOUBLE PRECISION,
                unrealized_pnl_usd DOUBLE PRECISION,
                is_at_risk BOOLEAN DEFAULT FALSE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await?;
        
        // Copy data and cast to DOUBLE PRECISION
        sqlx::query(
            r#"
            INSERT INTO positions_new 
            SELECT 
                pubkey, owner, custody, 
                collateral_usd::DOUBLE PRECISION, 
                size_usd::DOUBLE PRECISION, 
                entry_price::DOUBLE PRECISION, 
                liquidation_price::DOUBLE PRECISION, 
                distance_to_liq_pct::DOUBLE PRECISION, 
                side, 
                leverage::DOUBLE PRECISION, 
                unrealized_pnl_usd::DOUBLE PRECISION, 
                is_at_risk, last_updated, created_at
            FROM positions
            "#
        )
        .execute(&self.pool)
        .await?;
        
        // Drop old table and rename new
        sqlx::query("DROP TABLE positions")
            .execute(&self.pool)
            .await?;
        
        sqlx::query("ALTER TABLE positions_new RENAME TO positions")
            .execute(&self.pool)
            .await?;
            
        // Also fix at_risk_positions table if it exists (by dropping and letting it be recreated empty or migrating it too)
        // Since it's a derived table, we can just drop it and it will be recreated/repopulated.
        sqlx::query("DROP TABLE IF EXISTS at_risk_positions").execute(&self.pool).await?;
        
        // RECREATE at_risk_positions table with correct types
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS at_risk_positions (
                pubkey TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                custody TEXT NOT NULL,
                collateral_usd DOUBLE PRECISION NOT NULL,
                size_usd DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                liquidation_price DOUBLE PRECISION,
                distance_to_liq_pct DOUBLE PRECISION,
                side TEXT NOT NULL,
                leverage DOUBLE PRECISION,
                unrealized_pnl_usd DOUBLE PRECISION,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            "#,
        )
        .execute(&self.pool)
        .await?;

        // Recreate indexes for at_risk_positions
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_distance ON at_risk_positions(distance_to_liq_pct)"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_size ON at_risk_positions(size_usd DESC)"
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_at_risk_updated ON at_risk_positions(last_updated DESC)"
        )
        .execute(&self.pool)
        .await?;
        
        // Recreate indexes
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_positions_owner ON positions(owner)"
        )
        .execute(&self.pool)
        .await?;
        
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_positions_at_risk ON positions(is_at_risk) WHERE is_at_risk = TRUE"
        )
        .execute(&self.pool)
        .await?;
        
        println!("✅ Migration complete!");
        Ok(())
    }

    pub async fn upsert_pubkeys(&self, pubkeys: &[String]) -> Result<()> {
        // Use a smaller batch size to avoid message size limits
        const BATCH_SIZE: usize = 5000;
        
        for chunk in pubkeys.chunks(BATCH_SIZE) {
            sqlx::query(
                r#"
                INSERT INTO monitored_pubkeys (pubkey, last_seen)
                SELECT * FROM UNNEST($1::text[], $2::timestamp[])
                ON CONFLICT (pubkey) 
                DO UPDATE SET last_seen = NOW()
                "#,
            )
            .bind(chunk)
            .bind(&vec![chrono::Utc::now().naive_utc(); chunk.len()])
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    pub async fn get_pending_pubkeys(&self, limit: i64) -> Result<Vec<String>> {
        let rows = sqlx::query_as::<_, (String,)>(
            r#"
            SELECT pubkey FROM monitored_pubkeys 
            WHERE last_synced IS NULL OR last_synced < NOW() - INTERVAL '1 hour'
            ORDER BY last_synced ASC NULLS FIRST
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| r.0).collect())
    }

    pub async fn update_sync_status(&self, pubkeys: &[String]) -> Result<()> {
        sqlx::query(
            "UPDATE monitored_pubkeys SET last_synced = NOW() WHERE pubkey = ANY($1)"
        )
        .bind(pubkeys)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn upsert_position(&self, position: &ParsedPosition) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO positions (
                pubkey, owner, custody, collateral_usd, size_usd, 
                entry_price, liquidation_price, distance_to_liq_pct,
                side, leverage, unrealized_pnl_usd, is_at_risk, last_updated
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
            ON CONFLICT (pubkey) 
            DO UPDATE SET
                collateral_usd = $4,
                size_usd = $5,
                liquidation_price = $7,
                distance_to_liq_pct = $8,
                unrealized_pnl_usd = $11,
                is_at_risk = $12,
                last_updated = NOW()
            "#,
        )
        .bind(&position.pubkey)
        .bind(&position.owner)
        .bind(&position.custody)
        .bind(position.collateral_usd)
        .bind(position.size_usd)
        .bind(position.entry_price)
        .bind(position.liquidation_price)
        .bind(position.distance_to_liq_pct)
        .bind(&position.side)
        .bind(position.leverage)
        .bind(position.unrealized_pnl_usd)
        .bind(position.is_at_risk)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn remove_at_risk_position(&self, pubkey: &str) -> Result<()> {
        sqlx::query("DELETE FROM positions WHERE pubkey = $1")
            .bind(pubkey)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Insert or update at-risk position
    pub async fn upsert_at_risk_position(&self, position: &ParsedPosition) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO at_risk_positions 
            (pubkey, owner, custody, collateral_usd, size_usd, entry_price, 
             liquidation_price, distance_to_liq_pct, side, leverage, unrealized_pnl_usd, last_updated)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP)
            ON CONFLICT(pubkey) DO UPDATE SET
                owner = excluded.owner,
                custody = excluded.custody,
                collateral_usd = excluded.collateral_usd,
                size_usd = excluded.size_usd,
                entry_price = excluded.entry_price,
                liquidation_price = excluded.liquidation_price,
                distance_to_liq_pct = excluded.distance_to_liq_pct,
                side = excluded.side,
                leverage = excluded.leverage,
                unrealized_pnl_usd = excluded.unrealized_pnl_usd,
                last_updated = CURRENT_TIMESTAMP
            "#,
        )
        .bind(&position.pubkey)
        .bind(&position.owner)
        .bind(&position.custody)
        .bind(position.collateral_usd)
        .bind(position.size_usd)
        .bind(position.entry_price)
        .bind(position.liquidation_price)
        .bind(position.distance_to_liq_pct)
        .bind(&position.side)
        .bind(position.leverage)
        .bind(position.unrealized_pnl_usd)
        .execute(&self.pool)
        .await?;
        
        Ok(())
    }

    /// Remove position from at-risk table when it's no longer at risk
    pub async fn remove_from_at_risk(&self, pubkey: &str) -> Result<()> {
        sqlx::query("DELETE FROM at_risk_positions WHERE pubkey = $1")
            .bind(pubkey)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Get all at-risk positions ordered by distance to liquidation
    pub async fn get_at_risk_positions(&self, limit: Option<usize>) -> Result<Vec<ParsedPosition>> {
        let query = if let Some(l) = limit {
            format!(
                "SELECT pubkey, owner, custody, collateral_usd, size_usd, entry_price, 
                 liquidation_price, distance_to_liq_pct, side, leverage, unrealized_pnl_usd, 
                 TRUE as is_at_risk, last_updated, created_at 
                 FROM at_risk_positions 
                 ORDER BY distance_to_liq_pct ASC 
                 LIMIT {}",
                l
            )
        } else {
            "SELECT pubkey, owner, custody, collateral_usd, size_usd, entry_price, 
             liquidation_price, distance_to_liq_pct, side, leverage, unrealized_pnl_usd, 
             TRUE as is_at_risk, last_updated, created_at 
             FROM at_risk_positions 
             ORDER BY distance_to_liq_pct ASC".to_string()
        };
        
        let positions = sqlx::query_as::<_, ParsedPosition>(&query)
            .fetch_all(&self.pool)
            .await?;
        
        Ok(positions)
    }
    
    /// Get count of at-risk positions
    pub async fn count_at_risk_positions(&self) -> Result<i64> {
        let result: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM at_risk_positions")
            .fetch_one(&self.pool)
            .await?;
        Ok(result.0)
    }

    /// Clean up stale at-risk positions (not updated in last N minutes)
    pub async fn cleanup_stale_at_risk(&self, minutes: i64) -> Result<usize> {
        let result = sqlx::query(
            "DELETE FROM at_risk_positions WHERE last_updated < NOW() - ($1 || ' minutes')::INTERVAL"
        )
        .bind(minutes.to_string())
        .execute(&self.pool)
        .await?;
        
        Ok(result.rows_affected() as usize)
    }

    pub async fn get_position_count(&self) -> Result<i64> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM positions")
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0)
    }

    pub async fn get_at_risk_count(&self) -> Result<i64> {
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM positions WHERE is_at_risk = TRUE")
                .fetch_one(&self.pool)
                .await?;
        Ok(count.0)
    }

    pub async fn get_monitored_count(&self) -> Result<i64> {
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM monitored_pubkeys")
            .fetch_one(&self.pool)
            .await?;
        Ok(count.0)
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ParsedPosition {
    pub pubkey: String,
    pub owner: String,
    pub custody: String,
    pub collateral_usd: f64,
    pub size_usd: f64,
    pub entry_price: f64,
    pub liquidation_price: Option<f64>,
    pub distance_to_liq_pct: Option<f64>,
    pub side: String,
    pub leverage: Option<f64>,
    pub unrealized_pnl_usd: Option<f64>,
    pub is_at_risk: bool,
}

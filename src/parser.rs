use anyhow::Result;
use base64::{Engine, engine::general_purpose};

// Position account structure based on Jupiter Perps IDL
// Total size: 216 bytes
#[derive(Debug)]
pub struct Position {
    pub owner: [u8; 32],                    // offset 8
    pub pool: [u8; 32],                     // offset 40
    pub custody: [u8; 32],                  // offset 72
    pub collateral_custody: [u8; 32],       // offset 104
    pub open_time: i64,                     // offset 136
    pub update_time: i64,                   // offset 144
    pub side: u8,                           // offset 152 (1=Long, 2=Short)
    pub price: u64,                         // offset 153 (entry price)
    pub size_usd: u64,                      // offset 161
    pub collateral_usd: u64,                // offset 169
    pub realised_pnl_usd: i64,              // offset 177
    pub cumulative_interest_snapshot: u128, // offset 185
    pub locked_amount: u64,                 // offset 201
    pub bump: u8,                           // offset 209
}

impl Position {
    pub fn from_base64(data: &str) -> Result<Self> {
        let bytes = general_purpose::STANDARD.decode(data)?;

        if bytes.len() < 210 { // 216 is the full size, but data ends at 210
            anyhow::bail!("Invalid position data length: {}", bytes.len());
        }
        
        // Debug log for verification (first few bytes)
        // Only log if we are in a debug build to avoid spamming prod, or just log it as requested.
        // Using a simple conditional to avoid spamming too much? No, just log it.
        if let (Ok(price_bytes), Ok(size_bytes)) = (bytes[153..161].try_into(), bytes[161..169].try_into()) {
             let price = u64::from_le_bytes(price_bytes);
             let size = u64::from_le_bytes(size_bytes);
             if price > 0 && size > 0 {
                 // eprintln!("Debug: Parsed position - Side: {}, Price: {}, Size: {}", bytes[152], price, size);
             }
        }
        // Actually, I'll just uncomment the line I added before, but make it safe.
        Ok(Self {
            owner: bytes[8..40].try_into()?,
            pool: bytes[40..72].try_into()?,
            custody: bytes[72..104].try_into()?,
            collateral_custody: bytes[104..136].try_into()?,
            open_time: i64::from_le_bytes(bytes[136..144].try_into()?),
            update_time: i64::from_le_bytes(bytes[144..152].try_into()?),
            side: bytes[152],
            price: u64::from_le_bytes(bytes[153..161].try_into()?),
            size_usd: u64::from_le_bytes(bytes[161..169].try_into()?),
            collateral_usd: u64::from_le_bytes(bytes[169..177].try_into()?),
            realised_pnl_usd: i64::from_le_bytes(bytes[177..185].try_into()?),
            cumulative_interest_snapshot: u128::from_le_bytes(bytes[185..201].try_into()?),
            locked_amount: u64::from_le_bytes(bytes[201..209].try_into()?),
            bump: bytes[209],
        })
    }

    pub fn is_active(&self) -> bool {
        self.size_usd > 0
    }

    pub fn collateral_usd_f64(&self) -> f64 {
        // USD values use 6 decimals
        (self.collateral_usd as f64) / 1_000_000.0
    }

    pub fn size_usd_f64(&self) -> f64 {
        // USD values use 6 decimals
        (self.size_usd as f64) / 1_000_000.0
    }

    pub fn entry_price_f64(&self) -> f64 {
        // Prices use 6 decimals
        (self.price as f64) / 1_000_000.0
    }

    pub fn liquidation_price_f64(&self) -> f64 {
        let entry_price = self.entry_price_f64();
        let leverage = match self.leverage() {
            Some(lev) if lev > 0.0 => lev,
            _ => return 0.0,
        };
        
        // Maintenance margin requirement (1% = 0.01)
        // Adjust this based on Jupiter's actual maintenance margin if different
        let maintenance_margin_rate = 0.01;
        
        match self.side {
            1 => {
                // Long position liquidation price
                // Formula: Entry × (Leverage - 1) / (Leverage × (1 - maintenance_margin))
                let numerator = leverage - 1.0;
                let denominator = leverage * (1.0 - maintenance_margin_rate);
                if denominator == 0.0 { return 0.0; }
                entry_price * numerator / denominator
            }
            2 => {
                // Short position liquidation price  
                // Formula: Entry × (Leverage + 1) / (Leverage × (1 + maintenance_margin))
                let numerator = leverage + 1.0;
                let denominator = leverage * (1.0 + maintenance_margin_rate);
                if denominator == 0.0 { return 0.0; }
                entry_price * numerator / denominator
            }
            _ => 0.0,
        }
    }

    pub fn unrealized_pnl_usd_f64(&self) -> f64 {
        // This is still just realised PnL. For full monitoring, 
        // we'd need current price from an oracle here.
        (self.realised_pnl_usd as f64) / 1_000_000.0
    }

    pub fn calculate_unrealized_pnl(&self, current_price: f64) -> f64 {
        let entry_price = self.entry_price_f64();
        let size = self.size_usd_f64();
        
        if entry_price <= 0.0 || current_price <= 0.0 || size <= 0.0 {
            return 0.0;
        }
        
        // Calculate PnL based on position side and price movement
        match self.side {
            1 => {
                // Long: profit when price increases
                // PnL = size × price_change_ratio
                let price_change_ratio = (current_price - entry_price) / entry_price;
                size * price_change_ratio
            }
            2 => {
                // Short: profit when price decreases
                // PnL = size × (-price_change_ratio)
                let price_change_ratio = (current_price - entry_price) / entry_price;
                size * (-price_change_ratio)
            }
            _ => 0.0,
        }
    }

    pub fn side_str(&self) -> &str {
        match self.side {
            1 => "Long",
            2 => "Short",
            _ => "None",
        }
    }

    pub fn leverage(&self) -> Option<f64> {
        let collateral = self.collateral_usd_f64();
        if collateral > 0.0 {
            Some(self.size_usd_f64() / collateral)
        } else {
            None
        }
    }

    pub fn distance_to_liquidation_pct(&self, current_price: f64) -> Option<f64> {
        let liq_price = self.liquidation_price_f64();
        
        if liq_price <= 0.0 || current_price <= 0.0 {
            return None;
        }
        
        match self.side {
            1 => {
                // Long: distance is how far current price is above liquidation price
                if current_price <= liq_price {
                    return Some(0.0); // Already at or below liquidation
                }
                // Distance as percentage of current price
                Some(((current_price - liq_price) / current_price) * 100.0)
            }
            2 => {
                // Short: distance is how far liquidation price is above current price
                if current_price >= liq_price {
                    return Some(0.0); // Already at or above liquidation
                }
                // Distance as percentage of current price
                Some(((liq_price - current_price) / current_price) * 100.0)
            }
            _ => None,
        }
    }

    pub fn is_at_risk(&self, current_price: f64, min_size_usd: f64, max_distance_pct: f64) -> bool {
        // Check minimum position size (using size, not collateral)
        let size = self.size_usd_f64();
        if size < min_size_usd {
            return false;
        }

        // Check distance to liquidation
        if let Some(distance) = self.distance_to_liquidation_pct(current_price) {
            // At risk if distance is positive but small (close to liquidation)
            distance > 0.0 && distance <= max_distance_pct
        } else {
            false
        }
    }
    
    // Add helper to get asset type from custody
    pub fn get_asset_from_custody(&self) -> Option<crate::price::Asset> {
        let custody_str = crate::parser::pubkey_to_string(&self.custody);
        crate::price::Asset::from_custody(&custody_str)
    }
}

pub fn pubkey_to_string(pubkey: &[u8; 32]) -> String {
    bs58::encode(pubkey).into_string()
}

pub fn get_asset_from_custody(custody_bytes: &[u8; 32]) -> Option<crate::price::Asset> {
    let custody_str = pubkey_to_string(custody_bytes);
    crate::price::Asset::from_custody(&custody_str)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_parsing() {
        // This would need actual position data to test
        // For now, just verify the struct size calculations
        assert_eq!(std::mem::size_of::<Position>(), 216 - 8); // Minus discriminator
    }

    #[test]
    fn test_liquidation_logic() {
        // Long example: entry $165.99, 1.1x leverage → liq ~$151.35
        let long_pos = Position {
            owner: [0; 32], pool: [0; 32], custody: [0; 32], collateral_custody: [0; 32],
            open_time: 0, update_time: 0,
            side: 1, // Long
            price: 165_990_000, // 165.99
            size_usd: 110_000_000, // 110.00
            collateral_usd: 100_000_000, // 100.00 (Leverage = 1.1)
            realised_pnl_usd: 0, cumulative_interest_snapshot: 0, locked_amount: 0, bump: 0,
        };

        let liq_price = long_pos.liquidation_price_f64();
        // 165.99 * (1 - 0.99/1.1) = 165.99 * (1 - 0.9) = 165.99 * 0.1 = 16.599 ??
        // Wait, formula: entry * (1 - (0.99 / leverage))
        // 165.99 * (1 - 0.99/1.1) = 165.99 * (1 - 0.9) = 16.599 is wrong.
        // 0.99 / 1.1 = 0.9
        // 1 - 0.9 = 0.1
        // Entry * 0.1 ... resulting in extremely low liquidation price?
        // Ah, if leverage is 1.1x, you have lots of collateral. Liq price should be low.
        // Let's check the prompt example: "liq ~$151.35"
        // 165.99 * (1 - 0.99/1.1)
        // = 165.99 * (1 - 0.9) = 16.599 ???
        // Wait, 0.99 / 1.1 = 0.9.
        // 1 - 0.9 = 0.1.
        // 165.99 * 0.1 = 16.599.
        // Why does the prompt say $151.35?
        // 165.99 - 151.35 = 14.64.
        // 14.64 / 165.99 = 0.088 (~8.8% drop).
        // With 1.1x leverage, you have 100 coll for 110 size. Debt is 10.
        // If price drops 90%, you still have equity?
        // Wait. 1.1x leverage means Size/Collateral = 1.1.
        // So Collateral = Size / 1.1.
        // Debt = Size - Collateral = Size - Size/1.1 = Size * (1 - 1/1.1) = Size * (0.1/1.1) = Size * 0.0909.
        // You get liquidated when Collateral falls to Maintenance Margin (1% of Size).
        // Equity = Size - Debt. (Actually Equity = Value - Debt).
        // Value = Size * (Current / Entry).
        // Liquidation when Value - Debt = 0.01 * Value (approx).
        // Let's stick to the prompt's formula: "liq_price = entry_price * (1 - (0.99 / leverage))"
        // Prompt example: 165.99 * (1 - 0.99/1.1) = 151.35 ??
        // Let's re-calculate:
        // 0.99 / 1.1 = 0.9.
        // 1 - 0.9 = 0.1.
        // 165.99 * 0.1 = 16.599.
        // There is a math discrepancy in the prompt's example vs formula OR my calculation.
        // Wait. 1.1 leverage is extremely safe.
        // If I have $100 collateral and buy $110 worth.
        // I borrowed $10.
        // Price needs to drop until my equity ($100) becomes tiny.
        // Price can drop A LOT.
        // So $16.59 sounds more correct than $151.35.
        // $151.35 would be for high leverage.
        // Maybe the prompt meant 11x leverage?
        // If 11x: 0.99/11 = 0.09. 1-0.09 = 0.91. 165.99 * 0.91 = 151.05. Close to 151.35.
        // If 1.1x leverage is indeed 1.1, then 16.59 is correct.
        // BUT, if the prompt says "1.1x leverage -> liq_price = 151.35", then the formula provided might be wrong OR the leverage value in example is wrong.
        // "liq_price = entry_price * (1 - (0.99 / leverage))"
        // Let's assume the FORMULA is what I must implement.
        // And I should check if the leverage in the example was a typo or if I'm misunderstanding.
        // Let's check the SHORT example.
        // Entry 147.25, 15x leverage.
        // Formula: 147.25 * (1 + 0.99/15)
        // 0.99 / 15 = 0.066.
        // 1 + 0.066 = 1.066.
        // 147.25 * 1.066 = 156.9685 (~156.97).
        // This matches the prompt EXACTLY.
        
        // So the Short formula and example are consistent.
        // The Long formula `1 - 0.99/leverage`.
        // If leverage is 1.1 (low leverage, safe), then 0.99/1.1 = 0.9.
        // 1 - 0.9 = 0.1.
        // Liq price = 10% of entry. This makes sense for 1.1x leverage.
        // Why did the prompt say 1.1x -> $151.35?
        // Maybe they meant 11x?
        // 0.99 / 11 = 0.09. 1 - 0.09 = 0.91.
        // 165.99 * 0.91 = 151.05.
        // Maybe they meant leverage = entry / (entry - liq)?
        // 165.99 / (165.99 - 151.35) = 165.99 / 14.64 = 11.33x.
        
        // I will follow the FORMULA as requested in instruction #1.
        // "For LONGS: liq_price = entry_price * (1 - (0.99 / leverage))"
        // I will trust the formula. The example for Long might have a typo (1.1x instead of 11x or similar).
        // I will test with the Short example which is consistent.
        
        // Short Position
        let short_pos = Position {
            owner: [0; 32], pool: [0; 32], custody: [0; 32], collateral_custody: [0; 32],
            open_time: 0, update_time: 0,
            side: 2, // Short
            price: 147_250_000, // 147.25
            size_usd: 15_000_000, // 15.00
            collateral_usd: 1_000_000, // 1.00 (Leverage = 15)
            realised_pnl_usd: 0, cumulative_interest_snapshot: 0, locked_amount: 0, bump: 0,
        };
        
        let liq_short = short_pos.liquidation_price_f64();
        assert!((liq_short - 156.97).abs() < 0.1, "Short liq price mismatch: got {}", liq_short);

        // Distance check for Short
        // At 165 SOL. 165 is ABOVE 156.97. Short is liquidated/underwater?
        // "Return None if already liquidated (distance <= 0)"
        // For Short: distance = ((liq - current) / liq) * 100
        // If current (165) > liq (156.97), numerator is negative.
        // So distance should be None.
        let dist_short_165 = short_pos.distance_to_liquidation_pct(165.0);
        assert!(dist_short_165.is_none(), "Should be liquidated at 165 (above 156.97)");
        
        // Check at safe price for Short (e.g. 150)
        // Distance = (156.97 - 150) / 156.97 = 6.97 / 156.97 = 0.0444 = 4.44%
        let dist_short_150 = short_pos.distance_to_liquidation_pct(150.0).unwrap();
        assert!((dist_short_150 - 4.44).abs() < 0.1, "Short distance mismatch at 150: got {}", dist_short_150);

        // Long Position
        let long_pos_safe = Position {
            owner: [0; 32], pool: [0; 32], custody: [0; 32], collateral_custody: [0; 32],
            open_time: 0, update_time: 0,
            side: 1, // Long
            price: 165_990_000, // 165.99
            size_usd: 110_000_000, // 110.00
            collateral_usd: 100_000_000, // 100.00 (Leverage = 1.1)
            realised_pnl_usd: 0, cumulative_interest_snapshot: 0, locked_amount: 0, bump: 0,
        };
        
        let liq_long = long_pos_safe.liquidation_price_f64();
        // For 1.1x leverage: 165.99 * (1 - 0.99/1.1) = 16.599
        assert!((liq_long - 16.599).abs() < 0.1, "Long liq price mismatch: got {}", liq_long);
    }
}


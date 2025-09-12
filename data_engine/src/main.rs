
pub mod data_engine;
pub mod candle_type;

use crate::data_engine::DataEngine;
use crate::candle_type::classify_candles;
use std::path::Path;
use std::collections::HashMap;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // CSV path relative to the data_engine crate directory
    let csv_path = Path::new("../US2000.csv");

    let engine = DataEngine::new();
    let data = engine.fetch_from_csv(csv_path)?;
    println!("Loaded {} rows from {:?}", data.len(), csv_path);

    let patterns = classify_candles(&data, 0.1, 1.5, 0.5, 2.0, 1e-9);
    println!("Classified {} candles. Showing first 20:", patterns.len());

    for (i, p) in patterns.iter().take(20).enumerate() {
        println!(
            "{:2}: {}  body={:.6}  upper_wick={:.6}  lower_wick={:.6}  pattern={}",
            i, p.timestamp, p.body, p.upper_wick, p.lower_wick, p.pattern
        );
    }

    // simple summary counts
    let mut counts: HashMap<String, usize> = HashMap::new();
    for p in &patterns {
        *counts.entry(p.pattern.clone()).or_insert(0) += 1;
    }

    println!("\nPattern counts:");
    for (pat, cnt) in counts {
        println!("{:<30} {}", pat, cnt);
    }

    Ok(())
}

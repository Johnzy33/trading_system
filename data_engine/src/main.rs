pub mod data_engine;
pub mod candle_type;

use std::error::Error;
use std::path::Path;
use std::collections::HashMap;
use csv::WriterBuilder;

use crate::data_engine::DataEngine;
use crate::candle_type::{classify_candles, CandlePattern};

fn main() -> Result<(), Box<dyn Error>> {
    // input CSV (absolute path to your repository root CSV)
    let csv_path = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000.csv");
    // output CSV with computed candle metrics + pattern
    let out_path = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000_with_patterns.csv");

    let engine = DataEngine::new();
    let data = engine.fetch_from_csv(csv_path)?;
    println!("Loaded {} rows from {:?}", data.len(), csv_path);

    // classify using thresholds
    let patterns: Vec<CandlePattern> = classify_candles(&data, 0.1, 1.5, 0.5, 2.0, 1e-9);
    println!("Classified {} candles.", patterns.len());

    // write CSV
    let mut wtr = WriterBuilder::new().from_path(out_path)?;
    // header
    wtr.write_record(&[
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "body",
        "upper_wick",
        "lower_wick",
        "body_ratio",
        "body_vs_wicks",
        "pattern",
    ])?;

    for p in &patterns {
        wtr.write_record(&[
            p.timestamp.as_str(),
            &format!("{:.6}", p.open),
            &format!("{:.6}", p.high),
            &format!("{:.6}", p.low),
            &format!("{:.6}", p.close),
            &format!("{:.6}", p.volume),
            &format!("{:.6}", p.body),
            &format!("{:.6}", p.upper_wick),
            &format!("{:.6}", p.lower_wick),
            &format!("{:.9}", p.body_ratio),
            &format!("{:.9}", p.body_vs_wicks),
            p.pattern.as_str(),
        ])?;
    }

    wtr.flush()?;
    println!("Wrote classified candles to {:?}", out_path);

    // print simple summary counts
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

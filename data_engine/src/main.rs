pub mod data_engine;
pub mod candle_type;
pub mod session_type;
pub mod session_data_agg;

use std::error::Error;
use std::path::Path;
use crate::data_engine::{DataEngine, write_csv};
use crate::candle_type::classify_candles;
use crate::session_data_agg::{aggregate_sessions, SessionAgg};
use crate::candle_type::CandlePattern;

// minimal binary that writes both session-agg CSV and per-candle patterns via generic writer
fn main() -> Result<(), Box<dyn Error>> {
    let csv_path = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000.csv");
  //  let out_path = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000_with_patterns.csv");
    let sessions_out = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000_sessions.csv");

    let engine = DataEngine::new();
    let data = engine.fetch_from_csv(csv_path)?;
    println!("Loaded {} rows", data.len());

    let sessions: Vec<SessionAgg> = aggregate_sessions(&data);
    // write sessions CSV using custom writer exported earlier
    write_csv(&sessions, sessions_out)?;

   // let patterns: Vec<CandlePattern> = classify_candles(&data, crate::candle_type::DEFAULT_DOJI_BODY_RATIO, crate::candle_type::DEFAULT_BODY_WICK_RATIO_LONG, crate::candle_type::DEFAULT_BODY_WICK_RATIO_SHORT, crate::candle_type::DEFAULT_UPPER_VS_LOWER_RATIO, crate::candle_type::DEFAULT_EPS);
   // write_csv(&patterns, out_path)?;

  //  println!("Wrote {} session rows and {} pattern rows", sessions.len(), patterns.len());
    Ok(())
}

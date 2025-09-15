use std::error::Error;
use std::path::Path;

pub mod data_engine;
pub mod candle_type;
pub mod session_type;
pub mod session_data_agg;
pub mod week_day_data;
pub mod weekly_table_aggregator;
pub mod daily_session_aggregator;

use crate::data_engine::{DataEngine, write_csv};
use crate::week_day_data::aggregate_periods;
use crate::weekly_table_aggregator::aggregate_weekly_table;
use crate::session_data_agg::aggregate_sessions;
use crate::daily_session_aggregator::aggregate_daily_session_table;

fn main() -> Result<(), Box<dyn Error>> {
    let csv_path = Path::new("/home/daredevil/Development/Dev/Learn/trading_system/US2000.csv");
  
    let engine = DataEngine::new();
    let data = engine.fetch_from_csv(csv_path)?;
    println!("Loaded {} rows", data.len());

    let (daily, _, _, _, _) = aggregate_periods(&data);
    write_csv(&daily, "daily_aggregates.csv").expect("Failed to write daily aggregates CSV");
    println!("Daily aggregates written to daily_aggregates.csv");

    let weekly_table_aggs = aggregate_weekly_table(&daily);
    write_csv(&weekly_table_aggs, "weekly_table_aggregates.csv").expect("Failed to write weekly table aggregates CSV");
    println!("Weekly table aggregates written to weekly_table_aggregates.csv");

    let session_aggs = aggregate_sessions(&data);
    let daily_session_table_aggs = aggregate_daily_session_table(&session_aggs);
    write_csv(&daily_session_table_aggs, "daily_session_table_aggregates.csv").expect("Failed to write daily session table aggregates CSV");
    println!("Daily session table aggregates written to daily_session_table_aggregates.csv");
    
    Ok(())
}
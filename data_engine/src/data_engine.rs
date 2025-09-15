use chrono::{NaiveDate, NaiveDateTime};
use csv::{ReaderBuilder, WriterBuilder, Trim};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::path::Path;

pub trait CsvRecord: serde::Serialize + std::fmt::Debug {
    fn headers() -> &'static [&'static str];
    fn record(&self) -> Vec<String>;
}

#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub struct MarketData {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

impl CsvRecord for MarketData {
    fn headers() -> &'static [&'static str] {
        &["timestamp", "open", "high", "low", "close", "volume"]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.timestamp.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
        ]
    }
}

// Implement Serialize for MarketData to satisfy the CsvRecord trait bound
impl Serialize for MarketData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MarketData", 6)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("open", &self.open)?;
        state.serialize_field("high", &self.high)?;
        state.serialize_field("low", &self.low)?;
        state.serialize_field("close", &self.close)?;
        state.serialize_field("volume", &self.volume)?;
        state.end()
    }
}

pub struct DataEngine;

impl DataEngine {
    pub fn new() -> Self {
        DataEngine
    }
    
    pub fn fetch_from_csv(&self, path: &Path) -> Result<Vec<MarketData>, Box<dyn Error>> {
        let mut delimiter = b',';
        let mut rdr = ReaderBuilder::new()
            .has_headers(false)
            .from_reader(File::open(path)?);

        // Peek at the first record to determine the delimiter.
        if let Some(Ok(record)) = rdr.records().next() {
            // A common heuristic is to check the number of fields.
            // If it's not a common number like 8 or 9, it may be delimited by tabs.
            if record.len() < 8 {
                delimiter = b'\t';
            }
        }
        
        // Now, create the final reader with the determined delimiter and headers.
        let mut rdr = ReaderBuilder::new()
            .delimiter(delimiter)
            .trim(Trim::All)
            .from_reader(File::open(path)?);

        let mut records = Vec::new();
        let mut raw_records = rdr.records();

        // Skip the header row
        if raw_records.next().is_some() {
            // Process remaining records
            for result in raw_records {
                let record = result?;
                
                // Manually map columns by index based on your provided format
                let date = &record[0];
                let time = &record[1];
                let open: f64 = record[2].parse()?;
                let high: f64 = record[3].parse()?;
                let low: f64 = record[4].parse()?;
                let close: f64 = record[5].parse()?;
                let volume: f64 = record[6].parse()?; // Correctly read TICKVOL as volume

                let timestamp = format!("{}T{}", date, time);


                records.push(MarketData {
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
            }
        }
        
        Ok(records)
    }
}

pub fn write_csv<T: CsvRecord + serde::Serialize + std::fmt::Debug>(
    records: &[T],
    file_path: &str,
) -> Result<(), Box<dyn Error>> {
    let mut writer = WriterBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;

    writer.write_record(T::headers())?;

    for record in records.iter() {
        if let Err(e) = writer.serialize(record) {
            eprintln!("Error serializing record: {:?} -> {}", record, e);
        }
    }
    writer.flush()?;
    Ok(())
}

pub fn parse_ts_to_naive(ts: &str) -> Option<NaiveDateTime> {
    let s = ts.trim();

    if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(dt.and_hms_opt(0, 0, 0).unwrap());
    }
    if let Ok(dt) = NaiveDate::parse_from_str(s, "%Y.%m.%d") {
        return Some(dt.and_hms_opt(0, 0, 0).unwrap());
    }

    let fmts = [
        "%Y.%m.%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M",
    ];
    for f in &fmts {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, f) {
            return Some(dt);
        }
    }
    
    None
}
use std::collections::HashMap;
use crate::data_engine::{CsvRecord, MarketData, parse_ts_to_naive};
use std::error::Error;
use std::path::Path;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Weekday};
use crate::candle_type::{pattern_from_ohlc, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS};
use std::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)] 
pub struct PeriodAgg {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub members: String,
    pub pattern: String,
}

impl CsvRecord for PeriodAgg {
    fn headers() -> &'static [&'static str] {
        &["date", "open", "high", "low", "close", "volume", "members", "pattern"]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.date.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
            self.members.clone(),
            self.pattern.clone(),
        ]
    }
}

pub fn aggregate_periods(data: &[MarketData]) -> (Vec<PeriodAgg>, Vec<PeriodAgg>, Vec<PeriodAgg>, Vec<PeriodAgg>, Vec<PeriodAgg>) {
    let mut aggs: HashMap<String, PeriodAgg> = HashMap::new();

    for r in data {
        let date_part = r.timestamp.split(['T', ' ']).next().unwrap_or("").trim().replace('.', "-");

        aggs.entry(date_part.clone())
            .and_modify(|agg| {
                if r.high > agg.high { agg.high = r.high; }
                if r.low < agg.low { agg.low = r.low; }
                agg.close = r.close;
                agg.volume += r.volume;
            })
            .or_insert_with(|| PeriodAgg {
                date: date_part,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                volume: r.volume,
                members: String::new(),
                pattern: String::new(),
            });
    }
    
    let mut daily_aggs: Vec<PeriodAgg> = aggs.into_values().map(|mut agg| {
        agg.pattern = pattern_from_ohlc(
            agg.open, agg.high, agg.low, agg.close,
            DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS,
        );
        agg
    }).collect();
    daily_aggs.sort_by(|a, b| a.date.cmp(&b.date));

    (
        daily_aggs,
        Vec::new(), // weekly (placeholder)
        Vec::new(), // weekday (placeholder)
        Vec::new(), // monthly (placeholder)
        Vec::new(), // yearly (placeholder)
    )
}
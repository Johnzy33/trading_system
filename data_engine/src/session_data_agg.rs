use std::cmp::Ordering;
use std::collections::HashMap;
use crate::data_engine::{CsvRecord, MarketData};
use crate::session_type::{session_from_timestamp_enum, Session};
use serde::{Deserialize, Serialize};
use crate::candle_type::{pattern_from_ohlc, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS};
use std::error::Error;
use std::path::Path;
use csv::WriterBuilder;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionAgg {
    pub date: String,
    pub session: Session,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub high_ts: String, // New field to store the timestamp of the high
    pub low_ts: String, // New field to store the timestamp of the low
    pub pattern: String,
}

pub fn aggregate_sessions(data: &[MarketData]) -> Vec<SessionAgg> {
    let mut aggs: HashMap<(String, Session), SessionAgg> = HashMap::new();

    for r in data {
        let date_part = r.timestamp.split('T').next().unwrap_or("").to_string();
        let session = session_from_timestamp_enum(&r.timestamp);
        if session == Session::Unknown { continue; }
        let key = (date_part.clone(), session);

        aggs.entry(key)
            .and_modify(|agg| {
                if r.high > agg.high { 
                    agg.high = r.high;
                    agg.high_ts = r.timestamp.clone();
                }
                if r.low < agg.low { 
                    agg.low = r.low;
                    agg.low_ts = r.timestamp.clone();
                }
                agg.close = r.close;
                agg.volume += r.volume;
            })
            .or_insert_with(|| SessionAgg {
                date: date_part,
                session,
                open: r.open,
                high: r.high,
                low: r.low,
                close: r.close,
                volume: r.volume,
                high_ts: r.timestamp.clone(),
                low_ts: r.timestamp.clone(),
                pattern: String::new(),
            });
    }

    let mut out_aggs: Vec<SessionAgg> = aggs.into_iter().map(|(_k, mut v)| {
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );
        v
    }).collect();

    out_aggs.sort_by(|a, b| {
        match a.date.cmp(&b.date) {
            Ordering::Equal => a.session.as_str().cmp(b.session.as_str()),
            other => other,
        }
    });

    out_aggs
}

impl CsvRecord for SessionAgg {
    fn headers() -> &'static [&'static str] {
        &["date", "session", "open", "high", "low", "close", "volume", "pattern"]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.date.clone(), self.session.as_str().to_string(),
            format!("{:.6}", self.open), format!("{:.6}", self.high),
            format!("{:.6}", self.low), format!("{:.6}", self.close),
            format!("{:.6}", self.volume), self.pattern.clone(),
        ]
    }
}
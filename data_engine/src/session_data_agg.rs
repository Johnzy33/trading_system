use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use csv::WriterBuilder;
use crate::session_type::{session_from_timestamp_enum, Session};
use crate::candle_type::{pattern_from_ohlc, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS};
use crate::data_engine::CsvRecord;

#[derive(Debug, Clone)]
pub struct SessionAgg {
    pub date: String,
    pub session: Session,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub first_ts: String,
    pub last_ts: String,
    pub pattern: String, // computed from aggregated OHLC
}

/// Aggregate input MarketData into session-level OHLC per date.
pub fn aggregate_sessions(data: &[crate::data_engine::MarketData]) -> Vec<SessionAgg> {
    let mut refs: Vec<&crate::data_engine::MarketData> = data.iter().collect();
    refs.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    let mut aggs: HashMap<(String, Session), SessionAgg> = HashMap::new();

    for r in refs {
        let date_part = r.timestamp
            .split(|c| c == 'T' || c == ' ')
            .next()
            .unwrap_or("")
            .trim()
            .replace('.', "-");
        let session = session_from_timestamp_enum(&r.timestamp);
        if session == Session::Unknown { continue; }
        let key = (date_part.clone(), session);

        match aggs.get_mut(&key) {
            Some(a) => {
                if r.high > a.high { a.high = r.high; }
                if r.low < a.low { a.low = r.low; }
                if r.timestamp < a.first_ts {
                    a.first_ts = r.timestamp.clone();
                    a.open = r.open;
                }
                if r.timestamp > a.last_ts {
                    a.last_ts = r.timestamp.clone();
                    a.close = r.close;
                }
                a.volume += r.volume;
            }
            None => {
                aggs.insert(key, SessionAgg {
                    date: date_part.clone(),
                    session,
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    first_ts: r.timestamp.clone(),
                    last_ts: r.timestamp.clone(),
                    pattern: String::new(),
                });
            }
        }
    }

    let mut out_aggs: Vec<SessionAgg> = aggs.into_iter().map(|(_k, mut v)| {
        // compute pattern for each session using canonical helper
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
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

/// Write session CSV (includes computed pattern)
pub fn write_sessions_csv<P: AsRef<Path>>(aggs: &[SessionAgg], out_path: P) -> Result<(), Box<dyn Error>> {
    let mut w = WriterBuilder::new().from_path(out_path)?;
    w.write_record(&["date", "session", "open", "high", "low", "close", "volume", "pattern"])?;
    for s in aggs {
        w.write_record(&[
            s.date.as_str(),
            s.session.as_str(),
            &format!("{:.6}", s.open),
            &format!("{:.6}", s.high),
            &format!("{:.6}", s.low),
            &format!("{:.6}", s.close),
            &format!("{:.6}", s.volume),
            s.pattern.as_str(),
        ])?;
    }
    w.flush()?;
    Ok(())
}

// implement CsvRecord so write_csv(&sessions, ...) compiles
impl CsvRecord for SessionAgg {
    fn headers() -> &'static [&'static str] {
        &["date", "session", "open", "high", "low", "close", "volume", "pattern"]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.date.clone(),
            self.session.as_str().to_string(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
            self.pattern.clone(),
        ]
    }
}
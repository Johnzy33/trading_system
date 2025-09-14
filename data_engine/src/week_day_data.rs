use std::cmp::Ordering;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use chrono::{Datelike, NaiveDateTime, Weekday};
use csv::WriterBuilder;

// These would be in another module, but are included here for completeness.
// In a real project, you would reference them via `crate::...`
mod candle_type {
    pub const DEFAULT_DOJI_BODY_RATIO: f64 = 0.05;
    pub const DEFAULT_BODY_WICK_RATIO_LONG: f64 = 0.7;
    pub const DEFAULT_BODY_WICK_RATIO_SHORT: f64 = 0.3;
    pub const DEFAULT_UPPER_VS_LOWER_RATIO: f64 = 2.0;
    pub const DEFAULT_EPS: f64 = 1e-9;

    pub fn pattern_from_ohlc(
        _open: f64, _high: f64, _low: f64, _close: f64,
        _doji_body_ratio: f64, _body_wick_ratio_long: f64, _body_wick_ratio_short: f64,
        _upper_vs_lower_ratio: f64, _eps: f64
    ) -> String {
        // This is a placeholder for the actual pattern recognition logic.
        "Pattern".to_string()
    }
}

mod data_engine {
    #[derive(Debug, Clone)]
    pub struct MarketData {
        pub timestamp: String,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: f64,
    }
    
    pub trait CsvRecord {
        fn headers() -> &'static [&'static str];
        fn record(&self) -> Vec<String>;
    }
}

use crate::candle_type::{pattern_from_ohlc, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_DOJI_BODY_RATIO, DEFAULT_EPS, DEFAULT_UPPER_VS_LOWER_RATIO};
use crate::data_engine::{CsvRecord, MarketData};


#[derive(Debug, Clone)]
pub struct PeriodAgg {
    pub date: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub first_ts: String,
    pub last_ts: String,
    pub pattern: String,
    pub members: Vec<String>, // list of YYYY-MM-DD strings that contributed
}

fn parse_ts_to_naive(ts: &str) -> Option<chrono::NaiveDateTime> {
    let s = ts.trim().replace('.', "-");
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(&s) {
        return Some(dt.naive_utc());
    }
    let fmts = [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ];
    for f in &fmts {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(&s, f) {
            return Some(dt);
        }
    }
    None
}

/// Aggregate and return period-only aggregates (daily, weekly, weekday, monthly).
///
/// Weekly aggregation rules:
/// - A trading week is Mon→Fri (only weekday data counted).
/// - Each ISO-week (year + iso_week number) is assigned to the month of the FIRST observed trading-day
///   encountered in the dataset for that ISO-week. That owning month then gets the entire ISO-week
///   as a single month-relative week (YYYY-MM-Wn), where n is assigned in first-seen order within the month.
/// - Only records whose month matches the iso-week's owning month are aggregated into that monthly week bucket.
pub fn aggregate_period(data: &[crate::data_engine::MarketData]) -> (Vec<PeriodAgg>, Vec<PeriodAgg>, Vec<PeriodAgg>, Vec<PeriodAgg>) {
    let mut refs: Vec<&crate::data_engine::MarketData> = data.iter().collect();
    refs.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

    // Keys without Session for period-only aggregation
    let mut daily: HashMap<String, PeriodAgg> = HashMap::new();
    // weekly will use "YYYY-MM-Wn" where n is month-relative index assigned in first-seen order
    let mut weekly: HashMap<String, PeriodAgg> = HashMap::new();      // key = "YYYY-MM-Wn"
    let mut weekday_map: HashMap<String, PeriodAgg> = HashMap::new(); // key = "Mon".."Fri"
    let mut monthly: HashMap<String, PeriodAgg> = HashMap::new();     // key = "YYYY-MM"

    // Tracks assigned index for (year, month, iso_week) -> index (1-based)
    let mut monthly_iso_week_index: HashMap<(i32, u32, i32), usize> = HashMap::new();
    // map (year, iso_week) -> assigned_month (u32) to lock where the iso-week is attributed
    // The key uses the ISO-week's year (iso.week().year()) and iso.week() number to avoid cross-year confusion.
    let mut iso_week_assigned_month: HashMap<(i32, i32), u32> = HashMap::new();
    // NEW: Tracks the next available week index for a given (year, month) to avoid conflicting borrows.
    let mut monthly_week_counter: HashMap<(i32, u32), usize> = HashMap::new();

    // Helper to get iso-year and iso-week number tuple for a NaiveDateTime
    fn iso_year_week(ndt: &NaiveDateTime) -> (i32, i32) {
        let iso = ndt.iso_week();
        (iso.year(), iso.week() as i32)
    }

    for r in refs {
        let ndt_opt = parse_ts_to_naive(&r.timestamp);
        if ndt_opt.is_none() { continue; }
        let ndt = ndt_opt.unwrap();

        // DAILY
        let date_str = ndt.date().format("%Y-%m-%d").to_string();
        match daily.get_mut(&date_str) {
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
                daily.insert(date_str.clone(), PeriodAgg {
                    date: date_str.clone(),
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    first_ts: r.timestamp.clone(),
                    last_ts: r.timestamp.clone(),
                    pattern: String::new(),
                    members: Vec::new(),
                });
            }
        }

               // WEEKLY (month-relative index), attribute ISO-week to month of first-seen trading day
        // Use ISO-week YEAR and WEEK number as the key to avoid misassignment across Dec/Jan boundaries.
        let (iso_year, iso_weeknum) = iso_year_week(&ndt);
        let iso_key = (iso_year, iso_weeknum);
        let record_month = ndt.month(); // u32
        
        // Determine owning month for this ISO-week: first-seen trading-day month wins.
        // We lock the owning month when we first encounter any trading-day for this IsoWeek (chronological order).
        let owning_month = *iso_week_assigned_month.entry(iso_key).or_insert(record_month);

        // THE FIX: The enclosing 'if' statement has been removed to allow all members of an
        // ISO week to be aggregated, even if they cross a month boundary.
        
        // idx_key: (iso_year, owning_month, iso_weeknum) — iso_year used to avoid cross-year mixups
        let idx_key = (iso_year, owning_month, iso_weeknum);

        let week_index = *monthly_iso_week_index.entry(idx_key).or_insert_with(|| {
            let counter_key = (iso_year, owning_month);
            let counter = monthly_week_counter.entry(counter_key).or_insert(0);
            *counter += 1;
            *counter
        });

        let week_key = format!("{:04}-{:02}-W{}", iso_year, owning_month, week_index);
        let day_str = ndt.date().format("%Y-%m-%d").to_string();

        match weekly.get_mut(&week_key) {
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
                if !a.members.contains(&day_str) {
                    a.members.push(day_str);
                }
            }
            None => {
                weekly.insert(week_key.clone(), PeriodAgg {
                    date: week_key.clone(),
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    first_ts: r.timestamp.clone(),
                    last_ts: r.timestamp.clone(),
                    pattern: String::new(),
                    members: vec![day_str],
                });
            }
        }

        // WEEKDAY (Mon..Fri)
        let wd = ndt.weekday();
        match wd {
            Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri => {
                let wd_str = match wd {
                    Weekday::Mon => "Mon",
                    Weekday::Tue => "Tue",
                    Weekday::Wed => "Wed",
                    Weekday::Thu => "Thu",
                    Weekday::Fri => "Fri",
                    _ => unreachable!(),
                }.to_string();
                match weekday_map.get_mut(&wd_str) {
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
                        let key = wd_str.clone();
                        weekday_map.insert(key.clone(), PeriodAgg {
                            date: key,
                            open: r.open,
                            high: r.high,
                            low: r.low,
                            close: r.close,
                            volume: r.volume,
                            first_ts: r.timestamp.clone(),
                            last_ts: r.timestamp.clone(),
                            pattern: String::new(),
                            members: Vec::new(),
                        });
                    }
                }
            }
            _ => {}
        }

        // MONTHLY
        let month_key = format!("{}-{:02}", ndt.year(), ndt.month());
        match monthly.get_mut(&month_key) {
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
                monthly.insert(month_key.clone(), PeriodAgg {
                    date: month_key.clone(),
                    open: r.open,
                    high: r.high,
                    low: r.low,
                    close: r.close,
                    volume: r.volume,
                    first_ts: r.timestamp.clone(),
                    last_ts: r.timestamp.clone(),
                    pattern: String::new(),
                    members: Vec::new(),
                });
            }
        }
    }

    // compute patterns and collect vectors
    let mut daily_vec: Vec<PeriodAgg> = daily.into_iter().map(|(_k, mut v)| {
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );
        v.members.sort();
        v
    }).collect();

    let mut weekly_vec: Vec<PeriodAgg> = weekly.into_iter().map(|(_k, mut v)| {
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );
        v.members.sort();
        v
    }).collect();

    let mut weekday_vec: Vec<PeriodAgg> = weekday_map.into_iter().map(|(_k, mut v)| {
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );
        v.members.sort();
        v
    }).collect();

    let mut monthly_vec: Vec<PeriodAgg> = monthly.into_iter().map(|(_k, mut v)| {
        v.pattern = pattern_from_ohlc(
            v.open, v.high, v.low, v.close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );
        v.members.sort();
        v
    }).collect();

    // sort
    daily_vec.sort_by(|a, b| a.date.cmp(&b.date));
    weekly_vec.sort_by(|a, b| a.date.cmp(&b.date));
    weekday_vec.sort_by(|a, b| {
        fn wd_ord(s: &str) -> u8 {
            match s {
                "Mon" => 1,
                "Tue" => 2,
                "Wed" => 3,
                "Thu" => 4,
                "Fri" => 5,
                _ => 99,
            }
        }
        match wd_ord(&a.date).cmp(&wd_ord(&b.date)) {
            Ordering::Equal => a.date.cmp(&b.date),
            other => other,
        }
    });
    monthly_vec.sort_by(|a, b| a.date.cmp(&b.date));

    (daily_vec, weekly_vec, weekday_vec, monthly_vec)
}

pub fn write_period_csv<P: AsRef<Path>>(aggs: &[PeriodAgg], out_path: P) -> Result<(), Box<dyn Error>> {
    let mut w = WriterBuilder::new().from_path(out_path)?;
    w.write_record(&["date", "open", "high", "low", "close", "volume", "pattern", "members"])?;
    for s in aggs {
        w.write_record(&[
            s.date.as_str(),
            &format!("{:.6}", s.open),
            &format!("{:.6}", s.high),
            &format!("{:.6}", s.low),
            &format!("{:.6}", s.close),
            &format!("{:.6}", s.volume),
            s.pattern.as_str(),
            &s.members.join(","),
        ])?;
    }
    w.flush()?;
    Ok(())
}

impl CsvRecord for PeriodAgg {
    fn headers() -> &'static [&'static str] {
        &["date", "open", "high", "low", "close", "volume", "pattern", "members"]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.date.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
            self.pattern.clone(),
            self.members.join(","),
        ]
    }
}
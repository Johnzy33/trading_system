use std::cmp::Ordering;
use std::collections::HashMap;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Weekday};
use std::error::Error;
use std::path::Path;
use serde::{Deserialize, Serialize};

use crate::data_engine::{CsvRecord, MarketData, parse_ts_to_naive};
use crate::candle_type::{pattern_from_ohlc, CandlePattern, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS};
use crate::week_day_data::PeriodAgg;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyTableAgg {
    pub year: String,
    pub month: String,
    pub week: String,
    pub monday_pattern: String,
    pub tuesday_pattern: String,
    pub wednesday_pattern: String,
    pub thursday_pattern: String,
    pub friday_pattern: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub high_day: String,
    pub low_day: String,
    pub week_pattern: String,
}

impl CsvRecord for WeeklyTableAgg {
    fn headers() -> &'static [&'static str] {
        &[
            "Year", "Month", "Week", "Monday", "Tuesday", "Wednesday", "Thursday",
            "Friday", "Open", "High", "Low", "Close", "Volume", "HighDay", "LowDay", "WeekPattern",
        ]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.year.clone(),
            self.month.clone(),
            self.week.clone(),
            self.monday_pattern.clone(),
            self.tuesday_pattern.clone(),
            self.wednesday_pattern.clone(),
            self.thursday_pattern.clone(),
            self.friday_pattern.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
            self.high_day.clone(),
            self.low_day.clone(),
            self.week_pattern.clone(),
        ]
    }
}

pub fn aggregate_weekly_table(daily_aggs: &[PeriodAgg]) -> Vec<WeeklyTableAgg> {
    let mut weekly_map: HashMap<String, Vec<&PeriodAgg>> = HashMap::new();
    
    for d_agg in daily_aggs {
        let ndt = match parse_ts_to_naive(&d_agg.date) {
            Some(dt) => dt,
            None => continue,
        };
        let week_key = format!("{}{}", ndt.iso_week().year(), ndt.iso_week().week());
        weekly_map.entry(week_key)
            .or_insert_with(Vec::new)
            .push(d_agg);
    }

    let mut result: Vec<WeeklyTableAgg> = Vec::new();

    for (_key, daily_days) in weekly_map {
        if daily_days.is_empty() { continue; }

        let mut daily_days_sorted = daily_days;
        daily_days_sorted.sort_by(|a, b| a.date.cmp(&b.date));

        let open = daily_days_sorted.first().unwrap().open;
        let close = daily_days_sorted.last().unwrap().close;
        let mut high = f64::MIN;
        let mut low = f64::MAX;
        let mut volume = 0.0;
        let mut high_day = Weekday::Mon;
        let mut low_day = Weekday::Mon;

        let mut daily_patterns = HashMap::new();

        for day in &daily_days_sorted {
            let ndt = parse_ts_to_naive(&day.date).unwrap();

            if day.high > high {
                high = day.high;
                high_day = ndt.weekday();
            }
            if day.low < low {
                low = day.low;
                low_day = ndt.weekday();
            }
            
            volume += day.volume;
            daily_patterns.insert(ndt.weekday(), day.pattern.clone());
        }
        
        let week_pattern = pattern_from_ohlc(
            open, high, low, close,
            DEFAULT_DOJI_BODY_RATIO,
            DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT,
            DEFAULT_UPPER_VS_LOWER_RATIO,
            DEFAULT_EPS,
        );

        let first_day = daily_days_sorted.first().unwrap();
        let first_day_ndt = parse_ts_to_naive(&first_day.date).unwrap();

        let weekly_agg = WeeklyTableAgg {
            year: first_day_ndt.year().to_string(),
            month: format!("{:02}", first_day_ndt.month()),
            week: format!("Week {}", first_day_ndt.iso_week().week()),
            monday_pattern: daily_patterns.get(&Weekday::Mon).cloned().unwrap_or_default(),
            tuesday_pattern: daily_patterns.get(&Weekday::Tue).cloned().unwrap_or_default(),
            wednesday_pattern: daily_patterns.get(&Weekday::Wed).cloned().unwrap_or_default(),
            thursday_pattern: daily_patterns.get(&Weekday::Thu).cloned().unwrap_or_default(),
            friday_pattern: daily_patterns.get(&Weekday::Fri).cloned().unwrap_or_default(),
            open,
            high,
            low,
            close,
            volume,
            high_day: high_day.to_string(),
            low_day: low_day.to_string(),
            week_pattern,
        };
        result.push(weekly_agg);
    }
    
    result.sort_by(|a, b| a.year.cmp(&b.year).then_with(|| a.week.cmp(&b.week)));

    result
}
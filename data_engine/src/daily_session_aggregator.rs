use std::cmp::Ordering;
use std::collections::HashMap;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Weekday};
use std::error::Error;
use std::path::Path;
use serde::{Deserialize, Serialize};

use crate::data_engine::{CsvRecord, MarketData, parse_ts_to_naive};
use crate::candle_type::{pattern_from_ohlc, CandlePattern, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS};
use crate::session_data_agg::{SessionAgg};
use crate::session_type::Session;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailySessionTableAgg {
    pub date: String,
    pub week: String,
    pub day: String,
    pub day_candle_pattern: String,
    pub as_candle_pattern: String,
    pub ln_candle_pattern: String,
    pub nyam_candle_pattern: String,
    pub nyl_candle_pattern: String,
    pub nypm_candle_pattern: String,
    pub day_high_session: String,
    pub day_low_session: String,
    pub as_low: f64,
    pub as_high: f64,
    pub ln_low: f64,
    pub ln_high: f64,
    pub nyam_low: f64,
    pub nyam_high: f64,
    pub nyl_low: f64,
    pub nyl_high: f64,
    pub nypm_low: f64,
    pub nypm_high: f64,
}

impl CsvRecord for DailySessionTableAgg {
    fn headers() -> &'static [&'static str] {
        &[
            "Date", "Week", "Day", "DayCandlePattern", "AS_CandlePattern", "LN_CandlePattern",
            "NYAM_CandlePattern", "NYL_CandlePattern", "NYPM_CandlePattern", 
            "DayHighSession", "DayLowSession",
            "AS_Low", "AS_High", "LN_Low", "LN_High", "NYAM_Low", "NYAM_High",
            "NYL_Low", "NYL_High", "NYPM_Low", "NYPM_High",
        ]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.date.clone(),
            self.week.clone(),
            self.day.clone(),
            self.day_candle_pattern.clone(),
            self.as_candle_pattern.clone(),
            self.ln_candle_pattern.clone(),
            self.nyam_candle_pattern.clone(),
            self.nyl_candle_pattern.clone(),
            self.nypm_candle_pattern.clone(),
            self.day_high_session.clone(),
            self.day_low_session.clone(),
            format!("{:.6}", self.as_low),
            format!("{:.6}", self.as_high),
            format!("{:.6}", self.ln_low),
            format!("{:.6}", self.ln_high),
            format!("{:.6}", self.nyam_low),
            format!("{:.6}", self.nyam_high),
            format!("{:.6}", self.nyl_low),
            format!("{:.6}", self.nyl_high),
            format!("{:.6}", self.nypm_low),
            format!("{:.6}", self.nypm_high),
        ]
    }
}

pub fn aggregate_daily_session_table(session_aggs: &[SessionAgg]) -> Vec<DailySessionTableAgg> {
    let mut daily_map: HashMap<String, Vec<&SessionAgg>> = HashMap::new();

    for s_agg in session_aggs {
        let ndt = match parse_ts_to_naive(&s_agg.date) {
            Some(dt) => dt,
            None => continue,
        };
        let date_key = ndt.format("%Y-%m-%d").to_string();
        daily_map.entry(date_key)
            .or_insert_with(Vec::new)
            .push(s_agg);
    }

    let mut result: Vec<DailySessionTableAgg> = Vec::new();

    for (_key, sessions) in daily_map {
        if sessions.is_empty() { continue; }

        let mut sorted_sessions = sessions;
        sorted_sessions.sort_by(|a, b| a.start_ts.cmp(&b.start_ts));

        let mut day_high = f64::MIN;
        let mut day_low = f64::MAX;
        let mut day_high_session = String::new();
        let mut day_low_session = String::new();

        let mut session_data: HashMap<String, (f64, f64, String)> = HashMap::new();

        for session in &sorted_sessions {
            if session.high > day_high {
                day_high = session.high;
                day_high_session = session.session.as_str().to_string();
            }
            if session.low < day_low {
                day_low = session.low;
                day_low_session = session.session.as_str().to_string();
            }

            session_data.insert(
                session.session.as_str().to_string(),
                (
                    session.low,
                    session.high,
                    pattern_from_ohlc(
                        session.open, session.high, session.low, session.close,
                        DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
                        DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS,
                    ),
                ),
            );
        }

        let day_open = sorted_sessions.first().unwrap().open;
        let day_close = sorted_sessions.last().unwrap().close;
        let day_candle_pattern = pattern_from_ohlc(
            day_open, day_high, day_low, day_close,
            DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS,
        );

        let first_session_ndt = parse_ts_to_naive(&sorted_sessions.first().unwrap().date).unwrap();

        let day_agg = DailySessionTableAgg {
            date: first_session_ndt.format("%Y-%m-%d").to_string(),
            week: format!("Week {}", first_session_ndt.iso_week().week()),
            day: first_session_ndt.weekday().to_string(),
            day_candle_pattern,
            as_candle_pattern: session_data.get(Session::AS.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            ln_candle_pattern: session_data.get(Session::LN.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nyam_candle_pattern: session_data.get(Session::NYAM.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nyl_candle_pattern: session_data.get(Session::NYL.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nypm_candle_pattern: session_data.get(Session::NYPM.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            day_high_session,
            day_low_session,
            as_low: session_data.get(Session::AS.as_str()).map(|t| t.0).unwrap_or_default(),
            as_high: session_data.get(Session::AS.as_str()).map(|t| t.1).unwrap_or_default(),
            ln_low: session_data.get(Session::LN.as_str()).map(|t| t.0).unwrap_or_default(),
            ln_high: session_data.get(Session::LN.as_str()).map(|t| t.1).unwrap_or_default(),
            nyam_low: session_data.get(Session::NYAM.as_str()).map(|t| t.0).unwrap_or_default(),
            nyam_high: session_data.get(Session::NYAM.as_str()).map(|t| t.1).unwrap_or_default(),
            nyl_low: session_data.get(Session::NYL.as_str()).map(|t| t.0).unwrap_or_default(),
            nyl_high: session_data.get(Session::NYL.as_str()).map(|t| t.1).unwrap_or_default(),
            nypm_low: session_data.get(Session::NYPM.as_str()).map(|t| t.0).unwrap_or_default(),
            nypm_high: session_data.get(Session::NYPM.as_str()).map(|t| t.1).unwrap_or_default(),
        };
        result.push(day_agg);
    }

    result.sort_by(|a, b| a.date.cmp(&b.date));
    result
}
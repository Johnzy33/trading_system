use std::cmp::Ordering;
use std::collections::HashMap;
use chrono::{Datelike, NaiveDate, NaiveDateTime, Weekday, Timelike};
use std::error::Error;
use std::path::Path;
use serde::{Deserialize, Serialize};

use crate::data_engine::{CsvRecord, parse_ts_to_naive};
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
    pub as_low_time: String,
    pub as_high_time: String,
    pub ln_low_time: String,
    pub ln_high_time: String,
    pub ny_low_time: String, // Combined NY low time
    pub ny_high_time: String, // Combined NY high time
}

impl CsvRecord for DailySessionTableAgg {
    fn headers() -> &'static [&'static str] {
        &[
            "Date", "Week", "Day", "DayCandlePattern", "AS_CandlePattern", "LN_CandlePattern",
            "NYAM_CandlePattern", "NYL_CandlePattern", "NYPM_CandlePattern", 
            "DayHighSession", "DayLowSession",
            "AS_LowTime", "AS_HighTime", "LN_LowTime", "LN_HighTime", 
            "NY_LowTime", "NY_HighTime",
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
            self.as_low_time.clone(),
            self.as_high_time.clone(),
            self.ln_low_time.clone(),
            self.ln_high_time.clone(),
            self.ny_low_time.clone(),
            self.ny_high_time.clone(),
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

    for (_key, sessions) in daily_map.into_iter() {
        if sessions.is_empty() { continue; }

        let mut sorted_sessions = sessions;
        sorted_sessions.sort_by(|a, b| a.date.cmp(&b.date).then_with(|| a.session.as_str().cmp(b.session.as_str())));

        let mut day_high = f64::MIN;
        let mut day_low = f64::MAX;
        let mut day_high_session = String::new();
        let mut day_low_session = String::new();

        let mut ny_high = f64::MIN;
        let mut ny_low = f64::MAX;
        let mut ny_high_time = String::new();
        let mut ny_low_time = String::new();

        let mut session_data: HashMap<String, (String, String, String)> = HashMap::new();

        for session in &sorted_sessions {
            // 1. Calculate overall day high/low
            if session.high > day_high {
                day_high = session.high;
                day_high_session = session.session.as_str().to_string();
            }
            if session.low < day_low {
                day_low = session.low;
                day_low_session = session.session.as_str().to_string();
            }

            // 2. Calculate combined NY high/low and their times
            if session.session == Session::NYAM || session.session == Session::NYL || session.session == Session::NYPM {
                if session.high > ny_high {
                    ny_high = session.high;
                    // Store the formatted hour
                    ny_high_time = parse_ts_to_naive(&session.high_ts).map(|dt| dt.hour().to_string()).unwrap_or_default();
                }
                if session.low < ny_low {
                    ny_low = session.low;
                    // Store the formatted hour
                    ny_low_time = parse_ts_to_naive(&session.low_ts).map(|dt| dt.hour().to_string()).unwrap_or_default();
                }
            }

            // 3. Store individual session data for the final output
            session_data.insert(
                session.session.as_str().to_string(),
                (
                    // Format all times to only show the hour
                    parse_ts_to_naive(&session.low_ts).map(|dt| dt.hour().to_string()).unwrap_or_default(),
                    parse_ts_to_naive(&session.high_ts).map(|dt| dt.hour().to_string()).unwrap_or_default(),
                    pattern_from_ohlc(
                        session.open, session.high, session.low, session.close,
                        DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
                        DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS,
                    ),
                ),
            );
        }

        let first_session = match sorted_sessions.first() {
            Some(s) => s,
            None => continue,
        };

        let day_open = first_session.open;
        let day_close = sorted_sessions.last().unwrap().close;

        let day_candle_pattern = pattern_from_ohlc(
            day_open, day_high, day_low, day_close,
            DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG,
            DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS,
        );

        let day_agg = DailySessionTableAgg {
            date: first_session.date.clone(),
            week: format!("Week {}", parse_ts_to_naive(&first_session.date).unwrap().iso_week().week()),
            day: parse_ts_to_naive(&first_session.date).unwrap().weekday().to_string(),
            day_candle_pattern,
            as_candle_pattern: session_data.get(Session::AS.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            ln_candle_pattern: session_data.get(Session::LN.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nyam_candle_pattern: session_data.get(Session::NYAM.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nyl_candle_pattern: session_data.get(Session::NYL.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            nypm_candle_pattern: session_data.get(Session::NYPM.as_str()).map(|t| t.2.clone()).unwrap_or_default(),
            day_high_session,
            day_low_session,
            as_low_time: session_data.get(Session::AS.as_str()).map(|t| t.0.clone()).unwrap_or_default(),
            as_high_time: session_data.get(Session::AS.as_str()).map(|t| t.1.clone()).unwrap_or_default(),
            ln_low_time: session_data.get(Session::LN.as_str()).map(|t| t.0.clone()).unwrap_or_default(),
            ln_high_time: session_data.get(Session::LN.as_str()).map(|t| t.1.clone()).unwrap_or_default(),
            ny_low_time,
            ny_high_time,
        };
        result.push(day_agg);
    }

    result.sort_by(|a, b| a.date.cmp(&b.date));
    result
}
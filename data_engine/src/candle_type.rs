use crate::data_engine::MarketData;
use crate::session_type::session_from_timestamp;
use crate::data_engine::CsvRecord;

#[derive(Debug, Clone)]
pub struct CandlePattern {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,

    pub body: f64,
    pub upper_wick: f64,
    pub lower_wick: f64,
    pub body_ratio: f64,
    pub body_vs_wicks: f64,
    pub pattern: String,
    pub session: String,
}

// shared defaults
pub const DEFAULT_DOJI_BODY_RATIO: f64 = 0.1;
pub const DEFAULT_BODY_WICK_RATIO_LONG: f64 = 1.5;
pub const DEFAULT_BODY_WICK_RATIO_SHORT: f64 = 0.5;
pub const DEFAULT_UPPER_VS_LOWER_RATIO: f64 = 2.0;
pub const DEFAULT_EPS: f64 = 1e-9;

pub fn pattern_from_ohlc(
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    doji_body_ratio: f64,
    body_wick_ratio_long: f64,
    body_wick_ratio_short: f64,
    upper_vs_lower_ratio: f64,
    eps: f64,
) -> String {
    let body = (c - o).abs();
    let max_co = if c > o { c } else { o };
    let min_co = if c < o { c } else { o };

    let mut upper = h - max_co;
    if upper < 0.0 { upper = 0.0; }
    let mut lower = min_co - l;
    if lower < 0.0 { lower = 0.0; }

    let rng = (h - l).max(0.0);
    let body_ratio = body / (rng + eps);
    let sum_wicks = upper + lower;
    let body_vs_wicks = body / (sum_wicks + eps);

    let close_up = c > o;
    let close_down = c < o;
    let is_doji = body_ratio <= doji_body_ratio;
    let is_dominant_body = body_vs_wicks >= body_wick_ratio_long;
    let is_wick_dominated = body_vs_wicks <= body_wick_ratio_short;

    let long_upper = (upper >= upper_vs_lower_ratio * lower) && (upper > 0.0);
    let long_lower = (lower >= upper_vs_lower_ratio * upper) && (lower > 0.0);
    let both_wicks = (upper > 0.0) && (lower > 0.0) && (!long_upper) && (!long_lower);

    if is_doji {
        "Doji/SpinningTop".to_string()
    } else if is_dominant_body {
        if close_up { "Bullish Long Body".to_string() }
        else if close_down { "Bearish Long Body".to_string() }
        else { "Neutral Long Body".to_string() }
    } else if is_wick_dominated {
        if long_upper { "Long Upper Wick".to_string() }
        else if long_lower { "Long Lower Wick".to_string() }
        else if both_wicks { "Long Both Wicks / SpinningTop".to_string() }
        else { "Long Wicks (unspecified)".to_string() }
    } else {
        if close_up { "Mild Bullish".to_string() }
        else if close_down { "Mild Bearish".to_string() }
        else { "Neutral".to_string() }
    }
}

pub fn classify_candles(
    input: &[MarketData],
    doji_body_ratio: f64,
    body_wick_ratio_long: f64,
    body_wick_ratio_short: f64,
    upper_vs_lower_ratio: f64,
    eps: f64,
) -> Vec<CandlePattern> {
    input.iter().map(|r| {
        let o = r.open; let h = r.high; let l = r.low; let c = r.close;
        let body = (c - o).abs();
        let max_co = if c > o { c } else { o };
        let min_co = if c < o { c } else { o };
        let mut upper = h - max_co; if upper < 0.0 { upper = 0.0; }
        let mut lower = min_co - l; if lower < 0.0 { lower = 0.0; }
        let rng = (h - l).max(0.0);
        let body_ratio = body / (rng + eps);
        let sum_wicks = upper + lower;
        let body_vs_wicks = body / (sum_wicks + eps);

        let pattern = pattern_from_ohlc(
            o, h, l, c,
            doji_body_ratio,
            body_wick_ratio_long,
            body_wick_ratio_short,
            upper_vs_lower_ratio,
            eps,
        );
        let session = session_from_timestamp(&r.timestamp);
        CandlePattern {
            timestamp: r.timestamp.clone(),
            open: o, high: h, low: l, close: c, volume: r.volume,
            body, upper_wick: upper, lower_wick: lower,
            body_ratio, body_vs_wicks, pattern, session,
        }
    }).collect()
}

impl CsvRecord for CandlePattern {
    fn headers() -> &'static [&'static str] {
        &[
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "body",
            "upper_wick",
            "lower_wick",
            "body_ratio",
            "body_vs_wicks",
            "pattern",
            "session",
        ]
    }

    fn record(&self) -> Vec<String> {
        vec![
            self.timestamp.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
            format!("{:.6}", self.body),
            format!("{:.6}", self.upper_wick),
            format!("{:.6}", self.lower_wick),
            format!("{:.9}", self.body_ratio),
            format!("{:.9}", self.body_vs_wicks),
            self.pattern.clone(),
            self.session.clone(),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_engine::MarketData;

    #[test]
    fn basic_classify() {
        let rows = vec![
            MarketData { timestamp: "2023-03-27T02:00:00".into(), open: 10.0, high: 12.0, low: 9.5, close: 11.5, volume: 1.0 },
            MarketData { timestamp: "2023-03-27T09:30:00".into(), open: 5.0, high: 5.2, low: 4.8, close: 5.01, volume: 2.0 },
            MarketData { timestamp: "2023-03-27T19:15:00".into(), open: 2.0, high: 3.5, low: 1.9, close: 2.01, volume: 0.5 },
        ];

        let out = classify_candles(&rows, 0.1, 1.5, 0.5, 2.0, 1e-9);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].session, "AS");
        assert_eq!(out[1].session, "LN");
        assert_eq!(out[2].session, "NYL");
    }
}

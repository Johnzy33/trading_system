pub mod data_engine;
pub mod candle_type;
pub mod session_type;
pub mod session_data_agg;
pub mod week_day_data;
pub mod weekly_table_aggregator;
pub mod daily_session_aggregator;

// re-exports for simple upstream use
// pub use data_engine::{DataEngine, write_csv, MarketData};
// pub use candle_type::{classify_candles, pattern_from_ohlc, DEFAULT_DOJI_BODY_RATIO, DEFAULT_BODY_WICK_RATIO_LONG, DEFAULT_BODY_WICK_RATIO_SHORT, DEFAULT_UPPER_VS_LOWER_RATIO, DEFAULT_EPS, CandlePattern};
// pub use session_data_agg::{aggregate_sessions, SessionAgg, write_sessions_csv};
// pub use week_day_data::{aggregate_period, PeriodAgg, write_period_csv};

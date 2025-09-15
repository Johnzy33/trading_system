use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash,Serialize, Deserialize)]
pub enum Session {
    AS,
    LN,
    NYAM,
    NYL,
    NYPM,
    Unknown,
}

impl Session {
    pub fn as_str(&self) -> &'static str {
        match self {
            Session::AS => "AS",
            Session::LN => "LN",
            Session::NYAM => "NYAM",
            Session::NYL => "NYL",
            Session::NYPM => "NYPM",
            Session::Unknown => "Unknown",
        }
    }

    pub fn from_hour(hour: u32) -> Self {
        match hour {
            1..=7 => Session::AS,
            8..=14 => Session::LN,
            15..=18 => Session::NYAM,
            19..=20 => Session::NYL,
            21..=23 => Session::NYPM,
            _ => Session::Unknown,
        }
    }

    pub fn from_timestamp(ts: &str) -> Self {
        let time_part_opt = ts.split(['T', ' ']).nth(1);
        if let Some(tp) = time_part_opt {
            let hour_str = tp.split(':').next().unwrap_or("");
            if let Ok(hour) = hour_str.parse::<u32>() {
                return Session::from_hour(hour);
            }
        }
        Session::Unknown
    }
}

impl fmt::Display for Session {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub fn session_from_timestamp(ts: &str) -> String {
    Session::from_timestamp(ts).as_str().to_string()
}

pub fn session_from_timestamp_enum(ts: &str) -> Session {
    Session::from_timestamp(ts)
}
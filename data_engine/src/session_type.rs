use std::fmt;

/// Market session code.
///
/// Variants correspond to:
/// - AS   : Asian Session (01:00 - 07:59)
/// - LN   : London Session (08:00 - 14:59)
/// - NYAM : New York AM (15:00 - 18:59)
/// - NYL  : New York Lunch (19:00 - 20:59)
/// - NYPM : New York PM (21:00 - 23:59)
/// - Unknown : fallback for unparsable timestamps / hours outside ranges
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[derive(Hash)]
pub enum Session {
    AS,
    LN,
    NYAM,
    NYL,
    NYPM,
    Unknown,
}

impl Session {
    /// Return the short code string for the session.
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

    /// Determine a Session from an hour (0-23).
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

    /// Parse an ISO-like timestamp string and return the corresponding Session.
    ///
    /// Accepts timestamps with a 'T' separator or a space, e.g.:
    /// - "2023-03-27T11:00:00"
    /// - "2023.03.27 11:00:00"
    pub fn from_timestamp(ts: &str) -> Self {
        let time_part_opt = ts.split(|c| c == 'T' || c == ' ').nth(1);
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

/// Compatibility helper: existing code expects a String return.
/// This keeps the previous public API while providing the Session enum for new code.
pub fn session_from_timestamp(ts: &str) -> String {
    Session::from_timestamp(ts).as_str().to_string()
}

/// Also export the enum-based API for callers that prefer the typed Session.
pub fn session_from_timestamp_enum(ts: &str) -> Session {
    Session::from_timestamp(ts)
}
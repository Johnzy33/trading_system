use std::error::Error;
use std::fs::File;
use std::path::Path;
use csv::ReaderBuilder;
use csv::Trim;
use csv::WriterBuilder;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json;

#[derive(Debug, Deserialize, Clone)]
pub struct MarketData {
    pub timestamp: String,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

#[derive(Debug, Deserialize)]
struct CsvRow {
    #[serde(rename = "<DATE>")]
    date: String,
    #[serde(rename = "<TIME>")]
    time: String,
    #[serde(rename = "<OPEN>")]
    open: f64,
    #[serde(rename = "<HIGH>")]
    high: f64,
    #[serde(rename = "<LOW>")]
    low: f64,
    #[serde(rename = "<CLOSE>")]
    close: f64,
    #[serde(rename = "<TICKVOL>")]
    tickvol: Option<f64>,
}

pub struct DataEngine {
    client: Client,
}

impl DataEngine {
    pub fn new() -> Self {
        DataEngine {
            client: Client::new(),
        }
    }

    /// Read tab-separated file with headers like `<DATE>`, `<TIME>`, `<OPEN>`, `<TICKVOL>`, etc.
    pub fn fetch_from_csv<P: AsRef<Path>>(&self, file_path: P) -> Result<Vec<MarketData>, Box<dyn Error>> {
        let file = File::open(file_path)?;
        let mut rdr = ReaderBuilder::new()
            .delimiter(b'\t')
            .has_headers(true)
            .trim(Trim::All)
            .flexible(true)
            .from_reader(file);

        let mut data = Vec::new();
        for result in rdr.deserialize() {
            let row: CsvRow = result?;
            // combine DATE and TIME into an ISO-like timestamp (adjust format if needed)
            let timestamp = format!("{}T{}", row.date.trim(), row.time.trim());
            data.push(MarketData {
                timestamp,
                open: row.open,
                high: row.high,
                low: row.low,
                close: row.close,
                volume: row.tickvol.unwrap_or(0.0),
            });
        }
        Ok(data)
    }

    /// Fetches market data from Binance Klines API and adapts it to MarketData.
    pub fn fetch_from_binance_klines(&self, url: &str) -> Result<Vec<MarketData>, Box<dyn Error>> {
        let response = self.client.get(url).send()?;
        let raw: Vec<Vec<serde_json::Value>> = response.json()?;
        let mut data = Vec::new();

        for entry in raw {
            let timestamp = entry[0].as_i64().unwrap_or(0).to_string();
            let open = entry[1].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let high = entry[2].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let low = entry[3].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let close = entry[4].as_str().unwrap_or("0").parse().unwrap_or(0.0);
            let volume = entry[5].as_str().unwrap_or("0").parse().unwrap_or(0.0);

            data.push(MarketData {
                timestamp,
                open,
                high,
                low,
                close,
                volume,
            });
        }
        Ok(data)
    }
}

// Generic CSV writer
pub trait CsvRecord {
    fn headers() -> &'static [&'static str];
    fn record(&self) -> Vec<String>;
}

impl CsvRecord for MarketData {
    fn headers() -> &'static [&'static str] {
        &["timestamp", "open", "high", "low", "close", "volume"]
    }
    fn record(&self) -> Vec<String> {
        vec![
            self.timestamp.clone(),
            format!("{:.6}", self.open),
            format!("{:.6}", self.high),
            format!("{:.6}", self.low),
            format!("{:.6}", self.close),
            format!("{:.6}", self.volume),
        ]
    }
}

// implement CsvRecord for CandlePattern and SessionAgg in their respective modules.
// Generic writer used by callers across crate
pub fn write_csv<T: CsvRecord, P: AsRef<Path>>(items: &[T], out_path: P) -> Result<(), Box<dyn Error>> {
    let mut w = WriterBuilder::new().from_path(out_path)?;
    w.write_record(T::headers())?;
    for it in items {
        w.write_record(&it.record())?;
    }
    w.flush()?;
    Ok(())
}


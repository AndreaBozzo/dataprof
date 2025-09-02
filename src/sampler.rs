use polars::prelude::*;
use std::path::Path;
use anyhow::Result;

const MIN_SAMPLE_SIZE: usize = 10_000;
const MAX_SAMPLE_SIZE: usize = 1_000_000;
#[allow(dead_code)]
const CONFIDENCE_LEVEL: f64 = 0.95;
#[allow(dead_code)]
const MARGIN_OF_ERROR: f64 = 0.01;

pub struct Sampler {
    pub target_rows: usize,
}

impl Sampler {
    pub fn new(file_size_mb: f64) -> Self {
        // Calcola sample size basato su file size
        let target_rows = if file_size_mb < 100.0 {
            MIN_SAMPLE_SIZE
        } else if file_size_mb > 10_000.0 {
            MAX_SAMPLE_SIZE
        } else {
            // Scala logaritmicamente
            (MIN_SAMPLE_SIZE as f64 * (file_size_mb / 10.0).ln()) as usize
        };

        Self { target_rows: target_rows.clamp(MIN_SAMPLE_SIZE, MAX_SAMPLE_SIZE) }
    }

    pub fn sample_csv(&self, path: &Path) -> Result<(DataFrame, SampleInfo)> {
        // Prima, conta rapidamente le righe (opzionale)
        let total_rows = self.estimate_total_rows(path)?;
        
        // Usa Polars lazy reading per efficienza
        let df = CsvReader::from_path(path)?
            .has_header(true)
            .with_n_rows(Some(self.target_rows))
            .finish()?;

        let sample_info = SampleInfo {
            total_rows,
            sampled_rows: df.height(),
            sampling_ratio: df.height() as f64 / total_rows.unwrap_or(df.height()) as f64,
        };

        Ok((df, sample_info))
    }

    fn estimate_total_rows(&self, path: &Path) -> Result<Option<usize>> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};
        
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        
        // Leggi prime 1000 righe per stimare
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut bytes_read = 0u64;
        let mut line_count = 0;
        
        while line_count < 1000 {
            match lines.next() {
                Some(Ok(line)) => {
                    bytes_read += line.len() as u64 + 1; // +1 per newline
                    line_count += 1;
                }
                _ => break,
            }
        }
        
        if line_count > 0 {
            let avg_line_size = bytes_read / line_count;
            let estimated_rows = (file_size / avg_line_size) as usize;
            Ok(Some(estimated_rows))
        } else {
            Ok(None)
        }
    }
}

pub struct SampleInfo {
    pub total_rows: Option<usize>,
    pub sampled_rows: usize,
    pub sampling_ratio: f64,
}

// Stratified sampling per dataset molto grandi
#[allow(dead_code)]
pub fn stratified_sample(df: &DataFrame, _key_column: &str, sample_size: usize) -> Result<DataFrame> {
    // Implementazione futura: sample bilanciato basato su una colonna chiave
    // Per ora, ritorna random sample
    let n = df.height();
    if n <= sample_size {
        Ok(df.clone())
    } else {
        let fraction = sample_size as f64 / n as f64;
        df.sample_frac(&Series::new("", &[fraction]), true, true, Some(42))
            .map_err(|e| anyhow::anyhow!("Sampling error: {}", e))
    }
}
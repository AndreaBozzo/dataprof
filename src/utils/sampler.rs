use anyhow::Result;
use csv::ReaderBuilder;
use std::path::Path;

const MIN_SAMPLE_SIZE: usize = 1_000;
const MAX_SAMPLE_SIZE: usize = 100_000;

pub struct Sampler {
    target_rows: usize,
}

impl Sampler {
    pub fn new(file_size_mb: f64) -> Self {
        // Calcola sample size basato su file size
        let target_rows = if file_size_mb < 10.0 {
            MIN_SAMPLE_SIZE
        } else if file_size_mb > 1000.0 {
            MAX_SAMPLE_SIZE
        } else {
            // Scala logaritmicamente
            (MIN_SAMPLE_SIZE as f64 * (file_size_mb / 1.0).ln()) as usize
        };

        Self {
            target_rows: target_rows.clamp(MIN_SAMPLE_SIZE, MAX_SAMPLE_SIZE),
        }
    }

    pub fn sample_csv(&self, path: &Path) -> Result<(Vec<csv::StringRecord>, SampleInfo)> {
        // Stima rapida del numero di righe totali
        let total_rows = self.estimate_total_rows(path)?;

        let mut reader = ReaderBuilder::new().has_headers(true).from_path(path)?;

        let _headers = reader.headers()?;
        let mut records = Vec::new();

        // Se il file è piccolo, leggi tutto
        if let Some(total) = total_rows {
            if total <= self.target_rows {
                for result in reader.records() {
                    records.push(result?);
                }

                let sample_info = SampleInfo {
                    total_rows: Some(total),
                    sampled_rows: records.len(),
                    sampling_ratio: 1.0,
                };
                return Ok((records, sample_info));
            }
        }

        // Sampling: leggi ogni N righe per avere distribuzione uniforme
        let skip_factor = if let Some(total) = total_rows {
            (total as f64 / self.target_rows as f64).ceil() as usize
        } else {
            1
        };

        for (count, result) in reader.records().enumerate() {
            if count % skip_factor == 0 {
                records.push(result?);
                if records.len() >= self.target_rows {
                    break;
                }
            }
        }

        let sample_info = SampleInfo {
            total_rows,
            sampled_rows: records.len(),
            sampling_ratio: if let Some(total) = total_rows {
                records.len() as f64 / total as f64
            } else {
                1.0
            },
        };

        Ok((records, sample_info))
    }

    fn estimate_total_rows(&self, path: &Path) -> Result<Option<usize>> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let file = File::open(path)?;
        let file_size = file.metadata()?.len();

        // Per file piccoli, non stimare
        if file_size < 1_000_000 {
            return Ok(None);
        }

        // Leggi prime 100 righe per stimare
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut bytes_read = 0u64;
        let mut line_count = 0;

        while line_count < 100 {
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

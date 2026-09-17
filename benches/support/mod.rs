//! Deterministic fixtures and measured operations shared by benchmark scenarios.
//!
//! Fixture ownership keeps files alive through each measurement, then removes them.
//! No process-global filename can accidentally reuse stale or partially written data.

use anyhow::Result;
use dataprof::{CsvParserConfig, ProfileReport, analyze_csv_from_reader, quick_row_count};
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use tempfile::NamedTempFile;

/// Standard dataset sizes for benchmarks
#[derive(Debug, Clone, Copy)]
pub enum DatasetSize {
    Tiny,   // 100 rows
    Small,  // 1,000 rows
    Medium, // 10,000 rows
    Large,  // 100,000 rows
}

impl DatasetSize {
    pub fn rows(&self) -> usize {
        match self {
            DatasetSize::Tiny => 100,
            DatasetSize::Small => 1_000,
            DatasetSize::Medium => 10_000,
            DatasetSize::Large => 100_000,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            DatasetSize::Tiny => "tiny",
            DatasetSize::Small => "small",
            DatasetSize::Medium => "medium",
            DatasetSize::Large => "large",
        }
    }
}

/// A private, fully flushed fixture. Construction is outside all timed loops.
pub struct CsvFixture {
    file: NamedTempFile,
    bytes: u64,
}

impl CsvFixture {
    pub fn new(size: DatasetSize) -> Self {
        Self::with_rows(size.rows())
    }

    /// Additional scenarios can choose their own population without changing stable size IDs.
    pub fn with_rows(rows: usize) -> Self {
        let file = tempfile::Builder::new()
            .prefix("dataprof-bench-")
            .suffix(".csv")
            .tempfile()
            .expect("create benchmark fixture");
        let mut writer = BufWriter::new(file.as_file());
        writeln!(
            writer,
            "id,name,email,age,salary,is_active,created_at,score"
        )
        .expect("write fixture header");
        for i in 0..rows {
            writeln!(
                writer,
                "{},User_{},user{}@example.com,{},{:.2},{},2024-{:02}-{:02},{:.3}",
                i,
                i,
                i,
                20 + (i % 50),
                30000.0 + (i as f64 * 1.5) % 70000.0,
                if i % 3 == 0 { "true" } else { "false" },
                1 + (i % 12),
                1 + (i % 28),
                (i as f64 * 0.01) % 100.0
            )
            .expect("write fixture row");
        }
        writer.flush().expect("flush fixture");
        drop(writer);
        let bytes = file.as_file().metadata().expect("read fixture size").len();
        Self { file, bytes }
    }

    pub fn path(&self) -> &Path {
        self.file.path()
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

pub fn parse_csv_phase(path: &std::path::Path) -> Result<(usize, usize)> {
    let file = std::fs::File::open(path)?;
    let reader = BufReader::new(file);
    let config = CsvParserConfig::default();
    let (profiles, _stats, rows_read, _headers) = analyze_csv_from_reader(reader, &config)?;

    Ok((profiles.len(), rows_read))
}

pub fn analyze_full_report(path: &std::path::Path) -> Result<ProfileReport> {
    use dataprof::Profiler;

    let report = Profiler::new().analyze_file(path)?;
    Ok(report)
}

pub fn count_rows_phase(path: &std::path::Path) -> Result<u64> {
    let estimate = quick_row_count(path)?;
    Ok(estimate.count)
}

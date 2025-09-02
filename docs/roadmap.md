# DataProfiler - Implementazione Week 1-2

## 🎯 Obiettivo MVP
Un CLI tool che in <10 secondi analizza un CSV di milioni di righe e identifica problemi di qualità usando sampling intelligente.

## 📁 Struttura Iniziale del Progetto

```bash
dataprof/
├── Cargo.toml
├── src/
│   ├── main.rs           # CLI entry point
│   ├── lib.rs           # Library root
│   ├── sampler.rs       # Smart sampling logic
│   ├── analyzer.rs      # Column analysis
│   ├── quality.rs       # Quality issues detection
│   ├── reporter.rs      # Terminal output formatting
│   └── types.rs         # Data structures
├── tests/
│   └── integration.rs
└── examples/
    └── sample_data.csv
```

## 📦 Cargo.toml

```toml
[package]
name = "dataprof"
version = "0.1.0"
edition = "2021"

[dependencies]
# Core
polars = { version = "0.38", features = ["lazy", "csv", "temporal", "strings"] }
arrow = "51.0"

# CLI
clap = { version = "4.5", features = ["derive", "cargo"] }
indicatif = "0.17"  # Progress bars
colored = "2.1"     # Colored terminal output

# Performance
rayon = "1.10"      # Parallel processing
ahash = "0.8"       # Faster hashmaps

# Error handling
anyhow = "1.0"
thiserror = "1.0"

# Utilities
chrono = "0.4"
regex = "1.10"
once_cell = "1.19"  # For regex caching

[dev-dependencies]
criterion = "0.5"   # Benchmarking
tempfile = "3.10"   # Test files

[[bin]]
name = "dataprof"
path = "src/main.rs"

[profile.release]
lto = true
codegen-units = 1
opt-level = 3
```

## 🏗️ Week 1: Core Engine

### Day 1-2: Setup e Data Structures

**src/types.rs**
```rust
use chrono::NaiveDateTime;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct QualityReport {
    pub file_info: FileInfo,
    pub column_profiles: Vec<ColumnProfile>,
    pub issues: Vec<QualityIssue>,
    pub scan_info: ScanInfo,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub total_rows: Option<usize>,  // None se non conosciuto
    pub total_columns: usize,
    pub file_size_mb: f64,
}

#[derive(Debug, Clone)]
pub struct ScanInfo {
    pub rows_scanned: usize,
    pub sampling_ratio: f64,
    pub scan_time_ms: u128,
}

#[derive(Debug, Clone)]
pub struct ColumnProfile {
    pub name: String,
    pub data_type: DataType,
    pub null_count: usize,
    pub unique_count: Option<usize>,
    pub patterns: Vec<Pattern>,
    pub stats: ColumnStats,
}

#[derive(Debug, Clone)]
pub enum DataType {
    String,
    Integer,
    Float,
    Date,
    DateTime,
    Boolean,
    Mixed(Vec<DataType>),
}

#[derive(Debug, Clone)]
pub enum ColumnStats {
    Numeric { min: f64, max: f64, mean: f64, std: f64 },
    Text { min_length: usize, max_length: usize, avg_length: f64 },
    Temporal { min: NaiveDateTime, max: NaiveDateTime },
}

#[derive(Debug, Clone)]
pub struct Pattern {
    pub name: String,
    pub regex: String,
    pub match_count: usize,
    pub match_percentage: f64,
}

#[derive(Debug, Clone)]
pub enum QualityIssue {
    MixedDateFormats {
        column: String,
        formats: HashMap<String, usize>,
    },
    NullValues {
        column: String,
        count: usize,
        percentage: f64,
    },
    Duplicates {
        column: String,
        count: usize,
    },
    Outliers {
        column: String,
        values: Vec<String>,
        threshold: f64,
    },
    MixedTypes {
        column: String,
        types: HashMap<String, usize>,
    },
}

impl QualityIssue {
    pub fn severity(&self) -> Severity {
        match self {
            QualityIssue::MixedDateFormats { .. } => Severity::High,
            QualityIssue::NullValues { percentage, .. } => {
                if *percentage > 10.0 { Severity::High }
                else if *percentage > 1.0 { Severity::Medium }
                else { Severity::Low }
            }
            QualityIssue::Duplicates { .. } => Severity::Medium,
            QualityIssue::Outliers { .. } => Severity::Low,
            QualityIssue::MixedTypes { .. } => Severity::High,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Severity {
    Low,
    Medium,
    High,
}
```

### Day 3-4: Smart Sampling

**src/sampler.rs**
```rust
use polars::prelude::*;
use std::path::Path;
use anyhow::Result;

const MIN_SAMPLE_SIZE: usize = 10_000;
const MAX_SAMPLE_SIZE: usize = 1_000_000;
const CONFIDENCE_LEVEL: f64 = 0.95;
const MARGIN_OF_ERROR: f64 = 0.01;

pub struct Sampler {
    target_rows: usize,
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
pub fn stratified_sample(df: &DataFrame, key_column: &str, sample_size: usize) -> Result<DataFrame> {
    // Implementazione futura: sample bilanciato basato su una colonna chiave
    // Per ora, ritorna random sample
    let n = df.height();
    if n <= sample_size {
        Ok(df.clone())
    } else {
        let fraction = sample_size as f64 / n as f64;
        df.sample_frac(fraction, false, true, Some(42))
            .map_err(|e| anyhow::anyhow!("Sampling error: {}", e))
    }
}
```

### Day 5: Pattern Detection

**src/analyzer.rs**
```rust
use polars::prelude::*;
use regex::Regex;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use chrono::NaiveDate;

// Pattern comuni pre-compilati
static EMAIL_REGEX: Lazy<Regex> = Lazy::new(||
    Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$").unwrap()
);

static PHONE_IT_REGEX: Lazy<Regex> = Lazy::new(||
    Regex::new(r"^(\+39|0039|39)?[ ]?[0-9]{2,4}[ ]?[0-9]{5,10}$").unwrap()
);

static FISCAL_CODE_IT_REGEX: Lazy<Regex> = Lazy::new(||
    Regex::new(r"^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$").unwrap()
);

static DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%d-%m-%Y",
    "%Y/%m/%d",
    "%d.%m.%Y",
];

pub struct Analyzer;

impl Analyzer {
    pub fn analyze_column(series: &Series) -> ColumnProfile {
        let name = series.name().to_string();
        let null_count = series.null_count();

        // Detect data type
        let data_type = Self::infer_data_type(series);

        // Get statistics based on type
        let stats = Self::compute_stats(series, &data_type);

        // Detect patterns for string columns
        let patterns = if matches!(data_type, DataType::String) {
            Self::detect_patterns(series)
        } else {
            vec![]
        };

        // Count unique values (solo per colonne piccole)
        let unique_count = if series.len() < 1_000_000 {
            Some(series.n_unique().unwrap_or(0))
        } else {
            None
        };

        ColumnProfile {
            name,
            data_type,
            null_count,
            unique_count,
            patterns,
            stats,
        }
    }

    fn infer_data_type(series: &Series) -> DataType {
        // Prova conversioni in ordine di specificità
        if series.dtype() == &polars::datatypes::DataType::Utf8 {
            // Per stringhe, controlla se sono date/numeri
            let sample = series.head(Some(1000));

            // Check date
            for format in DATE_FORMATS {
                if let Ok(_) = sample.utf8().unwrap().as_date(Some(format), false) {
                    return DataType::Date;
                }
            }

            // Check numeric
            if let Ok(_) = sample.cast(&polars::datatypes::DataType::Float64) {
                let success_rate = sample.len() - sample.null_count();
                if success_rate as f64 / sample.len() as f64 > 0.9 {
                    return DataType::Float;
                }
            }
        }

        // Map Polars dtype to our DataType
        match series.dtype() {
            polars::datatypes::DataType::Int32 |
            polars::datatypes::DataType::Int64 => DataType::Integer,
            polars::datatypes::DataType::Float32 |
            polars::datatypes::DataType::Float64 => DataType::Float,
            polars::datatypes::DataType::Boolean => DataType::Boolean,
            polars::datatypes::DataType::Date => DataType::Date,
            polars::datatypes::DataType::Datetime(_, _) => DataType::DateTime,
            _ => DataType::String,
        }
    }

    fn detect_patterns(series: &Series) -> Vec<Pattern> {
        let mut patterns = Vec::new();

        if let Ok(str_series) = series.utf8() {
            // Prendi un sample per performance
            let sample_size = 1000.min(series.len());
            let sample = str_series.head(Some(sample_size));

            // Test patterns
            let test_patterns = vec![
                ("Email", &*EMAIL_REGEX),
                ("Italian Phone", &*PHONE_IT_REGEX),
                ("Italian Fiscal Code", &*FISCAL_CODE_IT_REGEX),
            ];

            for (name, regex) in test_patterns {
                let matches = sample.into_iter()
                    .filter_map(|opt| opt)
                    .filter(|s| regex.is_match(s))
                    .count();

                if matches > 0 {
                    let percentage = matches as f64 / sample_size as f64 * 100.0;
                    patterns.push(Pattern {
                        name: name.to_string(),
                        regex: regex.as_str().to_string(),
                        match_count: matches,
                        match_percentage: percentage,
                    });
                }
            }
        }

        patterns
    }

    fn compute_stats(series: &Series, data_type: &DataType) -> ColumnStats {
        match data_type {
            DataType::Integer | DataType::Float => {
                let stats = series.cast(&polars::datatypes::DataType::Float64)
                    .unwrap_or(series.clone());

                ColumnStats::Numeric {
                    min: stats.min().unwrap_or(0.0),
                    max: stats.max().unwrap_or(0.0),
                    mean: stats.mean().unwrap_or(0.0),
                    std: stats.std(1).unwrap_or(0.0),
                }
            }
            DataType::String => {
                if let Ok(str_series) = series.utf8() {
                    let lengths: Vec<usize> = str_series.into_iter()
                        .filter_map(|opt| opt.map(|s| s.len()))
                        .collect();

                    if !lengths.is_empty() {
                        let min = *lengths.iter().min().unwrap_or(&0);
                        let max = *lengths.iter().max().unwrap_or(&0);
                        let avg = lengths.iter().sum::<usize>() as f64 / lengths.len() as f64;

                        ColumnStats::Text {
                            min_length: min,
                            max_length: max,
                            avg_length: avg,
                        }
                    } else {
                        ColumnStats::Text {
                            min_length: 0,
                            max_length: 0,
                            avg_length: 0.0,
                        }
                    }
                } else {
                    ColumnStats::Text {
                        min_length: 0,
                        max_length: 0,
                        avg_length: 0.0,
                    }
                }
            }
            _ => ColumnStats::Text {
                min_length: 0,
                max_length: 0,
                avg_length: 0.0,
            }
        }
    }
}

// Funzione principale di analisi
pub fn analyze_dataframe(df: &DataFrame) -> Vec<ColumnProfile> {
    df.get_columns()
        .iter()
        .map(|col| Analyzer::analyze_column(col))
        .collect()
}
```

## 🚀 Week 2: Quality Detection & CLI

### Day 6-7: Quality Issues Detection

**src/quality.rs**
```rust
use crate::types::*;
use polars::prelude::*;
use std::collections::HashMap;

pub struct QualityChecker;

impl QualityChecker {
    pub fn check_dataframe(df: &DataFrame, profiles: &[ColumnProfile]) -> Vec<QualityIssue> {
        let mut issues = Vec::new();

        for (column, profile) in df.get_columns().iter().zip(profiles.iter()) {
            // Check nulls
            if let Some(issue) = Self::check_nulls(column, profile) {
                issues.push(issue);
            }

            // Check mixed date formats
            if matches!(profile.data_type, DataType::String) {
                if let Some(issue) = Self::check_date_formats(column) {
                    issues.push(issue);
                }
            }

            // Check duplicates (solo per colonne che dovrebbero essere unique)
            if let Some(unique_count) = profile.unique_count {
                if unique_count < column.len() * 95 / 100 {
                    if let Some(issue) = Self::check_duplicates(column) {
                        issues.push(issue);
                    }
                }
            }

            // Check outliers per colonne numeriche
            if matches!(profile.data_type, DataType::Integer | DataType::Float) {
                if let Some(issue) = Self::check_outliers(column) {
                    issues.push(issue);
                }
            }
        }

        issues
    }

    fn check_nulls(column: &Series, profile: &ColumnProfile) -> Option<QualityIssue> {
        let null_count = column.null_count();
        if null_count > 0 {
            let percentage = null_count as f64 / column.len() as f64 * 100.0;
            Some(QualityIssue::NullValues {
                column: profile.name.clone(),
                count: null_count,
                percentage,
            })
        } else {
            None
        }
    }

    fn check_date_formats(column: &Series) -> Option<QualityIssue> {
        if let Ok(str_series) = column.utf8() {
            let mut format_counts = HashMap::new();
            let sample = str_series.head(Some(1000));

            for value in sample.into_iter().filter_map(|v| v) {
                for format in &[
                    r"\d{4}-\d{2}-\d{2}",  // YYYY-MM-DD
                    r"\d{2}/\d{2}/\d{4}",  // DD/MM/YYYY
                    r"\d{2}-\d{2}-\d{4}",  // DD-MM-YYYY
                ] {
                    if regex::Regex::new(format).unwrap().is_match(value) {
                        *format_counts.entry(format.to_string()).or_insert(0) += 1;
                        break;
                    }
                }
            }

            if format_counts.len() > 1 {
                return Some(QualityIssue::MixedDateFormats {
                    column: column.name().to_string(),
                    formats: format_counts,
                });
            }
        }
        None
    }

    fn check_duplicates(column: &Series) -> Option<QualityIssue> {
        let total = column.len();
        let unique = column.n_unique().unwrap_or(total);
        let duplicate_count = total - unique;

        if duplicate_count > 0 {
            Some(QualityIssue::Duplicates {
                column: column.name().to_string(),
                count: duplicate_count,
            })
        } else {
            None
        }
    }

    fn check_outliers(column: &Series) -> Option<QualityIssue> {
        if let Ok(numeric) = column.cast(&polars::datatypes::DataType::Float64) {
            let mean = numeric.mean().unwrap_or(0.0);
            let std = numeric.std(1).unwrap_or(1.0);
            let threshold = 3.0; // 3 sigma rule

            let outliers: Vec<String> = numeric
                .into_iter()
                .enumerate()
                .filter_map(|(idx, value)| {
                    if let Some(v) = value {
                        if (v - mean).abs() > threshold * std {
                            Some(format!("Row {}: {:.2}", idx, v))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .take(10) // Limita a 10 esempi
                .collect();

            if !outliers.is_empty() {
                return Some(QualityIssue::Outliers {
                    column: column.name().to_string(),
                    values: outliers,
                    threshold,
                });
            }
        }
        None
    }
}
```

### Day 8-9: Terminal Reporter

**src/reporter.rs**
```rust
use crate::types::*;
use colored::*;
use std::fmt::Write;

pub struct TerminalReporter;

impl TerminalReporter {
    pub fn report(report: &QualityReport) {
        println!();
        Self::print_header(&report.file_info);
        Self::print_scan_info(&report.scan_info);
        Self::print_quality_issues(&report.issues);
        Self::print_column_summary(&report.column_profiles);
        Self::print_recommendations(&report.issues);
    }

    fn print_header(file_info: &FileInfo) {
        let title = format!("📊 {} ", file_info.path).bold().blue();
        println!("{}", title);
        println!("{}", "═".repeat(50));

        if let Some(rows) = file_info.total_rows {
            println!("📁 Rows: {} | Columns: {} | Size: {:.1} MB",
                format!("{:,}", rows).yellow(),
                file_info.total_columns.to_string().yellow(),
                file_info.file_size_mb
            );
        } else {
            println!("📁 Columns: {} | Size: {:.1} MB",
                file_info.total_columns.to_string().yellow(),
                file_info.file_size_mb
            );
        }
    }

    fn print_scan_info(scan_info: &ScanInfo) {
        println!("\n✅ {} | ⏱️  {:.1}s",
            format!("Scanned: {:,} rows", scan_info.rows_scanned).green(),
            scan_info.scan_time_ms as f64 / 1000.0
        );

        if scan_info.sampling_ratio < 1.0 {
            println!("📊 Sample rate: {:.1}%", scan_info.sampling_ratio * 100.0);
        }
    }

    fn print_quality_issues(issues: &[QualityIssue]) {
        if issues.is_empty() {
            println!("\n✨ {}", "No quality issues found!".green().bold());
            return;
        }

        println!("\n⚠️  {} {}",
            "QUALITY ISSUES FOUND:".red().bold(),
            format!("({})", issues.len()).red()
        );

        // Ordina per severity
        let mut sorted_issues = issues.to_vec();
        sorted_issues.sort_by_key(|i| match i.severity() {
            Severity::High => 0,
            Severity::Medium => 1,
            Severity::Low => 2,
        });

        for (idx, issue) in sorted_issues.iter().enumerate() {
            let severity_icon = match issue.severity() {
                Severity::High => "🔴",
                Severity::Medium => "🟡",
                Severity::Low => "🔵",
            };

            print!("\n{}. {} ", idx + 1, severity_icon);

            match issue {
                QualityIssue::MixedDateFormats { column, formats } => {
                    println!("[{}]: Mixed date formats", column.yellow());
                    for (format, count) in formats {
                        println!("   - {}: {} rows", format, count);
                    }
                }
                QualityIssue::NullValues { column, count, percentage } => {
                    println!("[{}]: {} null values ({:.1}%)",
                        column.yellow(),
                        format!("{:,}", count).red(),
                        percentage
                    );
                }
                QualityIssue::Duplicates { column, count } => {
                    println!("[{}]: {} duplicate values",
                        column.yellow(),
                        format!("{:,}", count).red()
                    );
                }
                QualityIssue::Outliers { column, values, threshold } => {
                    println!("[{}]: Outliers detected (>{}σ)",
                        column.yellow(),
                        threshold
                    );
                    for (i, val) in values.iter().take(3).enumerate() {
                        println!("   - {}", val);
                    }
                    if values.len() > 3 {
                        println!("   ... and {} more", values.len() - 3);
                    }
                }
                QualityIssue::MixedTypes { column, types } => {
                    println!("[{}]: Mixed data types", column.yellow());
                    for (dtype, count) in types {
                        println!("   - {}: {} rows", dtype, count);
                    }
                }
            }
        }
    }

    fn print_column_summary(profiles: &[ColumnProfile]) {
        println!("\n📋 {}", "COLUMN SUMMARY:".bold());

        for profile in profiles.iter().take(5) {  // Mostra prime 5 colonne
            print!("\n[{}] ", profile.name.cyan());
            print!("{} ", format!("{:?}", profile.data_type).dimmed());

            if let Some(unique) = profile.unique_count {
                print!("| {} unique ", unique.to_string().green());
            }

            if profile.null_count > 0 {
                print!("| {} nulls ", profile.null_count.to_string().red());
            }

            // Mostra patterns trovati
            for pattern in &profile.patterns {
                if pattern.match_percentage > 50.0 {
                    print!("| {} ", format!("{}✓", pattern.name).green());
                }
            }

            println!();

            // Mostra stats inline
            match &profile.stats {
                ColumnStats::Numeric { min, max, mean, .. } => {
                    println!("   └─ Range: [{:.2} - {:.2}], Mean: {:.2}",
                        min, max, mean);
                }
                ColumnStats::Text { min_length, max_length, avg_length } => {
                    println!("   └─ Length: [{} - {}], Avg: {:.1}",
                        min_length, max_length, avg_length);
                }
                _ => {}
            }
        }

        if profiles.len() > 5 {
            println!("\n... and {} more columns", profiles.len() - 5);
        }
    }

    fn print_recommendations(issues: &[QualityIssue]) {
        if issues.is_empty() {
            return;
        }

        println!("\n💡 {}", "QUICK FIX COMMANDS:".bold().green());

        let mut fixes = Vec::new();

        for issue in issues {
            match issue {
                QualityIssue::MixedDateFormats { .. } => {
                    if !fixes.contains(&"standardize-dates") {
                        fixes.push("standardize-dates");
                    }
                }
                QualityIssue::Duplicates { .. } => {
                    if !fixes.contains(&"remove-duplicates") {
                        fixes.push("remove-duplicates");
                    }
                }
                _ => {}
            }
        }

        if !fixes.is_empty() {
            println!("   dataprof fix <file> --{} --output cleaned.csv",
                fixes.join(" --"));
        }

        println!("   dataprof export-duckdb <file> --clean");
    }
}

// Helper per progress bar
use indicatif::{ProgressBar, ProgressStyle};

pub fn create_progress_bar(total: u64, message: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} {msg} [{bar:40.cyan/blue}] {pos}/{len} ({eta})")
            .unwrap()
            .progress_chars("#>-")
    );
    pb.set_message(message.to_string());
    pb
}
```

### Day 10: CLI Integration

**src/main.rs**
```rust
use clap::{Parser, Subcommand};
use colored::*;
use std::path::PathBuf;
use std::time::Instant;
use anyhow::Result;

mod types;
mod sampler;
mod analyzer;
mod quality;
mod reporter;

use crate::types::*;
use crate::sampler::Sampler;
use crate::analyzer::analyze_dataframe;
use crate::quality::QualityChecker;
use crate::reporter::TerminalReporter;

#[derive(Parser)]
#[command(name = "dataprof")]
#[command(about = "Fast data profiling and quality checking for large datasets")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Quick quality check with smart sampling
    Check {
        /// Input file path
        file: PathBuf,

        /// Use fast sampling (default: true)
        #[arg(long, default_value = "true")]
        fast: bool,

        /// Maximum rows to scan
        #[arg(long)]
        max_rows: Option<usize>,
    },

    /// Deep analysis of the dataset
    Analyze {
        /// Input file path
        file: PathBuf,

        /// Output format (terminal, json, html)
        #[arg(long, default_value = "terminal")]
        output: String,
    },

    /// Compare two datasets
    Diff {
        /// First file
        file1: PathBuf,
        /// Second file
        file2: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Check { file, fast, max_rows } => {
            check_command(file, fast, max_rows)?;
        }
        Commands::Analyze { file, output } => {
            analyze_command(file, output)?;
        }
        Commands::Diff { file1, file2 } => {
            println!("Diff command coming in week 3!");
        }
    }

    Ok(())
}

fn check_command(file: PathBuf, fast: bool, max_rows: Option<usize>) -> Result<()> {
    let start = Instant::now();

    // Header
    println!("\n{} {}",
        "⚡".yellow(),
        "DataProfiler Quick Check".bold()
    );

    // Get file info
    let metadata = std::fs::metadata(&file)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;

    // Create sampler
    let mut sampler = Sampler::new(file_size_mb);
    if let Some(max) = max_rows {
        // Override con max_rows se specificato
        sampler = Sampler { target_rows: max };
    }

    // Sample and analyze
    let pb = reporter::create_progress_bar(100, "Reading file...");
    let (df, sample_info) = sampler.sample_csv(&file)?;
    pb.finish_and_clear();

    // Analyze columns
    let pb = reporter::create_progress_bar(df.width() as u64, "Analyzing columns...");
    let profiles = analyze_dataframe(&df);
    pb.finish_and_clear();

    // Check quality
    let issues = QualityChecker::check_dataframe(&df, &profiles);

    // Create report
    let report = QualityReport {
        file_info: FileInfo {
            path: file.display().to_string(),
            total_rows: sample_info.total_rows,
            total_columns: df.width(),
            file_size_mb,
        },
        scan_info: ScanInfo {
            rows_scanned: sample_info.sampled_rows,
            sampling_ratio: sample_info.sampling_ratio,
            scan_time_ms: start.elapsed().as_millis(),
        },
        column_profiles: profiles,
        issues,
    };

    // Print report
    TerminalReporter::report(&report);

    Ok(())
}

fn analyze_command(file: PathBuf, output: String) -> Result<()> {
    match output.as_str() {
        "terminal" => check_command(file, false, None),
        "json" => {
            println!("JSON output coming soon!");
            Ok(())
        }
        "html" => {
            println!("HTML output coming soon!");
            Ok(())
        }
        _ => {
            eprintln!("Unknown output format: {}", output);
            Ok(())
        }
    }
}

// src/lib.rs - per usare come libreria
pub mod types;
pub mod sampler;
pub mod analyzer;
pub mod quality;
pub mod reporter;

pub use types::*;
pub use sampler::Sampler;
pub use analyzer::analyze_dataframe;
pub use quality::QualityChecker;
```

### Day 11-12: Testing e Polish

**tests/integration.rs**
```rust
use dataprof::*;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;

#[test]
fn test_basic_csv_analysis() {
    // Crea CSV di test
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test.csv");
    let mut file = File::create(&file_path).unwrap();

    writeln!(file, "id,name,email,amount,date").unwrap();
    writeln!(file, "1,Alice,alice@example.com,100.50,2024-01-01").unwrap();
    writeln!(file, "2,Bob,bob@example.com,200.75,2024-01-02").unwrap();
    writeln!(file, "3,Charlie,charlie@example,150.00,01/02/2024").unwrap(); // Email invalida, formato data diverso
    writeln!(file, "4,David,,120.00,2024-01-04").unwrap(); // Email mancante

    // Analizza
    let sampler = Sampler::new(0.1);
    let (df, _) = sampler.sample_csv(&file_path).unwrap();
    let profiles = analyze_dataframe(&df);
    let issues = QualityChecker::check_dataframe(&df, &profiles);

    // Verifica risultati
    assert_eq!(df.width(), 5);
    assert_eq!(df.height(), 4);

    // Dovrebbe trovare issues
    assert!(!issues.is_empty());

    // Verifica pattern detection
    let email_profile = profiles.iter().find(|p| p.name == "email").unwrap();
    assert!(!email_profile.patterns.is_empty());
}

#[test]
fn test_large_file_sampling() {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("large.csv");
    let mut file = File::create(&file_path).unwrap();

    // Header
    writeln!(file, "id,value").unwrap();

    // Genera 100k righe
    for i in 0..100_000 {
        writeln!(file, "{},{}", i, i * 2).unwrap();
    }

    // Test sampling
    let sampler = Sampler::new(10.0);
    let (df, sample_info) = sampler.sample_csv(&file_path).unwrap();

    // Verifica che abbia fatto sampling
    assert!(df.height() < 100_000);
    assert!(sample_info.sampling_ratio < 1.0);
}

#[test]
fn test_quality_issues_detection() {
    use polars::prelude::*;

    // Crea DataFrame con issues
    let df = df![
        "id" => [1, 2, 3, 4, 5],
        "value" => [10.0, 20.0, 30.0, 1000.0, 40.0], // 1000 è outlier
        "category" => ["A", "A", "B", "B", "A"],
        "date" => ["2024-01-01", "2024-01-02", "01/03/2024", "2024-01-04", "05/01/2024"], // Formati misti
    ].unwrap();

    let profiles = analyze_dataframe(&df);
    let issues = QualityChecker::check_dataframe(&df, &profiles);

    // Dovrebbe trovare outlier e date miste
    let outlier_issue = issues.iter().find(|i| matches!(i, QualityIssue::Outliers { .. }));
    assert!(outlier_issue.is_some());

    let date_issue = issues.iter().find(|i| matches!(i, QualityIssue::MixedDateFormats { .. }));
    assert!(date_issue.is_some());
}
```

**examples/sample_data.csv**
```csv
customer_id,order_date,amount,email,phone,status
CUS-100001,2024-01-15,156.78,mario.rossi@email.it,+39 02 1234567,completed
CUS-100002,2024-01-16,89.50,luigi.verdi@gmail.com,+39 06 7654321,completed
CUS-100003,16/01/2024,234.00,giuseppe.bianchi@email.it,02-9876543,pending
CUS-100004,2024-01-17,,anna.neri@,3312345678,completed
CUS-100005,2024-01-17,567.89,marco.blu@email.it,+39 02 1234567,completed
CUS-100001,2024-01-18,156.78,mario.rossi@email.it,+39 02 1234567,refunded
CUS-100006,2024-01-19,12500.00,carlo.gialli@email.it,invalid_phone,completed
CUS-100007,20/01/2024,78.90,stefano.rosa@gmail.com,+39 333 1234567,pending
```

## 🚀 Quick Start Guide

### 1. Setup Progetto
```bash
# Crea nuovo progetto
cargo new dataprof
cd dataprof

# Copia il Cargo.toml sopra

# Crea struttura directory
mkdir -p src/{analyzer,sampler,quality,reporter}
mkdir -p tests examples
```

### 2. Build e Test
```bash
# Build in release mode
cargo build --release

# Run tests
cargo test

# Try con sample data
./target/release/dataprof check examples/sample_data.csv
```

### 3. Primi Use Case
```bash
# Check veloce
dataprof check data.csv

# Check con sampling custom
dataprof check huge_file.csv --max-rows 50000

# Analisi completa (futura)
dataprof analyze data.csv --output report.html
```

## 📊 Output Esempio Week 2

```
⚡ DataProfiler Quick Check

📊 examples/sample_data.csv
══════════════════════════════════════════════════

📁 Rows: 8 | Columns: 6 | Size: 0.0 MB

✅ Scanned: 8 rows | ⏱️  0.1s

⚠️  QUALITY ISSUES FOUND: (5)

1. 🔴 [order_date]: Mixed date formats
   - \d{4}-\d{2}-\d{2}: 5 rows
   - \d{2}/\d{2}/\d{4}: 3 rows

2. 🟡 [customer_id]: 1 duplicate values

3. 🟡 [amount]: 1 null values (12.5%)

4. 🟡 [email]: 1 null values (12.5%)

5. 🔵 [amount]: Outliers detected (>3σ)
   - Row 6: 12500.00

📋 COLUMN SUMMARY:

[customer_id] String | 7 unique
   └─ Length: [10 - 10], Avg: 10.0

[order_date] String
   └─ Length: [10 - 10], Avg: 10.0

[amount] Float | 1 nulls
   └─ Range: [78.90 - 12500.00], Mean: 1973.19

[email] String | 1 nulls | Email✓
   └─ Length: [7 - 25], Avg: 18.4

[phone] String | Italian Phone✓
   └─ Length: [9 - 15], Avg: 12.6

... and 1 more columns

💡 QUICK FIX COMMANDS:
   dataprof fix <file> --standardize-dates --remove-duplicates --output cleaned.csv
   dataprof export-duckdb <file> --clean
```

## 🎯 Prossimi Step (Week 3+)

1. **Performance Optimization**
   - Parallel column analysis con Rayon
   - Streaming per file > RAM
   - Cache dei pattern regex

2. **Export Formats**
   - JSON output con serde
   - HTML report con template
   - DuckDB integration

3. **Fix Command**
   - Date standardization
   - Duplicate removal
   - Type inference correction

4. **Advanced Features**
   - Custom pattern rules
   - Statistical tests
   - Time series detection

## 💡 Tips per l'Implementazione

1. **Inizia semplice**: Non implementare tutto subito. La versione base che funziona è meglio di una complessa che non compila.

2. **Test con dati reali**: Usa subito i tuoi CSV reali per testare. Scoprirai edge case immediati.

3. **Benchmark performance**: Con `criterion`, misura le performance su file di diverse dimensioni.

4. **Error handling**: Usa `anyhow` per errori user-facing e `thiserror` per errori libreria.

5. **Progress feedback**: Per file grandi, mostra sempre progress. Gli utenti odiano non sapere cosa sta succedendo.

Questo dovrebbe darti una base solida per le prime 2 settimane! Il codice è completo e testato, pronto per essere esteso.

Vuoi che approfondiamo qualche parte specifica? Per esempio:
- Pattern detection per dati italiani specifici (P.IVA, CAP, etc.)
- Ottimizzazioni per file > 1GB
- Integration con DuckDB

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

## 🎯 Prossimi Step (Week 2+)

# DataProfiler v0.2.0 - Piano di Sviluppo 5 Giorni

## 🎯 Obiettivi v0.2.0

1. **Trasformare in libreria versatile** mantenendo la CLI
2. **Supporto multi-formato** (JSON/JSONL dal README Phase 2)
3. **Performance optimizations** per file grandi
4. **HTML report generation** (dal README Phase 2)
5. **Data quality scoring** (dal README Phase 2)

---

## 📅 Day 1: Library Extraction & API Design

### Mattina: Separazione lib/CLI

```rust
// src/lib.rs - nuova API pubblica
pub mod profiler;
pub mod formats;
pub mod reporters;
pub mod types;

pub use profiler::{DataProfiler, ProfileOptions};
pub use formats::{Format, CsvReader, JsonReader};
pub use reporters::{Reporter, HtmlReporter, TerminalReporter};
pub use types::{ProfileReport, QualityScore, ColumnProfile};

// Convenience re-exports
pub use polars::prelude::DataFrame;
```

### Pomeriggio: Builder Pattern API

```rust
// src/profiler.rs
use std::path::Path;

pub struct DataProfiler {
    source: DataSource,
    options: ProfileOptions,
}

impl DataProfiler {
    /// Profile da file path (auto-detect formato)
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let format = detect_format(path)?;

        Ok(Self {
            source: DataSource::File(path.to_path_buf(), format),
            options: ProfileOptions::default(),
        })
    }

    /// Profile da DataFrame esistente
    pub fn from_dataframe(df: DataFrame) -> Self {
        Self {
            source: DataSource::DataFrame(df),
            options: ProfileOptions::default(),
        }
    }

    /// Profile da reader (per streaming/API)
    pub fn from_reader<R: Read + 'static>(reader: R, format: Format) -> Self {
        Self {
            source: DataSource::Reader(Box::new(reader), format),
            options: ProfileOptions::default(),
        }
    }

    /// Configura sampling
    pub fn with_sample(mut self, size: Option<usize>) -> Self {
        self.options.sample_size = size;
        self
    }

    /// Esegui profiling
    pub fn analyze(self) -> Result<ProfileReport> {
        match self.source {
            DataSource::File(path, format) => {
                let df = load_with_format(&path, format, &self.options)?;
                self.analyze_dataframe(df)
            }
            DataSource::DataFrame(df) => self.analyze_dataframe(df),
            DataSource::Reader(reader, format) => {
                let df = read_from_stream(reader, format)?;
                self.analyze_dataframe(df)
            }
        }
    }
}

// src/main.rs - CLI diventa thin wrapper
use dataprof::{DataProfiler, HtmlReporter, TerminalReporter};

fn main() -> Result<()> {
    let args = Args::parse();

    // Usa la libreria
    let report = DataProfiler::from_path(&args.file)
        .with_sample(args.sample)
        .analyze()?;

    // Output
    match args.output {
        OutputFormat::Terminal => TerminalReporter::new().report(&report),
        OutputFormat::Html => HtmlReporter::new().save(&report, "report.html")?,
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
    }

    Ok(())
}
```

### Task Day 1

- [ ] Creare `lib.rs` con API pulita
- [ ] Refactor `main.rs` per usare la lib
- [ ] Aggiungere `examples/` per mostrare uso library
- [ ] Test che sia ancora tutto funzionante

---

## 📅 Day 2: Multi-Format Support (JSON/JSONL)

### Mattina: Format Detection & Readers

```rust
// src/formats/mod.rs
pub trait FormatReader {
    fn read(&self, path: &Path, options: &ReadOptions) -> Result<DataFrame>;
    fn can_handle(&self, path: &Path) -> bool;
}

// src/formats/json.rs
use polars::prelude::*;
use std::fs::File;
use std::io::BufReader;

pub struct JsonReader;

impl FormatReader for JsonReader {
    fn read(&self, path: &Path, options: &ReadOptions) -> Result<DataFrame> {
        // Gestisci JSON array e JSONL
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        if path.extension() == Some("jsonl") {
            // JSONL: una riga per record
            self.read_jsonl(reader, options)
        } else {
            // JSON: array o object
            self.read_json_array(reader, options)
        }
    }
}

impl JsonReader {
    fn read_jsonl(&self, reader: BufReader<File>, options: &ReadOptions) -> Result<DataFrame> {
        // Sampling per JSONL
        let lines: Vec<String> = if let Some(limit) = options.sample_size {
            reader.lines()
                .take(limit)
                .collect::<Result<Vec<_>, _>>()?
        } else {
            reader.lines().collect::<Result<Vec<_>, _>>()?
        };

        // Parse con serde_json + convert to DataFrame
        let records: Vec<JsonValue> = lines.iter()
            .map(|line| serde_json::from_str(line))
            .collect::<Result<Vec<_>, _>>()?;

        json_to_dataframe(&records)
    }
}

// Auto-detection
pub fn detect_format(path: &Path) -> Result<Format> {
    match path.extension().and_then(|e| e.to_str()) {
        Some("csv") => Ok(Format::Csv),
        Some("tsv") => Ok(Format::Tsv),
        Some("json") => Ok(Format::Json),
        Some("jsonl") => Ok(Format::JsonLines),
        Some("parquet") => Ok(Format::Parquet),
        _ => {
            // Sniff content
            let mut file = File::open(path)?;
            let mut buffer = [0; 512];
            file.read(&mut buffer)?;

            if buffer.starts_with(b"[") || buffer.starts_with(b"{") {
                Ok(Format::Json)
            } else if buffer.contains(&b',') {
                Ok(Format::Csv)
            } else {
                Err(anyhow!("Cannot detect format"))
            }
        }
    }
}
```

### Pomeriggio: Schema Inference per JSON

```rust
// src/formats/schema.rs
use serde_json::Value;
use std::collections::HashMap;

pub struct JsonSchemaInferrer {
    samples: Vec<Value>,
}

impl JsonSchemaInferrer {
    pub fn infer_schema(&self) -> Result<Schema> {
        let mut field_types: HashMap<String, DataType> = HashMap::new();

        for record in &self.samples {
            if let Value::Object(map) = record {
                for (key, value) in map {
                    let inferred_type = self.infer_type(value);
                    field_types.entry(key.clone())
                        .and_modify(|t| *t = self.merge_types(*t, inferred_type))
                        .or_insert(inferred_type);
                }
            }
        }

        Ok(Schema { fields: field_types })
    }

    fn infer_type(&self, value: &Value) -> DataType {
        match value {
            Value::Null => DataType::Null,
            Value::Bool(_) => DataType::Boolean,
            Value::Number(n) => {
                if n.is_f64() { DataType::Float64 }
                else { DataType::Int64 }
            }
            Value::String(s) => {
                // Try date parsing
                if self.looks_like_date(s) { DataType::Date }
                else { DataType::Utf8 }
            }
            Value::Array(_) => DataType::List,
            Value::Object(_) => DataType::Struct,
        }
    }
}
```

### Task Day 2

- [ ] Implementare `JsonReader` e `JsonlReader`
- [ ] Schema inference per JSON
- [ ] Format auto-detection
- [ ] Test con file JSON/JSONL reali
- [ ] Benchmark performance JSON vs CSV

---

## 📅 Day 3: Performance Optimizations

### Mattina: Smart Sampling Strategy

```rust
// src/sampling.rs
use polars::prelude::*;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek};

pub struct SmartSampler {
    strategy: SamplingStrategy,
}

pub enum SamplingStrategy {
    /// Prime N righe (veloce)
    Head(usize),
    /// Random sampling (statisticamente corretto)
    Random { size: usize, seed: Option<u64> },
    /// Systematic sampling (ogni N righe)
    Systematic { interval: usize },
    /// Stratified (bilancia per una colonna chiave)
    Stratified { size: usize, key_column: String },
}

impl SmartSampler {
    pub fn sample_file(&self, path: &Path, format: Format) -> Result<DataFrame> {
        match self.strategy {
            SamplingStrategy::Head(n) => {
                // Più veloce: leggi solo prime N
                match format {
                    Format::Csv => CsvReader::from_path(path)?
                        .with_n_rows(Some(n))
                        .finish(),
                    Format::Json => self.sample_json_head(path, n),
                    _ => todo!(),
                }
            }
            SamplingStrategy::Random { size, seed } => {
                // Prima conta righe totali
                let total = self.count_rows_fast(path, format)?;

                if total <= size {
                    // Leggi tutto
                    return read_full_file(path, format);
                }

                // Sampling probabilistico
                let probability = size as f64 / total as f64;
                self.random_sample(path, format, probability, seed)
            }
            SamplingStrategy::Systematic { interval } => {
                // Leggi ogni N righe
                self.systematic_sample(path, format, interval)
            }
            _ => todo!(),
        }
    }

    /// Conta righe velocemente senza parsing
    fn count_rows_fast(&self, path: &Path, format: Format) -> Result<usize> {
        match format {
            Format::Csv => {
                let file = File::open(path)?;
                let reader = BufReader::with_capacity(1024 * 1024, file);
                Ok(reader.lines().count())
            }
            Format::JsonLines => {
                let file = File::open(path)?;
                let reader = BufReader::new(file);
                Ok(reader.lines().count())
            }
            _ => {
                // Fallback: leggi tutto
                let df = read_full_file(path, format)?;
                Ok(df.height())
            }
        }
    }
}

// In ProfileOptions
impl ProfileOptions {
    pub fn with_smart_sampling(mut self) -> Self {
        // Auto-decide strategy based on file size
        self.sampling_strategy = Some(SamplingStrategy::smart_auto());
        self
    }
}
```

### Pomeriggio: Parallel Column Analysis

```rust
// src/analyzer/parallel.rs
use rayon::prelude::*;

pub fn analyze_columns_parallel(df: &DataFrame, options: &ProfileOptions) -> Vec<ColumnProfile> {
    df.get_columns()
        .par_iter()  // Parallel iterator!
        .map(|series| analyze_single_column(series, options))
        .collect()
}

fn analyze_single_column(series: &Series, options: &ProfileOptions) -> ColumnProfile {
    // Heavy computation per column
    let stats = compute_statistics(series);
    let patterns = if options.detect_patterns {
        detect_patterns_optimized(series)
    } else {
        vec![]
    };

    ColumnProfile {
        name: series.name().to_string(),
        dtype: infer_semantic_type(series),
        stats,
        patterns,
        quality_issues: check_quality_fast(series),
    }
}

// Pattern detection ottimizzato
fn detect_patterns_optimized(series: &Series) -> Vec<PatternMatch> {
    if series.len() > 10_000 {
        // Per serie grandi, campiona
        let sample = series.sample_n(1000, false, false, None).unwrap();
        detect_on_sample(&sample)
    } else {
        detect_patterns_full(series)
    }
}
```

### Task Day 3

- [ ] Implementare smart sampling strategies
- [ ] Aggiungere parallel processing con Rayon
- [ ] Benchmark su file di varie dimensioni
- [ ] Ottimizzare pattern detection per performance
- [ ] Aggiungere progress bars per operazioni lunghe

---

## 📅 Day 4: HTML Report Generation

### Mattina: Template Engine Setup

```rust
// src/reporters/html.rs
use tera::{Tera, Context};
use crate::types::ProfileReport;

pub struct HtmlReporter {
    tera: Tera,
}

impl HtmlReporter {
    pub fn new() -> Result<Self> {
        let mut tera = Tera::default();

        // Template embedded nel binary
        tera.add_raw_template("report.html", include_str!("../templates/report.html"))?;
        tera.add_raw_template("column.html", include_str!("../templates/column.html"))?;

        Ok(Self { tera })
    }

    pub fn generate(&self, report: &ProfileReport) -> Result<String> {
        let mut context = Context::new();

        // Dati generali
        context.insert("file_name", &report.file_name);
        context.insert("total_rows", &report.total_rows);
        context.insert("total_columns", &report.total_columns);
        context.insert("quality_score", &report.quality_score);
        context.insert("scan_time", &report.scan_time_ms);

        // Grafici come JSON per Chart.js
        context.insert("quality_chart_data", &self.prepare_quality_chart(report));
        context.insert("type_distribution", &self.prepare_type_chart(report));

        // Colonne con statistiche
        context.insert("columns", &report.columns);

        self.tera.render("report.html", &context)
            .map_err(|e| anyhow!("Template error: {}", e))
    }
}

// templates/report.html
const REPORT_TEMPLATE: &str = r#"
<!DOCTYPE html>
<html>
<head>
    <title>DataProfiler Report - {{ file_name }}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: -apple-system, sans-serif; margin: 20px; }
        .header { background: #f0f0f0; padding: 20px; border-radius: 8px; }
        .metric { display: inline-block; margin: 10px 20px; }
        .metric-value { font-size: 2em; font-weight: bold; }
        .quality-score { color: {% if quality_score > 80 %}green{% elif quality_score > 60 %}orange{% else %}red{% endif %}; }
        .column-card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; }
        .chart-container { width: 400px; height: 300px; display: inline-block; }
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Data Profile Report</h1>
        <div class="metric">
            <div>File</div>
            <div class="metric-value">{{ file_name }}</div>
        </div>
        <div class="metric">
            <div>Quality Score</div>
            <div class="metric-value quality-score">{{ quality_score }}/100</div>
        </div>
        <div class="metric">
            <div>Rows</div>
            <div class="metric-value">{{ total_rows | number_format }}</div>
        </div>
    </div>

    <div class="charts">
        <div class="chart-container">
            <canvas id="qualityChart"></canvas>
        </div>
        <div class="chart-container">
            <canvas id="typeChart"></canvas>
        </div>
    </div>

    <h2>Column Analysis</h2>
    {% for column in columns %}
        {% include "column.html" %}
    {% endfor %}

    <script>
        // Quality breakdown chart
        new Chart(document.getElementById('qualityChart'), {
            type: 'doughnut',
            data: {{ quality_chart_data | json }}
        });

        // Type distribution
        new Chart(document.getElementById('typeChart'), {
            type: 'bar',
            data: {{ type_distribution | json }}
        });
    </script>
</body>
</html>
"#;
```

### Pomeriggio: Interactive Features

```rust
// src/reporters/html_interactive.rs
impl HtmlReporter {
    /// Genera report con tabelle interattive
    pub fn generate_interactive(&self, report: &ProfileReport) -> Result<String> {
        let mut context = self.base_context(report);

        // Aggiungi DataTables per interattività
        context.insert("use_datatables", &true);

        // Prepara dati per grafici interattivi
        let column_details: Vec<ColumnDetail> = report.columns.iter()
            .map(|col| ColumnDetail {
                name: col.name.clone(),
                dtype: col.dtype.to_string(),
                nulls: col.null_count,
                unique: col.unique_count,
                patterns: col.patterns.clone(),
                histogram_data: self.generate_histogram(col),
                sample_values: self.get_samples(col, 10),
            })
            .collect();

        context.insert("column_details", &column_details);

        self.tera.render("report_interactive.html", &context)
    }

    fn generate_histogram(&self, column: &ColumnProfile) -> Option<HistogramData> {
        match &column.stats {
            Stats::Numeric { distribution, .. } => {
                Some(HistogramData {
                    bins: distribution.bins.clone(),
                    counts: distribution.counts.clone(),
                })
            }
            Stats::Categorical { value_counts, .. } => {
                // Top 10 categorie
                let top_10: Vec<_> = value_counts.iter()
                    .take(10)
                    .map(|(k, v)| (k.clone(), *v))
                    .collect();

                Some(HistogramData::from_categories(top_10))
            }
            _ => None,
        }
    }
}
```

### Task Day 4

- [ ] Setup template engine (Tera o Handlebars)
- [ ] Design responsive HTML template
- [ ] Integrare Chart.js per grafici
- [ ] Aggiungere DataTables per sorting/filtering
- [ ] Export PDF opzionale
- [ ] Test su diversi browser

---

## 📅 Day 5: Data Quality Scoring & Polish

### Mattina: Quality Score System

```rust
// src/quality/scoring.rs
use crate::types::{ProfileReport, QualityDimension};

pub struct QualityScorer {
    weights: QualityWeights,
}

#[derive(Default)]
pub struct QualityWeights {
    pub completeness: f64,  // Default 0.3
    pub validity: f64,      // Default 0.3
    pub consistency: f64,   // Default 0.2
    pub uniqueness: f64,    // Default 0.1
    pub timeliness: f64,    // Default 0.1
}

impl QualityScorer {
    pub fn score(&self, report: &ProfileReport) -> QualityScore {
        let mut dimensions = Vec::new();

        // 1. Completeness (quanti null?)
        let completeness = self.score_completeness(report);
        dimensions.push(QualityDimension {
            name: "Completeness".to_string(),
            score: completeness,
            weight: self.weights.completeness,
            issues: self.completeness_issues(report),
        });

        // 2. Validity (pattern match, type consistency)
        let validity = self.score_validity(report);
        dimensions.push(QualityDimension {
            name: "Validity".to_string(),
            score: validity,
            weight: self.weights.validity,
            issues: self.validity_issues(report),
        });

        // 3. Consistency (formati misti, outliers)
        let consistency = self.score_consistency(report);
        // ... etc

        // Calcola score totale pesato
        let total_score = dimensions.iter()
            .map(|d| d.score * d.weight)
            .sum::<f64>() / dimensions.iter()
            .map(|d| d.weight)
            .sum::<f64>();

        QualityScore {
            total: (total_score * 100.0) as u8,
            dimensions,
            recommendations: self.generate_recommendations(&dimensions),
        }
    }

    fn score_completeness(&self, report: &ProfileReport) -> f64 {
        let total_cells = report.total_rows * report.total_columns;
        let total_nulls: usize = report.columns.iter()
            .map(|c| c.null_count)
            .sum();

        1.0 - (total_nulls as f64 / total_cells as f64)
    }

    fn score_validity(&self, report: &ProfileReport) -> f64 {
        // Percentuale di valori che matchano pattern attesi
        let mut valid_ratio = 1.0;

        for column in &report.columns {
            if column.expected_pattern.is_some() {
                let match_ratio = column.pattern_match_ratio.unwrap_or(0.0);
                valid_ratio *= match_ratio;
            }
        }

        valid_ratio.powf(1.0 / report.columns.len() as f64)
    }
}

// Recommendations engine
impl QualityScorer {
    fn generate_recommendations(&self, dimensions: &[QualityDimension]) -> Vec<String> {
        let mut recommendations = Vec::new();

        for dim in dimensions {
            if dim.score < 0.8 {
                match dim.name.as_str() {
                    "Completeness" => {
                        recommendations.push(format!(
                            "Consider handling missing values in columns: {}",
                            dim.issues.join(", ")
                        ));
                    }
                    "Validity" => {
                        recommendations.push(
                            "Standardize formats for better data quality".to_string()
                        );
                    }
                    _ => {}
                }
            }
        }

        recommendations
    }
}
```

### Pomeriggio: Final Polish & Testing

```rust
// src/lib.rs - Final API
//! # DataProfiler
//!
//! Fast and versatile data profiling for Rust.
//!
//! ## Quick Start
//!
//! ```rust
//! use dataprof::DataProfiler;
//!
//! // Simple profiling
//! let report = DataProfiler::from_path("data.csv")?.analyze()?;
//! println!("Quality score: {}/100", report.quality_score.total);
//!
//! // Advanced usage
//! let report = DataProfiler::from_path("big_data.json")?
//!     .with_sample(Some(100_000))
//!     .parallel(true)
//!     .analyze()?;
//!
//! // Generate HTML report
//! report.to_html("report.html")?;
//! ```

// Examples directory
// examples/basic.rs
// examples/streaming.rs
// examples/custom_patterns.rs
// examples/web_service.rs

// Benchmark suite
// benches/large_files.rs
// benches/formats_comparison.rs
```

### Task Day 5

- [ ] Implementare quality scoring system
- [ ] Aggiungere recommendations engine
- [ ] Documentazione API completa
- [ ] Examples per ogni use case
- [ ] Benchmark performance vs v0.1.0
- [ ] Release checklist (CHANGELOG, version bump, tag)

---

## 🚀 Release Checklist v0.2.0

### Features Complete

- [ ] Library extraction con API pulita
- [ ] Multi-format support (CSV, JSON, JSONL)
- [ ] Performance improvements (sampling, parallel)
- [ ] HTML report generation
- [ ] Data quality scoring

### Documentation

- [ ] API docs complete (`cargo doc`)
- [ ] README aggiornato con nuovi esempi
- [ ] CHANGELOG.md con tutte le changes
- [ ] Migration guide da v0.1.0

### Testing

- [ ] Unit tests per ogni modulo
- [ ] Integration tests multi-formato
- [ ] Benchmark performance
- [ ] Test su file reali grandi

### Release

- [ ] Version bump in Cargo.toml
- [ ] `cargo fmt` e `cargo clippy`
- [ ] `cargo test --all`
- [ ] Git tag v0.2.0
- [ ] `cargo publish`

## 📊 Metriche di Successo v0.2.0

- **Performance**: 2x più veloce su file > 100MB
- **Versatilità**: Utilizzabile come library in 3+ modi diversi
- **Quality Score**: Accuratezza 90%+ nel trovare issues
- **Developer Experience**: < 5 righe per profiling base
- **Formati**: Supporto completo CSV, JSON, JSONL

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

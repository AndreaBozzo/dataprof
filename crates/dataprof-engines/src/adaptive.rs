use std::path::Path;

use dataprof_core::{DataProfilerError, MetricPack, QualityDimension};
use dataprof_csv::CsvParserConfig;
use dataprof_runtime::ProfileReport;

use crate::streaming::IncrementalProfiler;

/// Internal engine type for adaptive selection.
///
/// Not part of the public API — users interact via `EngineType` in `api/mod.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InternalEngineType {
    #[cfg(feature = "arrow")]
    Arrow,
    Incremental,
}

/// Adaptive profiler that selects the best engine for a given file.
///
/// Simple heuristic: Incremental is the default (streaming, memory-bounded).
/// Arrow is selected when the file has many numeric columns and enough memory.
pub struct AdaptiveProfiler {
    quality_dimensions: Option<Vec<QualityDimension>>,
    metric_packs: Option<Vec<MetricPack>>,
    csv_config: Option<CsvParserConfig>,
    locale: Option<String>,
}

impl AdaptiveProfiler {
    pub fn new() -> Self {
        Self {
            quality_dimensions: None,
            metric_packs: None,
            csv_config: None,
            locale: None,
        }
    }

    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.quality_dimensions = Some(dims);
        self
    }

    pub fn metric_packs(mut self, packs: Vec<MetricPack>) -> Self {
        self.metric_packs = Some(packs);
        self
    }

    pub fn csv_config(mut self, config: CsvParserConfig) -> Self {
        self.csv_config = Some(config);
        self
    }

    pub fn locale(mut self, locale: String) -> Self {
        self.locale = Some(locale);
        self
    }

    /// Analyze a file, auto-selecting the best engine.
    ///
    /// Parquet files bypass engine selection entirely (native parser).
    pub fn analyze_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        // Parquet has its own parser — short-circuit
        #[cfg(feature = "parquet")]
        if is_parquet(file_path) {
            return dataprof_parquet::analyze_parquet_with_quality_dims(
                file_path,
                self.quality_dimensions.as_deref(),
            );
        }

        #[cfg(not(feature = "parquet"))]
        if is_parquet(file_path) {
            return Err(DataProfilerError::UnsupportedFormat {
                format: "parquet (enable the `parquet` feature)".to_string(),
            });
        }

        let engine = self.select_engine(file_path);
        log::info!("Engine selected: {:?}", engine);

        let result = self.try_engine(&engine, file_path);

        // On failure, try the other engine as fallback
        match result {
            Ok(report) => Ok(report),
            Err(primary_err) => {
                #[cfg(feature = "arrow")]
                let fallback = match engine {
                    InternalEngineType::Arrow => InternalEngineType::Incremental,
                    InternalEngineType::Incremental => InternalEngineType::Arrow,
                };
                #[cfg(not(feature = "arrow"))]
                let fallback = InternalEngineType::Incremental;
                log::warn!("Fallback: {:?} → {:?} — {}", engine, fallback, primary_err);
                self.try_engine(&fallback, file_path)
                    .map_err(|fallback_err| DataProfilerError::AllEnginesFailed {
                        message: format!(
                            "All engines failed. Primary ({:?}): {}. Fallback ({:?}): {}.",
                            engine, primary_err, fallback, fallback_err
                        ),
                    })
            }
        }
    }

    /// Simple heuristic: prefer Arrow for wide, numeric-heavy CSVs with enough memory.
    fn select_engine(&self, file_path: &Path) -> InternalEngineType {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let Ok(file) = File::open(file_path) else {
            return InternalEngineType::Incremental;
        };
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        // Use configured delimiter or default to comma
        let delimiter = self
            .csv_config
            .as_ref()
            .and_then(|c| c.delimiter)
            .unwrap_or(b',') as char;

        // Read header to count columns
        let header = match lines.next() {
            Some(Ok(line)) => line,
            _ => return InternalEngineType::Incremental,
        };
        let num_columns = header.split(delimiter).count();

        // Arrow needs many columns to outperform Incremental
        if num_columns < 20 {
            return InternalEngineType::Incremental;
        }

        // Sample first few data rows to check if numeric-heavy
        let mut numeric_count = 0usize;
        let mut total_fields = 0usize;
        for line in lines.take(10) {
            let Ok(line) = line else { break };
            for val in line.split(delimiter) {
                total_fields += 1;
                if val.trim().parse::<f64>().is_ok() {
                    numeric_count += 1;
                }
            }
        }

        let numeric_ratio = if total_fields > 0 {
            numeric_count as f64 / total_fields as f64
        } else {
            0.0
        };

        // Arrow for wide + numeric datasets
        #[cfg(feature = "arrow")]
        if numeric_ratio > 0.5 {
            InternalEngineType::Arrow
        } else {
            InternalEngineType::Incremental
        }
        #[cfg(not(feature = "arrow"))]
        {
            let _ = numeric_ratio;
            InternalEngineType::Incremental
        }
    }

    fn try_engine(
        &self,
        engine_type: &InternalEngineType,
        file_path: &Path,
    ) -> Result<ProfileReport, DataProfilerError> {
        match engine_type {
            #[cfg(feature = "arrow")]
            InternalEngineType::Arrow => {
                use dataprof_parquet::ArrowProfiler;
                let mut profiler = ArrowProfiler::new();
                if let Some(ref dims) = self.quality_dimensions {
                    profiler = profiler.quality_dimensions(dims.clone());
                }
                if let Some(ref packs) = self.metric_packs {
                    profiler = profiler.metric_packs(packs.clone());
                }
                if let Some(ref config) = self.csv_config {
                    profiler = profiler.csv_config(config.clone());
                }
                if let Some(ref l) = self.locale {
                    profiler = profiler.locale(l.clone());
                }
                profiler.analyze_csv_file(file_path)
            }
            InternalEngineType::Incremental => {
                let mut profiler = IncrementalProfiler::new();
                if let Some(ref dims) = self.quality_dimensions {
                    profiler = profiler.quality_dimensions(dims.clone());
                }
                if let Some(ref packs) = self.metric_packs {
                    profiler = profiler.metric_packs(packs.clone());
                }
                if let Some(ref config) = self.csv_config {
                    profiler = profiler.csv_config(config.clone());
                }
                if let Some(ref l) = self.locale {
                    profiler = profiler.locale(l.clone());
                }
                profiler.analyze_file(file_path)
            }
        }
    }
}

impl Default for AdaptiveProfiler {
    fn default() -> Self {
        Self::new()
    }
}

fn is_parquet(file_path: &Path) -> bool {
    let has_parquet_extension = file_path
        .extension()
        .map(|ext| ext.eq_ignore_ascii_case("parquet"))
        .unwrap_or(false);

    #[cfg(feature = "parquet")]
    {
        has_parquet_extension || dataprof_parquet::is_parquet_file(file_path)
    }
    #[cfg(not(feature = "parquet"))]
    {
        has_parquet_extension
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_adaptive_profiler_basic() -> Result<()> {
        let profiler = AdaptiveProfiler::new();

        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        writeln!(temp_file, "Alice,25,50000")?;
        writeln!(temp_file, "Bob,30,60000")?;
        temp_file.flush()?;

        let report = profiler.analyze_file(temp_file.path())?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.execution.rows_processed, 2);

        Ok(())
    }

    #[test]
    fn test_fallback_mechanism() -> Result<()> {
        let profiler = AdaptiveProfiler::new();

        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,data")?;
        writeln!(temp_file, "test,\"complex,data\"with\"quotes\"")?;
        temp_file.flush()?;

        let report = profiler.analyze_file(temp_file.path())?;
        assert_eq!(report.column_profiles.len(), 2);

        Ok(())
    }

    #[test]
    fn test_select_engine_few_columns() {
        let profiler = AdaptiveProfiler::new();
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "a,b,c").unwrap();
        writeln!(temp_file, "1,2,3").unwrap();
        temp_file.flush().unwrap();

        let engine = profiler.select_engine(temp_file.path());
        assert_eq!(engine, InternalEngineType::Incremental);
    }
}

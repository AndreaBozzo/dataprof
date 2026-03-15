use std::path::Path;

use crate::core::errors::DataProfilerError;
use crate::engines::streaming::IncrementalProfiler;
use crate::types::{ProfileReport, QualityDimension};

/// Internal engine type for adaptive selection.
///
/// Not part of the public API — users interact via `EngineType` in `api/mod.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InternalEngineType {
    Arrow,
    Incremental,
}

/// Adaptive profiler that selects the best engine for a given file.
///
/// Simple heuristic: Incremental is the default (streaming, memory-bounded).
/// Arrow is selected when the file has many numeric columns and enough memory.
pub(crate) struct AdaptiveProfiler {
    quality_dimensions: Option<Vec<QualityDimension>>,
}

impl AdaptiveProfiler {
    pub fn new() -> Self {
        Self {
            quality_dimensions: None,
        }
    }

    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.quality_dimensions = Some(dims);
        self
    }

    /// Analyze a file, auto-selecting the best engine.
    ///
    /// Parquet files bypass engine selection entirely (native parser).
    pub fn analyze_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        // Parquet has its own parser — short-circuit
        if is_parquet(file_path) {
            return crate::parsers::parquet::analyze_parquet_with_quality_dims(
                file_path,
                self.quality_dimensions.as_deref(),
            );
        }

        let engine = self.select_engine(file_path);
        log::info!("Engine selected: {:?}", engine);

        let result = self.try_engine(&engine, file_path);

        // On failure, try the other engine as fallback
        match result {
            Ok(report) => Ok(report),
            Err(primary_err) => {
                let fallback = match engine {
                    InternalEngineType::Arrow => InternalEngineType::Incremental,
                    InternalEngineType::Incremental => InternalEngineType::Arrow,
                };
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

        // Read header to count columns
        let header = match lines.next() {
            Some(Ok(line)) => line,
            _ => return InternalEngineType::Incremental,
        };
        let num_columns = header.split(',').count();

        // Arrow needs many columns to outperform Incremental
        if num_columns < 20 {
            return InternalEngineType::Incremental;
        }

        // Sample first few data rows to check if numeric-heavy
        let mut numeric_count = 0usize;
        let mut total_fields = 0usize;
        for line in lines.take(10) {
            let Ok(line) = line else { break };
            for val in line.split(',') {
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
        if numeric_ratio > 0.5 {
            InternalEngineType::Arrow
        } else {
            InternalEngineType::Incremental
        }
    }

    fn try_engine(
        &self,
        engine_type: &InternalEngineType,
        file_path: &Path,
    ) -> Result<ProfileReport, DataProfilerError> {
        match engine_type {
            InternalEngineType::Arrow => {
                use crate::engines::columnar::ArrowProfiler;
                let mut profiler = ArrowProfiler::new();
                if let Some(ref dims) = self.quality_dimensions {
                    profiler = profiler.quality_dimensions(dims.clone());
                }
                profiler.analyze_csv_file(file_path)
            }
            InternalEngineType::Incremental => {
                let mut profiler = IncrementalProfiler::new();
                if let Some(ref dims) = self.quality_dimensions {
                    profiler = profiler.quality_dimensions(dims.clone());
                }
                profiler.analyze_file(file_path)
            }
        }
    }
}

fn is_parquet(file_path: &Path) -> bool {
    file_path
        .extension()
        .map(|ext| ext.eq_ignore_ascii_case("parquet"))
        .unwrap_or(false)
        || crate::parsers::parquet::is_parquet_file(file_path)
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

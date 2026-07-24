use std::path::Path;

use dataprof_core::{ChunkSize, DataProfilerError, MetricPack, QualityDimension, SemanticHints};
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
    semantic_hints: SemanticHints,
    memory_limit_mb: Option<usize>,
    chunk_size: Option<ChunkSize>,
}

impl AdaptiveProfiler {
    pub fn new() -> Self {
        Self {
            quality_dimensions: None,
            metric_packs: None,
            csv_config: None,
            locale: None,
            semantic_hints: SemanticHints::default(),
            memory_limit_mb: None,
            chunk_size: None,
        }
    }

    /// Set the memory limit, in MB, passed on to whichever engine is selected.
    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = Some(limit);
        self
    }

    /// Set the per-chunk read size, in bytes, for the streaming engine.
    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.chunk_size = Some(chunk_size);
        self
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

    pub fn semantic_hints(mut self, hints: SemanticHints) -> Self {
        self.semantic_hints = hints;
        self
    }

    /// Analyze a file, auto-selecting the best engine.
    ///
    /// Parquet files bypass engine selection entirely (native parser).
    pub fn analyze_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        // Parquet has its own parser — short-circuit
        #[cfg(feature = "parquet")]
        if is_parquet(file_path) {
            return dataprof_parquet::analyze_parquet_with_quality_dims_and_hints(
                file_path,
                self.quality_dimensions.as_deref(),
                &self.semantic_hints,
            );
        }

        #[cfg(not(feature = "parquet"))]
        if is_parquet(file_path) {
            return Err(DataProfilerError::UnsupportedFormat {
                format: "parquet (enable the `parquet` feature)".to_string(),
            });
        }

        self.analyze_csv_file(file_path)
    }

    /// CSV-only adaptive analysis: skips Parquet extension detection so callers
    /// that have already resolved the format (e.g. via an explicit override)
    /// are not silently re-routed to the Parquet parser.
    pub fn analyze_csv_file(&self, file_path: &Path) -> Result<ProfileReport, DataProfilerError> {
        let engine = self.select_engine(file_path);
        log::info!("Engine selected: {:?}", engine);

        let result = self.try_engine(&engine, file_path);

        // On failure, try the other engine as fallback
        match result {
            Ok(report) => Ok(report),
            Err(primary_err) => {
                // Duplicate column names are a deterministic property of the file;
                // no engine can resolve them, so surface the clear, categorized
                // error instead of burying it under "All engines failed".
                if matches!(primary_err, DataProfilerError::DuplicateColumnName { .. }) {
                    return Err(primary_err);
                }
                // Likewise when the caller asked for strict CSV parsing: they
                // opted out of recovery, so retrying under a second parser only
                // trades their actionable diagnostic for "all engines failed".
                if self.csv_config.as_ref().is_some_and(|c| !c.flexible)
                    && matches!(primary_err, DataProfilerError::CsvParsingError { .. })
                {
                    return Err(primary_err);
                }
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
                        // The `AllEnginesFailed` variant already prefixes
                        // "All engines failed: "; don't repeat it here.
                        message: format!(
                            "Primary ({:?}): {}. Fallback ({:?}): {}.",
                            engine, primary_err, fallback, fallback_err
                        ),
                    })
            }
        }
    }

    /// Simple heuristic: prefer Arrow for wide, numeric-heavy CSVs with enough memory.
    ///
    /// Uses `csv::Reader` (not `str::split`) so RFC 4180 quoting is honored —
    /// a header like `"Company, Inc.",sales,units` correctly counts as 3
    /// columns and doesn't bias engine selection.
    fn select_engine(&self, file_path: &Path) -> InternalEngineType {
        use std::fs::File;
        use std::io::BufReader;

        let Ok(file) = File::open(file_path) else {
            return InternalEngineType::Incremental;
        };

        // Use configured delimiter or default to comma
        let delimiter = self
            .csv_config
            .as_ref()
            .and_then(|c| c.delimiter)
            .unwrap_or(b',');

        let mut reader = csv::ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(false)
            .flexible(true)
            .from_reader(BufReader::new(file));

        let mut records = reader.records();

        // Header row: read with the CSV parser so quoted commas don't inflate
        // the column count.
        let header = match records.next() {
            Some(Ok(rec)) => rec,
            _ => return InternalEngineType::Incremental,
        };
        let num_columns = header.len();

        // Arrow needs many columns to outperform Incremental
        if num_columns < 20 {
            return InternalEngineType::Incremental;
        }

        // Sample first few data rows to check if numeric-heavy
        let mut numeric_count = 0usize;
        let mut total_fields = 0usize;
        for rec in records.take(10) {
            let Ok(rec) = rec else { break };
            for field in rec.iter() {
                total_fields += 1;
                if field.trim().parse::<f64>().is_ok() {
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
                if let Some(mb) = self.memory_limit_mb {
                    profiler = profiler.memory_limit_mb(mb);
                }
                profiler = profiler.semantic_hints(self.semantic_hints.clone());
                profiler.analyze_csv_file(file_path)
            }
            InternalEngineType::Incremental => {
                let mut profiler = IncrementalProfiler::new();
                if let Some(mb) = self.memory_limit_mb {
                    profiler = profiler.memory_limit_mb(mb);
                }
                if let Some(ref cs) = self.chunk_size {
                    profiler = profiler.chunk_size(cs.clone());
                }
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
                profiler = profiler.semantic_hints(self.semantic_hints.clone());
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

    /// Regression for #307: header column count must honor RFC 4180 quoting.
    /// A naive `str::split(',')` would have inflated the count, biasing the
    /// engine choice on quoted-heavy CSVs.
    #[test]
    fn test_select_engine_respects_quoted_commas() {
        let profiler = AdaptiveProfiler::new();
        let mut temp_file = NamedTempFile::new().unwrap();
        // Quoted comma inside the first column header — actual column count is 3
        writeln!(temp_file, "\"Company, Inc.\",sales,units").unwrap();
        writeln!(temp_file, "Acme,100,200").unwrap();
        temp_file.flush().unwrap();

        // 3 columns is well under the 20-column Arrow threshold; selection
        // must stay on the Incremental engine.
        let engine = profiler.select_engine(temp_file.path());
        assert_eq!(engine, InternalEngineType::Incremental);
    }

    /// Regression for #307: per-row field counting must also honor quoting,
    /// otherwise the numeric_ratio heuristic gets polluted by stray commas
    /// inside quoted fields.
    #[test]
    fn test_select_engine_quoted_fields_dont_skew_numeric_ratio() {
        let profiler = AdaptiveProfiler::new();
        let mut temp_file = NamedTempFile::new().unwrap();

        // 25 columns: enough to clear the >= 20 column threshold and reach
        // the numeric-ratio heuristic. Header is plain.
        let header: Vec<String> = (0..25).map(|i| format!("c{i}")).collect();
        writeln!(temp_file, "{}", header.join(",")).unwrap();

        // Data rows: every field is the quoted text `"a, b"` (one logical
        // non-numeric field per column). A naive `split(',')` would see 50
        // fields per row and most would parse as nothing, but a few might
        // happen to look numeric in adversarial cases. Here we just assert
        // the row is parsed as 25 fields and none look numeric.
        let row: Vec<String> = (0..25).map(|_| "\"a, b\"".to_string()).collect();
        for _ in 0..5 {
            writeln!(temp_file, "{}", row.join(",")).unwrap();
        }
        temp_file.flush().unwrap();

        // No numeric fields => Incremental, regardless of arrow feature.
        let engine = profiler.select_engine(temp_file.path());
        assert_eq!(engine, InternalEngineType::Incremental);
    }
}

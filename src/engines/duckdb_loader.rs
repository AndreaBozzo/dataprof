use crate::core::config::ProfilerConfig;
use crate::engines::columnar::arrow_profiler::ArrowProfiler;
use crate::analysis::AnalysisResult;
use anyhow::{Context, Result};
use duckdb::{Connection, Arrow};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

pub struct DuckDbLoader {
    config: Arc<ProfilerConfig>,
}

impl DuckDbLoader {
    pub fn new(config: Arc<ProfilerConfig>) -> Self {
        Self { config }
    }

    pub fn profile_query(&self, query: &str) -> Result<AnalysisResult> {
        
        let conn = Connection::open_in_memory()
            .context("Failed to open DuckDB connection")?;

            .context(format!("Failed to prepare query: {}", query))?;

        let arrow_stream = stmt.query_arrow([])
            .context("Failed to execute query via Arrow integration")?;

        let mut profiler = ArrowProfiler::new(&self.config);

        for batch in arrow_stream {
            let record_batch = batch.context("Failed to read Arrow batch")?;
            profiler.process_batch(&record_batch)?;
        }
        Ok(profiler.finalize())
    }
}
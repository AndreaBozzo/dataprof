//! DataFusion SQL query engine integration
//!
//! Provides SQL-based data profiling using Apache DataFusion.
//! DataFusion is a fast, extensible query engine built on Apache Arrow.

use crate::engines::columnar::RecordBatchAnalyzer;
use crate::types::{DataQualityMetrics, DataSource, QualityReport, QueryEngine, ScanInfo};

use anyhow::{Context, Result};
use datafusion::prelude::*;
use futures::stream::{Stream, StreamExt};
use std::time::Instant;

/// DataFusion loader for profiling SQL queries using Arrow integration
pub struct DataFusionLoader {
    batch_size: usize,
    ctx: SessionContext,
}

impl Default for DataFusionLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl DataFusionLoader {
    pub fn new() -> Self {
        let config = SessionConfig::new().with_batch_size(8192);
        let ctx = SessionContext::new_with_config(config);

        Self {
            batch_size: 8192,
            ctx,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Get the underlying SessionContext for advanced configuration
    pub fn context(&self) -> &SessionContext {
        &self.ctx
    }

    /// Get mutable access to the SessionContext
    pub fn context_mut(&mut self) -> &mut SessionContext {
        &mut self.ctx
    }

    /// Register a CSV file as a table for querying
    pub async fn register_csv(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_csv(table_name, path, CsvReadOptions::default())
            .await
            .context(format!(
                "Failed to register CSV file '{}' as '{}'",
                path, table_name
            ))?;
        Ok(())
    }

    /// Register a Parquet file as a table for querying
    pub async fn register_parquet(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_parquet(table_name, path, ParquetReadOptions::default())
            .await
            .context(format!(
                "Failed to register Parquet file '{}' as '{}'",
                path, table_name
            ))?;
        Ok(())
    }

    /// Register a JSON file as a table for querying
    pub async fn register_json(&self, table_name: &str, path: &str) -> Result<()> {
        self.ctx
            .register_json(table_name, path, NdJsonReadOptions::default())
            .await
            .context(format!(
                "Failed to register JSON file '{}' as '{}'",
                path, table_name
            ))?;
        Ok(())
    }

    /// Execute a SQL query and profile the results using Arrow
    pub async fn profile_query(&self, query: &str) -> Result<QualityReport> {
        let start = Instant::now();
        log::info!("DataFusion: Preparing query");

        // Execute query and get DataFrame
        let df = self
            .ctx
            .sql(query)
            .await
            .context(format!("Failed to execute query: '{}'", query))?;

        // Collect as RecordBatch stream
        let batches = df
            .collect()
            .await
            .context("Failed to collect query results")?;

        // Initialize the RecordBatchAnalyzer
        let mut analyzer = RecordBatchAnalyzer::new();
        let mut batch_count = 0;

        // Process each batch
        for record_batch in batches {
            if record_batch.num_rows() > 0 {
                batch_count += 1;
                analyzer.process_batch(&record_batch)?;
            }
        }

        let total_rows = analyzer.total_rows();
        log::info!(
            "DataFusion: Processed {} rows in {} batches",
            total_rows, batch_count
        );

        // Build the report
        let column_profiles = analyzer.to_profiles();
        let sample_columns = analyzer.create_sample_columns();

        // Calculate quality metrics
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        Ok(QualityReport::new(
            DataSource::Query {
                engine: QueryEngine::DataFusion,
                statement: query.to_string(),
                database: None,
                execution_id: None,
            },
            column_profiles,
            ScanInfo::new(total_rows, num_columns, total_rows, 1.0, scan_time_ms),
            data_quality_metrics,
        ))
    }

    /// Profile a registered table directly
    pub async fn profile_table(&self, table_name: &str) -> Result<QualityReport> {
        if !table_name.chars().all(|c| c.is_alphanumeric() || c == '_') {
            return Err(anyhow::anyhow!("Invalid table name: {}", table_name));
        }

        let query = format!("SELECT * FROM {}", table_name);
        self.profile_query(&query).await
    }

    /// Execute a SQL query and profile the results using Arrow (streaming version)
    pub async fn profile_query_streaming(
        &self,
        query: &str,
    ) -> Result<impl Stream<Item = Result<QualityReport>>> {
        let start = Instant::now();
        log::info!("DataFusion: Preparing query (streaming)");

        // Execute query and get DataFrame
        let df = self
            .ctx
            .sql(query)
            .await
            .context(format!("Failed to execute query: '{}'", query))?;

        // Initialize the RecordBatchAnalyzer
        let mut analyzer = RecordBatchAnalyzer::new();

        // Stream batches and process each one
        let stream = df
            .execute_stream()
            .await
            .context("Failed to execute query stream")?
            .map(move |batch| {
                let batch = batch.context("Failed to fetch batch")?;
                if batch.num_rows() > 0 {
                    analyzer.process_batch(&batch)?;
                }
                let column_profiles = analyzer.to_profiles();
                let sample_columns = analyzer.create_sample_columns();

                let total_rows = analyzer.total_rows();

                // Calculate quality metrics
                let data_quality_metrics =
                    DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                        .map_err(|e| {
                            anyhow::anyhow!("Quality metrics calculation failed: {}", e)
                        })?;

                let scan_time_ms = start.elapsed().as_millis();
                let num_columns = column_profiles.len();

                Ok(QualityReport::new(
                    DataSource::Query {
                        engine: QueryEngine::DataFusion,
                        statement: query.to_string(),
                        database: None,
                        execution_id: None,
                    },
                    column_profiles,
                    ScanInfo::new(total_rows, num_columns, total_rows, 1.0, scan_time_ms),
                    data_quality_metrics,
                ))
            });

        Ok(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::Builder;

    #[tokio::test]
    async fn test_datafusion_csv_profiling() -> Result<()> {
        let mut temp_file = Builder::new().suffix(".csv").tempfile()?;
        writeln!(temp_file, "name,age,salary")?;
        writeln!(temp_file, "Alice,25,50000.0")?;
        writeln!(temp_file, "Bob,30,60000.5")?;
        writeln!(temp_file, "Charlie,35,70000.0")?;
        temp_file.flush()?;

        let loader = DataFusionLoader::new();
        loader
            .register_csv("test_table", temp_file.path().to_str().unwrap())
            .await?;

        let report = loader.profile_query("SELECT * FROM test_table").await?;

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.scan_info.total_rows, 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_datafusion_sql_aggregation() -> Result<()> {
        let mut temp_file = Builder::new().suffix(".csv").tempfile()?;
        writeln!(temp_file, "category,value")?;
        writeln!(temp_file, "A,10")?;
        writeln!(temp_file, "B,20")?;
        writeln!(temp_file, "A,30")?;
        writeln!(temp_file, "B,40")?;
        temp_file.flush()?;

        let loader = DataFusionLoader::new();
        loader
            .register_csv("data", temp_file.path().to_str().unwrap())
            .await?;

        let report = loader
            .profile_query("SELECT category, SUM(value) as total FROM data GROUP BY category")
            .await?;

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.scan_info.total_rows, 2); // 2 groups

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_table_name() -> Result<()> {
        let loader = DataFusionLoader::new();
        let result = loader.profile_table("invalid-table-name").await;
        assert!(result.is_err());
        Ok(())
    }
}

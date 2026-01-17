use crate::engines::columnar::RecordBatchAnalyzer;
use crate::types::{DataQualityMetrics, DataSource, QualityReport, QueryEngine, ScanInfo};

use anyhow::{Context, Result};
use duckdb::{Arrow, Connection};
use std::time::Instant;

/// DuckDB loader for profiling SQL queries using Arrow integration
pub struct DuckDbLoader {
    batch_size: usize,
}

impl Default for DuckDbLoader {
    fn default() -> Self {
        Self::new()
    }
}

impl DuckDbLoader {
    pub fn new() -> Self {
        Self { batch_size: 8192 }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Esegue una query SQL (su file o in-memory) e profila i risultati usando Arrow.
    /// Non materializza mai tutto il dataset in RAM.
    pub fn profile_query(&self, query: &str) -> Result<QualityReport> {
        let start = Instant::now();
        println!("🦆 DuckDB: Preparing query...");

        // 1. Setup DuckDB (In-Memory)
        let conn = Connection::open_in_memory().context("Failed to open DuckDB connection")?;

        // 2. Prepara lo statement
        let mut stmt = conn
            .prepare(query)
            .context(format!("Failed to prepare query: '{}'", query))?;

        // 3. Ottieni lo stream Arrow (ZERO COPY)
        // Questo non esegue ancora tutta la query, prepara solo l'iteratore
        let arrow_stream: Arrow<'_> = stmt
            .query_arrow([])
            .context("Failed to execute query via Arrow integration")?;

        // 4. Inizializza il RecordBatchAnalyzer
        let mut analyzer = RecordBatchAnalyzer::new();

        let mut batch_count = 0;

        // 5. Loop sui batch (Streaming reale)
        for record_batch in arrow_stream {
            if record_batch.num_rows() > 0 {
                batch_count += 1;
                // Passiamo il batch all'analyzer
                analyzer.process_batch(&record_batch)?;
            }
        }

        let total_rows = analyzer.total_rows();
        println!(
            "🦆 DuckDB: Processed {} rows in {} batches",
            total_rows, batch_count
        );

        // 6. Costruisci il report
        let column_profiles = analyzer.to_profiles();
        let sample_columns = analyzer.create_sample_columns();

        // Calcola le metriche di qualità
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        Ok(QualityReport::new(
            DataSource::Query {
                engine: QueryEngine::DuckDb,
                statement: query.to_string(),
                database: None, // In-memory
                execution_id: None,
            },
            column_profiles,
            ScanInfo::new(total_rows, num_columns, total_rows, 1.0, scan_time_ms),
            data_quality_metrics,
        ))
    }
}

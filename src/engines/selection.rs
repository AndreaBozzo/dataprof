use anyhow::Result;
use std::path::Path;
use sysinfo::System;

/// Characteristics analyzed for intelligent engine selection
#[derive(Debug, Clone)]
pub struct FileCharacteristics {
    pub file_size_mb: f64,
    pub estimated_rows: Option<usize>,
    pub estimated_columns: Option<usize>,
    pub has_mixed_types: bool,
    pub has_numeric_majority: bool,
    pub complexity_score: f64, // 0.0 = simple, 1.0 = complex
}

/// System resources available for processing
#[derive(Debug, Clone)]
pub struct SystemResources {
    pub available_memory_mb: f64,
    pub cpu_cores: usize,
    pub memory_pressure: f64, // 0.0 = low, 1.0 = high
}

/// Processing context for engine selection
#[derive(Debug, Clone)]
pub enum ProcessingType {
    BatchAnalysis,
    StreamingRequired,
    AggregationHeavy,
    QualityFocused,
}

/// Engine selection recommendation with confidence score
#[derive(Debug, Clone)]
pub struct EngineRecommendation {
    pub primary_engine: EngineType,
    pub fallback_engines: Vec<EngineType>,
    pub confidence: f64, // 0.0 = low, 1.0 = high
    pub reasoning: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EngineType {
    Arrow,
    TrueStreaming,
    MemoryEfficient,
    Streaming,
}

/// Intelligent engine selector
pub struct EngineSelector {
    system_resources: SystemResources,
    arrow_available: bool,
}

impl EngineSelector {
    pub fn new() -> Self {
        let mut system = System::new_all();
        system.refresh_all();

        let total_memory = system.total_memory() as f64 / 1_048_576.0; // Convert to MB
        let available_memory = system.available_memory() as f64 / 1_048_576.0;
        let memory_pressure = 1.0 - (available_memory / total_memory);

        Self {
            system_resources: SystemResources {
                available_memory_mb: available_memory,
                cpu_cores: num_cpus::get(),
                memory_pressure,
            },
            arrow_available: Self::detect_arrow_availability(),
        }
    }

    /// Create EngineSelector with fixed system resources for testing
    #[cfg(test)]
    pub fn new_with_resources(
        available_memory_mb: f64,
        cpu_cores: usize,
        memory_pressure: f64,
        arrow_available: bool,
    ) -> Self {
        Self {
            system_resources: SystemResources {
                available_memory_mb,
                cpu_cores,
                memory_pressure,
            },
            arrow_available,
        }
    }

    /// Detect if Arrow is available at runtime
    fn detect_arrow_availability() -> bool {
        // Try to create an ArrowProfiler to test availability
        #[cfg(feature = "arrow")]
        {
            true
        }
        #[cfg(not(feature = "arrow"))]
        {
            false
        }
    }

    /// Analyze file characteristics for intelligent selection
    pub fn analyze_file_characteristics(&self, file_path: &Path) -> Result<FileCharacteristics> {
        let metadata = std::fs::metadata(file_path)?;
        let file_size_mb = metadata.len() as f64 / 1_048_576.0;

        // Quick scan of first few lines to estimate structure
        let (estimated_rows, estimated_columns, has_mixed_types, has_numeric_majority) =
            self.quick_file_scan(file_path, &metadata)?;

        // Calculate complexity score based on various factors
        let complexity_score = self.calculate_complexity_score(
            file_size_mb,
            estimated_columns.unwrap_or(0),
            has_mixed_types,
        );

        Ok(FileCharacteristics {
            file_size_mb,
            estimated_rows,
            estimated_columns,
            has_mixed_types,
            has_numeric_majority,
            complexity_score,
        })
    }

    /// Quick scan to estimate file structure
    fn quick_file_scan(
        &self,
        file_path: &Path,
        metadata: &std::fs::Metadata,
    ) -> Result<(Option<usize>, Option<usize>, bool, bool)> {
        use std::fs::File;
        use std::io::{BufRead, BufReader};

        let file = File::open(file_path)?;
        let mut reader = BufReader::new(file);
        let mut line = String::new();

        // Read first line to get column count
        reader.read_line(&mut line)?;
        let columns = line.split(',').count();

        // Sample a few lines to detect data types
        let mut numeric_columns = 0;
        let mixed_type_columns = 0;
        let mut line_count = 0;

        // Reset reader for sampling
        let file = File::open(file_path)?;
        let reader = BufReader::new(file);

        for (i, line_result) in reader.lines().enumerate() {
            if i == 0 {
                continue;
            } // Skip header
            if i > 100 {
                break;
            } // Sample first 100 lines

            let line = line_result?;
            let values: Vec<&str> = line.split(',').collect();

            // Analyze each column for type consistency
            for (col_idx, value) in values.iter().enumerate() {
                if col_idx == 0 && i == 1 {
                    // Initialize tracking for each column
                }

                // Simple type detection
                if value.trim().parse::<f64>().is_ok() && col_idx == 0 {
                    numeric_columns += 1;
                }
            }

            line_count += 1;
        }

        // Estimate total rows based on file size and sampled line length
        let estimated_rows = if line_count > 0 {
            Some((metadata.len() as f64 / (line_count as f64 * 50.0)) as usize) // Rough estimate
        } else {
            None
        };

        let has_numeric_majority = numeric_columns > columns / 2;
        let has_mixed_types = mixed_type_columns > 0;

        Ok((
            estimated_rows,
            Some(columns),
            has_mixed_types,
            has_numeric_majority,
        ))
    }

    /// Calculate file complexity score
    fn calculate_complexity_score(
        &self,
        file_size_mb: f64,
        columns: usize,
        has_mixed_types: bool,
    ) -> f64 {
        let mut score = 0.0;

        // Size factor (larger files are more complex to process)
        score += (file_size_mb / 1000.0).min(0.3);

        // Column count factor (more columns = more complex)
        score += (columns as f64 / 100.0).min(0.3);

        // Mixed types add complexity
        if has_mixed_types {
            score += 0.2;
        }

        // Memory pressure adds complexity
        score += self.system_resources.memory_pressure * 0.2;

        score.min(1.0)
    }

    /// Select the best engine based on all factors
    pub fn select_engine(
        &self,
        characteristics: &FileCharacteristics,
        processing_type: ProcessingType,
    ) -> EngineRecommendation {
        let mut scores = std::collections::HashMap::new();

        // Score each engine type
        scores.insert(
            EngineType::Arrow,
            self.score_arrow(characteristics, &processing_type),
        );
        scores.insert(
            EngineType::TrueStreaming,
            self.score_true_streaming(characteristics, &processing_type),
        );
        scores.insert(
            EngineType::MemoryEfficient,
            self.score_memory_efficient(characteristics, &processing_type),
        );
        scores.insert(
            EngineType::Streaming,
            self.score_streaming(characteristics, &processing_type),
        );

        // Find the best engine
        let mut sorted_engines: Vec<_> = scores.into_iter().collect();
        sorted_engines.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));


        let primary_engine = sorted_engines[0].0.clone();
        let confidence = sorted_engines[0].1;

        // Create fallback list (excluding primary)
        let fallback_engines: Vec<_> = sorted_engines
            .iter()
            .skip(1)
            .filter(|(_, score)| *score > 0.3) // Only include viable alternatives
            .map(|(engine, _)| engine.clone())
            .collect();

        let reasoning = self.generate_reasoning(&primary_engine, characteristics, &processing_type);

        EngineRecommendation {
            primary_engine,
            fallback_engines,
            confidence,
            reasoning,
        }
    }

    fn score_arrow(
        &self,
        characteristics: &FileCharacteristics,
        processing_type: &ProcessingType,
    ) -> f64 {
        if !self.arrow_available {
            return 0.0; // Can't use if not available
        }

        let mut score: f64 = 0.0;

        // Arrow excels with large files
        if characteristics.file_size_mb > 100.0 {
            score += 0.4;
        }

        // Arrow is great for many columns
        if let Some(columns) = characteristics.estimated_columns {
            if columns > 20 {
                score += 0.3;
            }
        }

        // Arrow prefers homogeneous data types
        if !characteristics.has_mixed_types && characteristics.has_numeric_majority {
            score += 0.3;
        }

        // Batch processing benefits from Arrow
        if matches!(
            processing_type,
            ProcessingType::BatchAnalysis | ProcessingType::AggregationHeavy
        ) {
            score += 0.2;
        }

        // Memory availability
        if self.system_resources.available_memory_mb > 1000.0 {
            score += 0.1;
        }

        score.min(1.0)
    }

    fn score_true_streaming(
        &self,
        characteristics: &FileCharacteristics,
        processing_type: &ProcessingType,
    ) -> f64 {
        let mut score: f64 = 0.0;

        // True streaming for very large files
        if characteristics.file_size_mb > 500.0 {
            score += 0.4;
        }

        // Good for memory-constrained environments
        if self.system_resources.memory_pressure > 0.7 {
            score += 0.3;
        }

        // Required for streaming operations
        if matches!(processing_type, ProcessingType::StreamingRequired) {
            score += 0.5;
        }

        // Handles complex data well
        if characteristics.complexity_score > 0.6 {
            score += 0.2;
        }

        score.min(1.0)
    }

    fn score_memory_efficient(
        &self,
        characteristics: &FileCharacteristics,
        _processing_type: &ProcessingType,
    ) -> f64 {
        let mut score: f64 = 0.0;

        // Good for medium-sized files
        if characteristics.file_size_mb > 50.0 && characteristics.file_size_mb < 500.0 {
            score += 0.4;
        }

        // Good balance of memory and performance
        if self.system_resources.memory_pressure > 0.3
            && self.system_resources.memory_pressure < 0.7
        {
            score += 0.3;
        }

        // Moderate complexity
        if characteristics.complexity_score > 0.3 && characteristics.complexity_score < 0.7 {
            score += 0.3;
        }

        score.min(1.0)
    }

    fn score_streaming(
        &self,
        characteristics: &FileCharacteristics,
        _processing_type: &ProcessingType,
    ) -> f64 {
        let mut score: f64 = 0.0;

        // Good for smaller files
        if characteristics.file_size_mb < 100.0 {
            score += 0.4;
        }

        // Low complexity data
        if characteristics.complexity_score < 0.5 {
            score += 0.3;
        }

        // Plenty of memory available
        if self.system_resources.memory_pressure < 0.3 {
            score += 0.3;
        }

        score.min(1.0)
    }

    fn generate_reasoning(
        &self,
        engine: &EngineType,
        characteristics: &FileCharacteristics,
        _processing_type: &ProcessingType,
    ) -> String {
        match engine {
            EngineType::Arrow => format!(
                "Arrow selected for {:.1}MB file with {} columns. Optimal for columnar processing and aggregations.",
                characteristics.file_size_mb,
                characteristics.estimated_columns.unwrap_or(0)
            ),
            EngineType::TrueStreaming => format!(
                "True streaming selected for {:.1}MB file due to memory constraints (pressure: {:.1}) or streaming requirements.",
                characteristics.file_size_mb,
                self.system_resources.memory_pressure
            ),
            EngineType::MemoryEfficient => format!(
                "Memory-efficient engine selected for {:.1}MB file with moderate complexity ({:.2}).",
                characteristics.file_size_mb,
                characteristics.complexity_score
            ),
            EngineType::Streaming => format!(
                "Standard streaming selected for {:.1}MB file with low complexity ({:.2}).",
                characteristics.file_size_mb,
                characteristics.complexity_score
            ),
        }
    }
}

impl Default for EngineSelector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_engine_selection_small_file() -> Result<()> {
        // Use fixed resources for deterministic testing
        let selector = EngineSelector::new_with_resources(
            4096.0, // 4GB available memory
            4,      // 4 CPU cores
            0.3,    // Low memory pressure
            false,  // Arrow not available
        );

        // Create small test file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,salary")?;
        writeln!(temp_file, "Alice,25,50000")?;
        writeln!(temp_file, "Bob,30,60000")?;
        temp_file.flush()?;

        let characteristics = selector.analyze_file_characteristics(temp_file.path())?;
        let recommendation =
            selector.select_engine(&characteristics, ProcessingType::BatchAnalysis);

        // Small files should prefer streaming
        assert!(matches!(
            recommendation.primary_engine,
            EngineType::Streaming
        ));

        Ok(())
    }

    #[test]
    fn test_engine_selection_large_file() {
        // Use fixed resources for deterministic testing
        // High memory pressure to ensure TrueStreaming gets bonus points
        let selector = EngineSelector::new_with_resources(
            2048.0, // 2GB available memory
            8,      // 8 CPU cores
            0.75,   // High memory pressure (>0.7) for TrueStreaming bonus
            false,  // Arrow not available for this test
        );

        let characteristics = FileCharacteristics {
            file_size_mb: 1200.0, // Much larger to clearly favor TrueStreaming
            estimated_rows: Some(5_000_000),
            estimated_columns: Some(50),
            has_mixed_types: false,
            has_numeric_majority: true,
            complexity_score: 0.4,
        };

        let recommendation =
            selector.select_engine(&characteristics, ProcessingType::BatchAnalysis);

        // Large files with numeric data should prefer Arrow if available, otherwise TrueStreaming
        if selector.arrow_available {
            assert!(matches!(recommendation.primary_engine, EngineType::Arrow));
        } else {
            assert!(matches!(
                recommendation.primary_engine,
                EngineType::TrueStreaming
            ));
        }
    }
}

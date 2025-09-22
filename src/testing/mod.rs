/// Testing utilities and dataset generation
pub mod dataset_generator;
pub mod engine_benchmarks;
pub mod result_collection;

pub use dataset_generator::{
    DatasetConfig, DatasetGenerator, DatasetPattern, DatasetSize, StandardDatasets,
};
pub use engine_benchmarks::{
    EngineBenchmarkConfig, EngineBenchmarkFramework, EngineComparisonResult,
    EngineSelectionAccuracy, EngineSelectionTestResult, SelectionOutcome, TradeoffAnalysis,
};
pub use result_collection::{
    BenchmarkResult,
    BenchmarkSuiteResults,
    CriterionResultParams,
    DatasetInfo,
    ExecutionMetadata,
    FileBenchmarkResult,
    MetricCollection,
    MetricMeasurement,
    // New comprehensive metrics system
    MetricType,
    PerformanceAccuracyAnalysis,
    ResultCollector,
    SuiteMetadata,
    SystemInfo,
    TradeoffRating,
};

// Domain datasets functionality
pub mod domain {
    include!("../../tests/fixtures/domain_datasets.rs");
}

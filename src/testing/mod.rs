/// Testing utilities and dataset generation
pub mod dataset_generator;
pub mod result_collection;

pub use dataset_generator::{
    DatasetConfig, DatasetGenerator, DatasetPattern, DatasetSize, StandardDatasets,
};
pub use result_collection::{
    BenchmarkResult, BenchmarkSuiteResults, FileBenchmarkResult, ResultCollector, SuiteMetadata,
};

// Domain datasets functionality
pub mod domain {
    include!("../../tests/fixtures/domain_datasets.rs");
}

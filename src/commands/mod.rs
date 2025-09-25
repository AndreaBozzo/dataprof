pub mod analyze;
pub mod batch;
pub mod benchmark;
pub mod script_generator;

#[cfg(feature = "database")]
pub mod database;

pub use analyze::run_analysis;
pub use benchmark::show_engine_info;
pub use script_generator::generate_preprocessing_script;

#[cfg(feature = "database")]
#[allow(unused_imports)] // Conditional compilation
pub use database::run_database_analysis;

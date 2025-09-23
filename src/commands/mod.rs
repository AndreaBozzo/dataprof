pub mod analyze;
pub mod batch;
pub mod benchmark;

#[cfg(feature = "database")]
pub mod database;

pub use analyze::run_analysis;
pub use benchmark::show_engine_info;

#[cfg(feature = "database")]
#[allow(unused_imports)] // Conditional compilation
pub use database::run_database_analysis;

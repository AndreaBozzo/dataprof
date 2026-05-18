pub(crate) mod adaptive;
pub mod columnar;
pub(crate) mod common;
pub mod streaming;

#[cfg(feature = "datafusion")]
pub mod datafusion_loader;

pub use streaming::*;

#[cfg(feature = "datafusion")]
pub use datafusion_loader::{CsvReadOptions, DataFusionLoader, RecordBatch};

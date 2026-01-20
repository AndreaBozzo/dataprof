pub mod adaptive;
pub mod columnar;
pub mod selection;
pub mod streaming;

#[cfg(feature = "datafusion")]
pub mod datafusion_loader;

#[cfg(any(feature = "connectorx-postgres", feature = "connectorx-mysql"))]
pub mod connectorx_loader;

pub use adaptive::*;
pub use selection::*;
pub use streaming::*;

#[cfg(feature = "datafusion")]
pub use datafusion_loader::DataFusionLoader;

#[cfg(any(feature = "connectorx-postgres", feature = "connectorx-mysql"))]
pub use connectorx_loader::{ConnectorXConfig, ConnectorXLoader, DatabaseType};

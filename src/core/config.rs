use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

// ============================================================================
// Configuration Constants
// ============================================================================
// These constants define the default values for various configuration options.
// Each constant is documented with its rationale to avoid "magic numbers".

// Output Configuration Defaults
/// Default output format for CLI commands.
/// "text" provides human-readable output suitable for terminal display.
const DEFAULT_OUTPUT_FORMAT: &str = "text";

/// Enable colored output by default.
/// Colors improve readability in modern terminals that support ANSI codes.
const DEFAULT_COLORED_OUTPUT: bool = true;

/// Default verbosity level (1 = normal).
/// 0=quiet, 1=normal, 2=verbose, 3=debug
/// Level 1 strikes a balance between informativeness and noise.
const DEFAULT_VERBOSITY: u8 = 1;

/// Show progress bars by default.
/// Progress feedback is important for long-running operations.
const DEFAULT_SHOW_PROGRESS: bool = true;

// HTML Report Defaults
/// Don't auto-generate HTML reports by default.
/// Users should opt-in to HTML generation to avoid unexpected file creation.
const DEFAULT_HTML_AUTO_GENERATE: bool = false;

/// Include detailed statistics in HTML reports when generated.
/// Detailed stats provide more value for quality analysis.
const DEFAULT_HTML_DETAILED_STATS: bool = true;

// Quality Configuration Defaults
/// Enable quality checking by default.
/// Quality analysis is a core feature of DataProf.
/// When enabled, all ISO 8000/25012 quality metrics are calculated.
const DEFAULT_QUALITY_ENABLED: bool = true;

// Engine Configuration Defaults
/// Default engine selection: "auto".
/// Automatic engine selection adapts to file size and format.
const DEFAULT_ENGINE: &str = "auto";

/// Enable parallel processing by default.
/// Modern systems benefit from parallel processing for large datasets.
const DEFAULT_PARALLEL_PROCESSING: bool = true;

// Memory Configuration Defaults
/// Maximum memory usage in MB (0 = unlimited).
/// 0 allows the OS to manage memory, suitable for most use cases.
const DEFAULT_MAX_MEMORY_MB: usize = 0;

/// Enable memory monitoring by default.
/// Monitoring helps detect memory leaks and optimize performance.
const DEFAULT_MEMORY_MONITORING: bool = true;

/// Auto-switch to streaming for files larger than 100MB.
/// 100MB is a reasonable threshold where streaming provides clear benefits
/// over loading the entire file into memory.
const DEFAULT_AUTO_STREAMING_THRESHOLD_MB: f64 = 100.0;

// Database Configuration Defaults (when database feature is enabled)
#[cfg(feature = "database")]
/// Default connection timeout in seconds.
/// 30 seconds allows for slow networks while avoiding indefinite hangs.
const DEFAULT_DB_CONNECTION_TIMEOUT_SECS: u64 = 30;

#[cfg(feature = "database")]
/// Default batch size for database queries.
/// 10,000 rows per batch balances memory usage and query performance.
const DEFAULT_DB_BATCH_SIZE: usize = 10_000;

#[cfg(feature = "database")]
/// Maximum number of database connections in the pool.
/// 10 connections supports moderate concurrency without overwhelming the database.
const DEFAULT_DB_MAX_CONNECTIONS: usize = 10;

#[cfg(feature = "database")]
/// Enable SSL by default for security.
/// SSL should be the default for production database connections.
const DEFAULT_DB_SSL_ENABLED: bool = true;

#[cfg(feature = "database")]
/// Enable sampling for large database tables by default.
/// Sampling provides faster analysis for exploratory data profiling.
const DEFAULT_DB_SAMPLING_ENABLED: bool = true;

#[cfg(feature = "database")]
/// Default sample size for database tables.
/// 100,000 rows provides statistical significance for most analyses
/// while keeping query times reasonable.
const DEFAULT_DB_SAMPLE_SIZE: usize = 100_000;

#[cfg(feature = "database")]
/// Threshold for automatic sampling (number of rows).
/// Tables with over 1 million rows benefit significantly from sampling.
const DEFAULT_DB_AUTO_SAMPLE_THRESHOLD: usize = 1_000_000;

/// Main configuration structure for DataProfiler CLI
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataprofConfig {
    /// Output configuration
    pub output: OutputConfig,

    /// Quality checking configuration
    pub quality: QualityConfig,

    /// Engine selection and performance tuning
    pub engine: EngineConfig,

    /// Database configuration
    #[cfg(feature = "database")]
    pub database: Option<DatabaseSettings>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Default output format (text, json, csv, yaml, plain)
    pub default_format: String,

    /// Enable colored output by default
    pub colored: bool,

    /// Default verbosity level (0=quiet, 1=normal, 2=verbose, 3=debug)
    pub verbosity: u8,

    /// Show progress bars by default
    pub show_progress: bool,

    /// Default HTML report settings
    pub html: HtmlConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HtmlConfig {
    /// Auto-generate HTML reports for quality analysis
    pub auto_generate: bool,

    /// Default output directory for HTML reports
    pub output_dir: Option<PathBuf>,

    /// Include detailed statistics in HTML reports
    pub include_detailed_stats: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityConfig {
    /// Enable quality checking by default
    pub enabled: bool,

    /// ISO 8000/25012 compliant thresholds
    ///
    /// All quality metrics (null detection, duplicate detection, type consistency,
    /// date format checking, outlier detection, etc.) are controlled through
    /// these ISO-compliant thresholds.
    ///
    /// Use `IsoQualityThresholds::strict()` for high-compliance industries,
    /// `IsoQualityThresholds::lenient()` for exploratory data, or customize
    /// individual thresholds as needed.
    pub iso_thresholds: IsoQualityThresholds,
}

/// ISO 8000/25012 compliant quality thresholds
/// These thresholds are configurable for industry-specific requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsoQualityThresholds {
    // Completeness thresholds (ISO 8000-8)
    /// Maximum acceptable null percentage for a column (default: 50%)
    pub max_null_percentage: f64,

    /// Threshold for reporting null value issues (default: 10%)
    pub null_report_threshold: f64,

    // Consistency thresholds (ISO 8000-61)
    /// Minimum acceptable type consistency percentage (default: 95%)
    pub min_type_consistency: f64,

    // Uniqueness thresholds (ISO 8000-110)
    /// Threshold for reporting duplicate rows (default: 5%)
    pub duplicate_report_threshold: f64,

    /// High cardinality warning threshold (uniqueness ratio, default: 95%)
    pub high_cardinality_threshold: f64,

    // Accuracy thresholds (ISO 25012)
    /// IQR multiplier for outlier detection (default: 1.5, ISO standard)
    /// Values outside [Q1 - k*IQR, Q3 + k*IQR] are considered outliers
    pub outlier_iqr_multiplier: f64,

    /// Minimum samples required for outlier detection (default: 4)
    pub outlier_min_samples: usize,

    // Timeliness thresholds (ISO 8000-8)
    /// Maximum age in years for data to be considered fresh (default: 5 years)
    pub max_data_age_years: f64,

    /// Percentage threshold for reporting stale data (default: 20%)
    pub stale_data_threshold: f64,
}

impl Default for IsoQualityThresholds {
    fn default() -> Self {
        Self {
            // ISO 8000-8 Completeness
            max_null_percentage: 50.0,
            null_report_threshold: 10.0,

            // ISO 8000-61 Consistency
            min_type_consistency: 95.0,

            // ISO 8000-110 Uniqueness
            duplicate_report_threshold: 5.0,
            high_cardinality_threshold: 95.0,

            // ISO 25012 Accuracy
            outlier_iqr_multiplier: 1.5, // ISO standard
            outlier_min_samples: 4,

            // ISO 8000-8 Timeliness
            max_data_age_years: 5.0,
            stale_data_threshold: 20.0,
        }
    }
}

impl IsoQualityThresholds {
    /// Create strict thresholds for high-compliance industries (finance, healthcare)
    pub fn strict() -> Self {
        Self {
            max_null_percentage: 30.0,
            null_report_threshold: 5.0,
            min_type_consistency: 98.0,
            duplicate_report_threshold: 1.0,
            high_cardinality_threshold: 98.0,
            outlier_iqr_multiplier: 1.5,
            outlier_min_samples: 10,
            max_data_age_years: 2.0, // Financial data must be recent
            stale_data_threshold: 10.0,
        }
    }

    /// Create lenient thresholds for exploratory/marketing data
    pub fn lenient() -> Self {
        Self {
            max_null_percentage: 70.0,
            null_report_threshold: 20.0,
            min_type_consistency: 90.0,
            duplicate_report_threshold: 10.0,
            high_cardinality_threshold: 90.0,
            outlier_iqr_multiplier: 2.0,
            outlier_min_samples: 4,
            max_data_age_years: 10.0, // Historical analysis tolerates older data
            stale_data_threshold: 30.0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// Default engine selection (auto, streaming, memory_efficient, etc.)
    pub default_engine: String,

    /// Default chunk size for streaming operations
    pub default_chunk_size: Option<usize>,

    /// Enable parallel processing by default
    pub parallel: bool,

    /// Maximum concurrent operations
    pub max_concurrent: usize,

    /// Memory usage limits
    pub memory: MemoryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    /// Maximum memory usage in MB (0 = unlimited)
    pub max_usage_mb: usize,

    /// Enable memory monitoring
    pub monitor: bool,

    /// Auto-switch to streaming for large files
    pub auto_streaming_threshold_mb: f64,
}

#[cfg(feature = "database")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSettings {
    /// Default connection timeout in seconds
    pub connection_timeout: u64,

    /// Default batch size for database queries
    pub batch_size: usize,

    /// Maximum number of database connections
    pub max_connections: usize,

    /// Enable SSL by default
    pub ssl_enabled: bool,

    /// Default sampling configuration
    pub sampling: DatabaseSamplingConfig,
}

#[cfg(feature = "database")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseSamplingConfig {
    /// Enable sampling for large tables
    pub enabled: bool,

    /// Default sample size
    pub default_sample_size: usize,

    /// Threshold for automatic sampling (number of rows)
    pub auto_sample_threshold: usize,
}

impl Default for OutputConfig {
    fn default() -> Self {
        Self {
            default_format: DEFAULT_OUTPUT_FORMAT.to_string(),
            colored: DEFAULT_COLORED_OUTPUT,
            verbosity: DEFAULT_VERBOSITY,
            show_progress: DEFAULT_SHOW_PROGRESS,
            html: HtmlConfig::default(),
        }
    }
}

impl Default for HtmlConfig {
    fn default() -> Self {
        Self {
            auto_generate: DEFAULT_HTML_AUTO_GENERATE,
            output_dir: None,
            include_detailed_stats: DEFAULT_HTML_DETAILED_STATS,
        }
    }
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_QUALITY_ENABLED,
            iso_thresholds: IsoQualityThresholds::default(),
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            default_engine: DEFAULT_ENGINE.to_string(),
            default_chunk_size: None,
            parallel: DEFAULT_PARALLEL_PROCESSING,
            max_concurrent: num_cpus::get(),
            memory: MemoryConfig::default(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_usage_mb: DEFAULT_MAX_MEMORY_MB,
            monitor: DEFAULT_MEMORY_MONITORING,
            auto_streaming_threshold_mb: DEFAULT_AUTO_STREAMING_THRESHOLD_MB,
        }
    }
}

#[cfg(feature = "database")]
impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            connection_timeout: DEFAULT_DB_CONNECTION_TIMEOUT_SECS,
            batch_size: DEFAULT_DB_BATCH_SIZE,
            max_connections: DEFAULT_DB_MAX_CONNECTIONS,
            ssl_enabled: DEFAULT_DB_SSL_ENABLED,
            sampling: DatabaseSamplingConfig::default(),
        }
    }
}

#[cfg(feature = "database")]
impl Default for DatabaseSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_DB_SAMPLING_ENABLED,
            default_sample_size: DEFAULT_DB_SAMPLE_SIZE,
            auto_sample_threshold: DEFAULT_DB_AUTO_SAMPLE_THRESHOLD,
        }
    }
}

impl DataprofConfig {
    /// Load configuration from file, with fallback to default
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let config: DataprofConfig = toml::from_str(&content)?;
        Ok(config)
    }

    /// Save configuration to file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Load configuration with automatic file discovery.
    ///
    /// Searches for configuration files in the following order:
    /// 1. `.dataprof.toml` in current directory
    /// 2. `$HOME/.config/dataprof/config.toml` (Linux/macOS)
    /// 3. `%USERPROFILE%\.config\dataprof\config.toml` (Windows)
    /// 4. `dataprof.toml` in current directory
    ///
    /// If no config file is found, returns default configuration with
    /// environment variable overrides applied.
    pub fn load_with_discovery() -> Self {
        // Build list of config paths to try
        let mut config_paths = vec![
            // Current directory - highest priority
            PathBuf::from(".dataprof.toml"),
            PathBuf::from("dataprof.toml"),
        ];

        // User config directory - cross-platform
        if let Some(home) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
            let user_config = PathBuf::from(home)
                .join(".config")
                .join("dataprof")
                .join("config.toml");
            config_paths.push(user_config);
        }

        // Try loading from each path
        for path in &config_paths {
            if path.exists() {
                match Self::load_from_file(path) {
                    Ok(mut config) => {
                        log::info!("✓ Loaded configuration from: {}", path.display());
                        // Apply environment overrides on top of file config
                        config.apply_env_overrides();
                        return config;
                    }
                    Err(e) => {
                        log::warn!(
                            "⚠ Found config file at {} but failed to load: {}",
                            path.display(),
                            e
                        );
                    }
                }
            }
        }

        // No config file found - use defaults
        log::debug!("No configuration file found. Using defaults with environment overrides.");
        let mut config = Self::default();
        config.apply_env_overrides();
        config
    }

    /// Apply environment variable overrides
    pub fn apply_env_overrides(&mut self) {
        // Output format override
        if let Ok(format) = std::env::var("DATAPROF_FORMAT") {
            self.output.default_format = format;
        }

        // Verbosity override
        if let Ok(verbosity) = std::env::var("DATAPROF_VERBOSITY") {
            if let Ok(level) = verbosity.parse::<u8>() {
                self.output.verbosity = level;
            }
        }

        // Engine override
        if let Ok(engine) = std::env::var("DATAPROF_ENGINE") {
            self.engine.default_engine = engine;
        }

        // Quality checking override
        if let Ok(quality) = std::env::var("DATAPROF_QUALITY") {
            if let Ok(enabled) = quality.parse::<bool>() {
                self.quality.enabled = enabled;
            }
        }

        // Disable colors if NO_COLOR is set
        if std::env::var("NO_COLOR").is_ok() {
            self.output.colored = false;
        }

        // Progress override
        if let Ok(progress) = std::env::var("DATAPROF_PROGRESS") {
            if let Ok(enabled) = progress.parse::<bool>() {
                self.output.show_progress = enabled;
            }
        }
    }

    /// Merge CLI arguments with configuration
    pub fn merge_with_cli_args(
        &mut self,
        cli_format: Option<&str>,
        cli_quality: Option<bool>,
        cli_progress: Option<bool>,
    ) {
        // CLI arguments take precedence over config file
        if let Some(format) = cli_format {
            self.output.default_format = format.to_string();
        }

        if let Some(quality) = cli_quality {
            self.quality.enabled = quality;
        }

        if let Some(progress) = cli_progress {
            self.output.show_progress = progress;
        }
    }

    /// Create a sample configuration file for users
    pub fn create_sample_config<P: AsRef<Path>>(path: P) -> Result<()> {
        let sample_config = Self::default();
        sample_config.save_to_file(path)?;
        Ok(())
    }
}

/// Configuration validation with comprehensive error messages
impl DataprofConfig {
    /// Validate the configuration and return detailed error messages.
    ///
    /// This method checks:
    /// - Output format validity
    /// - Verbosity level range
    /// - Quality threshold ranges
    /// - ISO threshold consistency
    /// - Engine validity
    /// - Memory configuration sanity
    /// - Chunk size validity
    pub fn validate(&self) -> Result<()> {
        // Validate output format
        let valid_formats = ["text", "json", "csv", "plain"];
        if !valid_formats.contains(&self.output.default_format.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid output format '{}'. Valid formats: {}\n\
                 → Fix: Set output.default_format to one of the valid formats in your config file.",
                self.output.default_format,
                valid_formats.join(", ")
            ));
        }

        // Validate verbosity level
        if self.output.verbosity > 3 {
            return Err(anyhow::anyhow!(
                "Invalid verbosity level {}. Must be between 0 (quiet) and 3 (debug).\n\
                 → Fix: Set output.verbosity to 0, 1, 2, or 3 in your config file.",
                self.output.verbosity
            ));
        }

        // Validate ISO thresholds
        let iso = &self.quality.iso_thresholds;

        if iso.outlier_iqr_multiplier <= 0.0 {
            return Err(anyhow::anyhow!(
                "IQR multiplier must be positive (standard value: 1.5), got {}.\n\
                 → Fix: Set quality.iso_thresholds.outlier_iqr_multiplier to a positive value.\n\
                 → Recommended: Use 1.5 (ISO standard) for normal cases, 3.0 for lenient detection.",
                iso.outlier_iqr_multiplier
            ));
        }

        if iso.max_null_percentage < 0.0 || iso.max_null_percentage > 100.0 {
            return Err(anyhow::anyhow!(
                "Max null percentage must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.max_null_percentage to a value between 0.0 and 100.0.",
                iso.max_null_percentage
            ));
        }

        if iso.null_report_threshold < 0.0 || iso.null_report_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "Null report threshold must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.null_report_threshold to a value between 0.0 and 100.0.",
                iso.null_report_threshold
            ));
        }

        if iso.min_type_consistency < 0.0 || iso.min_type_consistency > 100.0 {
            return Err(anyhow::anyhow!(
                "Min type consistency must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.min_type_consistency to a value between 0.0 and 100.0.",
                iso.min_type_consistency
            ));
        }

        if iso.high_cardinality_threshold < 0.0 || iso.high_cardinality_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "High cardinality threshold must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.high_cardinality_threshold to a value between 0.0 and 100.0.",
                iso.high_cardinality_threshold
            ));
        }

        if iso.duplicate_report_threshold < 0.0 || iso.duplicate_report_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "Duplicate report threshold must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.duplicate_report_threshold to a value between 0.0 and 100.0.",
                iso.duplicate_report_threshold
            ));
        }

        if iso.max_data_age_years < 0.0 {
            return Err(anyhow::anyhow!(
                "Max data age must be non-negative, got {} years.\n\
                 → Fix: Set quality.iso_thresholds.max_data_age_years to a positive value.",
                iso.max_data_age_years
            ));
        }

        if iso.stale_data_threshold < 0.0 || iso.stale_data_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "Stale data threshold must be between 0 and 100, got {}.\n\
                 → Fix: Set quality.iso_thresholds.stale_data_threshold to a value between 0.0 and 100.0.",
                iso.stale_data_threshold
            ));
        }

        // Validate engine
        let valid_engines = ["auto", "streaming", "memory_efficient", "true_streaming"];
        if !valid_engines.contains(&self.engine.default_engine.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid engine '{}'. Valid engines: {}\n\
                 → Fix: Set engine.default_engine to one of the valid engines in your config file.\n\
                 → Recommended: Use 'auto' for automatic selection based on file size.",
                self.engine.default_engine,
                valid_engines.join(", ")
            ));
        }

        // Validate memory configuration
        if self.engine.memory.auto_streaming_threshold_mb < 0.0 {
            return Err(anyhow::anyhow!(
                "Auto-streaming threshold must be non-negative, got {} MB.\n\
                 → Fix: Set engine.memory.auto_streaming_threshold_mb to a positive value.\n\
                 → Recommended: 100.0 MB is a good default for most systems.",
                self.engine.memory.auto_streaming_threshold_mb
            ));
        }

        // Validate chunk size if specified
        if let Some(chunk_size) = self.engine.default_chunk_size {
            if chunk_size == 0 {
                return Err(anyhow::anyhow!(
                    "Chunk size must be greater than 0, got {}.\n\
                     → Fix: Set engine.default_chunk_size to a positive value or null for adaptive sizing.\n\
                     → Recommended: 8192-65536 rows for most CSV files.",
                    chunk_size
                ));
            }

            if chunk_size > 1_000_000 {
                return Err(anyhow::anyhow!(
                    "Chunk size {} is very large and may cause memory issues.\n\
                     → Fix: Set engine.default_chunk_size to a smaller value.\n\
                     → Recommended: 8192-65536 rows for most CSV files.",
                    chunk_size
                ));
            }
        }

        // Validate max concurrent operations
        if self.engine.max_concurrent == 0 {
            return Err(anyhow::anyhow!(
                "Max concurrent operations must be greater than 0.\n\
                 → Fix: Set engine.max_concurrent to a positive value.\n\
                 → Recommended: Use num_cpus::get() or leave unspecified for automatic detection.",
            ));
        }

        // Validate database settings if present
        #[cfg(feature = "database")]
        if let Some(ref db) = self.database {
            if db.connection_timeout == 0 {
                return Err(anyhow::anyhow!(
                    "Database connection timeout must be greater than 0 seconds.\n\
                     → Fix: Set database.connection_timeout to a positive value.\n\
                     → Recommended: 30 seconds for most network conditions.",
                ));
            }

            if db.batch_size == 0 {
                return Err(anyhow::anyhow!(
                    "Database batch size must be greater than 0.\n\
                     → Fix: Set database.batch_size to a positive value.\n\
                     → Recommended: 10000 rows for most databases.",
                ));
            }

            if db.max_connections == 0 {
                return Err(anyhow::anyhow!(
                    "Database max connections must be greater than 0.\n\
                     → Fix: Set database.max_connections to a positive value.\n\
                     → Recommended: 10 connections for most use cases.",
                ));
            }

            if db.sampling.default_sample_size == 0 {
                return Err(anyhow::anyhow!(
                    "Database sample size must be greater than 0.\n\
                     → Fix: Set database.sampling.default_sample_size to a positive value.\n\
                     → Recommended: 100000 rows for statistical significance.",
                ));
            }
        }

        Ok(())
    }
}

// ============================================================================
// Builder Pattern Implementation
// ============================================================================

/// Builder for constructing DataprofConfig with a fluent API.
///
/// The builder pattern provides:
/// - Clear, self-documenting configuration
/// - Type-safe construction
/// - Validation at build time
/// - Easy preset configurations
///
/// # Examples
///
/// ```
/// use dataprof::core::config::{DataprofConfigBuilder, IsoQualityThresholds};
///
/// // Simple configuration with defaults
/// let config = DataprofConfigBuilder::new()
///     .build()
///     .expect("Failed to build config");
///
/// // Configuration with custom settings
/// let config = DataprofConfigBuilder::new()
///     .output_format("json")
///     .verbosity(2)
///     .engine("streaming")
///     .build()
///     .expect("Failed to build config");
///
/// // Strict quality profile for finance/healthcare
/// let config = DataprofConfigBuilder::new()
///     .iso_quality_profile_strict()
///     .build()
///     .expect("Failed to build config");
/// ```
#[derive(Debug, Clone)]
pub struct DataprofConfigBuilder {
    output: OutputConfig,
    quality: QualityConfig,
    engine: EngineConfig,
    #[cfg(feature = "database")]
    database: Option<DatabaseSettings>,
}

impl DataprofConfigBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            output: OutputConfig::default(),
            quality: QualityConfig::default(),
            engine: EngineConfig::default(),
            #[cfg(feature = "database")]
            database: Some(DatabaseSettings::default()),
        }
    }

    // ========================================================================
    // Output Configuration Methods
    // ========================================================================

    /// Set the default output format.
    ///
    /// Valid formats: "text", "json", "csv", "plain"
    pub fn output_format(mut self, format: &str) -> Self {
        self.output.default_format = format.to_string();
        self
    }

    /// Enable or disable colored output.
    pub fn colored(mut self, enabled: bool) -> Self {
        self.output.colored = enabled;
        self
    }

    /// Set verbosity level (0=quiet, 1=normal, 2=verbose, 3=debug).
    pub fn verbosity(mut self, level: u8) -> Self {
        self.output.verbosity = level;
        self
    }

    /// Enable or disable progress bars.
    pub fn show_progress(mut self, enabled: bool) -> Self {
        self.output.show_progress = enabled;
        self
    }

    /// Enable HTML report auto-generation.
    pub fn html_auto_generate(mut self, enabled: bool) -> Self {
        self.output.html.auto_generate = enabled;
        self
    }

    /// Set HTML report output directory.
    pub fn html_output_dir(mut self, dir: PathBuf) -> Self {
        self.output.html.output_dir = Some(dir);
        self
    }

    // ========================================================================
    // Quality Configuration Methods
    // ========================================================================

    /// Enable or disable quality checking.
    ///
    /// When enabled, all ISO 8000/25012 quality metrics are calculated:
    /// - Completeness (null detection)
    /// - Consistency (type consistency, mixed types)
    /// - Uniqueness (duplicate detection, high cardinality)
    /// - Accuracy (outlier detection)
    /// - Timeliness (stale data detection)
    pub fn quality_enabled(mut self, enabled: bool) -> Self {
        self.quality.enabled = enabled;
        self
    }

    /// Set custom ISO quality thresholds.
    pub fn iso_quality_thresholds(mut self, thresholds: IsoQualityThresholds) -> Self {
        self.quality.iso_thresholds = thresholds;
        self
    }

    /// Use strict ISO quality thresholds (finance, healthcare).
    ///
    /// Stricter thresholds for high-compliance industries:
    /// - Lower null tolerance (30% vs 50%)
    /// - Higher type consistency requirements (98% vs 95%)
    /// - Stricter duplicate detection (1% vs 5%)
    /// - More recent data requirements (2 years vs 5 years)
    pub fn iso_quality_profile_strict(mut self) -> Self {
        self.quality.iso_thresholds = IsoQualityThresholds::strict();
        self
    }

    /// Use lenient ISO quality thresholds (exploratory, marketing).
    ///
    /// More relaxed thresholds for exploratory data:
    /// - Higher null tolerance (70% vs 50%)
    /// - Lower type consistency requirements (90% vs 95%)
    /// - More lenient duplicate detection (10% vs 5%)
    /// - Older data accepted (10 years vs 5 years)
    pub fn iso_quality_profile_lenient(mut self) -> Self {
        self.quality.iso_thresholds = IsoQualityThresholds::lenient();
        self
    }

    // ========================================================================
    // Engine Configuration Methods
    // ========================================================================

    /// Set the default engine selection.
    ///
    /// Valid engines: "auto", "streaming", "memory_efficient", "true_streaming"
    pub fn engine(mut self, engine: &str) -> Self {
        self.engine.default_engine = engine.to_string();
        self
    }

    /// Set default chunk size for streaming operations.
    pub fn chunk_size(mut self, size: usize) -> Self {
        self.engine.default_chunk_size = Some(size);
        self
    }

    /// Enable or disable parallel processing.
    pub fn parallel(mut self, enabled: bool) -> Self {
        self.engine.parallel = enabled;
        self
    }

    /// Set maximum concurrent operations.
    pub fn max_concurrent(mut self, max: usize) -> Self {
        self.engine.max_concurrent = max;
        self
    }

    /// Set maximum memory usage in MB (0 = unlimited).
    pub fn max_memory_mb(mut self, mb: usize) -> Self {
        self.engine.memory.max_usage_mb = mb;
        self
    }

    /// Set auto-streaming threshold in MB.
    pub fn auto_streaming_threshold_mb(mut self, mb: f64) -> Self {
        self.engine.memory.auto_streaming_threshold_mb = mb;
        self
    }

    // ========================================================================
    // Database Configuration Methods (optional feature)
    // ========================================================================

    #[cfg(feature = "database")]
    /// Set database connection timeout in seconds.
    pub fn db_connection_timeout(mut self, seconds: u64) -> Self {
        if let Some(ref mut db) = self.database {
            db.connection_timeout = seconds;
        }
        self
    }

    #[cfg(feature = "database")]
    /// Set database batch size for queries.
    pub fn db_batch_size(mut self, size: usize) -> Self {
        if let Some(ref mut db) = self.database {
            db.batch_size = size;
        }
        self
    }

    #[cfg(feature = "database")]
    /// Enable or disable database sampling.
    pub fn db_sampling_enabled(mut self, enabled: bool) -> Self {
        if let Some(ref mut db) = self.database {
            db.sampling.enabled = enabled;
        }
        self
    }

    // ========================================================================
    // Preset Configurations
    // ========================================================================

    /// Create a configuration optimized for CI/CD pipelines.
    ///
    /// - No colors (for log compatibility)
    /// - No progress bars (cleaner output)
    /// - JSON output format
    /// - Verbose logging
    pub fn ci_preset() -> Self {
        Self::new()
            .colored(false)
            .show_progress(false)
            .output_format("json")
            .verbosity(2)
    }

    /// Create a configuration for interactive terminal use.
    ///
    /// - Colors enabled
    /// - Progress bars enabled
    /// - Text output format
    /// - Normal verbosity
    pub fn interactive_preset() -> Self {
        Self::new()
            .colored(true)
            .show_progress(true)
            .output_format("text")
            .verbosity(1)
    }

    /// Create a configuration for production quality checks.
    ///
    /// - Strict ISO quality thresholds
    /// - Quality checking enabled
    /// - Memory monitoring enabled
    /// - Conservative memory limits
    pub fn production_quality_preset() -> Self {
        Self::new()
            .iso_quality_profile_strict()
            .quality_enabled(true)
            .max_memory_mb(512) // 512MB limit for predictability
    }

    // ========================================================================
    // Build and Validation
    // ========================================================================

    /// Build the configuration and validate it.
    ///
    /// Returns an error if the configuration is invalid.
    pub fn build(self) -> Result<DataprofConfig> {
        let config = DataprofConfig {
            output: self.output,
            quality: self.quality,
            engine: self.engine,
            #[cfg(feature = "database")]
            database: self.database,
        };

        // Validate the configuration before returning
        config.validate()?;

        Ok(config)
    }

    /// Build the configuration without validation.
    ///
    /// Use this only if you're certain the configuration is valid
    /// and want to skip validation for performance reasons.
    pub fn build_unchecked(self) -> DataprofConfig {
        DataprofConfig {
            output: self.output,
            quality: self.quality,
            engine: self.engine,
            #[cfg(feature = "database")]
            database: self.database,
        }
    }
}

impl Default for DataprofConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

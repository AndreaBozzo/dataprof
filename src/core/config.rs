use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// Main configuration structure for DataProfiler CLI
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DataprofConfig {
    /// Output configuration
    pub output: OutputConfig,

    /// ML/Data Science specific configuration
    pub ml: MlConfig,

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
pub struct MlConfig {
    /// Enable ML readiness scoring by default
    pub auto_score: bool,

    /// ML readiness score threshold for warnings (0-100)
    pub warning_threshold: f64,

    /// Include ML recommendations in output
    pub include_recommendations: bool,

    /// ML feature importance calculation
    pub calculate_feature_importance: bool,

    /// Data preprocessing suggestions
    pub suggest_preprocessing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityConfig {
    /// Enable quality checking by default
    pub enabled: bool,

    /// Threshold for null value warnings (percentage)
    pub null_threshold: f64,

    /// Outlier detection sensitivity (standard deviations)
    pub outlier_threshold: f64,

    /// Enable duplicate detection
    pub detect_duplicates: bool,

    /// Enable mixed type detection
    pub detect_mixed_types: bool,

    /// Enable date format consistency checking
    pub check_date_formats: bool,
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
            default_format: "text".to_string(),
            colored: true,
            verbosity: 1,
            show_progress: true,
            html: HtmlConfig::default(),
        }
    }
}

impl Default for HtmlConfig {
    fn default() -> Self {
        Self {
            auto_generate: false,
            output_dir: None,
            include_detailed_stats: true,
        }
    }
}

impl Default for MlConfig {
    fn default() -> Self {
        Self {
            auto_score: false,
            warning_threshold: 70.0,
            include_recommendations: true,
            calculate_feature_importance: false,
            suggest_preprocessing: true,
        }
    }
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            null_threshold: 10.0,
            outlier_threshold: 3.0,
            detect_duplicates: true,
            detect_mixed_types: true,
            check_date_formats: true,
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            default_engine: "auto".to_string(),
            default_chunk_size: None,
            parallel: true,
            max_concurrent: num_cpus::get(),
            memory: MemoryConfig::default(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            max_usage_mb: 0, // Unlimited
            monitor: true,
            auto_streaming_threshold_mb: 100.0,
        }
    }
}

#[cfg(feature = "database")]
impl Default for DatabaseSettings {
    fn default() -> Self {
        Self {
            connection_timeout: 30,
            batch_size: 10000,
            max_connections: 10,
            ssl_enabled: true,
            sampling: DatabaseSamplingConfig::default(),
        }
    }
}

#[cfg(feature = "database")]
impl Default for DatabaseSamplingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_sample_size: 100000,
            auto_sample_threshold: 1000000,
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

    /// Load configuration with automatic file discovery
    pub fn load_with_discovery() -> Self {
        // Try loading from common locations
        let config_paths = [".dataprof.toml", ".config/dataprof.toml", "dataprof.toml"];

        for path in &config_paths {
            if Path::new(path).exists() {
                match Self::load_from_file(path) {
                    Ok(config) => {
                        log::info!("Loaded configuration from: {}", path);
                        return config;
                    }
                    Err(e) => {
                        log::warn!("Failed to load config from {}: {}", path, e);
                    }
                }
            }
        }

        // Check environment variables for config overrides
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

        // ML auto-scoring override
        if let Ok(auto_score) = std::env::var("DATAPROF_ML_AUTO_SCORE") {
            if let Ok(enabled) = auto_score.parse::<bool>() {
                self.ml.auto_score = enabled;
            }
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

/// Configuration validation
impl DataprofConfig {
    pub fn validate(&self) -> Result<()> {
        // Validate output format
        let valid_formats = ["text", "json", "csv", "plain"];
        if !valid_formats.contains(&self.output.default_format.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid output format '{}'. Valid formats: {}",
                self.output.default_format,
                valid_formats.join(", ")
            ));
        }

        // Validate verbosity level
        if self.output.verbosity > 3 {
            return Err(anyhow::anyhow!(
                "Invalid verbosity level {}. Must be 0-3",
                self.output.verbosity
            ));
        }

        // Validate ML threshold
        if self.ml.warning_threshold < 0.0 || self.ml.warning_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "ML warning threshold must be between 0 and 100, got {}",
                self.ml.warning_threshold
            ));
        }

        // Validate quality thresholds
        if self.quality.null_threshold < 0.0 || self.quality.null_threshold > 100.0 {
            return Err(anyhow::anyhow!(
                "Null threshold must be between 0 and 100, got {}",
                self.quality.null_threshold
            ));
        }

        if self.quality.outlier_threshold <= 0.0 {
            return Err(anyhow::anyhow!(
                "Outlier threshold must be positive, got {}",
                self.quality.outlier_threshold
            ));
        }

        // Validate engine
        let valid_engines = ["auto", "streaming", "memory_efficient", "true_streaming"];
        if !valid_engines.contains(&self.engine.default_engine.as_str()) {
            return Err(anyhow::anyhow!(
                "Invalid engine '{}'. Valid engines: {}",
                self.engine.default_engine,
                valid_engines.join(", ")
            ));
        }

        Ok(())
    }
}

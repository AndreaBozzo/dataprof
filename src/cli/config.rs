use anyhow::Result;
use dataprof::core::{exit_codes, DataprofConfig, InputValidator};
use std::path::Path;

use crate::cli::args::{Cli, CliOutputFormat};

pub fn load_config(cli: &Cli) -> Result<DataprofConfig> {
    let mut config = if let Some(config_path) = &cli.config {
        // Validate config file first
        if let Err(e) = InputValidator::validate_config_file(config_path) {
            eprintln!("❌ Configuration Error: {}", e);
            std::process::exit(exit_codes::CONFIG_ERROR);
        }
        DataprofConfig::load_from_file(config_path)?
    } else {
        DataprofConfig::load_with_discovery()
    };

    // Apply CLI overrides to configuration
    config.merge_with_cli_args(
        Some(match cli.format {
            CliOutputFormat::Text => "text",
            CliOutputFormat::Json => "json",
            CliOutputFormat::Csv => "csv",
            CliOutputFormat::Plain => "plain",
        }),
        Some(cli.quality),
        Some(cli.progress),
    );

    // Handle verbosity and colors
    if cli.no_color || std::env::var("NO_COLOR").is_ok() {
        config.output.colored = false;
    }
    if cli.verbosity > 0 {
        config.output.verbosity = cli.verbosity;
    }

    // Validate configuration
    if let Err(e) = config.validate() {
        eprintln!("❌ Configuration Error: {}", e);
        std::process::exit(exit_codes::CONFIG_ERROR);
    }

    Ok(config)
}

use anyhow::Result;
use clap::Parser;

// Local modules
mod cli;
mod commands;
mod error;

use cli::{load_config, validate_cli_inputs, Cli};
use commands::{run_analysis, show_engine_info};
use dataprof::core::{exit_codes, InputValidator};
use error::{determine_exit_code, handle_error};

fn main() -> Result<()> {
    // Custom parsing to handle engine-info without file argument
    let args: Vec<String> = std::env::args().collect();
    if args.contains(&"--engine-info".to_string()) {
        return show_engine_info();
    }

    let cli = Cli::parse();

    // Input validation with helpful error messages
    if let Err(e) = validate_cli_inputs(&cli) {
        eprintln!("❌ {}", e);
        std::process::exit(InputValidator::get_exit_code(&e));
    }

    // Load configuration with CLI integration
    let config = load_config(&cli)?;

    // Enhanced error handling wrapper with proper exit codes
    match run_analysis(&cli, &config) {
        Ok(_) => std::process::exit(exit_codes::SUCCESS),
        Err(e) => {
            let exit_code = determine_exit_code(&e);
            handle_error(&e, &cli.file);
            std::process::exit(exit_code);
        }
    }
}

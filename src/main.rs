use anyhow::Result;
use clap::Parser;
use env_logger::Env;
use std::io::Write;

// Local modules
mod cli;
mod error;

use cli::{Command, route_command};
use dataprof::core::exit_codes;

fn main() -> Result<()> {
    // Initialize logger with sensible default so log::info!/warn! are visible by default
    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format(|buf, record| writeln!(buf, "{}: {}", record.level(), record.args()))
        .init();

    // Parse subcommand CLI
    #[derive(Parser)]
    #[command(name = "dataprof")]
    #[command(
        version,
        about = "Fast data profiler with ISO 8000/25012 quality metrics"
    )]
    struct SubcommandCli {
        #[command(subcommand)]
        command: Command,
    }

    let cli = SubcommandCli::parse();

    // Route to appropriate command handler
    match route_command(cli.command) {
        Ok(_) => std::process::exit(exit_codes::SUCCESS),
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(exit_codes::GENERAL_ERROR);
        }
    }
}

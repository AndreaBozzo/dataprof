use clap::{Parser, Subcommand};
use crate::cli::commands::Command;

/// DataProf CLI with subcommands
#[derive(Parser)]
#[command(name = "dataprof")]
#[command(version, about = "Fast CSV data profiler with quality checking", long_about = None)]
pub struct CliV2 {
    #[command(subcommand)]
    pub command: Option<Command>,
}

impl CliV2 {
    /// Check if using new subcommand style
    pub fn is_subcommand_mode(&self) -> bool {
        self.command.is_some()
    }
}

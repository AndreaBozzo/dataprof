use clap::Parser;
use crate::cli::{Cli, Command};

/// Unified CLI parser that supports both old-style flags and new subcommands
#[derive(Parser)]
#[command(name = "dataprof")]
#[command(version, about, long_about = None)]
pub struct UnifiedCli {
    #[command(subcommand)]
    pub command: Option<Command>,

    #[command(flatten)]
    pub legacy: Option<Cli>,
}

impl UnifiedCli {
    /// Check if using new subcommand style
    pub fn is_subcommand_mode(&self) -> bool {
        self.command.is_some()
    }

    /// Parse CLI args with smart detection
    pub fn parse_smart() -> Self {
        // First try to detect if subcommand is present
        let args: Vec<String> = std::env::args().collect();

        let has_subcommand = args.iter().any(|arg| {
            matches!(arg.as_str(), "check" | "analyze" | "ml" | "report" | "batch")
        });

        if has_subcommand {
            // Use subcommand parsing
            Self::parse()
        } else {
            // Use legacy parsing - construct from legacy Cli
            let legacy_cli = Cli::parse();
            Self {
                command: None,
                legacy: Some(legacy_cli),
            }
        }
    }
}
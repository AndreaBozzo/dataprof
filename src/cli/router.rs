use crate::cli::commands::Command;
use anyhow::Result;

/// Route subcommand to appropriate handler
pub fn route_command(command: Command) -> Result<()> {
    match command {
        Command::Analyze(args) => crate::cli::commands::analyze::execute(&args),
        #[cfg(feature = "database")]
        Command::Database(args) => crate::cli::commands::database::execute(&args),
    }
}

use crate::cli::commands::Command;
use anyhow::Result;

/// Route subcommand to appropriate handler
pub fn route_command(command: Command) -> Result<()> {
    match command {
        Command::Analyze(args) => crate::cli::commands::analyze::execute(&args),
        Command::Schema(args) => crate::cli::commands::schema::execute(&args),
        Command::Count(args) => crate::cli::commands::count::execute(&args),
        #[cfg(feature = "database")]
        Command::Database(args) => crate::cli::commands::database::execute(&args),
    }
}

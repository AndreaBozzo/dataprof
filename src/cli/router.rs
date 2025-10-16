use crate::cli::commands::Command;
use anyhow::Result;

/// Route subcommand to appropriate handler
pub fn route_command(command: Command) -> Result<()> {
    match command {
        Command::Analyze(args) => crate::cli::commands::analyze::execute(&args),
        Command::Report(args) => crate::cli::commands::report::execute(&args),
        Command::Batch(args) => crate::cli::commands::batch::execute(&args),
        #[cfg(feature = "database")]
        Command::Database(args) => crate::cli::commands::database::execute(&args),
        Command::Benchmark(args) => crate::cli::commands::benchmark::execute(&args),
        Command::Info(args) => crate::cli::commands::info::execute(&args),
    }
}

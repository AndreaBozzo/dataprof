use crate::cli::commands::Command;
use anyhow::Result;

/// Route subcommand to appropriate handler
pub fn route_command(command: Command) -> Result<()> {
    match command {
        Command::Check(args) => crate::cli::commands::check_impl::execute(&args),
        Command::Analyze(args) => crate::cli::commands::analyze_impl::execute(&args),
        Command::Ml(args) => crate::cli::commands::ml_impl::execute(&args),
        Command::Report(args) => crate::cli::commands::report_impl::execute(&args),
        Command::Batch(args) => crate::cli::commands::batch_impl::execute(&args),
    }
}

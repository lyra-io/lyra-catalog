use clap::Parser;
use lyra_catalog_cli::serve::ServeArgs;
use lyra_catalog_cli::sql::SqlArgs;

#[derive(Parser)]
#[command(name = "lyra-catalog", about = "Lyra catalog: SQL parsing, planning and pgwire")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Subcommand)]
enum Commands {
    /// Start the catalog and serve the PostgreSQL wire protocol.
    Serve(ServeArgs),
    /// Interactive SQL shell.
    Sql(SqlArgs),
}

#[tokio::main(worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => lyra_catalog_cli::serve::run(args).await?,
        Commands::Sql(args) => lyra_catalog_cli::sql::run(args).await?,
    }

    Ok(())
}

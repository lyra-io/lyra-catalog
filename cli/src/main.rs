use clap::{Args, Parser, Subcommand};
use lyra_catalog::{Cata, options::CataOptions};
use lyra_catalog_cli::{
    banner, health, manifest::Manifest, observability::Telemetry, password::read_verifier,
};
use lyra_meta::metadata::{
    Metadata, MetadataError,
    oxia::{OxiaMetadata, OxiaOptions},
};
use lyra_meta::proto::pb_meta::ScramSha256Verifier;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::timeout;

#[derive(Parser)]
#[command(name = "lyra-catalog", version, about = "Lyra database catalog")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Initialize an empty metadata namespace once.
    Init {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        password_file: PathBuf,
    },
    /// Start an already initialized catalog.
    Start(Start),
}
#[derive(Args)]
struct Start {
    #[arg(long)]
    manifest: PathBuf,
}

#[tokio::main(worker_threads = 4)]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            // Manifest/password validation precedes provider setup. Even these
            // failures have a structured, sanitized local diagnostic.
            let subscriber = tracing_subscriber::fmt()
                .json()
                .with_writer(std::io::stderr)
                .finish();
            tracing::subscriber::with_default(subscriber, || {
                tracing::error!(
                    service = "lyra-catalog",
                    event = "command_failed",
                    outcome = "error",
                    message
                );
            });
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), &'static str> {
    let path = match &cli.command {
        Command::Init { manifest, .. } => manifest,
        Command::Start(args) => &args.manifest,
    };
    let manifest = Manifest::read(path)?;
    let verifier = match &cli.command {
        Command::Init { password_file, .. } => Some(read_verifier(password_file)?),
        _ => None,
    };
    let telemetry = Telemetry::new()?;
    if matches!(&cli.command, Command::Start(_)) {
        banner::print_banner();
    }
    let result = run0(cli.command, manifest, verifier, &telemetry).await;
    if result.is_err() {
        tracing::error!(
            operation = "cli",
            outcome = "failed",
            "catalog command failed"
        );
    }
    telemetry.shutdown().await;
    result
}

async fn run0(
    command: Command,
    manifest: Manifest,
    verifier: Option<ScramSha256Verifier>,
    telemetry: &Telemetry,
) -> Result<(), &'static str> {
    let options = OxiaOptions::new(
        manifest.metadata.oxia.endpoint,
        manifest.metadata.oxia.namespace,
    );
    let metadata = Arc::new(
        OxiaMetadata::with_meter(&options, telemetry.meter("lyra-meta"))
            .await
            .map_err(|_| "metadata connection failed")?,
    );
    let result = match command {
        Command::Init { .. } => metadata
            .initialize(verifier.expect("init verifier validated"))
            .await
            .map_err(|error| match error {
                MetadataError::AlreadyInitialized => {
                    "AlreadyInitialized: initialization made no changes"
                }
                MetadataError::IncompleteInitialization => {
                    "IncompleteInitialization: automatic resumption is disabled"
                }
                _ => "initialization failed; inspect sanitized logs and metadata health",
            }),
        Command::Start(_) => {
            let options = CataOptions {
                listen: manifest.postgres.listen.to_string(),
                ..CataOptions::default()
            };
            let catalog = Arc::new(
                Cata::with_meter(options, metadata.clone(), telemetry.meter("lyra-catalog"))
                    .await
                    .map_err(|_| "catalog startup validation failed")?,
            );
            // Bind every requested listener before publishing presence/readiness.
            let sql = TcpListener::bind(manifest.postgres.listen)
                .await
                .map_err(|_| "SQL listener could not be bound")?;
            let health = if let Some(options) = manifest.health {
                Some(
                    TcpListener::bind(options.listen)
                        .await
                        .map_err(|_| "health listener could not be bound")?,
                )
            } else {
                None
            };
            let shutdown = catalog.cancellation();
            let mut terminate =
                signal(SignalKind::terminate()).map_err(|_| "signal handler failed")?;
            let signal_token = shutdown.clone();
            let signal_task = tokio::spawn(async move {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
                signal_token.cancel();
            });
            let health_task = health.map(|listener| {
                let catalog = Arc::clone(&catalog);
                tokio::spawn(health::serve(listener, catalog, telemetry.registry()))
            });
            let result = catalog
                .start_with_listener(sql)
                .await
                .map_err(|_| "catalog server stopped with an error");
            shutdown.cancel();
            signal_task.abort();
            if let Some(mut task) = health_task
                && timeout(Duration::from_secs(2), &mut task).await.is_err()
            {
                task.abort();
                let _ = task.await;
            }
            result
        }
    };
    let _ = metadata.close().await;
    result
}

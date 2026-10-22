use clap::{Parser, Subcommand};
use lyra_catalog::{Cata, options::CataOptions};
use lyra_catalog_cli::{
    banner,
    health::{self, Health},
    manifest::{Manifest, Reload},
    password::read_verifier,
};
use lyra_meta::metadata::{
    Metadata,
    oxia::{OxiaMetadata, OxiaOptions},
};
use lyra_meta::observability::Telemetry;
use lyra_meta::toolkit::{ManifestWatcher, load, resolve_path};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::signal::unix::{SignalKind, signal};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(name = "lyra-catalog", version, about = "Lyra Catalog")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Init {
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        password_file: PathBuf,
    },
    Start {
        #[arg(long)]
        manifest: PathBuf,
    },
}

#[tokio::main(worker_threads = 4)]
async fn main() {
    let cli = Cli::parse();
    if matches!(&cli.command, Command::Start { .. }) {
        banner::print_banner();
    }
    if let Err(reason) = run(cli.command).await {
        // Static sanitized codes only, including failures before logging exists.
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();
        println!(
            "{{\"timestamp\":{timestamp},\"severity\":\"ERROR\",\"service\":\"lyra-catalog\",\"component\":\"catalog\",\"event\":\"process_failed\",\"message\":\"Catalog command failed\",\"reason\":\"{reason}\"}}"
        );
        std::process::exit(1);
    }
}

async fn run(command: Command) -> Result<(), &'static str> {
    let path = match &command {
        Command::Init { manifest, .. } | Command::Start { manifest } => manifest,
    };
    let path = resolve_path(path).map_err(|e| e.0)?;
    let manifest = Manifest::parse(&load(&path).await.map_err(|e| e.0)?).map_err(|e| e.0)?;
    let verifier = match command {
        Command::Init { password_file, .. } => Some(
            tokio::task::spawn_blocking(move || read_verifier(&password_file))
                .await
                .map_err(|_| "password_reader_failed")??,
        ),
        Command::Start { .. } => None,
    };
    let telemetry = Arc::new(
        Telemetry::new(
            manifest.observability.clone(),
            manifest.server.health.is_some(),
            manifest.server.tokio_console.as_ref().map(|l| l.listen),
            &Handle::current(),
        )
        .map_err(|e| e.0)?,
    );
    std::panic::set_hook(Box::new(|_| {
        tracing::error!(event = "process_panic", "a process task panicked")
    }));
    let oxia = &manifest.metadata.oxia;
    let metadata: Arc<dyn Metadata> = match OxiaMetadata::with_meter(
        &OxiaOptions::new(&oxia.endpoint, &oxia.namespace),
        telemetry.meter(),
    )
    .await
    {
        Ok(meta) => Arc::new(meta),
        Err(_) => {
            telemetry.close().await;
            return Err("metadata_connect_failed");
        }
    };
    let result = if let Some(verifier) = verifier {
        metadata
            .initialize(verifier)
            .await
            .map_err(|_| "initialization_failed")
    } else {
        start(
            path,
            manifest,
            Arc::clone(&metadata),
            Arc::clone(&telemetry),
        )
        .await
    };
    let closed = timeout(Duration::from_secs(10), metadata.close()).await;
    telemetry.close().await;
    result?;
    match closed {
        Ok(Ok(())) => Ok(()),
        _ => Err("metadata_close_unconfirmed"),
    }
}

async fn start(
    path: PathBuf,
    manifest: Manifest,
    metadata: Arc<dyn Metadata>,
    telemetry: Arc<Telemetry>,
) -> Result<(), &'static str> {
    let catalog = Arc::new(
        Cata::with_meter(
            CataOptions {
                listen: manifest.server.public.listen.to_string(),
                max_connections: 1024,
            },
            metadata,
            telemetry.meter(),
        )
        .await
        .map_err(|_| "metadata_not_initialized")?,
    );
    let public = TcpListener::bind(manifest.server.public.listen)
        .await
        .map_err(|_| "public_bind_failed")?;
    let health_listener = match &manifest.server.health {
        Some(config) => Some(
            TcpListener::bind(config.listen)
                .await
                .map_err(|_| "health_bind_failed")?,
        ),
        None => None,
    };
    let prepared = telemetry
        .prepare(manifest.observability.clone())
        .await
        .map_err(|e| e.0)?;
    telemetry.commit(prepared).await;
    let mut watcher = ManifestWatcher::new(
        path,
        manifest,
        Arc::new(Reload {
            telemetry: Arc::clone(&telemetry),
        }),
        &telemetry.meter(),
    );
    let stop = catalog.cancellation();
    let server = Arc::clone(&catalog);
    let mut serving = tokio::spawn(async move { server.start_with_listener(public).await });
    let health_stop = CancellationToken::new();
    let health_cancel = health_stop.clone();
    let mut health_task = tokio::spawn(async move {
        if let Some(listener) = health_listener {
            axum::serve(
                listener,
                health::router(Health {
                    catalog,
                    profiler: telemetry.profiler(),
                }),
            )
            .with_graceful_shutdown(health_cancel.cancelled_owned())
            .await
            .map_err(|_| ())
        } else {
            health_cancel.cancelled().await;
            Ok(())
        }
    });
    tracing::info!(
        event = "process_started",
        service = "lyra-catalog",
        "Catalog foundation started"
    );
    let result = tokio::select! {
        result = &mut serving => match result { Ok(Ok(())) => Ok(()), _ => Err("catalog_service_failed") },
        _ = &mut health_task => Err("health_service_failed"),
        _ = shutdown_signal() => Ok(()),
    };
    stop.cancel();
    let watcher_result = watcher.close().await.map_err(|e| e.0);
    health_stop.cancel();
    if !serving.is_finished() && timeout(Duration::from_secs(6), &mut serving).await.is_err() {
        serving.abort();
        let _ = serving.await;
    }
    if !health_task.is_finished()
        && timeout(Duration::from_secs(2), &mut health_task)
            .await
            .is_err()
    {
        health_task.abort();
        let _ = health_task.await;
    }
    result.and(watcher_result)
}
async fn shutdown_signal() {
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_require_explicit_manifest_and_separate_init_password_file() {
        assert!(
            Cli::try_parse_from(["lyra-catalog", "start", "--manifest", "catalog.toml"]).is_ok()
        );
        assert!(
            Cli::try_parse_from([
                "lyra-catalog",
                "init",
                "--manifest",
                "catalog.toml",
                "--password-file",
                "root.password"
            ])
            .is_ok()
        );
        for args in [
            vec!["lyra-catalog", "serve"],
            vec!["lyra-catalog", "start"],
            vec!["lyra-catalog", "init", "--manifest", "catalog.toml"],
            vec![
                "lyra-catalog",
                "start",
                "--manifest",
                "catalog.toml",
                "--password-file",
                "root.password",
            ],
        ] {
            assert!(Cli::try_parse_from(args).is_err());
        }
    }
}

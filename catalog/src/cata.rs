use crate::Result;
use crate::authentication::{AuthenticationHandler, Slot};
use crate::inventory;
use crate::inventory::SqlSession;
use crate::options::CataOptions;
use crate::query::Query;
use crate::state::State;
use crate::wire::process_socket;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::{
    ConnectionManager, PgWireServerHandlers, RandomPidSecretKeyGenerator,
};
use lyra_meta::metadata::{Metadata, MetadataError};
use lyra_meta::observability::{Meter, meter};
use lyra_meta::proto::pb_meta::ScramSha256Verifier;
use lyra_meta::utils::verifier::make_verifier;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

pub struct Cata {
    // Immutable state
    options: CataOptions,
    state: Arc<State>,
    fallback: Arc<SqlSession>,
    manager: Arc<ConnectionManager>,
    pids: Arc<RandomPidSecretKeyGenerator>,
    dummy: ScramSha256Verifier,
}

impl Cata {
    pub async fn new(options: CataOptions, metadata: Arc<dyn Metadata>) -> Result<Self> {
        Self::with_meter(options, metadata, meter("lyra-catalog")).await
    }
    pub async fn with_meter(
        options: CataOptions,
        metadata: Arc<dyn Metadata>,
        meter: Meter,
    ) -> Result<Self> {
        let state = State::new(Arc::clone(&metadata), meter).await?;
        Ok(Self {
            options,
            state,
            fallback: inventory::session("public", metadata)?,
            manager: Arc::new(ConnectionManager::new()),
            pids: Arc::new(RandomPidSecretKeyGenerator::default()),
            dummy: make_verifier("not-a-valid-user-credential")?,
        })
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.state.shutdown.clone()
    }
    /// Each caller performs a fresh bounded exact-key read through Meta.
    pub async fn probe(&self) -> (bool, bool) {
        let registration = self.state.metadata.is_registered().await;
        let live = !matches!(
            registration,
            Err(MetadataError::Closed
                | MetadataError::RegistrationMonitorFailed
                | MetadataError::Integrity(_)
                | MetadataError::InvalidRecord(_)
                | MetadataError::Decode(_))
        );
        let ready = matches!(registration, Ok(true))
            && self.state.ready.load(Ordering::Acquire)
            && !self.state.shutdown.is_cancelled();
        self.state.metrics.ready(ready);
        (live, ready)
    }
    pub async fn start(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(&self.options.listen).await?;
        self.start_with_listener(listener).await
    }
    pub async fn start_with_listener(self: Arc<Self>, listener: TcpListener) -> Result<()> {
        self.state
            .metadata
            .register_catalog_component()
            .await
            .unwrap_or_else(|_| {
                tracing::error!(
                    event = "startup_registration_failed",
                    "initial component registration failed"
                );
                panic!("catalog initial registration failed");
            });
        self.state.set_ready(true);
        tracing::info!(
            event = "process_ready",
            operation = "start",
            outcome = "ready",
            "catalog listener ready"
        );
        let capacity = Arc::new(Semaphore::new(self.options.max_connections.max(1)));
        let mut connections = JoinSet::new();
        let outcome = loop {
            tokio::select! {
                biased;
                _ = self.state.shutdown.cancelled() => break Ok(()),
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
                accepted = listener.accept() => {
                    let (socket, _) = match accepted { Ok(value) => value, Err(error) => break Err(error.into()) };
                    let Ok(permit) = Arc::clone(&capacity).try_acquire_owned() else { drop(socket); continue; };
                    let cancellation = self.state.shutdown.child_token();
                    let slot: Slot = Arc::new(OnceLock::new());
                    let factory = Factory {
                        query: Arc::new(Query::new(Arc::clone(&self.state), Arc::clone(&slot), self.fallback.service())),
                        startup: Arc::new(AuthenticationHandler::new(Arc::clone(&self.state), slot, Arc::clone(&self.manager), Arc::clone(&self.pids), self.dummy.clone())),
                        cancel: Arc::new(DefaultCancelHandler::new(Arc::clone(&self.manager))),
                    };
                    connections.spawn(async move {
                        let _permit = permit;
                        tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => {}
                            result = process_socket(socket, factory) => {
                                if result.is_err() { tracing::debug!(operation = "connection", outcome = "closed", "connection ended"); }
                            }
                        }
                    });
                }
            }
        };
        self.state.set_ready(false);
        self.state.shutdown.cancel();
        if timeout(Duration::from_secs(5), async {
            while connections.join_next().await.is_some() {}
        })
        .await
        .is_err()
        {
            connections.abort_all();
            while connections.join_next().await.is_some() {}
        }
        tracing::info!(
            event = "process_stopped",
            operation = "shutdown",
            outcome = "complete",
            "catalog stopped"
        );
        outcome
    }
}

struct Factory {
    query: Arc<Query>,
    startup: Arc<AuthenticationHandler>,
    cancel: Arc<DefaultCancelHandler>,
}
impl PgWireServerHandlers for Factory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.query)
    }
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.query)
    }
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::clone(&self.startup)
    }
    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::clone(&self.cancel)
    }
}

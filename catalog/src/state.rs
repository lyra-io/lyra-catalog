use crate::telemetry::Metrics;
use crate::{CataError, Result};
use lyra_meta::metadata::{Metadata, MetadataError, SYSTEM_DATABASE_NAME};
use lyra_meta::observability::Meter;
use lyra_meta::proto::pb_meta::DatabaseState;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

pub(crate) struct State {
    // Control state
    pub shutdown: CancellationToken,
    // Immutable state
    pub metadata: Arc<dyn Metadata>,
    pub metrics: Metrics,
    // Mutable state
    pub ready: AtomicBool,
}
pub(crate) struct Admission {
    pub database_name: String,
}

impl State {
    pub(crate) async fn new(metadata: Arc<dyn Metadata>, meter: Meter) -> Result<Arc<Self>> {
        if !metadata.is_initialized().await? {
            return Err(MetadataError::NotInitialized.into());
        }
        let state = Arc::new(Self {
            shutdown: CancellationToken::new(),
            metadata,
            metrics: Metrics::new(meter),
            ready: AtomicBool::new(false),
        });
        state.set_ready(false);
        Ok(state)
    }
    pub(crate) fn set_ready(&self, value: bool) {
        self.ready.store(value, Ordering::Release);
        self.metrics.ready(value);
    }
    pub(crate) async fn admit(&self, name: &str) -> Result<Admission> {
        if !self.ready.load(Ordering::Acquire)
            || self.shutdown.is_cancelled()
            || !self.metadata.is_registered().await?
        {
            return Err(CataError::sql("57P03", "catalog is not ready"));
        }
        let database = self
            .metadata
            .fetch_database(name)
            .await?
            .ok_or_else(|| CataError::sql("3D000", "database does not exist"))?;
        if name == SYSTEM_DATABASE_NAME
            || database.value().state != DatabaseState::Ready as i32
            || !database.value().accepts_connections()
        {
            return Err(CataError::sql(
                "55000",
                "database does not allow SQL connections",
            ));
        }
        // Distributed database policy enforcement is not part of the foundation.
        // Do not mistake this process's session count for a cluster-wide limit.
        if database.value().effective_connection_limit() != -1 {
            return Err(CataError::sql(
                "0A000",
                "database connection-limit policy is not supported",
            ));
        }
        Ok(Admission {
            database_name: name.into(),
        })
    }
}

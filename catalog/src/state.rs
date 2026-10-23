use crate::command::{Alter, Command};
use crate::telemetry::Metrics;
use crate::{CataError, Result};
use lyra_meta::metadata::{Metadata, MetadataError, SYSTEM_DATABASE_NAME};
use lyra_meta::proto::pb_meta::{Database, DatabaseState};
use opentelemetry::metrics::Meter;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

pub(crate) struct State {
    // Control state
    pub shutdown: CancellationToken,
    pub operations: TaskTracker,
    // Immutable state
    pub metadata: Arc<dyn Metadata>,
    pub metrics: Metrics,
    // Mutable state
    pub gate: AsyncMutex<()>,
    pub ready: AtomicBool,
    healthy: AtomicBool,
    serial: AtomicU64,
    sessions: Mutex<HashMap<u64, Session>>,
    drained: Notify,
}

#[cfg(test)]
#[path = "state_tests.rs"]
mod tests;

struct Session {
    // Control state
    cancellation: CancellationToken,
    // Immutable state
    database_id: u32,
}

pub(crate) struct Admission {
    pub database_id: u32,
    pub user_id: u32,
    pub database_name: String,
    state: Weak<State>,
    serial: u64,
}

impl Drop for Admission {
    fn drop(&mut self) {
        if let Some(state) = self.state.upgrade() {
            let mut sessions = state.sessions.lock().unwrap();
            sessions.remove(&self.serial);
            state.metrics.active(sessions.len());
            state.drained.notify_waiters();
        }
    }
}

impl State {
    pub(crate) async fn new(metadata: Arc<dyn Metadata>, meter: Meter) -> Result<Arc<Self>> {
        metadata.validate_initialized().await?;
        // An interrupted drop is terminal, never retried automatically.
        for record in metadata.list_databases().await? {
            if record.value().state == DatabaseState::Dropping as i32 {
                let mut value = record.value().clone();
                value.state = DatabaseState::DropFailed as i32;
                metadata
                    .update_database(record.id(), value, record.version())
                    .await?;
                tracing::warn!(
                    operation = "drop",
                    outcome = "interrupted",
                    "database requires operator intervention"
                );
            }
        }
        let state = Arc::new(Self {
            shutdown: CancellationToken::new(),
            operations: TaskTracker::new(),
            metadata,
            metrics: Metrics::new(meter),
            gate: AsyncMutex::new(()),
            ready: AtomicBool::new(false),
            healthy: AtomicBool::new(true),
            serial: AtomicU64::new(0),
            sessions: Mutex::new(HashMap::new()),
            drained: Notify::new(),
        });
        state.metrics.active(0);
        state.set_ready(false);
        Ok(state)
    }

    pub(crate) fn set_ready(&self, value: bool) {
        let value = value && self.healthy.load(Ordering::Acquire);
        self.ready.store(value, Ordering::Release);
        self.metrics.ready(value);
    }

    pub(crate) async fn admit(
        self: &Arc<Self>,
        name: &str,
        user_id: u32,
        cancellation: CancellationToken,
    ) -> Result<Admission> {
        let _gate = self.gate.lock().await;
        if !self.ready.load(Ordering::Acquire) || self.shutdown.is_cancelled() {
            return Err(CataError::sql("57P03", "catalog is not ready"));
        }
        let database = self
            .metadata
            .get_database(name)
            .await?
            .ok_or_else(|| CataError::sql("3D000", "database does not exist"))?;
        if name == SYSTEM_DATABASE_NAME {
            return Err(CataError::sql(
                "42501",
                "system database does not allow SQL connections",
            ));
        }
        if database.value().state != DatabaseState::Ready as i32
            || !database.value().accepts_connections()
        {
            return Err(CataError::sql(
                "55000",
                "database does not allow connections",
            ));
        }
        let mut sessions = self.sessions.lock().unwrap();
        let count = sessions
            .values()
            .filter(|session| session.database_id == database.id())
            .count();
        let limit = database.value().effective_connection_limit();
        if limit >= 0 && count >= limit as usize {
            return Err(CataError::sql(
                "53300",
                "database connection limit exceeded",
            ));
        }
        let serial = self.serial.fetch_add(1, Ordering::Relaxed);
        sessions.insert(
            serial,
            Session {
                cancellation,
                database_id: database.id(),
            },
        );
        self.metrics.active(sessions.len());
        Ok(Admission {
            database_id: database.id(),
            user_id,
            database_name: name.into(),
            state: Arc::downgrade(self),
            serial,
        })
    }

    fn count(&self, database_id: u32) -> usize {
        self.sessions
            .lock()
            .unwrap()
            .values()
            .filter(|s| s.database_id == database_id)
            .count()
    }

    pub(crate) async fn execute(
        self: &Arc<Self>,
        command: Command,
        current_database: u32,
        current_user: u32,
    ) -> Result<bool> {
        // A client disconnect cannot cancel a confirmed lifecycle transition.
        let state = Arc::clone(self);
        self.operations
            .spawn(async move {
                let operation = match &command {
                    Command::Create { .. } => "create",
                    Command::Alter {
                        action: Alter::Owner(_),
                        ..
                    } => "alter_owner",
                    Command::Alter {
                        action: Alter::Options(_),
                        ..
                    } => "alter_connections",
                    Command::Alter {
                        action: Alter::ResetAll,
                        ..
                    } => "reset",
                    Command::Drop { force: true, .. } => "force_drop",
                    Command::Drop { .. } => "drop",
                    _ => return Err(CataError::sql("XX000", "invalid database operation")),
                };
                let started = Instant::now();
                let result = state
                    .execute0(command, current_database, current_user)
                    .await;
                if matches!(
                    result,
                    Err(CataError::Metadata(MetadataError::UncertainWrite))
                ) {
                    state.healthy.store(false, Ordering::Release);
                    state.set_ready(false);
                }
                state.metrics.operation(operation, started, &result);
                result
            })
            .await
            .map_err(|_| CataError::sql("XX000", "database operation task failed"))?
    }

    async fn execute0(
        &self,
        command: Command,
        current_database: u32,
        current_user: u32,
    ) -> Result<bool> {
        let gate = self.gate.lock().await;
        if !self.ready.load(Ordering::Acquire) {
            return Err(CataError::sql("57P03", "catalog is not ready"));
        }
        match command {
            Command::Create {
                name,
                if_not_exists,
                options,
            } => {
                if name == SYSTEM_DATABASE_NAME {
                    return Err(MetadataError::Reserved.into());
                }
                if self.metadata.get_database(&name).await?.is_some() {
                    return if if_not_exists {
                        Ok(false)
                    } else {
                        Err(MetadataError::AlreadyExists.into())
                    };
                }
                let owner = if let Some(name) = options.owner {
                    self.metadata
                        .get_user(&name)
                        .await?
                        .ok_or_else(|| CataError::sql("42704", "owner user does not exist"))?
                        .id()
                } else {
                    current_user
                };
                let mut value = Database::new(name, owner);
                if let Some(allow) = options.allow_connections {
                    value.allow_connections = Some(allow);
                }
                if let Some(limit) = options.connection_limit {
                    value.connection_limit = Some(limit);
                }
                self.metadata.create_database(value).await?;
                Ok(true)
            }
            Command::Alter { name, action } => {
                let record = self
                    .metadata
                    .get_database(&name)
                    .await?
                    .ok_or(MetadataError::NotFound)?;
                if name == SYSTEM_DATABASE_NAME {
                    return Err(MetadataError::Reserved.into());
                }
                if record.value().state != DatabaseState::Ready as i32 {
                    return Err(CataError::sql("55000", "database is not READY"));
                }
                let mut value = record.value().clone();
                match action {
                    Alter::Owner(name) => {
                        value.owner_user_id = self
                            .metadata
                            .get_user(&name)
                            .await?
                            .ok_or_else(|| CataError::sql("42704", "owner user does not exist"))?
                            .id();
                    }
                    Alter::Options(options) => {
                        if let Some(allow) = options.allow_connections {
                            value.allow_connections = Some(allow);
                        }
                        if let Some(limit) = options.connection_limit {
                            value.connection_limit = Some(limit);
                        }
                    }
                    Alter::ResetAll => return Ok(true),
                }
                self.metadata
                    .update_database(record.id(), value, record.version())
                    .await?;
                Ok(true)
            }
            Command::Drop {
                name,
                if_exists,
                force,
                timeout: deadline,
            } => {
                if name == SYSTEM_DATABASE_NAME {
                    return Err(MetadataError::Reserved.into());
                }
                let Some(record) = self.metadata.get_database(&name).await? else {
                    return if if_exists {
                        Ok(false)
                    } else {
                        Err(MetadataError::NotFound.into())
                    };
                };
                if record.id() == current_database {
                    return Err(CataError::sql("55006", "cannot drop the current database"));
                }
                if record.value().state != DatabaseState::Ready as i32 {
                    return Err(CataError::sql(
                        "55000",
                        "database is not READY; operator intervention is required",
                    ));
                }
                if !force {
                    if self.count(record.id()) > 0 {
                        return Err(CataError::sql("55006", "database is in use"));
                    }
                    self.metadata
                        .delete_database(record.id(), record.version())
                        .await?;
                    return Ok(true);
                }
                let mut value = record.value().clone();
                value.state = DatabaseState::Dropping as i32;
                let dropping = self
                    .metadata
                    .update_database(record.id(), value, record.version())
                    .await?;
                for session in self
                    .sessions
                    .lock()
                    .unwrap()
                    .values()
                    .filter(|s| s.database_id == record.id())
                {
                    session.cancellation.cancel();
                }
                drop(gate);
                let drained = timeout(deadline, async {
                    loop {
                        let notified = self.drained.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        if self.count(record.id()) == 0 {
                            break;
                        }
                        notified.await;
                    }
                })
                .await
                .is_ok();
                let _gate = self.gate.lock().await;
                let result = if drained {
                    self.metadata
                        .delete_database(dropping.id(), dropping.version())
                        .await
                        .map_err(CataError::from)
                } else {
                    Err(CataError::sql(
                        "57014",
                        "forced drop timed out; database is DROP_FAILED",
                    ))
                };
                if result.is_err() {
                    let mut value = dropping.value().clone();
                    value.state = DatabaseState::DropFailed as i32;
                    if self
                        .metadata
                        .update_database(dropping.id(), value, dropping.version())
                        .await
                        .is_err()
                    {
                        // Ambiguous transport outcomes remain non-connectable. Stop
                        // admission until an operator restarts and checks metadata.
                        self.healthy.store(false, Ordering::Release);
                        self.set_ready(false);
                    }
                    tracing::warn!(
                        operation = "drop",
                        outcome = "failed",
                        "database requires operator intervention"
                    );
                }
                result.map(|_| true)
            }
            _ => Err(CataError::sql("XX000", "invalid database operation")),
        }
    }
}

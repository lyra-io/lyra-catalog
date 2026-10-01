use crate::inventory;
use crate::inventory::SqlSession;
use crate::state::State;
use crate::{CataError, Result};
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::auth::{
    DefaultServerParameterProvider, StartupHandler, finish_authentication, protocol_negotiation,
    save_startup_parameters_to_metadata,
};
use datafusion_postgres::pgwire::api::{
    ClientInfo, ConnectionGuard, ConnectionHandle, ConnectionManager, PgWireConnectionState,
    PidSecretKeyGenerator, RandomPidSecretKeyGenerator,
};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::startup::{Authentication, PasswordMessageFamily};
use datafusion_postgres::pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use futures_util::{Sink, SinkExt};
use lyra_meta::metadata::validate_name;
use lyra_meta::proto::pb_meta::ScramSha256Verifier;
use rsasl::callback::{Context, Request, SessionCallback, SessionData};
use rsasl::mechanisms::scram::properties::ScramStoredPassword;
use rsasl::prelude::{Mechname, SASLConfig, SASLServer, Session, SessionError};
use rsasl::property::AuthzId;
use rsasl::validate::{Validate, Validation, ValidationError};
use std::fmt::Debug;
use std::sync::{Arc, Mutex, OnceLock};

pub(crate) struct Admitted {
    pub query: Arc<SqlSession>,
}
pub(crate) type Slot = Arc<OnceLock<Admitted>>;

struct Validated;
type ScramStep = (Vec<u8>, Option<(String, u32)>);
impl Validation for Validated {
    type Value = bool;
}

struct Verifier {
    value: ScramSha256Verifier,
    known: bool,
}
impl SessionCallback for Verifier {
    fn callback(
        &self,
        _: &SessionData,
        _: &Context,
        request: &mut Request,
    ) -> std::result::Result<(), SessionError> {
        // PostgreSQL authenticates the startup user, not SCRAM's AuthId.
        request.satisfy::<ScramStoredPassword>(&ScramStoredPassword::new(
            self.value.iterations,
            &self.value.salt,
            &self.value.stored_key,
            &self.value.server_key,
        ))?;
        Ok(())
    }
    fn validate(
        &self,
        _: &SessionData,
        context: &Context,
        validate: &mut Validate<'_>,
    ) -> std::result::Result<(), ValidationError> {
        validate.with::<Validated, _>(|| {
            Ok(self.known && context.get_ref::<AuthzId>().is_none_or(str::is_empty))
        })?;
        Ok(())
    }
}

struct Exchange {
    session: Session<Validated>,
    user_id: Option<u32>,
    database: String,
    first: bool,
}

pub(crate) struct AuthenticationHandler {
    // Control state
    // Immutable state
    state: Arc<State>,
    slot: Slot,
    manager: Arc<ConnectionManager>,
    pids: Arc<RandomPidSecretKeyGenerator>,
    dummy: ScramSha256Verifier,
    // Mutable state
    exchange: Mutex<Option<Exchange>>,
}

impl AuthenticationHandler {
    pub(crate) fn new(
        state: Arc<State>,
        slot: Slot,
        manager: Arc<ConnectionManager>,
        pids: Arc<RandomPidSecretKeyGenerator>,
        dummy: ScramSha256Verifier,
    ) -> Self {
        Self {
            state,
            slot,
            manager,
            pids,
            dummy,
            exchange: Mutex::new(None),
        }
    }
    fn failure(&self) -> PgWireError {
        self.state.metrics.authentication("invalid_credentials");
        CataError::sql("28P01", "password authentication failed").into()
    }
    fn step(&self, message: PgWireFrontendMessage) -> Result<ScramStep> {
        let mut guard = self.exchange.lock().unwrap();
        let exchange = guard
            .as_mut()
            .ok_or_else(|| CataError::sql("08P01", "unexpected authentication message"))?;
        let PgWireFrontendMessage::PasswordMessageFamily(message) = message else {
            return Err(CataError::sql("08P01", "unexpected authentication message"));
        };
        let data = if exchange.first {
            // Upstream's coercion assumes a complete SASL frame. Validate its
            // lengths before it can read/split the buffer or decode lossily.
            if let PasswordMessageFamily::Raw(body) = &message {
                let end = body
                    .iter()
                    .position(|byte| *byte == 0)
                    .ok_or_else(|| CataError::sql("28P01", "invalid SCRAM response"))?;
                if end > 64 || body.len() < end + 5 || std::str::from_utf8(&body[..end]).is_err() {
                    return Err(CataError::sql("28P01", "invalid SCRAM response"));
                }
                let length = i32::from_be_bytes(body[end + 1..end + 5].try_into().unwrap());
                if !(0..=4096).contains(&length) || body.len() != end + 5 + length as usize {
                    return Err(CataError::sql("28P01", "invalid SCRAM response"));
                }
            }
            let response = message
                .into_sasl_initial_response()
                .map_err(|_| CataError::sql("28P01", "invalid SCRAM response"))?;
            if response.auth_method != "SCRAM-SHA-256" {
                return Err(CataError::sql(
                    "28P01",
                    "unsupported authentication mechanism",
                ));
            }
            response
                .data
                .ok_or_else(|| CataError::sql("28P01", "missing SCRAM response"))?
        } else {
            message
                .into_sasl_response()
                .map_err(|_| CataError::sql("28P01", "invalid SCRAM response"))?
                .data
        };
        if data.len() > 4096 {
            return Err(CataError::sql("28P01", "SCRAM response is too large"));
        }
        let mut output = Vec::new();
        let state = exchange
            .session
            .step(Some(&data), &mut output)
            .map_err(|_| CataError::sql("28P01", "invalid SCRAM proof"))?;
        if state.is_running() {
            if !exchange.first {
                return Err(CataError::sql("28P01", "unexpected SCRAM round"));
            }
            exchange.first = false;
            Ok((output, None))
        } else {
            if exchange.first || exchange.session.validation() != Some(true) {
                return Err(CataError::sql("28P01", "invalid SCRAM proof"));
            }
            let id = exchange
                .user_id
                .ok_or_else(|| CataError::sql("28P01", "password authentication failed"))?;
            let database = exchange.database.clone();
            *guard = None;
            Ok((output, Some((database, id))))
        }
    }
}

#[async_trait]
impl StartupHandler for AuthenticationHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let PgWireFrontendMessage::Startup(startup) = message {
            if self.exchange.lock().unwrap().is_some() || self.slot.get().is_some() {
                return Err(self.failure());
            }
            protocol_negotiation(client, &startup).await?;
            save_startup_parameters_to_metadata(client, &startup);
            let user = startup
                .parameters
                .get("user")
                .ok_or_else(|| self.failure())?;
            validate_name(user).map_err(|_| self.failure())?;
            let database = startup
                .parameters
                .get("database")
                .map(String::as_str)
                .unwrap_or("public");
            validate_name(database)
                .map_err(|_| CataError::sql("3D000", "invalid startup database name"))?;
            if startup
                .parameters
                .get("client_encoding")
                .is_some_and(|v| !matches!(v.to_ascii_uppercase().as_str(), "UTF8" | "UTF-8"))
            {
                return Err(
                    CataError::sql("0A000", "only UTF8 client encoding is supported").into(),
                );
            }
            if startup.parameters.contains_key("options")
                || startup.parameters.contains_key("replication")
            {
                return Err(CataError::sql(
                    "0A000",
                    "startup options and replication are not supported",
                )
                .into());
            }
            client
                .metadata_mut()
                .insert("database".into(), database.into());
            let record = self
                .state
                .metadata
                .fetch_user(user)
                .await
                .map_err(CataError::from)?;
            let verifier = self
                .state
                .metadata
                .fetch_user_verifier(user)
                .await
                .map_err(CataError::from)?;
            let known = record.is_some() && verifier.is_some();
            let config = SASLConfig::builder()
                .with_defaults()
                .with_callback(Verifier {
                    value: verifier.unwrap_or_else(|| self.dummy.clone()),
                    known,
                })
                .map_err(|_| self.failure())?;
            let session = SASLServer::<Validated>::new(config)
                .start_suggested(Mechname::parse(b"SCRAM-SHA-256").expect("static mechanism"))
                .map_err(|_| self.failure())?;
            *self.exchange.lock().unwrap() = Some(Exchange {
                session,
                user_id: record.map(|r| r.id()),
                database: database.into(),
                first: true,
            });
            let (pid, secret) = self.pids.generate(client);
            client.set_pid_and_secret_key(pid, secret);
            client.set_state(PgWireConnectionState::AuthenticationInProgress);
            client
                .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                    vec!["SCRAM-SHA-256".into()],
                )))
                .await?;
            return Ok(());
        }
        let (output, authenticated) = self.step(message).map_err(|_| self.failure())?;
        if let Some((database, _user_id)) = authenticated {
            let admission = self.state.admit(&database).await?;
            let query =
                inventory::session(&admission.database_name, Arc::clone(&self.state.metadata))?;
            self.slot
                .set(Admitted { query })
                .map_err(|_| self.failure())?;
            let (pid, secret) = client.pid_and_secret_key();
            let (handle, guard) = self.manager.register(pid, secret);
            client
                .session_extensions()
                .insert::<Arc<ConnectionHandle>>(handle);
            client.session_extensions().insert::<ConnectionGuard>(guard);
            client
                .send(PgWireBackendMessage::Authentication(
                    Authentication::SASLFinal(output.into()),
                ))
                .await?;
            let mut parameters = DefaultServerParameterProvider::default();
            parameters.is_superuser = false;
            parameters.server_version = "16.0-lyra".into();
            finish_authentication(client, &parameters).await?;
        } else {
            client
                .send(PgWireBackendMessage::Authentication(
                    Authentication::SASLContinue(output.into()),
                ))
                .await?;
        }
        Ok(())
    }
}

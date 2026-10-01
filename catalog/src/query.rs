use crate::CataError;
use crate::authentication::Slot;
use crate::command::{self, Command, Parsed};
use crate::state::State;
use async_trait::async_trait;
use datafusion::logical_expr::LogicalPlan;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::pgwire::api::portal::{Format, Portal};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{
    DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag,
};
use datafusion_postgres::pgwire::api::stmt::QueryParser;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore, Type};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::pgwire::messages::response::TransactionStatus;
use datafusion_postgres::{DfSessionService, Parser as DfParser};
use futures_util::{Sink, SinkExt, stream};
use std::fmt::Debug;
use std::sync::Arc;

type Planned = (String, Option<(Statement, LogicalPlan)>);

pub(crate) struct Query {
    // Immutable state
    state: Arc<State>,
    slot: Slot,
    parser: Arc<Parser>,
}
pub(crate) struct Parser {
    // Immutable state
    slot: Slot,
    fallback: Arc<DfParser>,
}

impl Query {
    pub(crate) fn new(state: Arc<State>, slot: Slot, fallback: &DfSessionService) -> Self {
        Self {
            state,
            slot: Arc::clone(&slot),
            parser: Arc::new(Parser {
                slot,
                fallback: fallback.query_parser(),
            }),
        }
    }
    async fn execute<C>(
        &self,
        client: &mut C,
        parsed: Parsed,
        format: Option<&Format>,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _current = self
            .slot
            .get()
            .ok_or_else(|| CataError::sql("28000", "authentication required"))?;
        if client.transaction_status() == TransactionStatus::Error
            && !matches!(parsed.command, Command::Rollback | Command::Commit)
        {
            return Err(CataError::sql("25P02", "current transaction is aborted").into());
        }
        if parsed.truncated {
            notice(client, "42622", "identifier truncated to at most 63 bytes").await?;
        }
        match parsed.command {
            Command::Begin => {
                client.set_transaction_status(TransactionStatus::Transaction);
                Ok(Response::TransactionStart(Tag::new("BEGIN")))
            }
            Command::Commit => {
                let failed = client.transaction_status() == TransactionStatus::Error;
                client.set_transaction_status(TransactionStatus::Idle);
                Ok(Response::TransactionEnd(Tag::new(if failed {
                    "ROLLBACK"
                } else {
                    "COMMIT"
                })))
            }
            Command::Rollback => {
                client.set_transaction_status(TransactionStatus::Idle);
                Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
            }
            Command::Show { pattern } => {
                let result = self
                    .state
                    .metadata
                    .list_databases()
                    .await
                    .map_err(CataError::from);
                let mut names = result?
                    .into_iter()
                    .map(|r| r.value().name.clone())
                    .collect::<Vec<_>>();
                if let Some(pattern) = pattern {
                    names.retain(|name| matches_like(name, &pattern));
                }
                names.sort();
                let fields = Arc::new(fields(format));
                let rows_fields = Arc::clone(&fields);
                let rows = stream::iter(names.into_iter().map(move |name| {
                    let mut encoder = DataRowEncoder::new(Arc::clone(&rows_fields));
                    encoder.encode_field(&name)?;
                    Ok(encoder.take_row())
                }));
                let mut response = QueryResponse::new(fields, rows);
                response.set_command_tag("SHOW");
                Ok(Response::Query(response))
            }
            Command::Query(_) => Err(CataError::sql("XX000", "invalid query dispatch").into()),
        }
    }
}

async fn notice<C>(client: &mut C, code: &str, message: &str) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    client
        .send(PgWireBackendMessage::NoticeResponse(
            ErrorInfo::new("NOTICE".into(), code.into(), message.into()).into(),
        ))
        .await?;
    Ok(())
}

fn fields(format: Option<&Format>) -> Vec<FieldInfo> {
    vec![FieldInfo::new(
        "name".into(),
        None,
        None,
        Type::TEXT,
        format.map(|f| f.format_for(0)).unwrap_or(FieldFormat::Text),
    )]
}

#[async_trait]
impl SimpleQueryHandler for Query {
    async fn do_query<C>(&self, client: &mut C, sql: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut responses = Vec::new();
        let parsed = command::parse(sql)?;
        if parsed.is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        for parsed in parsed {
            let result = if let Command::Query(sql) = &parsed.command {
                if client.transaction_status() == TransactionStatus::Error {
                    Err(CataError::sql("25P02", "current transaction is aborted").into())
                } else {
                    let current = self
                        .slot
                        .get()
                        .ok_or_else(|| CataError::sql("28000", "authentication required"))?;
                    SimpleQueryHandler::do_query(current.query.service(), client, sql).await
                }
            } else {
                self.execute(client, parsed, None)
                    .await
                    .map(|response| vec![response])
            };
            match result {
                Ok(next) => responses.extend(next),
                Err(error) => {
                    // Preserve completed responses and stop at the first error.
                    responses.push(Response::Error(Box::new(error.into())));
                    break;
                }
            }
        }
        Ok(responses)
    }
}

#[async_trait]
impl ExtendedQueryHandler for Query {
    type Statement = Planned;
    type QueryParser = Parser;
    fn query_parser(&self) -> Arc<Parser> {
        Arc::clone(&self.parser)
    }
    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Planned>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Planned>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let mut parsed = command::parse(&portal.statement.statement.0)?;
        if parsed.is_empty() {
            return Ok(Response::EmptyQuery);
        }
        if parsed.len() != 1 {
            return Err(CataError::sql("42601", "prepared statements require one command").into());
        }
        let parsed = parsed.remove(0);
        if matches!(parsed.command, Command::Query(_)) {
            if client.transaction_status() == TransactionStatus::Error {
                return Err(CataError::sql("25P02", "current transaction is aborted").into());
            }
            let current = self
                .slot
                .get()
                .ok_or_else(|| CataError::sql("28000", "authentication required"))?;
            ExtendedQueryHandler::do_query(current.query.service(), client, portal, max_rows).await
        } else {
            self.execute(client, parsed, Some(&portal.result_column_format))
                .await
        }
    }
}

#[async_trait]
impl QueryParser for Parser {
    type Statement = Planned;
    async fn parse_sql<C>(
        &self,
        client: &C,
        sql: &str,
        types: &[Option<Type>],
    ) -> PgWireResult<Planned>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let parsed = command::parse(sql)?;
        if parsed.len() > 1 {
            return Err(CataError::sql("42601", "prepared statements require one command").into());
        }
        if parsed
            .first()
            .is_some_and(|p| matches!(p.command, Command::Query(_)))
        {
            let current = self
                .slot
                .get()
                .ok_or_else(|| CataError::sql("28000", "authentication required"))?;
            current
                .query
                .service()
                .query_parser()
                .parse_sql(client, sql, types)
                .await
        } else {
            if !types.is_empty() {
                return Err(
                    CataError::sql("0A000", "database DDL parameters are unsupported").into(),
                );
            }
            Ok((sql.into(), None))
        }
    }
    fn get_parameter_types(&self, statement: &Planned) -> PgWireResult<Vec<Type>> {
        if statement.1.is_none() {
            Ok(Vec::new())
        } else {
            self.fallback.get_parameter_types(statement)
        }
    }
    fn get_result_schema(
        &self,
        statement: &Planned,
        format: Option<&Format>,
    ) -> PgWireResult<Vec<FieldInfo>> {
        if statement.1.is_some() {
            return self.fallback.get_result_schema(statement, format);
        }
        let parsed = command::parse(&statement.0)?;
        if parsed
            .first()
            .is_some_and(|p| matches!(p.command, Command::Show { .. }))
        {
            Ok(fields(format))
        } else {
            Ok(Vec::new())
        }
    }
}

fn matches_like(value: &str, pattern: &str) -> bool {
    let value = value.chars().collect::<Vec<_>>();
    let mut matched = vec![false; value.len() + 1];
    matched[0] = true;
    let mut chars = pattern.chars();
    while let Some(mut ch) = chars.next() {
        let escaped = ch == '\\';
        if escaped {
            let Some(next) = chars.next() else {
                return false;
            };
            ch = next;
        }
        let any = ch == '%' && !escaped;
        let one = ch == '_' && !escaped;
        let mut next = vec![false; value.len() + 1];
        next[0] = any && matched[0];
        for i in 1..=value.len() {
            next[i] = if any {
                next[i - 1] || matched[i]
            } else {
                matched[i - 1] && (one || value[i - 1] == ch)
            };
        }
        matched = next;
    }
    matched[value.len()]
}

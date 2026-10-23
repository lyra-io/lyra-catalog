use super::QueryHandler;
use super::{client_database, client_schemas, client_user};
use crate::error::to_pgwire_error;
use crate::sql::{CataQueryParser, DataFusionStatement, parse_catalog_statement};
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::ExtendedQueryHandler;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures_util::Sink;
use std::fmt::Debug;
use std::sync::Arc;

#[async_trait]
impl ExtendedQueryHandler for QueryHandler {
    type Statement = DataFusionStatement;
    type QueryParser = CataQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.parser)
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let database = client_database(client).to_string();
        let schemas = client_schemas(client);
        let current_user = client_user(client).map(ToString::to_string);
        let sql = &portal.statement.statement.0;
        if let Some(statement) = parse_catalog_statement(sql).map_err(to_pgwire_error)? {
            return self
                .execute(
                    &database,
                    &schemas,
                    current_user.as_deref(),
                    statement,
                    Some(&portal.result_column_format),
                )
                .await;
        }

        let datafusion = self.databases.get(&database).map_err(to_pgwire_error)?;
        ExtendedQueryHandler::do_query(datafusion.as_ref(), client, portal, max_rows).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::DatabaseHandles;
    use datafusion_postgres::pgwire::api::portal::Portal;
    use datafusion_postgres::pgwire::api::query::ExtendedQueryHandler;
    use datafusion_postgres::pgwire::api::stmt::StoredStatement;
    use datafusion_postgres::testing::MockClient;
    use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataPutCondition, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use lyra_meta::proto::pb_catalog::{Database, Schema};

    async fn handler0() -> QueryHandler {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_database(
                Database {
                    name: DEFAULT_DATABASE_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        QueryHandler::new(databases, metadata)
    }

    fn portal0(sql: &str) -> Portal<DataFusionStatement> {
        let stored = Arc::new(StoredStatement::new(
            String::new(),
            (sql.to_string(), None),
            vec![],
        ));
        Portal::new_cursor(String::new(), stored)
    }

    #[tokio::test]
    async fn routes_catalog_statements_to_the_catalog_executor() {
        let handler = handler0().await;
        let mut client = MockClient::new();
        let portal = portal0("CREATE USER Alice WITH PASSWORD 's3cr3t'");

        let response = ExtendedQueryHandler::do_query(&handler, &mut client, &portal, 100)
            .await
            .unwrap();
        assert!(matches!(response, Response::Execution(_)));
    }

    #[tokio::test]
    async fn delegates_datafusion_statements() {
        let handler = handler0().await;
        let mut client = MockClient::new();
        let portal = portal0("SELECT 1");

        let response = ExtendedQueryHandler::do_query(&handler, &mut client, &portal, 100)
            .await
            .unwrap();
        assert!(!matches!(response, Response::Error(_)));
    }
}

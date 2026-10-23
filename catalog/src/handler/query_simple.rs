use super::QueryHandler;
use super::{client_database, client_schemas, client_user};
use crate::error::to_pgwire_error;
use crate::sql::parse_catalog_statement;
use async_trait::async_trait;
use datafusion_postgres::pgwire::api::query::SimpleQueryHandler;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use futures_util::Sink;
use std::fmt::Debug;

#[async_trait]
impl SimpleQueryHandler for QueryHandler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let database = client_database(client).to_string();
        let schemas = client_schemas(client);
        let current_user = client_user(client).map(ToString::to_string);
        if let Some(statement) = parse_catalog_statement(query).map_err(to_pgwire_error)? {
            return Ok(vec![
                self.execute(
                    &database,
                    &schemas,
                    current_user.as_deref(),
                    statement,
                    None,
                )
                .await?,
            ]);
        }

        let datafusion = self.databases.get(&database).map_err(to_pgwire_error)?;
        SimpleQueryHandler::do_query(datafusion.as_ref(), client, query).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::DatabaseHandles;
    use datafusion_postgres::pgwire::api::query::SimpleQueryHandler;
    use datafusion_postgres::testing::MockClient;
    use lyra_meta::metadata::{MemoryMetadata, Metadata, MetadataPutCondition, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use lyra_meta::proto::pb_catalog::{Database, Schema};
    use std::sync::Arc;

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

    #[tokio::test]
    async fn routes_catalog_statements_to_the_catalog_executor() {
        let handler = handler0().await;
        let mut client = MockClient::new();

        let responses = SimpleQueryHandler::do_query(
            &handler,
            &mut client,
            "CREATE USER Alice WITH PASSWORD 's3cr3t'",
        )
        .await
        .unwrap();

        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Response::Execution(_)));
    }

    #[tokio::test]
    async fn delegates_datafusion_statements() {
        let handler = handler0().await;
        let mut client = MockClient::new();

        let responses = SimpleQueryHandler::do_query(&handler, &mut client, "SELECT 1").await.unwrap();

        assert_eq!(responses.len(), 1);
        assert!(matches!(&responses[0], Response::Query(_)));
    }
}

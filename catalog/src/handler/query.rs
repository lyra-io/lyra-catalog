use super::DatabaseHandles;
use crate::error::to_pgwire_error;
use crate::executor::{DatabaseExecutor, SchemaExecutor, SecretExecutor, UserExecutor};
use crate::sql::{
    CataQueryParser, CatalogStatement, DatabaseStatement, SchemaStatement, SecretStatement,
    UserStatement, show_names_fields,
};
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{DataRowEncoder, QueryResponse, Response, Tag};
use datafusion_postgres::pgwire::error::PgWireResult;
use futures_util::stream;
use lyra_meta::metadata::Metadata;
use std::sync::Arc;

pub(crate) struct QueryHandler {
    // Immutable state
    pub(super) databases: Arc<DatabaseHandles>,
    pub(super) parser: Arc<CataQueryParser>,
    database_executor: DatabaseExecutor,
    schema_executor: SchemaExecutor,
    secret_executor: SecretExecutor,
    user_executor: UserExecutor,
}

impl QueryHandler {
    pub(crate) fn new(databases: Arc<DatabaseHandles>, metadata: Arc<dyn Metadata>) -> Self {
        let parser = Arc::new(CataQueryParser::new(Arc::clone(&databases)));
        let database_executor =
            DatabaseExecutor::new(Arc::clone(&metadata), Arc::clone(&databases));
        let schema_executor = SchemaExecutor::new(Arc::clone(&metadata));
        let secret_executor = SecretExecutor::new(Arc::clone(&metadata));
        let user_executor = UserExecutor::new(metadata);
        Self {
            databases,
            parser,
            database_executor,
            schema_executor,
            secret_executor,
            user_executor,
        }
    }

    pub(super) async fn execute(
        &self,
        database: &str,
        schemas: &[String],
        current_user: Option<&str>,
        statement: CatalogStatement,
        column_format: Option<&Format>,
    ) -> PgWireResult<Response> {
        match statement {
            CatalogStatement::Database(DatabaseStatement::Create(statement)) => {
                self.database_executor
                    .create(&statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE DATABASE")))
            }
            CatalogStatement::Database(DatabaseStatement::Alter(statement)) => {
                self.database_executor
                    .alter(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("ALTER DATABASE")))
            }
            CatalogStatement::Database(DatabaseStatement::Drop(statement)) => {
                self.database_executor
                    .drop(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("DROP DATABASE")))
            }
            CatalogStatement::Database(DatabaseStatement::Show(statement)) => {
                let databases = self
                    .database_executor
                    .show(&statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Self::show0(databases, column_format)
            }
            CatalogStatement::Schema(SchemaStatement::Create(statement)) => {
                self.schema_executor
                    .create(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SCHEMA")))
            }
            CatalogStatement::Schema(SchemaStatement::Alter(statement)) => {
                self.schema_executor
                    .alter(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("ALTER SCHEMA")))
            }
            CatalogStatement::Schema(SchemaStatement::Drop(statement)) => {
                self.schema_executor
                    .drop(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("DROP SCHEMA")))
            }
            CatalogStatement::Schema(SchemaStatement::Show(statement)) => {
                let schemas = self
                    .schema_executor
                    .show(database, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Self::show0(schemas, column_format)
            }
            CatalogStatement::Secret(SecretStatement::Create(statement)) => {
                self.secret_executor
                    .create(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Alter(statement)) => {
                self.secret_executor
                    .alter(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("ALTER SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Drop(statement)) => {
                self.secret_executor
                    .drop(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("DROP SECRET")))
            }
            CatalogStatement::Secret(SecretStatement::Show(statement)) => {
                let secrets = self
                    .secret_executor
                    .show(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Self::show0(secrets, column_format)
            }
            CatalogStatement::User(UserStatement::Create(statement)) => {
                self.user_executor
                    .create(database, schemas, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("CREATE USER")))
            }
            CatalogStatement::User(UserStatement::Alter(statement)) => {
                self.user_executor
                    .alter(database, schemas, current_user, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("ALTER USER")))
            }
            CatalogStatement::User(UserStatement::Drop(statement)) => {
                self.user_executor
                    .drop(current_user, &statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Ok(Response::Execution(Tag::new("DROP USER")))
            }
            CatalogStatement::User(UserStatement::Show(statement)) => {
                let users = self
                    .user_executor
                    .show(&statement)
                    .await
                    .map_err(to_pgwire_error)?;
                Self::show0(users, column_format)
            }
        }
    }

    fn show0(names: Vec<String>, column_format: Option<&Format>) -> PgWireResult<Response> {
        let fields = Arc::new(show_names_fields(column_format));
        let row_fields = Arc::clone(&fields);
        let rows = stream::iter(names.into_iter().map(move |name| {
            let mut encoder = DataRowEncoder::new(Arc::clone(&row_fields));
            encoder.encode_field(&Some(name.as_str()))?;
            Ok(encoder.take_row())
        }));
        let mut response = QueryResponse::new(fields, rows);
        response.set_command_tag("SHOW");
        Ok(Response::Query(response))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::{
        AlterDatabase, AlterSchema, AlterSecret, AlterUser, AlterUserAction, CreateDatabase,
        CreateSchema, CreateSecret, CreateUser, DropDatabase, DropSchema, DropSecret, DropUser,
        SchemaName, SecretName, ShowDatabases, ShowSchemas, ShowSecrets, ShowUsers, Value,
    };
    use lyra_meta::metadata::{MemoryMetadata, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
    use lyra_meta::proto::pb_catalog::{Database, Schema};

    async fn handler0() -> QueryHandler {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_database(
                Database {
                    name: DEFAULT_DATABASE_NAME.to_string(),
                },
                lyra_meta::metadata::MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                lyra_meta::metadata::MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        QueryHandler::new(databases, metadata)
    }

    fn is_execution(response: &Response) -> bool {
        matches!(response, Response::Execution(_))
    }

    fn is_query(response: &Response) -> bool {
        matches!(response, Response::Query(_))
    }

    #[tokio::test]
    async fn executes_database_statements() {
        let handler = handler0().await;

        let response = handler
            .execute(
                "other",
                &[],
                None,
                CatalogStatement::Database(DatabaseStatement::Create(CreateDatabase::new(
                    "db1".to_string(),
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                "other",
                &[],
                None,
                CatalogStatement::Database(DatabaseStatement::Show(ShowDatabases::new(None))),
                None,
            )
            .await
            .unwrap();
        assert!(is_query(&response));

        let response = handler
            .execute(
                "other",
                &[],
                None,
                CatalogStatement::Database(DatabaseStatement::Alter(AlterDatabase::new(
                    "db1".to_string(),
                    "db2".to_string(),
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

    }

    #[tokio::test]
    async fn executes_schema_statements() {
        let handler = handler0().await;
        let schemas = vec![DEFAULT_SCHEMA_NAME.to_string()];

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Schema(SchemaStatement::Create(CreateSchema::new(
                    SchemaName::new(None, "s1".to_string()),
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Schema(SchemaStatement::Show(ShowSchemas::new(None))),
                None,
            )
            .await
            .unwrap();
        assert!(is_query(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Schema(SchemaStatement::Alter(AlterSchema::new(
                    SchemaName::new(None, "s1".to_string()),
                    "s2".to_string(),
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Schema(SchemaStatement::Drop(DropSchema::new(
                    SchemaName::new(None, "s2".to_string()),
                    false,
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));
    }

    #[tokio::test]
    async fn executes_secret_statements() {
        let handler = handler0().await;
        let schemas = vec![DEFAULT_SCHEMA_NAME.to_string()];

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Secret(SecretStatement::Create(CreateSecret::new(
                    SecretName::new(None, "s1".to_string()),
                    b"v".to_vec(),
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Secret(SecretStatement::Show(ShowSecrets::new(None, None))),
                None,
            )
            .await
            .unwrap();
        assert!(is_query(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Secret(SecretStatement::Alter(AlterSecret::new(
                    SecretName::new(None, "s1".to_string()),
                    b"new".to_vec(),
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::Secret(SecretStatement::Drop(DropSecret::new(
                    SecretName::new(None, "s1".to_string()),
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));
    }

    #[tokio::test]
    async fn executes_user_statements() {
        let handler = handler0().await;
        let schemas = vec![DEFAULT_SCHEMA_NAME.to_string()];

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::User(UserStatement::Create(CreateUser::new(
                    "alice".to_string(),
                    Value::Literal("password".to_string()),
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::User(UserStatement::Show(ShowUsers::new(None))),
                None,
            )
            .await
            .unwrap();
        assert!(is_query(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::User(UserStatement::Alter(AlterUser::new(
                    "alice".to_string(),
                    AlterUserAction::Rename("bob".to_string()),
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));

        let response = handler
            .execute(
                DEFAULT_DATABASE_NAME,
                &schemas,
                None,
                CatalogStatement::User(UserStatement::Drop(DropUser::new(
                    vec!["bob".to_string()],
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));
    }

    #[tokio::test]
    async fn drops_an_empty_database() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .put_database(
                Database {
                    name: "empty".to_string(),
                },
                lyra_meta::metadata::MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let databases = Arc::new(DatabaseHandles::new(metadata.clone()).unwrap());
        let handler = QueryHandler::new(databases, metadata);

        let response = handler
            .execute(
                "other",
                &[],
                None,
                CatalogStatement::Database(DatabaseStatement::Drop(DropDatabase::new(
                    "empty".to_string(),
                    false,
                ))),
                None,
            )
            .await
            .unwrap();
        assert!(is_execution(&response));
    }
}

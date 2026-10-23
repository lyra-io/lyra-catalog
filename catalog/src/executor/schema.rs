use super::matches_like;
use crate::sql::{AlterSchema, CreateSchema, DropSchema, ShowSchemas};
use crate::{CataError, Result};
use lyra_meta::metadata::{Metadata, MetadataError, MetadataPutCondition};
use lyra_meta::proto::pb_catalog::Schema;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSchemaOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropSchemaOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct SchemaExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl SchemaExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(
        &self,
        current_database: &str,
        statement: &CreateSchema,
    ) -> Result<CreateSchemaOutcome> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;

        if self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateSchemaOutcome::AlreadyExists)
            } else {
                Err(CataError::SchemaAlreadyExists {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                })
            };
        }

        let schema = Schema {
            name: statement.schema().name().to_string(),
        };
        match self
            .metadata
            .put_schema(database, schema, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) => Ok(CreateSchemaOutcome::Created),
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                Ok(CreateSchemaOutcome::AlreadyExists)
            }
            Err(MetadataError::Conflict(_)) => Err(CataError::SchemaAlreadyExists {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            }),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn alter(&self, current_database: &str, statement: &AlterSchema) -> Result<()> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;
        let Some(record) = self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
        else {
            return Err(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        };
        if !schema_is_empty(&self.metadata, database, statement.schema().name()).await? {
            return Err(CataError::SchemaNotEmpty {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        }

        let mut new_schema = record.value().clone();
        new_schema.name = statement.new_name().to_string();
        let new_version = self
            .metadata
            .put_schema(database, new_schema, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::SchemaAlreadyExists {
                    database: database.to_string(),
                    schema: statement.new_name().to_string(),
                },
                error => error.into(),
            })?;

        if let Err(error) = self
            .metadata
            .delete_schema(database, statement.schema().name(), Some(record.version()))
            .await
        {
            let _ = self
                .metadata
                .delete_schema(database, statement.new_name(), Some(new_version))
                .await;
            return Err(match error {
                MetadataError::Conflict(_) => CataError::SchemaChanged {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                },
                error => error.into(),
            });
        }
        Ok(())
    }

    pub async fn drop(
        &self,
        current_database: &str,
        statement: &DropSchema,
    ) -> Result<DropSchemaOutcome> {
        let database = statement.schema().database().unwrap_or(current_database);
        self.require_database0(database).await?;
        let Some(record) = self
            .metadata
            .get_schema(database, statement.schema().name())
            .await?
        else {
            return if statement.if_exists() {
                Ok(DropSchemaOutcome::NotFound)
            } else {
                Err(CataError::SchemaNotFound {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                })
            };
        };

        if statement.cascade() {
            drop_schema_contents0(&self.metadata, database, statement.schema().name()).await?;
        } else if !schema_is_empty(&self.metadata, database, statement.schema().name()).await? {
            return Err(CataError::SchemaNotEmpty {
                database: database.to_string(),
                schema: statement.schema().name().to_string(),
            });
        }

        self.metadata
            .delete_schema(database, statement.schema().name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::SchemaChanged {
                    database: database.to_string(),
                    schema: statement.schema().name().to_string(),
                },
                error => error.into(),
            })?;
        Ok(DropSchemaOutcome::Dropped)
    }

    pub async fn show(&self, database: &str, statement: &ShowSchemas) -> Result<Vec<String>> {
        self.require_database0(database).await?;
        let mut schemas = self
            .metadata
            .list_schemas(database)
            .await?
            .into_iter()
            .filter(|schema| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&schema.value().name, pattern))
            })
            .map(|schema| schema.value().name.clone())
            .collect::<Vec<_>>();
        schemas.sort();
        Ok(schemas)
    }

    async fn require_database0(&self, database: &str) -> Result<()> {
        if self.metadata.get_database(database).await?.is_none() {
            return Err(CataError::DatabaseNotFound(database.to_string()));
        }
        Ok(())
    }
}

pub(crate) async fn schema_is_empty(
    metadata: &Arc<dyn Metadata>,
    database: &str,
    schema: &str,
) -> Result<bool> {
    Ok(metadata
        .list_connections(database, schema)
        .await?
        .is_empty()
        && metadata.list_secrets(database, schema).await?.is_empty())
}

async fn drop_schema_contents0(
    metadata: &Arc<dyn Metadata>,
    database: &str,
    schema: &str,
) -> Result<()> {
    for record in metadata.list_connections(database, schema).await? {
        metadata
            .delete_connection(
                database,
                schema,
                &record.value().name,
                Some(record.version()),
            )
            .await?;
    }
    for record in metadata.list_secrets(database, schema).await? {
        metadata
            .delete_secret(
                database,
                schema,
                &record.value().name,
                Some(record.version()),
            )
            .await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::SchemaName;
    use crate::test_support::FaultMetadata;
    use lyra_meta::metadata::MemoryMetadata;
    use lyra_meta::proto::pb_catalog::{Connection, Database, Secret, SecretRef};

    async fn seed_database(metadata: &dyn Metadata, database: &str) {
        metadata
            .put_database(
                Database {
                    name: database.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
    }

    async fn seed_schema(metadata: &dyn Metadata, database: &str, schema: &str) {
        metadata
            .put_schema(
                database,
                Schema {
                    name: schema.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_adds_schemas_to_the_current_or_explicit_database() {
        let metadata = MemoryMetadata::new();
        seed_database(&metadata, "db1").await;
        seed_database(&metadata, "db2").await;
        let executor = SchemaExecutor::new(Arc::new(metadata));

        assert_eq!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), false)
                )
                .await
                .unwrap(),
            CreateSchemaOutcome::Created
        );
        assert_eq!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(Some("db2".to_string()), "s2".to_string()), false)
                )
                .await
                .unwrap(),
            CreateSchemaOutcome::Created
        );
    }

    #[tokio::test]
    async fn create_rejects_missing_databases() {
        let executor = SchemaExecutor::new(Arc::new(MemoryMetadata::new()));

        assert!(matches!(
            executor
                .create(
                    "missing",
                    &CreateSchema::new(SchemaName::new(None, "s".to_string()), false)
                )
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn create_handles_existing_schemas() {
        let metadata = MemoryMetadata::new();
        seed_database(&metadata, "db1").await;
        seed_schema(&metadata, "db1", "s1").await;
        let executor = SchemaExecutor::new(Arc::new(metadata));

        assert_eq!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), true)
                )
                .await
                .unwrap(),
            CreateSchemaOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), false)
                )
                .await,
            Err(CataError::SchemaAlreadyExists { schema, .. }) if schema == "s1"
        ));
    }

    #[tokio::test]
    async fn create_handles_put_conflicts_and_failures() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        let executor = SchemaExecutor::new(metadata.clone());

        metadata.conflict("put_schema");
        assert_eq!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), true)
                )
                .await
                .unwrap(),
            CreateSchemaOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), false)
                )
                .await,
            Err(CataError::SchemaAlreadyExists { schema, .. }) if schema == "s1"
        ));

        metadata.clear("put_schema");
        metadata.fail("put_schema");
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &CreateSchema::new(SchemaName::new(None, "s1".to_string()), false)
                )
                .await,
            Err(CataError::Metadata(_))
        ));
    }

    #[tokio::test]
    async fn alter_renames_schemas() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        seed_schema(metadata.as_ref(), "db1", "s1").await;
        let executor = SchemaExecutor::new(metadata.clone());

        executor
            .alter(
                "db1",
                &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s2".to_string()),
            )
            .await
            .unwrap();
        assert!(metadata.get_schema("db1", "s1").await.unwrap().is_none());
        assert!(metadata.get_schema("db1", "s2").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn alter_rejects_missing_databases_schemas_and_nonempty() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        seed_schema(metadata.as_ref(), "db1", "s1").await;
        let executor = SchemaExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .alter(
                    "missing",
                    &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s2".to_string())
                )
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &AlterSchema::new(SchemaName::new(None, "missing".to_string()), "s2".to_string())
                )
                .await,
            Err(CataError::SchemaNotFound { schema, .. }) if schema == "missing"
        ));

        metadata
            .put_secret(
                "db1",
                "s1",
                Secret {
                    name: "password".to_string(),
                    value: b"x".to_vec().into(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s2".to_string())
                )
                .await,
            Err(CataError::SchemaNotEmpty { schema, .. }) if schema == "s1"
        ));
    }

    #[tokio::test]
    async fn alter_handles_conflicts_and_failures() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        seed_schema(metadata.as_ref(), "db1", "s1").await;
        seed_schema(metadata.as_ref(), "db1", "s2").await;
        let executor = SchemaExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s2".to_string())
                )
                .await,
            Err(CataError::SchemaAlreadyExists { schema, .. }) if schema == "s2"
        ));

        metadata.fail("delete_schema");
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s3".to_string())
                )
                .await,
            Err(CataError::Metadata(_))
        ));
        metadata.clear("delete_schema");

        metadata.conflict("delete_schema");
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &AlterSchema::new(SchemaName::new(None, "s1".to_string()), "s4".to_string())
                )
                .await,
            Err(CataError::SchemaChanged { schema, .. }) if schema == "s1"
        ));
    }

    #[tokio::test]
    async fn drop_handles_success_and_missing_schemas() {
        let metadata = MemoryMetadata::new();
        seed_database(&metadata, "db1").await;
        seed_schema(&metadata, "db1", "s1").await;
        let executor = SchemaExecutor::new(Arc::new(metadata));

        assert_eq!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "s1".to_string()), false, false)
                )
                .await
                .unwrap(),
            DropSchemaOutcome::Dropped
        );
        assert_eq!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "missing".to_string()), true, false)
                )
                .await
                .unwrap(),
            DropSchemaOutcome::NotFound
        );
        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "missing".to_string()), false, false)
                )
                .await,
            Err(CataError::SchemaNotFound { schema, .. }) if schema == "missing"
        ));
        assert!(matches!(
            executor
                .drop(
                    "missing",
                    &DropSchema::new(SchemaName::new(None, "s1".to_string()), false, false)
                )
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn drop_cascades_or_rejects_nonempty_schemas() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        seed_schema(metadata.as_ref(), "db1", "s1").await;
        metadata
            .put_secret(
                "db1",
                "s1",
                Secret {
                    name: "password".to_string(),
                    value: b"x".to_vec().into(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_connection(
                "db1",
                "s1",
                Connection {
                    name: "conn".to_string(),
                    options: Default::default(),
                    secret_refs: vec![SecretRef {
                        name: "password".to_string(),
                        data: "password".to_string(),
                    }],
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SchemaExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "s1".to_string()), false, false)
                )
                .await,
            Err(CataError::SchemaNotEmpty { schema, .. }) if schema == "s1"
        ));

        assert_eq!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "s1".to_string()), false, true)
                )
                .await
                .unwrap(),
            DropSchemaOutcome::Dropped
        );
        assert!(metadata.get_schema("db1", "s1").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn drop_reports_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_database(metadata.as_ref(), "db1").await;
        seed_schema(metadata.as_ref(), "db1", "s1").await;
        let executor = SchemaExecutor::new(metadata.clone());
        metadata.conflict("delete_schema");

        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &DropSchema::new(SchemaName::new(None, "s1".to_string()), false, false)
                )
                .await,
            Err(CataError::SchemaChanged { schema, .. }) if schema == "s1"
        ));
    }

    #[tokio::test]
    async fn show_lists_sorted_schemas_with_optional_like() {
        let metadata = MemoryMetadata::new();
        seed_database(&metadata, "db1").await;
        for name in ["zeta", "alpha", "alpine"] {
            seed_schema(&metadata, "db1", name).await;
        }
        let executor = SchemaExecutor::new(Arc::new(metadata));

        assert_eq!(
            executor
                .show("db1", &ShowSchemas::new(None))
                .await
                .unwrap(),
            vec!["alpha".to_string(), "alpine".to_string(), "zeta".to_string()]
        );
        assert_eq!(
            executor
                .show("db1", &ShowSchemas::new(Some("alp%".to_string())))
                .await
                .unwrap(),
            vec!["alpha".to_string(), "alpine".to_string()]
        );
        assert!(matches!(
            executor
                .show("missing", &ShowSchemas::new(None))
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
    }
}

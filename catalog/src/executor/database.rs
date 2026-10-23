use super::{matches_like, schema_is_empty};
use crate::handler::DatabaseHandles;
use crate::sql::{AlterDatabase, CreateDatabase, DropDatabase, ShowDatabases};
use crate::{CataError, Result};
use lyra_meta::metadata::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition,
};
use lyra_meta::proto::pb_catalog::{Database, Schema};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateDatabaseOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropDatabaseOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct DatabaseExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
    databases: Arc<DatabaseHandles>,
}

impl DatabaseExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>, databases: Arc<DatabaseHandles>) -> Self {
        Self {
            metadata,
            databases,
        }
    }

    pub async fn create(&self, statement: &CreateDatabase) -> Result<CreateDatabaseOutcome> {
        if self
            .metadata
            .get_database(statement.name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateDatabaseOutcome::AlreadyExists)
            } else {
                Err(CataError::DatabaseAlreadyExists(
                    statement.name().to_string(),
                ))
            };
        }

        let database = Database {
            name: statement.name().to_string(),
        };
        let database_version = match self
            .metadata
            .put_database(database, MetadataPutCondition::NotExists)
            .await
        {
            Ok(version) => version,
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                return Ok(CreateDatabaseOutcome::AlreadyExists);
            }
            Err(MetadataError::Conflict(_)) => {
                return Err(CataError::DatabaseAlreadyExists(
                    statement.name().to_string(),
                ));
            }
            Err(error) => return Err(error.into()),
        };

        let schema = Schema {
            name: DEFAULT_SCHEMA_NAME.to_string(),
        };
        match self
            .metadata
            .put_schema(statement.name(), schema, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) | Err(MetadataError::Conflict(_)) => {}
            Err(error) => {
                let _ = self
                    .metadata
                    .delete_database(statement.name(), Some(database_version))
                    .await;
                return Err(error.into());
            }
        }
        Ok(CreateDatabaseOutcome::Created)
    }

    pub async fn alter(&self, current_database: &str, statement: &AlterDatabase) -> Result<()> {
        self.require_not_active0(current_database, statement.name())?;
        let Some(record) = self.metadata.get_database(statement.name()).await? else {
            return Err(CataError::DatabaseNotFound(statement.name().to_string()));
        };
        if self
            .metadata
            .get_database(statement.new_name())
            .await?
            .is_some()
        {
            return Err(CataError::DatabaseAlreadyExists(
                statement.new_name().to_string(),
            ));
        }

        let schemas = self.metadata.list_schemas(statement.name()).await?;
        for schema in &schemas {
            if !schema_is_empty(&self.metadata, statement.name(), &schema.value().name).await? {
                return Err(CataError::DatabaseNotEmpty(statement.name().to_string()));
            }
        }

        let mut new_database = record.value().clone();
        new_database.name = statement.new_name().to_string();
        let new_database_version = self
            .metadata
            .put_database(new_database, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseAlreadyExists(statement.new_name().to_string())
                }
                error => error.into(),
            })?;

        let mut new_schemas = Vec::new();
        for schema in &schemas {
            match self
                .metadata
                .put_schema(
                    statement.new_name(),
                    schema.value().clone(),
                    MetadataPutCondition::NotExists,
                )
                .await
            {
                Ok(version) => new_schemas.push((schema.value().name.clone(), version)),
                Err(error) => {
                    for (name, version) in new_schemas {
                        let _ = self
                            .metadata
                            .delete_schema(statement.new_name(), &name, Some(version))
                            .await;
                    }
                    let _ = self
                        .metadata
                        .delete_database(statement.new_name(), Some(new_database_version))
                        .await;
                    return Err(error.into());
                }
            }
        }

        for schema in schemas {
            self.metadata
                .delete_schema(
                    statement.name(),
                    &schema.value().name,
                    Some(schema.version()),
                )
                .await
                .map_err(|error| match error {
                    MetadataError::Conflict(_) => {
                        CataError::DatabaseChanged(statement.name().to_string())
                    }
                    error => error.into(),
                })?;
        }
        self.metadata
            .delete_database(statement.name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseChanged(statement.name().to_string())
                }
                error => error.into(),
            })?;
        self.databases.remove(statement.name());
        Ok(())
    }

    pub async fn drop(
        &self,
        current_database: &str,
        statement: &DropDatabase,
    ) -> Result<DropDatabaseOutcome> {
        self.require_not_active0(current_database, statement.name())?;
        let Some(record) = self.metadata.get_database(statement.name()).await? else {
            return if statement.if_exists() {
                Ok(DropDatabaseOutcome::NotFound)
            } else {
                Err(CataError::DatabaseNotFound(statement.name().to_string()))
            };
        };
        if !self
            .metadata
            .list_schemas(statement.name())
            .await?
            .is_empty()
        {
            return Err(CataError::DatabaseNotEmpty(statement.name().to_string()));
        }

        self.metadata
            .delete_database(statement.name(), Some(record.version()))
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::DatabaseChanged(statement.name().to_string())
                }
                error => error.into(),
            })?;
        self.databases.remove(statement.name());
        Ok(DropDatabaseOutcome::Dropped)
    }

    pub async fn show(&self, statement: &ShowDatabases) -> Result<Vec<String>> {
        let mut databases = self
            .metadata
            .list_databases()
            .await?
            .into_iter()
            .filter(|database| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&database.value().name, pattern))
            })
            .map(|database| database.value().name.clone())
            .collect::<Vec<_>>();
        databases.sort();
        Ok(databases)
    }

    fn require_not_active0(&self, current_database: &str, database: &str) -> Result<()> {
        if database == current_database || database == DEFAULT_DATABASE_NAME {
            return Err(CataError::DatabaseInUse(database.to_string()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::DatabaseHandles;
    use crate::test_support::FaultMetadata;
    use lyra_meta::metadata::{MemoryMetadata, DEFAULT_SCHEMA_NAME};
    use lyra_meta::proto::pb_catalog::Secret;

    fn executor0(metadata: Arc<dyn Metadata>) -> DatabaseExecutor {
        let databases = Arc::new(DatabaseHandles::new(Arc::clone(&metadata)).unwrap());
        DatabaseExecutor::new(metadata, databases)
    }

    async fn seed_database(metadata: &Arc<MemoryMetadata>, name: &str) {
        metadata
            .put_database(
                Database { name: name.into() },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn create_adds_the_default_schema() {
        let metadata = Arc::new(MemoryMetadata::new());
        let executor = executor0(metadata.clone());

        let outcome = executor
            .create(&CreateDatabase::new("analytics".to_string(), false))
            .await
            .unwrap();
        assert_eq!(outcome, CreateDatabaseOutcome::Created);
        assert!(
            metadata
                .get_database("analytics")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            metadata
                .get_schema("analytics", DEFAULT_SCHEMA_NAME)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn create_handles_existing_databases() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        let executor = executor0(metadata);

        assert_eq!(
            executor
                .create(&CreateDatabase::new("analytics".to_string(), true))
                .await
                .unwrap(),
            CreateDatabaseOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(&CreateDatabase::new("analytics".to_string(), false))
                .await,
            Err(CataError::DatabaseAlreadyExists(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn create_handles_put_database_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        metadata.conflict("put_database");
        let executor = executor0(metadata.clone());

        assert_eq!(
            executor
                .create(&CreateDatabase::new("analytics".to_string(), true))
                .await
                .unwrap(),
            CreateDatabaseOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(&CreateDatabase::new("analytics".to_string(), false))
                .await,
            Err(CataError::DatabaseAlreadyExists(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn create_rolls_back_when_schema_provisioning_fails() {
        let metadata = Arc::new(FaultMetadata::new());
        metadata.fail("put_schema");
        let executor = executor0(metadata.clone());

        assert!(matches!(
            executor
                .create(&CreateDatabase::new("analytics".to_string(), false))
                .await,
            Err(CataError::Metadata(_))
        ));
        metadata.clear("put_schema");
        assert!(
            metadata
                .get_database("analytics")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn alter_renames_a_database_and_its_schemas() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        metadata
            .put_schema(
                "analytics",
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata.clone());

        executor
            .alter(
                "other",
                &AlterDatabase::new("analytics".to_string(), "reporting".to_string()),
            )
            .await
            .unwrap();

        assert!(
            metadata
                .get_database("analytics")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            metadata
                .get_schema("reporting", DEFAULT_SCHEMA_NAME)
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn alter_rejects_active_and_missing_databases() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        let executor = executor0(metadata);

        assert!(matches!(
            executor
                .alter(
                    "analytics",
                    &AlterDatabase::new("analytics".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseInUse(name)) if name == "analytics"
        ));
        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new(DEFAULT_DATABASE_NAME.to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseInUse(name)) if name == DEFAULT_DATABASE_NAME
        ));
        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new("missing".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn alter_rejects_existing_targets_and_nonempty_databases() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        seed_database(&metadata, "reporting").await;
        metadata
            .put_schema(
                "analytics",
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata.clone());

        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new("analytics".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseAlreadyExists(name)) if name == "reporting"
        ));

        metadata
            .put_secret(
                "analytics",
                DEFAULT_SCHEMA_NAME,
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
                    "other",
                    &AlterDatabase::new("analytics".to_string(), "warehouse".to_string())
                )
                .await,
            Err(CataError::DatabaseNotEmpty(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn alter_reports_target_conflicts_and_rolls_back() {
        let metadata = Arc::new(FaultMetadata::new());
        metadata
            .put_database(
                Database {
                    name: "analytics".to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_schema(
                "analytics",
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata.clone());

        metadata.conflict("put_database");
        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new("analytics".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseAlreadyExists(name)) if name == "reporting"
        ));

        metadata.clear("put_database");
        metadata.fail("put_schema");
        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new("analytics".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::Metadata(_))
        ));
        metadata.clear("put_schema");
        assert!(
            metadata
                .get_database("reporting")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn alter_reports_source_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        metadata
            .put_database(
                Database {
                    name: "analytics".to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_schema(
                "analytics",
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata.clone());

        metadata.conflict("delete_schema");
        assert!(matches!(
            executor
                .alter(
                    "other",
                    &AlterDatabase::new("analytics".to_string(), "reporting".to_string())
                )
                .await,
            Err(CataError::DatabaseChanged(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn drop_handles_success_and_missing_databases() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        let executor = executor0(metadata.clone());

        assert_eq!(
            executor
                .drop(
                    "other",
                    &DropDatabase::new("analytics".to_string(), false)
                )
                .await
                .unwrap(),
            DropDatabaseOutcome::Dropped
        );
        assert_eq!(
            executor
                .drop(
                    "other",
                    &DropDatabase::new("missing".to_string(), true)
                )
                .await
                .unwrap(),
            DropDatabaseOutcome::NotFound
        );
        assert!(matches!(
            executor
                .drop(
                    "other",
                    &DropDatabase::new("missing".to_string(), false)
                )
                .await,
            Err(CataError::DatabaseNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn drop_rejects_active_and_nonempty_databases() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_database(&metadata, "analytics").await;
        metadata
            .put_schema(
                "analytics",
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata);

        assert!(matches!(
            executor
                .drop(
                    "analytics",
                    &DropDatabase::new("analytics".to_string(), false)
                )
                .await,
            Err(CataError::DatabaseInUse(name)) if name == "analytics"
        ));
        assert!(matches!(
            executor
                .drop(
                    "other",
                    &DropDatabase::new("analytics".to_string(), false)
                )
                .await,
            Err(CataError::DatabaseNotEmpty(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn drop_reports_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        metadata
            .put_database(
                Database {
                    name: "analytics".to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = executor0(metadata.clone());
        metadata.conflict("delete_database");

        assert!(matches!(
            executor
                .drop(
                    "other",
                    &DropDatabase::new("analytics".to_string(), false)
                )
                .await,
            Err(CataError::DatabaseChanged(name)) if name == "analytics"
        ));
    }

    #[tokio::test]
    async fn show_lists_sorted_databases_with_optional_like() {
        let metadata = Arc::new(MemoryMetadata::new());
        for name in ["zeta", "alpha", "alpine"] {
            seed_database(&metadata, name).await;
        }
        let executor = executor0(metadata);

        assert_eq!(
            executor
                .show(&ShowDatabases::new(None))
                .await
                .unwrap(),
            vec!["alpha".to_string(), "alpine".to_string(), "zeta".to_string()]
        );
        assert_eq!(
            executor
                .show(&ShowDatabases::new(Some("alp%".to_string())))
                .await
                .unwrap(),
            vec!["alpha".to_string(), "alpine".to_string()]
        );
    }
}

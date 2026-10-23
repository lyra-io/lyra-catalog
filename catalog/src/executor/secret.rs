use super::matches_like;
use crate::sql::{AlterSecret, CreateSecret, DropSecret, SecretName, ShowSecrets};
use crate::{CataError, Result};
use lyra_meta::metadata::{
    DEFAULT_SCHEMA_NAME, Metadata, MetadataError, MetadataPutCondition, MetadataRecord,
};
use lyra_meta::proto::pb_catalog::Secret;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateSecretOutcome {
    Created,
    AlreadyExists,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropSecretOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct SecretExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl SecretExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(
        &self,
        database: &str,
        schemas: &[String],
        statement: &CreateSecret,
    ) -> Result<CreateSecretOutcome> {
        let schema = Self::target_schema0(schemas, statement.secret());
        self.require_schema0(database, schema).await?;

        if self
            .metadata
            .get_secret(database, schema, statement.secret().name())
            .await?
            .is_some()
        {
            return if statement.if_not_exists() {
                Ok(CreateSecretOutcome::AlreadyExists)
            } else {
                Err(CataError::SecretAlreadyExists(
                    statement.secret().name().to_string(),
                ))
            };
        }

        let secret = Secret {
            name: statement.secret().name().to_string(),
            value: statement.value().to_vec().into(),
        };
        match self
            .metadata
            .put_secret(database, schema, secret, MetadataPutCondition::NotExists)
            .await
        {
            Ok(_) => Ok(CreateSecretOutcome::Created),
            Err(MetadataError::Conflict(_)) if statement.if_not_exists() => {
                Ok(CreateSecretOutcome::AlreadyExists)
            }
            Err(MetadataError::Conflict(_)) => Err(CataError::SecretAlreadyExists(
                statement.secret().name().to_string(),
            )),
            Err(error) => Err(error.into()),
        }
    }

    pub async fn alter(
        &self,
        database: &str,
        schemas: &[String],
        statement: &AlterSecret,
    ) -> Result<()> {
        let Some((schema, record)) = self.resolve0(database, schemas, statement.secret()).await?
        else {
            return Err(CataError::SecretNotFound(
                statement.secret().name().to_string(),
            ));
        };
        let mut secret = record.value().clone();
        secret.value = statement.value().to_vec().into();

        self.metadata
            .put_secret(
                database,
                &schema,
                secret,
                MetadataPutCondition::Version(record.version()),
            )
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::SecretChanged(statement.secret().name().to_string())
                }
                error => error.into(),
            })?;
        Ok(())
    }

    pub async fn drop(
        &self,
        database: &str,
        schemas: &[String],
        statement: &DropSecret,
    ) -> Result<DropSecretOutcome> {
        let Some((schema, record)) = self.resolve0(database, schemas, statement.secret()).await?
        else {
            return if statement.if_exists() {
                Ok(DropSecretOutcome::NotFound)
            } else {
                Err(CataError::SecretNotFound(
                    statement.secret().name().to_string(),
                ))
            };
        };

        let qualified_name = format!("{schema}.{}", statement.secret().name());
        if let Some(connection) = self
            .metadata
            .list_connections(database, &schema)
            .await?
            .into_iter()
            .find(|connection| {
                connection.value().secret_refs.iter().any(|secret| {
                    secret.data == statement.secret().name() || secret.data == qualified_name
                })
            })
        {
            return Err(CataError::SecretInUse {
                secret: qualified_name,
                connection: connection.value().name.clone(),
            });
        }

        self.metadata
            .delete_secret(
                database,
                &schema,
                statement.secret().name(),
                Some(record.version()),
            )
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::SecretChanged(statement.secret().name().to_string())
                }
                error => error.into(),
            })?;
        Ok(DropSecretOutcome::Dropped)
    }

    pub async fn show(
        &self,
        database: &str,
        schemas: &[String],
        statement: &ShowSecrets,
    ) -> Result<Vec<String>> {
        let schemas = if let Some(schema) = statement.schema() {
            self.require_schema0(database, schema).await?;
            vec![schema.to_string()]
        } else {
            schemas.to_vec()
        };

        let mut secrets = Vec::new();
        for schema in schemas {
            if self.metadata.get_schema(database, &schema).await?.is_none() {
                continue;
            }
            secrets.extend(
                self.metadata
                    .list_secrets(database, &schema)
                    .await?
                    .into_iter()
                    .filter(|secret| {
                        statement
                            .like()
                            .is_none_or(|pattern| matches_like(&secret.value().name, pattern))
                    })
                    .map(|secret| format!("{schema}.{}", secret.value().name)),
            );
        }
        secrets.sort();
        Ok(secrets)
    }

    async fn resolve0(
        &self,
        database: &str,
        schemas: &[String],
        secret: &SecretName,
    ) -> Result<Option<(String, MetadataRecord<Secret>)>> {
        if let Some(schema) = secret.schema() {
            self.require_schema0(database, schema).await?;
            return Ok(self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
                .map(|record| (schema.to_string(), record)));
        }

        for schema in schemas {
            if let Some(record) = self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
            {
                return Ok(Some((schema.clone(), record)));
            }
        }
        Ok(None)
    }

    async fn require_schema0(&self, database: &str, schema: &str) -> Result<()> {
        if self.metadata.get_schema(database, schema).await?.is_none() {
            return Err(CataError::SchemaNotFound {
                database: database.to_string(),
                schema: schema.to_string(),
            });
        }
        Ok(())
    }

    fn target_schema0<'a>(schemas: &'a [String], secret: &'a SecretName) -> &'a str {
        secret
            .schema()
            .or_else(|| schemas.first().map(String::as_str))
            .unwrap_or(DEFAULT_SCHEMA_NAME)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FaultMetadata;
    use lyra_meta::metadata::MemoryMetadata;
    use lyra_meta::proto::pb_catalog::{Connection, Database, Schema, SecretRef};

    async fn seed_schema(metadata: &dyn Metadata, database: &str, schema: &str) {
        if metadata.get_database(database).await.unwrap().is_none() {
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

    fn secret0(name: &str, value: &[u8]) -> Secret {
        Secret {
            name: name.to_string(),
            value: value.to_vec().into(),
        }
    }

    #[tokio::test]
    async fn create_targets_the_first_or_explicit_schema() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        seed_schema(metadata.as_ref(), "db1", "other").await;
        let executor = SecretExecutor::new(metadata.clone());

        assert_eq!(
            executor
                .create(
                    "db1",
                    &["public".to_string(), "other".to_string()],
                    &CreateSecret::new(
                        SecretName::new(None, "s1".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await
                .unwrap(),
            CreateSecretOutcome::Created
        );
        assert_eq!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(
                        SecretName::new(Some("other".to_string()), "s2".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await
                .unwrap(),
            CreateSecretOutcome::Created
        );
        assert!(
            metadata
                .get_secret("db1", "public", "s1")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            metadata
                .get_secret("db1", "other", "s2")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn create_rejects_missing_schemas_and_duplicates() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata);

        assert!(matches!(
            executor
                .create(
                    "db1",
                    &["missing".to_string()],
                    &CreateSecret::new(
                        SecretName::new(None, "s".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await,
            Err(CataError::SchemaNotFound { schema, .. }) if schema == "missing"
        ));
        assert_eq!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(SecretName::new(None, "s1".to_string()), b"v".to_vec(), true)
                )
                .await
                .unwrap(),
            CreateSecretOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(
                        SecretName::new(None, "s1".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await,
            Err(CataError::SecretAlreadyExists(name)) if name == "s1"
        ));
    }

    #[tokio::test]
    async fn create_handles_put_conflicts_and_failures() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        let executor = SecretExecutor::new(metadata.clone());

        metadata.conflict("put_secret");
        assert_eq!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(SecretName::new(None, "s1".to_string()), b"v".to_vec(), true)
                )
                .await
                .unwrap(),
            CreateSecretOutcome::AlreadyExists
        );
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(
                        SecretName::new(None, "s1".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await,
            Err(CataError::SecretAlreadyExists(name)) if name == "s1"
        ));

        metadata.clear("put_secret");
        metadata.fail("put_secret");
        assert!(matches!(
            executor
                .create(
                    "db1",
                    &["public".to_string()],
                    &CreateSecret::new(
                        SecretName::new(None, "s1".to_string()),
                        b"v".to_vec(),
                        false
                    )
                )
                .await,
            Err(CataError::Metadata(_))
        ));
    }

    #[tokio::test]
    async fn alter_updates_existing_secrets() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"old"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata.clone());

        executor
            .alter(
                "db1",
                &["public".to_string()],
                &AlterSecret::new(SecretName::new(None, "s1".to_string()), b"new".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            metadata
                .get_secret("db1", "public", "s1")
                .await
                .unwrap()
                .unwrap()
                .value()
                .value
                .as_ref(),
            &b"new"[..]
        );
    }

    #[tokio::test]
    async fn alter_rejects_missing_secrets_and_reports_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        let executor = SecretExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &["public".to_string()],
                    &AlterSecret::new(
                        SecretName::new(None, "missing".to_string()),
                        b"v".to_vec()
                    )
                )
                .await,
            Err(CataError::SecretNotFound(name)) if name == "missing"
        ));

        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"old"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata.conflict("put_secret");
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &["public".to_string()],
                    &AlterSecret::new(SecretName::new(None, "s1".to_string()), b"new".to_vec())
                )
                .await,
            Err(CataError::SecretChanged(name)) if name == "s1"
        ));

        metadata.clear("put_secret");
        metadata.fail("put_secret");
        assert!(matches!(
            executor
                .alter(
                    "db1",
                    &["public".to_string()],
                    &AlterSecret::new(SecretName::new(None, "s1".to_string()), b"new".to_vec())
                )
                .await,
            Err(CataError::Metadata(_))
        ));
    }

    #[tokio::test]
    async fn drop_handles_success_and_missing_secrets() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata);

        assert_eq!(
            executor
                .drop(
                    "db1",
                    &["public".to_string()],
                    &DropSecret::new(SecretName::new(None, "s1".to_string()), false)
                )
                .await
                .unwrap(),
            DropSecretOutcome::Dropped
        );
        assert_eq!(
            executor
                .drop(
                    "db1",
                    &["public".to_string()],
                    &DropSecret::new(SecretName::new(None, "missing".to_string()), true)
                )
                .await
                .unwrap(),
            DropSecretOutcome::NotFound
        );
        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &["public".to_string()],
                    &DropSecret::new(SecretName::new(None, "missing".to_string()), false)
                )
                .await,
            Err(CataError::SecretNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn drop_rejects_in_use_secrets() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_connection(
                "db1",
                "public",
                Connection {
                    name: "conn".to_string(),
                    options: Default::default(),
                    secret_refs: vec![SecretRef {
                        name: "s1".to_string(),
                        data: "s1".to_string(),
                    }],
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &["public".to_string()],
                    &DropSecret::new(SecretName::new(None, "s1".to_string()), false)
                )
                .await,
            Err(CataError::SecretInUse { secret, connection })
                if secret == "public.s1" && connection == "conn"
        ));
    }

    #[tokio::test]
    async fn drop_reports_conflicts() {
        let metadata = Arc::new(FaultMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("s1", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata.clone());
        metadata.conflict("delete_secret");
        assert!(matches!(
            executor
                .drop(
                    "db1",
                    &["public".to_string()],
                    &DropSecret::new(SecretName::new(None, "s1".to_string()), false)
                )
                .await,
            Err(CataError::SecretChanged(name)) if name == "s1"
        ));
    }

    #[tokio::test]
    async fn show_lists_qualified_names_with_optional_like() {
        let metadata = Arc::new(MemoryMetadata::new());
        seed_schema(metadata.as_ref(), "db1", "public").await;
        seed_schema(metadata.as_ref(), "db1", "other").await;
        metadata
            .put_secret(
                "db1",
                "public",
                secret0("zeta", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        metadata
            .put_secret(
                "db1",
                "other",
                secret0("alpha", b"v"),
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = SecretExecutor::new(metadata);

        assert_eq!(
            executor
                .show(
                    "db1",
                    &["public".to_string(), "other".to_string()],
                    &ShowSecrets::new(None, None)
                )
                .await
                .unwrap(),
            vec!["other.alpha".to_string(), "public.zeta".to_string()]
        );
        assert_eq!(
            executor
                .show(
                    "db1",
                    &["public".to_string()],
                    &ShowSecrets::new(Some("public".to_string()), Some("ze%".to_string()))
                )
                .await
                .unwrap(),
            vec!["public.zeta".to_string()]
        );
        assert!(matches!(
            executor
                .show(
                    "db1",
                    &["public".to_string()],
                    &ShowSecrets::new(Some("missing".to_string()), None)
                )
                .await,
            Err(CataError::SchemaNotFound { schema, .. }) if schema == "missing"
        ));
    }
}

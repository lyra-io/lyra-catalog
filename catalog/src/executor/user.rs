use super::matches_like;
use crate::sql::{AlterUser, AlterUserAction, CreateUser, DropUser, SecretName, ShowUsers, Value};
use crate::{CataError, Result};
use lyra_meta::metadata::{Metadata, MetadataError, MetadataPutCondition};
use lyra_meta::proto::pb_catalog::{User, Value as MetadataValue, value};
use lyra_meta::utils::scram::make_scram;
use std::str;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateUserOutcome {
    Created,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropUserOutcome {
    Dropped,
    NotFound,
}

pub(crate) struct UserExecutor {
    // Immutable state
    metadata: Arc<dyn Metadata>,
}

impl UserExecutor {
    pub(crate) fn new(metadata: Arc<dyn Metadata>) -> Self {
        Self { metadata }
    }

    pub async fn create(
        &self,
        database: &str,
        schemas: &[String],
        statement: &CreateUser,
    ) -> Result<CreateUserOutcome> {
        if self.metadata.get_user(statement.name()).await?.is_some() {
            return Err(CataError::UserAlreadyExists(statement.name().to_string()));
        }

        let user = User {
            name: statement.name().to_string(),
            password: self
                .password0(database, schemas, statement.password())
                .await?,
        };
        self.metadata
            .put_user(user, MetadataPutCondition::NotExists)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => {
                    CataError::UserAlreadyExists(statement.name().to_string())
                }
                error => error.into(),
            })?;
        Ok(CreateUserOutcome::Created)
    }

    pub async fn alter(
        &self,
        database: &str,
        schemas: &[String],
        current_user: Option<&str>,
        statement: &AlterUser,
    ) -> Result<()> {
        let Some(record) = self.metadata.get_user(statement.name()).await? else {
            return Err(CataError::UserNotFound(statement.name().to_string()));
        };

        match statement.action() {
            AlterUserAction::Rename(new_name) => {
                if current_user == Some(statement.name()) {
                    return Err(CataError::UserInUse(statement.name().to_string()));
                }
                if self.metadata.get_user(new_name).await?.is_some() {
                    return Err(CataError::UserAlreadyExists(new_name.clone()));
                }
                let mut user = record.value().clone();
                user.name = new_name.clone();
                self.metadata
                    .rename_user(statement.name(), user, record.version())
                    .await
                    .map_err(|error| match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserChanged(statement.name().to_string())
                        }
                        error => error.into(),
                    })?;
            }
            AlterUserAction::Password(password) => {
                let mut user = record.value().clone();
                user.password = self.password0(database, schemas, password).await?;
                self.metadata
                    .put_user(user, MetadataPutCondition::Version(record.version()))
                    .await
                    .map_err(|error| match error {
                        MetadataError::Conflict(_) => {
                            CataError::UserChanged(statement.name().to_string())
                        }
                        error => error.into(),
                    })?;
            }
        }
        Ok(())
    }

    pub async fn drop(
        &self,
        current_user: Option<&str>,
        statement: &DropUser,
    ) -> Result<DropUserOutcome> {
        let mut records = Vec::new();
        for name in statement.names() {
            if current_user == Some(name.as_str()) {
                return Err(CataError::UserInUse(name.clone()));
            }
            if records.iter().any(|(existing, _)| existing == name) {
                continue;
            }
            match self.metadata.get_user(name).await? {
                Some(record) => records.push((name.clone(), record.version())),
                None if statement.if_exists() => {}
                None => return Err(CataError::UserNotFound(name.clone())),
            }
        }

        if records.is_empty() {
            return Ok(DropUserOutcome::NotFound);
        }
        let deletes = records
            .iter()
            .map(|(name, version)| (name.clone(), *version))
            .collect::<Vec<_>>();
        self.metadata
            .delete_users(&deletes)
            .await
            .map_err(|error| match error {
                MetadataError::Conflict(_) => CataError::UserChanged(deletes[0].0.clone()),
                error => error.into(),
            })?;
        Ok(DropUserOutcome::Dropped)
    }

    pub async fn show(&self, statement: &ShowUsers) -> Result<Vec<String>> {
        let mut users = self
            .metadata
            .list_users()
            .await?
            .into_iter()
            .filter(|user| {
                statement
                    .like()
                    .is_none_or(|pattern| matches_like(&user.value().name, pattern))
            })
            .map(|user| user.value().name.clone())
            .collect::<Vec<_>>();
        users.sort();
        Ok(users)
    }

    async fn password0(
        &self,
        database: &str,
        schemas: &[String],
        password: &Value,
    ) -> Result<Option<MetadataValue>> {
        let value = match password {
            Value::Null => return Ok(None),
            Value::Literal(password) => Value::Scram(make_scram(password)),
            Value::Secret(secret) => {
                let password = self.secret0(database, schemas, secret).await?;
                let password = str::from_utf8(&password)
                    .map_err(|_| CataError::InvalidPasswordSecret(secret.name().to_string()))?;
                Value::Scram(make_scram(password))
            }
            Value::Scram(scram) => Value::Scram(scram.clone()),
        };
        let Value::Scram(scram) = value else {
            unreachable!("password values are normalized to SCRAM before metadata storage")
        };
        Ok(Some(MetadataValue {
            kind: Some(value::Kind::Scram(scram)),
        }))
    }

    async fn secret0(
        &self,
        database: &str,
        schemas: &[String],
        secret: &SecretName,
    ) -> Result<Vec<u8>> {
        if let Some(schema) = secret.schema() {
            if self.metadata.get_schema(database, schema).await?.is_none() {
                return Err(CataError::SchemaNotFound {
                    database: database.to_string(),
                    schema: schema.to_string(),
                });
            }
            return self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
                .map(|record| record.value().value.to_vec())
                .ok_or_else(|| CataError::SecretNotFound(secret.name().to_string()));
        }

        for schema in schemas {
            if let Some(record) = self
                .metadata
                .get_secret(database, schema, secret.name())
                .await?
            {
                return Ok(record.value().value.to_vec());
            }
        }
        Err(CataError::SecretNotFound(secret.name().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::FaultMetadata;
    use lyra_meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, MemoryMetadata};
    use lyra_meta::proto::pb_catalog::{Schema, Secret};
    use lyra_meta::utils::scram::{as_scram, make_scram, verify_scram};

    async fn metadata0() -> Arc<MemoryMetadata> {
        let metadata = Arc::new(MemoryMetadata::new());
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
        metadata
    }

    #[tokio::test]
    async fn stores_literal_passwords_as_scram() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata.clone());
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("alice".to_string(), Value::Literal("password".to_string())),
            )
            .await
            .unwrap();

        let user = metadata.get_user("alice").await.unwrap().unwrap();
        let scram = user.value().password.as_ref().and_then(as_scram).unwrap();
        assert!(verify_scram("password", scram));
    }

    #[tokio::test]
    async fn resolves_secret_passwords_before_storage() {
        let metadata = metadata0().await;
        metadata
            .put_secret(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                Secret {
                    name: "login_password".to_string(),
                    value: b"password".to_vec().into(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(metadata.clone());
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new(
                    "alice".to_string(),
                    Value::Secret(SecretName::new(
                        Some(DEFAULT_SCHEMA_NAME.to_string()),
                        "login_password".to_string(),
                    )),
                ),
            )
            .await
            .unwrap();

        let user = metadata.get_user("alice").await.unwrap().unwrap();
        let scram = user.value().password.as_ref().and_then(as_scram).unwrap();
        assert!(verify_scram("password", scram));
    }

    #[tokio::test]
    async fn rejects_missing_password_secrets() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata);
        let result = executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new(
                    "alice".to_string(),
                    Value::Secret(SecretName::new(None, "missing".to_string())),
                ),
            )
            .await;

        assert!(matches!(
            result,
            Err(CataError::SecretNotFound(name)) if name == "missing"
        ));
    }

    #[tokio::test]
    async fn rejects_non_utf8_password_secrets() {
        let metadata = metadata0().await;
        metadata
            .put_secret(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                Secret {
                    name: "login_password".to_string(),
                    value: vec![0xff].into(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(metadata);
        let result = executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new(
                    "alice".to_string(),
                    Value::Secret(SecretName::new(None, "login_password".to_string())),
                ),
            )
            .await;

        assert!(matches!(
            result,
            Err(CataError::InvalidPasswordSecret(name)) if name == "login_password"
        ));
    }

    #[tokio::test]
    async fn create_rejects_duplicate_users_and_conflicts() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata.clone());
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("alice".to_string(), Value::Literal("password".to_string())),
            )
            .await
            .unwrap();

        assert!(matches!(
            executor
                .create(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    &CreateUser::new("alice".to_string(), Value::Literal("password".to_string()))
                )
                .await,
            Err(CataError::UserAlreadyExists(name)) if name == "alice"
        ));

        let fault = Arc::new(FaultMetadata::new());
        fault
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(fault.clone());
        fault.conflict("put_user");
        assert!(matches!(
            executor
                .create(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    &CreateUser::new("alice".to_string(), Value::Null)
                )
                .await,
            Err(CataError::UserAlreadyExists(name)) if name == "alice"
        ));
    }

    #[tokio::test]
    async fn alter_renames_users() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata.clone());
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("alice".to_string(), Value::Literal("password".to_string())),
            )
            .await
            .unwrap();

        assert!(matches!(
            executor
                .alter(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    None,
                    &AlterUser::new("missing".to_string(), AlterUserAction::Rename("bob".to_string()))
                )
                .await,
            Err(CataError::UserNotFound(name)) if name == "missing"
        ));
        assert!(matches!(
            executor
                .alter(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    Some("alice"),
                    &AlterUser::new("alice".to_string(), AlterUserAction::Rename("bob".to_string()))
                )
                .await,
            Err(CataError::UserInUse(name)) if name == "alice"
        ));

        executor
            .alter(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                None,
                &AlterUser::new("alice".to_string(), AlterUserAction::Rename("bob".to_string())),
            )
            .await
            .unwrap();
        assert!(metadata.get_user("alice").await.unwrap().is_none());
        assert!(metadata.get_user("bob").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn alter_rename_rejects_existing_targets_and_conflicts() {
        let metadata = metadata0().await;
        for name in ["alice", "bob"] {
            metadata
                .put_user(
                    lyra_meta::proto::pb_catalog::User {
                        name: name.to_string(),
                        password: None,
                    },
                    MetadataPutCondition::NotExists,
                )
                .await
                .unwrap();
        }
        let executor = UserExecutor::new(metadata.clone());
        assert!(matches!(
            executor
                .alter(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    None,
                    &AlterUser::new("alice".to_string(), AlterUserAction::Rename("bob".to_string()))
                )
                .await,
            Err(CataError::UserAlreadyExists(name)) if name == "bob"
        ));

        let fault = Arc::new(FaultMetadata::new());
        fault
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        fault
            .put_user(
                lyra_meta::proto::pb_catalog::User {
                    name: "alice".to_string(),
                    password: None,
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(fault.clone());
        fault.conflict("rename_user");
        assert!(matches!(
            executor
                .alter(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    None,
                    &AlterUser::new("alice".to_string(), AlterUserAction::Rename("bob".to_string()))
                )
                .await,
            Err(CataError::UserChanged(name)) if name == "alice"
        ));
    }

    #[tokio::test]
    async fn alter_password_updates_and_reports_conflicts() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata.clone());
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("alice".to_string(), Value::Null),
            )
            .await
            .unwrap();
        executor
            .alter(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                None,
                &AlterUser::new(
                    "alice".to_string(),
                    AlterUserAction::Password(Value::Literal("new-password".to_string())),
                ),
            )
            .await
            .unwrap();
        let user = metadata.get_user("alice").await.unwrap().unwrap();
        let scram = user.value().password.as_ref().and_then(as_scram).unwrap();
        assert!(verify_scram("new-password", scram));

        let fault = Arc::new(FaultMetadata::new());
        fault
            .put_schema(
                DEFAULT_DATABASE_NAME,
                Schema {
                    name: DEFAULT_SCHEMA_NAME.to_string(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        fault
            .put_user(
                lyra_meta::proto::pb_catalog::User {
                    name: "alice".to_string(),
                    password: None,
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(fault.clone());
        fault.conflict("put_user");
        assert!(matches!(
            executor
                .alter(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    None,
                    &AlterUser::new(
                        "alice".to_string(),
                        AlterUserAction::Password(Value::Literal("new-password".to_string()))
                    )
                )
                .await,
            Err(CataError::UserChanged(name)) if name == "alice"
        ));
    }

    #[tokio::test]
    async fn drop_handles_current_users_duplicates_and_missing() {
        let metadata = metadata0().await;
        for name in ["alice", "bob"] {
            metadata
                .put_user(
                    lyra_meta::proto::pb_catalog::User {
                        name: name.to_string(),
                        password: None,
                    },
                    MetadataPutCondition::NotExists,
                )
                .await
                .unwrap();
        }
        let executor = UserExecutor::new(metadata);

        assert!(matches!(
            executor
                .drop(Some("alice"), &DropUser::new(vec!["alice".to_string()], false))
                .await,
            Err(CataError::UserInUse(name)) if name == "alice"
        ));
        assert!(matches!(
            executor
                .drop(
                    None,
                    &DropUser::new(vec!["missing".to_string(), "bob".to_string()], false)
                )
                .await,
            Err(CataError::UserNotFound(name)) if name == "missing"
        ));
        assert_eq!(
            executor
                .drop(
                    None,
                    &DropUser::new(vec!["missing".to_string()], true)
                )
                .await
                .unwrap(),
            DropUserOutcome::NotFound
        );
        assert_eq!(
            executor
                .drop(
                    None,
                    &DropUser::new(
                        vec!["alice".to_string(), "alice".to_string(), "bob".to_string()],
                        false
                    )
                )
                .await
                .unwrap(),
            DropUserOutcome::Dropped
        );
    }

    #[tokio::test]
    async fn drop_reports_conflicts() {
        let fault = Arc::new(FaultMetadata::new());
        fault
            .put_user(
                lyra_meta::proto::pb_catalog::User {
                    name: "alice".to_string(),
                    password: None,
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(fault.clone());
        fault.conflict("delete_users");
        assert!(matches!(
            executor
                .drop(None, &DropUser::new(vec!["alice".to_string()], false))
                .await,
            Err(CataError::UserChanged(name)) if name == "alice"
        ));
    }

    #[tokio::test]
    async fn show_lists_users_with_optional_like() {
        let metadata = metadata0().await;
        for name in ["zeta", "alpha", "alpine"] {
            metadata
                .put_user(
                    lyra_meta::proto::pb_catalog::User {
                        name: name.to_string(),
                        password: None,
                    },
                    MetadataPutCondition::NotExists,
                )
                .await
                .unwrap();
        }
        let executor = UserExecutor::new(metadata);
        assert_eq!(
            executor.show(&ShowUsers::new(None)).await.unwrap(),
            vec!["alpha".to_string(), "alpine".to_string(), "zeta".to_string()]
        );
        assert_eq!(
            executor
                .show(&ShowUsers::new(Some("alp%".to_string())))
                .await
                .unwrap(),
            vec!["alpha".to_string(), "alpine".to_string()]
        );
    }

    #[tokio::test]
    async fn stores_null_and_scram_passwords_unchanged() {
        let metadata = metadata0().await;
        let executor = UserExecutor::new(metadata.clone());

        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("null_user".to_string(), Value::Null),
            )
            .await
            .unwrap();
        assert!(
            metadata
                .get_user("null_user")
                .await
                .unwrap()
                .unwrap()
                .value()
                .password
                .is_none()
        );

        let scram = make_scram("password");
        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new("scram_user".to_string(), Value::Scram(scram.clone())),
            )
            .await
            .unwrap();
        let stored = metadata
            .get_user("scram_user")
            .await
            .unwrap()
            .unwrap()
            .value()
            .password
            .as_ref()
            .and_then(as_scram)
            .unwrap()
            .clone();
        assert_eq!(stored, scram);
    }

    #[tokio::test]
    async fn explicit_schema_secrets_are_resolved_or_rejected() {
        let metadata = metadata0().await;
        metadata
            .put_secret(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                Secret {
                    name: "login_password".to_string(),
                    value: b"password".to_vec().into(),
                },
                MetadataPutCondition::NotExists,
            )
            .await
            .unwrap();
        let executor = UserExecutor::new(metadata.clone());

        assert!(matches!(
            executor
                .create(
                    DEFAULT_DATABASE_NAME,
                    &[DEFAULT_SCHEMA_NAME.to_string()],
                    &CreateUser::new(
                        "alice".to_string(),
                        Value::Secret(SecretName::new(
                            Some("missing".to_string()),
                            "login_password".to_string()
                        ))
                    )
                )
                .await,
            Err(CataError::SchemaNotFound { schema, .. }) if schema == "missing"
        ));

        executor
            .create(
                DEFAULT_DATABASE_NAME,
                &[DEFAULT_SCHEMA_NAME.to_string()],
                &CreateUser::new(
                    "alice".to_string(),
                    Value::Secret(SecretName::new(
                        Some(DEFAULT_SCHEMA_NAME.to_string()),
                        "login_password".to_string(),
                    )),
                ),
            )
            .await
            .unwrap();
        assert!(metadata.get_user("alice").await.unwrap().is_some());
    }
}

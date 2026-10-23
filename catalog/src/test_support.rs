//! Test-only metadata backend that wraps the in-memory implementation and can
//! inject failures for individual operations, so error and rollback branches in
//! the executors can be exercised deterministically.

use async_trait::async_trait;
use lyra_meta::metadata::{
    MemoryMetadata, Metadata, MetadataError, MetadataPutCondition, MetadataRecord,
    MetadataVersion, Result,
};
use lyra_meta::proto::pb_catalog::{Connection, Database, Schema, Secret, User};
use std::collections::HashSet;
use std::sync::Mutex;

pub(crate) struct FaultMetadata {
    // Immutable state
    inner: MemoryMetadata,

    // Mutable state
    faults: Mutex<HashSet<&'static str>>,
    conflicts: Mutex<HashSet<&'static str>>,
}

impl FaultMetadata {
    pub(crate) fn new() -> Self {
        Self {
            inner: MemoryMetadata::new(),
            faults: Mutex::new(HashSet::new()),
            conflicts: Mutex::new(HashSet::new()),
        }
    }

    pub(crate) fn fail(&self, operation: &'static str) {
        self.faults.lock().unwrap().insert(operation);
    }

    pub(crate) fn conflict(&self, operation: &'static str) {
        self.conflicts.lock().unwrap().insert(operation);
    }

    pub(crate) fn clear(&self, operation: &'static str) {
        self.faults.lock().unwrap().remove(operation);
        self.conflicts.lock().unwrap().remove(operation);
    }

    fn check(&self, operation: &'static str) -> Result<()> {
        if self.conflicts.lock().unwrap().contains(operation) {
            return Err(MetadataError::Conflict("injected".to_string()));
        }
        if self.faults.lock().unwrap().contains(operation) {
            return Err(MetadataError::InvalidKey {
                key: "injected".to_string(),
                reason: operation,
            });
        }
        Ok(())
    }
}

#[async_trait]
impl Metadata for FaultMetadata {
    async fn get_user(&self, name: &str) -> Result<Option<MetadataRecord<User>>> {
        self.check("get_user")?;
        self.inner.get_user(name).await
    }

    async fn put_user(
        &self,
        user: User,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        self.check("put_user")?;
        self.inner.put_user(user, condition).await
    }

    async fn delete_user(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        self.check("delete_user")?;
        self.inner.delete_user(name, expected_version).await
    }

    async fn list_users(&self) -> Result<Vec<MetadataRecord<User>>> {
        self.check("list_users")?;
        self.inner.list_users().await
    }

    async fn rename_user(
        &self,
        name: &str,
        user: User,
        expected_version: MetadataVersion,
    ) -> Result<MetadataVersion> {
        self.check("rename_user")?;
        self.inner.rename_user(name, user, expected_version).await
    }

    async fn delete_users(&self, users: &[(String, MetadataVersion)]) -> Result<()> {
        self.check("delete_users")?;
        self.inner.delete_users(users).await
    }

    async fn get_database(&self, name: &str) -> Result<Option<MetadataRecord<Database>>> {
        self.check("get_database")?;
        self.inner.get_database(name).await
    }

    async fn put_database(
        &self,
        database: Database,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        self.check("put_database")?;
        self.inner.put_database(database, condition).await
    }

    async fn delete_database(
        &self,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        self.check("delete_database")?;
        self.inner.delete_database(name, expected_version).await
    }

    async fn list_databases(&self) -> Result<Vec<MetadataRecord<Database>>> {
        self.check("list_databases")?;
        self.inner.list_databases().await
    }

    async fn get_schema(
        &self,
        database: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Schema>>> {
        self.check("get_schema")?;
        self.inner.get_schema(database, name).await
    }

    async fn put_schema(
        &self,
        database: &str,
        schema: Schema,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        self.check("put_schema")?;
        self.inner.put_schema(database, schema, condition).await
    }

    async fn delete_schema(
        &self,
        database: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        self.check("delete_schema")?;
        self.inner.delete_schema(database, name, expected_version).await
    }

    async fn list_schemas(&self, database: &str) -> Result<Vec<MetadataRecord<Schema>>> {
        self.check("list_schemas")?;
        self.inner.list_schemas(database).await
    }

    async fn get_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Secret>>> {
        self.check("get_secret")?;
        self.inner.get_secret(database, schema, name).await
    }

    async fn put_secret(
        &self,
        database: &str,
        schema: &str,
        secret: Secret,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        self.check("put_secret")?;
        self.inner.put_secret(database, schema, secret, condition).await
    }

    async fn delete_secret(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        self.check("delete_secret")?;
        self.inner
            .delete_secret(database, schema, name, expected_version)
            .await
    }

    async fn list_secrets(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Secret>>> {
        self.check("list_secrets")?;
        self.inner.list_secrets(database, schema).await
    }

    async fn get_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<MetadataRecord<Connection>>> {
        self.check("get_connection")?;
        self.inner.get_connection(database, schema, name).await
    }

    async fn put_connection(
        &self,
        database: &str,
        schema: &str,
        connection: Connection,
        condition: MetadataPutCondition,
    ) -> Result<MetadataVersion> {
        self.check("put_connection")?;
        self.inner
            .put_connection(database, schema, connection, condition)
            .await
    }

    async fn delete_connection(
        &self,
        database: &str,
        schema: &str,
        name: &str,
        expected_version: Option<MetadataVersion>,
    ) -> Result<()> {
        self.check("delete_connection")?;
        self.inner
            .delete_connection(database, schema, name, expected_version)
            .await
    }

    async fn list_connections(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<MetadataRecord<Connection>>> {
        self.check("list_connections")?;
        self.inner.list_connections(database, schema).await
    }
}

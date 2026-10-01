use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{CatalogProvider, SchemaProvider, Session, TableProvider};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_pg_catalog::{pg_catalog::context::EmptyContextProvider, setup_pg_catalog};
use datafusion_postgres::DfSessionService;
use lyra_meta::metadata::Metadata;
use lyra_meta::proto::pb_meta::DatabaseState;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub(crate) struct SqlSession {
    // Immutable state
    service: DfSessionService,
    catalog: Arc<dyn CatalogProvider>,
}

impl SqlSession {
    pub(crate) fn service(&self) -> &DfSessionService {
        &self.service
    }
}

impl Drop for SqlSession {
    fn drop(&mut self) {
        // datafusion-pg-catalog 0.18 retains the catalog list in its schema:
        // list -> catalog -> pg_catalog -> list. Break that private session's
        // cycle when its last owner goes away (including failed setup).
        // This is an in-memory compatibility schema, never stored metadata.
        let _ = self.catalog.deregister_schema("pg_catalog", true);
    }
}

pub(crate) fn session(
    database: &str,
    metadata: Arc<dyn Metadata>,
) -> crate::Result<Arc<SqlSession>> {
    let context = Arc::new(SessionContext::new_with_config(
        SessionConfig::new()
            .with_default_catalog_and_schema(database, "public")
            .with_information_schema(true),
    ));
    let catalog = context
        .catalog(database)
        .ok_or_else(|| DataFusionError::Internal("session catalog absent".into()))?;
    let session = SqlSession {
        service: DfSessionService::new(Arc::clone(&context)),
        catalog: Arc::clone(&catalog),
    };
    setup_pg_catalog(&context, database, EmptyContextProvider).map_err(|e| *e)?;
    let base = catalog
        .schema("pg_catalog")
        .ok_or_else(|| DataFusionError::Internal("compatibility catalog absent".into()))?;
    let mut tables = HashMap::new();
    for (name, kind) in [
        ("pg_database", Kind::Databases),
        ("pg_user", Kind::Users),
        ("pg_roles", Kind::Roles),
    ] {
        tables.insert(
            name.into(),
            Arc::new(Inventory::new(Arc::clone(&metadata), kind)) as Arc<dyn TableProvider>,
        );
    }
    catalog.register_schema("pg_catalog", Arc::new(Overlay { base, tables }))?;
    Ok(Arc::new(session))
}

#[derive(Debug)]
struct Overlay {
    base: Arc<dyn SchemaProvider>,
    tables: HashMap<String, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for Overlay {
    fn table_names(&self) -> Vec<String> {
        let mut names = self.base.table_names();
        names.extend(self.tables.keys().cloned());
        names.sort();
        names.dedup();
        names
    }
    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name) || self.base.table_exist(name)
    }
    async fn table(&self, name: &str) -> DfResult<Option<Arc<dyn TableProvider>>> {
        if let Some(table) = self.tables.get(name) {
            Ok(Some(Arc::clone(table)))
        } else {
            self.base.table(name).await
        }
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Databases,
    Users,
    Roles,
}

struct Inventory {
    metadata: Arc<dyn Metadata>,
    kind: Kind,
    schema: SchemaRef,
}

impl fmt::Debug for Inventory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inventory").finish_non_exhaustive()
    }
}

impl Inventory {
    fn new(metadata: Arc<dyn Metadata>, kind: Kind) -> Self {
        use DataType::{Boolean, Int32, Int64, Utf8};
        let fields = match kind {
            Kind::Databases => vec![
                ("oid", Int64),
                ("datname", Utf8),
                ("datdba", Int64),
                ("encoding", Int32),
                ("datlocprovider", Utf8),
                ("datcollate", Utf8),
                ("datctype", Utf8),
                ("datistemplate", Boolean),
                ("datallowconn", Boolean),
                ("datconnlimit", Int32),
                ("datstate", Utf8),
            ],
            Kind::Users => vec![
                ("usesysid", Int64),
                ("usename", Utf8),
                ("usesuper", Boolean),
                ("usecreatedb", Boolean),
            ],
            Kind::Roles => vec![
                ("oid", Int64),
                ("rolname", Utf8),
                ("rolsuper", Boolean),
                ("rolcanlogin", Boolean),
            ],
        };
        let schema = Arc::new(Schema::new(
            fields
                .into_iter()
                .map(|(name, ty)| Field::new(name, ty, false))
                .collect::<Vec<_>>(),
        ));
        Self {
            metadata,
            kind,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for Inventory {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let arrays: Vec<ArrayRef> = match self.kind {
            Kind::Databases => {
                let records = self.metadata.list_databases().await.map_err(|_| {
                    DataFusionError::Execution("metadata inventory unavailable".into())
                })?;
                let n = records.len();
                vec![
                    Arc::new(Int64Array::from(
                        records
                            .iter()
                            .map(|r| i64::from(r.id()))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        records
                            .iter()
                            .map(|r| r.value().name.as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        records
                            .iter()
                            .map(|r| i64::from(r.value().owner_user_id))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(vec![6; n])),
                    Arc::new(StringArray::from(vec!["builtin"; n])),
                    Arc::new(StringArray::from(vec!["C"; n])),
                    Arc::new(StringArray::from(vec!["C"; n])),
                    Arc::new(BooleanArray::from(vec![false; n])),
                    Arc::new(BooleanArray::from(
                        records
                            .iter()
                            .map(|r| {
                                r.value().accepts_connections()
                                    && r.value().state == DatabaseState::Ready as i32
                            })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        records
                            .iter()
                            .map(|r| r.value().effective_connection_limit())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        records
                            .iter()
                            .map(|r| match r.value().state() {
                                DatabaseState::Ready => "READY",
                                DatabaseState::Dropping => "DROPPING",
                                DatabaseState::DropFailed => "DROP_FAILED",
                            })
                            .collect::<Vec<_>>(),
                    )),
                ]
            }
            Kind::Users | Kind::Roles => {
                let records = self.metadata.list_users().await.map_err(|_| {
                    DataFusionError::Execution("metadata inventory unavailable".into())
                })?;
                let n = records.len();
                vec![
                    Arc::new(Int64Array::from(
                        records
                            .iter()
                            .map(|r| i64::from(r.id()))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        records
                            .iter()
                            .map(|r| r.value().name.as_str())
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(BooleanArray::from(vec![false; n])),
                    Arc::new(BooleanArray::from(vec![true; n])),
                ]
            }
        };
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), arrays)?;
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lyra_meta::metadata::MemoryMetadata;

    #[test]
    fn session_drop_releases_compatibility_catalog_and_metadata() {
        let backend = Arc::new(MemoryMetadata::new());
        let weak = Arc::downgrade(&backend);
        let session = session("public", backend).unwrap();
        let catalog = Arc::downgrade(&session.catalog);
        let schema = Arc::downgrade(&session.catalog.schema("pg_catalog").unwrap());
        let last_owner = Arc::clone(&session);
        assert!(weak.upgrade().is_some());
        drop(session);
        assert!(
            weak.upgrade().is_some(),
            "another owner still needs the session"
        );
        drop(last_owner);
        assert!(catalog.upgrade().is_none(), "session catalog cycle remains");
        assert!(schema.upgrade().is_none(), "compatibility tables remain");
        assert!(
            weak.upgrade().is_none(),
            "session catalog retained its metadata"
        );
    }
}

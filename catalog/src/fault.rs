use datafusion::error::DataFusionError;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};
use lyra_meta::metadata::MetadataError;
use std::io;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, CataError>;

#[derive(Debug, Error)]
pub enum CataError {
    #[error("{message}")]
    Sql {
        code: &'static str,
        message: &'static str,
    },
    #[error("metadata operation failed")]
    Metadata(#[from] MetadataError),
    #[error("query planning failed")]
    DataFusion(#[from] DataFusionError),
    #[error("server I/O failed")]
    Io(#[from] io::Error),
}

impl CataError {
    pub(crate) fn sql(code: &'static str, message: &'static str) -> Self {
        Self::Sql { code, message }
    }
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::Sql { code, .. } => code,
            Self::Metadata(MetadataError::AlreadyExists) => "42P04",
            Self::Metadata(MetadataError::NotFound) => "3D000",
            Self::Metadata(MetadataError::InvalidName) => "42602",
            Self::Metadata(MetadataError::Reserved) => "42501",
            Self::Metadata(MetadataError::Conflict(_)) => "40001",
            _ => "XX000",
        }
    }
}

impl From<CataError> for PgWireError {
    fn from(error: CataError) -> Self {
        // Never expose transport errors, SQL text, credentials, or metadata payloads.
        Self::UserError(Box::new(ErrorInfo::new(
            "ERROR".into(),
            error.code().into(),
            error.to_string(),
        )))
    }
}

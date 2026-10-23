use crate::CataError;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError};

pub(crate) fn to_pgwire_error(error: CataError) -> PgWireError {
    let (severity, code) = match error {
        CataError::Sql(_) => ("ERROR", "42601"),
        CataError::UserAlreadyExists(_) => ("ERROR", "42710"),
        CataError::UserNotFound(_) => ("ERROR", "42704"),
        CataError::UserChanged(_) => ("ERROR", "40001"),
        CataError::UserInUse(_) => ("ERROR", "55006"),
        CataError::AuthenticationRequired => ("ERROR", "28000"),
        CataError::SecretAlreadyExists(_) => ("ERROR", "42710"),
        CataError::SecretNotFound(_) => ("ERROR", "42704"),
        CataError::SecretChanged(_) => ("ERROR", "40001"),
        CataError::InvalidPasswordSecret(_) => ("ERROR", "22023"),
        CataError::SecretInUse { .. } => ("ERROR", "2BP01"),
        CataError::DatabaseNotFound(_) => ("ERROR", "3D000"),
        CataError::DatabaseAlreadyExists(_) => ("ERROR", "42P04"),
        CataError::DatabaseChanged(_) => ("ERROR", "40001"),
        CataError::DatabaseInUse(_) => ("ERROR", "55006"),
        CataError::DatabaseNotEmpty(_) => ("ERROR", "2BP01"),
        CataError::SchemaNotFound { .. } => ("ERROR", "3F000"),
        CataError::SchemaAlreadyExists { .. } => ("ERROR", "42P06"),
        CataError::SchemaChanged { .. } => ("ERROR", "40001"),
        CataError::SchemaNotEmpty { .. } => ("ERROR", "2BP01"),
        _ => ("ERROR", "XX000"),
    };
    PgWireError::UserError(Box::new(ErrorInfo::new(
        severity.to_string(),
        code.to_string(),
        error.to_string(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::error::DataFusionError;
    use datafusion::sql::sqlparser::parser::ParserError;
    use lyra_meta::metadata::MetadataError;

    fn code_for(error: CataError) -> (String, String) {
        match to_pgwire_error(error) {
            PgWireError::UserError(info) => (info.severity, info.code),
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn maps_sql_errors_to_their_sqlstate_codes() {
        let cases = [
            (CataError::Sql(ParserError::ParserError("bad".into())), "42601"),
            (CataError::UserAlreadyExists("u".into()), "42710"),
            (CataError::UserNotFound("u".into()), "42704"),
            (CataError::UserChanged("u".into()), "40001"),
            (CataError::UserInUse("u".into()), "55006"),
            (CataError::AuthenticationRequired, "28000"),
            (CataError::SecretAlreadyExists("s".into()), "42710"),
            (CataError::SecretNotFound("s".into()), "42704"),
            (CataError::SecretChanged("s".into()), "40001"),
            (CataError::InvalidPasswordSecret("s".into()), "22023"),
            (
                CataError::SecretInUse {
                    secret: "s".into(),
                    connection: "c".into(),
                },
                "2BP01",
            ),
            (CataError::DatabaseNotFound("d".into()), "3D000"),
            (CataError::DatabaseAlreadyExists("d".into()), "42P04"),
            (CataError::DatabaseChanged("d".into()), "40001"),
            (CataError::DatabaseInUse("d".into()), "55006"),
            (CataError::DatabaseNotEmpty("d".into()), "2BP01"),
            (
                CataError::SchemaNotFound {
                    database: "d".into(),
                    schema: "s".into(),
                },
                "3F000",
            ),
            (
                CataError::SchemaAlreadyExists {
                    database: "d".into(),
                    schema: "s".into(),
                },
                "42P06",
            ),
            (
                CataError::SchemaChanged {
                    database: "d".into(),
                    schema: "s".into(),
                },
                "40001",
            ),
            (
                CataError::SchemaNotEmpty {
                    database: "d".into(),
                    schema: "s".into(),
                },
                "2BP01",
            ),
        ];

        for (error, expected) in cases {
            let (severity, code) = code_for(error);
            assert_eq!(severity, "ERROR");
            assert_eq!(code, expected);
        }
    }

    #[test]
    fn maps_unlisted_errors_to_internal_error() {
        let cases = [
            CataError::Plan(DataFusionError::Internal("plan".into())),
            CataError::QueryCatalog(Box::new(DataFusionError::Internal("catalog".into()))),
            CataError::Metadata(MetadataError::Conflict("key".into())),
            CataError::Server(std::io::Error::other("io")),
            CataError::BootstrapUserRequired,
        ];
        for error in cases {
            assert_eq!(code_for(error), ("ERROR".to_string(), "XX000".to_string()));
        }
    }
}

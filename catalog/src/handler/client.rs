use datafusion_postgres::pgwire::api::{ClientInfo, METADATA_DATABASE, METADATA_USER};
use lyra_meta::metadata::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

pub(crate) fn client_database<C>(client: &C) -> &str
where
    C: ClientInfo,
{
    client
        .metadata()
        .get(METADATA_DATABASE)
        .map(String::as_str)
        .unwrap_or(DEFAULT_DATABASE_NAME)
}

pub(crate) fn client_schemas<C>(client: &C) -> Vec<String>
where
    C: ClientInfo,
{
    let schemas = client
        .metadata()
        .get("search_path")
        .into_iter()
        .flat_map(|search_path| search_path.split(','))
        .map(str::trim)
        .map(|schema| schema.trim_matches('"'))
        .filter(|schema| !schema.is_empty() && *schema != "$user")
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if schemas.is_empty() {
        vec![DEFAULT_SCHEMA_NAME.to_string()]
    } else {
        schemas
    }
}

pub(crate) fn client_user<C>(client: &C) -> Option<&str>
where
    C: ClientInfo,
{
    client.metadata().get(METADATA_USER).map(String::as_str)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_postgres::pgwire::api::{METADATA_DATABASE, METADATA_USER};
    use datafusion_postgres::testing::MockClient;

    #[test]
    fn resolves_defaults_when_metadata_is_absent() {
        let client = MockClient::new();
        assert_eq!(client_database(&client), DEFAULT_DATABASE_NAME);
        assert_eq!(
            client_schemas(&client),
            vec![DEFAULT_SCHEMA_NAME.to_string()]
        );
        assert_eq!(client_user(&client), Some("postgres"));
    }

    #[test]
    fn reads_database_and_user_from_metadata() {
        let mut client = MockClient::new();
        client
            .metadata_mut()
            .insert(METADATA_DATABASE.to_string(), "analytics".to_string());
        client
            .metadata_mut()
            .insert(METADATA_USER.to_string(), "alice".to_string());

        assert_eq!(client_database(&client), "analytics");
        assert_eq!(client_user(&client), Some("alice"));
    }

    #[test]
    fn parses_search_path_and_skips_sentinel_values() {
        let mut client = MockClient::new();
        client.metadata_mut().insert(
            "search_path".to_string(),
            "\"public\", other, $user, , \"\"".to_string(),
        );

        assert_eq!(
            client_schemas(&client),
            vec!["public".to_string(), "other".to_string()]
        );
    }
}

use arrow_array::{
    Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array,
    RecordBatch, StringArray, UInt64Array,
};
use arrow_flight::sql::client::FlightSqlServiceClient;
use clap::Args;
use futures_util::TryStreamExt;
use serde::Deserialize;
use std::io::{self, Write};
use std::path::Path;
use tonic::transport::{Channel, Endpoint};
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_CONFIG_PATH: &str = "/etc/lyra/options/sql.toml";

#[derive(Debug, Args)]
pub struct SqlArgs {
    #[arg(short, long)]
    pub config: Option<String>,

    #[arg(long)]
    pub endpoint: Option<String>,

    #[arg(short = 'e', long)]
    pub execute: Option<String>,
}

pub async fn run(args: SqlArgs) -> Result<(), Box<dyn std::error::Error>> {
    let config = load_config(args.config.as_deref())?;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.log.level)),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false)
        .compact()
        .try_init();

    let endpoint = args.endpoint.unwrap_or(config.sql.endpoint);
    info!(endpoint = %endpoint, "connecting to Flight SQL endpoint");
    let mut client = connect_client(&endpoint).await?;

    if let Some(statement) = args.execute {
        execute_and_print(&mut client, &statement).await?;
        return Ok(());
    }

    repl(&mut client).await
}

async fn repl(
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        print!("lyra> ");
        io::stdout().flush()?;

        line.clear();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }

        let statement = line.trim();
        if statement.is_empty() {
            continue;
        }
        if statement.eq_ignore_ascii_case("quit")
            || statement.eq_ignore_ascii_case("exit")
            || statement.eq_ignore_ascii_case("\\q")
        {
            break;
        }

        if let Err(error) = execute_and_print(client, statement).await {
            eprintln!("error: {error}");
        }
    }

    Ok(())
}

async fn execute_and_print(
    client: &mut FlightSqlServiceClient<Channel>,
    statement: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let flight_info = client.execute(statement.to_string(), None).await?;
    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        let Some(ticket) = endpoint.ticket else {
            continue;
        };
        let flight_data = client.do_get(ticket).await?;
        let mut endpoint_batches: Vec<RecordBatch> = flight_data.try_collect().await?;
        batches.append(&mut endpoint_batches);
    }
    print_batches(&batches);
    Ok(())
}

async fn connect_client(
    endpoint: &str,
) -> Result<FlightSqlServiceClient<Channel>, Box<dyn std::error::Error>> {
    let endpoint = normalize_endpoint(endpoint);
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    Ok(FlightSqlServiceClient::new(channel))
}

fn normalize_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn print_batches(batches: &[RecordBatch]) {
    for batch in batches {
        for row in 0..batch.num_rows() {
            let values = batch
                .columns()
                .iter()
                .map(|array| value_to_string(array.as_ref(), row))
                .collect::<Vec<_>>();
            println!("{}", values.join("\t"));
        }
    }
}

fn value_to_string(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "NULL".to_string();
    }

    match array.data_type() {
        arrow_schema::DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Float32 => array
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| array.value(row).to_string())
            .unwrap_or_default(),
        arrow_schema::DataType::Binary => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .map(|array| String::from_utf8_lossy(array.value(row)).into_owned())
            .unwrap_or_default(),
        other => format!("<{other}>"),
    }
}

#[derive(Debug, Deserialize)]
struct SqlConfig {
    #[serde(default)]
    sql: SqlClientConfig,
    #[serde(default)]
    log: LogConfig,
}

#[derive(Debug, Deserialize)]
struct SqlClientConfig {
    #[serde(default = "default_sql_endpoint")]
    endpoint: String,
}

impl Default for SqlClientConfig {
    fn default() -> Self {
        Self {
            endpoint: default_sql_endpoint(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct LogConfig {
    #[serde(default = "default_log_level")]
    level: String,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_sql_endpoint() -> String {
    "http://127.0.0.1:50051".to_string()
}

fn load_config(path: Option<&str>) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    match resolve_config_path(path) {
        Some(path) => read_config(&path),
        None => Ok(SqlConfig {
            sql: SqlClientConfig::default(),
            log: LogConfig::default(),
        }),
    }
}

fn resolve_config_path(path: Option<&str>) -> Option<String> {
    if let Some(path) = path {
        return Some(path.to_string());
    }
    if let Ok(path) = std::env::var("LYRA_SQL_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if let Ok(path) = std::env::var("LYRA_CONFIG")
        && !path.trim().is_empty()
    {
        return Some(path);
    }
    if Path::new(DEFAULT_CONFIG_PATH).exists() {
        return Some(DEFAULT_CONFIG_PATH.to_string());
    }
    None
}

fn read_config(path: &str) -> Result<SqlConfig, Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)
        .map_err(|error| format!("failed to read config file '{}': {}", path, error))?;
    toml::from_str(&contents)
        .map_err(|error| format!("failed to parse config file '{}': {}", path, error).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::NullArray;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::{Arc, Mutex};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn normalizes_endpoints() {
        assert_eq!(normalize_endpoint("http://localhost:1"), "http://localhost:1");
        assert_eq!(normalize_endpoint("https://localhost:1"), "https://localhost:1");
        assert_eq!(normalize_endpoint("localhost:1"), "http://localhost:1");
    }

    #[test]
    fn formats_array_values() {
        let nulls = StringArray::from(vec![Option::<&str>::None]);
        assert_eq!(value_to_string(&nulls, 0), "NULL");

        assert_eq!(
            value_to_string(&StringArray::from(vec!["hello"]), 0),
            "hello"
        );
        assert_eq!(value_to_string(&Int32Array::from(vec![42]), 0), "42");
        assert_eq!(value_to_string(&Int64Array::from(vec![42]), 0), "42");
        assert_eq!(value_to_string(&UInt64Array::from(vec![42]), 0), "42");
        assert_eq!(value_to_string(&Float32Array::from(vec![1.5]), 0), "1.5");
        assert_eq!(value_to_string(&Float64Array::from(vec![1.5]), 0), "1.5");
        assert_eq!(value_to_string(&BooleanArray::from(vec![true]), 0), "true");
        assert_eq!(
            value_to_string(&BinaryArray::from(vec![&b"abc"[..]]), 0),
            "abc"
        );
        assert_eq!(value_to_string(&NullArray::new(1), 0), "<Null>");
    }

    #[test]
    fn prints_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["x", "y"]))],
        )
        .unwrap();
        print_batches(&[batch]);
    }

    #[test]
    fn resolves_config_paths() {
        let _guard = ENV_LOCK.lock().unwrap();
        assert_eq!(
            resolve_config_path(Some("/tmp/custom.toml")),
            Some("/tmp/custom.toml".to_string())
        );

        unsafe { std::env::set_var("LYRA_SQL_CONFIG", "/tmp/lyra-sql.toml") };
        assert_eq!(
            resolve_config_path(None),
            Some("/tmp/lyra-sql.toml".to_string())
        );
        unsafe { std::env::remove_var("LYRA_SQL_CONFIG") };

        unsafe { std::env::set_var("LYRA_CONFIG", "/tmp/lyra.toml") };
        assert_eq!(
            resolve_config_path(None),
            Some("/tmp/lyra.toml".to_string())
        );
        unsafe { std::env::remove_var("LYRA_CONFIG") };

        assert_eq!(resolve_config_path(None), None);
    }

    #[test]
    fn loads_defaults_and_config_files() {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe {
            std::env::remove_var("LYRA_SQL_CONFIG");
            std::env::remove_var("LYRA_CONFIG");
        }
        let config = load_config(None).unwrap();
        assert_eq!(config.sql.endpoint, "http://127.0.0.1:50051");
        assert_eq!(config.log.level, "info");

        let path = std::env::temp_dir().join(format!("lyra-sql-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            r#"
                [sql]
                endpoint = "http://localhost:9999"

                [log]
                level = "debug"
            "#,
        )
        .unwrap();
        let config = load_config(Some(path.to_str().unwrap())).unwrap();
        assert_eq!(config.sql.endpoint, "http://localhost:9999");
        assert_eq!(config.log.level, "debug");
        std::fs::remove_file(&path).unwrap();

        assert!(load_config(Some("/tmp/does-not-exist.toml")).is_err());

        let bad = std::env::temp_dir().join(format!("lyra-sql-bad-{}.toml", std::process::id()));
        std::fs::write(&bad, "not: [valid").unwrap();
        assert!(load_config(Some(bad.to_str().unwrap())).is_err());
        std::fs::remove_file(&bad).unwrap();
    }
}

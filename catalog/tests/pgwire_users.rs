use lyra_catalog::{Cata, options::CataOptions};
use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::utils::verifier::make_verifier;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, NoTls};

async fn connect(
    port: u16,
    password: &str,
    database: &str,
) -> Result<Client, tokio_postgres::Error> {
    let parameters = format!(
        "host=127.0.0.1 port={port} user=lyrasys password={password} dbname={database} sslmode=disable"
    );
    let (client, connection) = tokio_postgres::connect(&parameters, NoTls).await?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_stateless_catalogs_authenticate_and_serve_read_only_queries() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-only").unwrap())
        .await
        .unwrap();
    let other = Arc::new(metadata.new_client());
    let mut servers = Vec::new();
    let mut catalogs = Vec::new();
    for backend in [metadata.clone(), other.clone()] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let catalog = Arc::new(Cata::new(CataOptions::default(), backend).await.unwrap());
        let server = Arc::clone(&catalog);
        servers.push(tokio::spawn(async move {
            server.start_with_listener(listener).await
        }));
        timeout(Duration::from_secs(3), async {
            while !catalog.probe().await.1 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(catalog.probe().await.0);
        let client = connect(port, "test-only", "public").await.unwrap();
        let value: i64 = client.query_one("SELECT 1", &[]).await.unwrap().get(0);
        assert_eq!(value, 1);
        let database: String = client
            .query_one("SELECT current_database()", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(database, "public");
        assert_eq!(client.query("SHOW DATABASES", &[]).await.unwrap().len(), 2);
        for sql in [
            "CREATE DATABASE forbidden",
            "ALTER DATABASE public OWNER TO lyrasys",
            "DROP DATABASE public",
            "CREATE USER forbidden",
        ] {
            assert!(client.batch_execute(sql).await.is_err());
        }
        assert!(connect(port, "wrong", "public").await.is_err());
        assert!(connect(port, "test-only", "lyrasys").await.is_err());
        catalogs.push(catalog);
    }
    assert_eq!(metadata.list_components().await.unwrap().len(), 2);
    for catalog in catalogs {
        catalog.cancellation().cancel();
    }
    for server in servers {
        server.await.unwrap().unwrap();
    }
    metadata.close().await.unwrap();
    other.close().await.unwrap();
}

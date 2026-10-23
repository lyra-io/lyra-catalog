use futures_util::future::join_all;
use lyra_catalog::{Cata, options::CataOptions};
use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::proto::pb_meta::{Database, DatabaseState};
use lyra_meta::utils::verifier::make_verifier;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client, Config, Error, NoTls};

struct Server {
    catalog: Arc<Cata>,
    task: tokio::task::JoinHandle<lyra_catalog::Result<()>>,
    port: u16,
}

impl Server {
    async fn new(metadata: Arc<dyn Metadata>) -> Self {
        let catalog = Arc::new(Cata::new(CataOptions::default(), metadata).await.unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let task = tokio::spawn(Arc::clone(&catalog).start_with_listener(listener));
        timeout(Duration::from_secs(5), async {
            while !catalog.is_ready() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        Self {
            catalog,
            task,
            port,
        }
    }
    async fn connect(&self, database: &str) -> Result<Client, Error> {
        self.login("lyrasys", "test-password", database).await
    }
    async fn login(&self, user: &str, password: &str, database: &str) -> Result<Client, Error> {
        let (client, connection) = Config::new()
            .host("127.0.0.1")
            .port(self.port)
            .user(user)
            .password(password)
            .dbname(database)
            .connect(NoTls)
            .await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(client)
    }
    async fn stop(self) {
        self.catalog.cancellation().cancel();
        timeout(Duration::from_secs(10), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn code(client: &Client, sql: &str, expected: &str) {
    let error = client.batch_execute(sql).await.expect_err(sql);
    assert_eq!(
        error.code().map(|c| c.code()),
        Some(expected),
        "{sql}: {error:?}"
    );
}

#[tokio::test]
async fn no_implicit_initialization() {
    let metadata = Arc::new(MemoryMetadata::new());
    assert!(
        Cata::new(CataOptions::default(), metadata.clone())
            .await
            .is_err()
    );
    assert!(metadata.instance().await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wire_database_contract() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let root_id = metadata.get_user("lyrasys").await.unwrap().unwrap().id();
    let owner = metadata
        .create_user("owner", make_verifier("owner-password").unwrap())
        .await
        .unwrap();
    let server = Server::new(metadata.clone()).await;
    let root = server.connect("public").await.unwrap();
    assert!(server.login("lyrasys", "wrong", "public").await.is_err());
    assert!(
        server
            .login("unknown", "test-password", "public")
            .await
            .is_err()
    );
    for database in ["", "lyrasys", "missing", &"x".repeat(64)] {
        assert!(server.connect(database).await.is_err(), "{database}");
    }
    assert_eq!(
        root.query_one("SELECT current_database()", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "public"
    );
    let rows = root
        .query(
            "SELECT oid, datname, datdba FROM pg_catalog.pg_database ORDER BY datname",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, String>(1), "lyrasys");
    assert_eq!(rows[1].get::<_, String>(1), "public");
    assert_eq!(rows[1].get::<_, i64>(2), i64::from(root_id));
    root.batch_execute("CREATE DATABASE Test WITH OWNER owner ENCODING 'UTF8' CONNECTION LIMIT 1")
        .await
        .unwrap();
    let database = metadata.get_database("test").await.unwrap().unwrap();
    assert_eq!(database.value().owner_user_id, owner.id());
    let target = server.connect("test").await.unwrap();
    assert!(server.connect("test").await.is_err());
    code(&root, "ALTER DATABASE test RENAME TO renamed", "0A000").await;
    code(&root, "DROP DATABASE test", "55006").await;
    code(&target, "DROP DATABASE test WITH (FORCE)", "55006").await;
    root.batch_execute("ALTER DATABASE test ALLOW_CONNECTIONS false")
        .await
        .unwrap();
    assert!(server.connect("test").await.is_err());
    assert_eq!(
        target
            .query_one("SELECT 1", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    root.batch_execute("DROP DATABASE test WITH (FORCE, TIMEOUT '2s')")
        .await
        .unwrap();
    assert!(target.query_one("SELECT 1", &[]).await.is_err());
    assert!(metadata.get_database("test").await.unwrap().is_none());
    assert_eq!(
        root.query_one("SELECT 2", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    root.execute("CREATE DATABASE original", &[]).await.unwrap();
    let id = metadata
        .get_database("original")
        .await
        .unwrap()
        .unwrap()
        .id();
    code(&root, "ALTER DATABASE original RENAME TO renamed", "0A000").await;
    let before_rejection = metadata.get_database("original").await.unwrap().unwrap();
    code(&root, "ALTER DATABASE original RENAME TO public", "0A000").await;
    let error = root
        .execute("ALTER DATABASE original RENAME TO renamed", &[])
        .await
        .unwrap_err();
    assert_eq!(error.as_db_error().unwrap().code().code(), "0A000");
    assert!(metadata.get_database("renamed").await.unwrap().is_none());
    assert_eq!(
        metadata.get_database("original").await.unwrap().unwrap(),
        before_rejection
    );
    assert_eq!(
        metadata
            .get_database("original")
            .await
            .unwrap()
            .unwrap()
            .id(),
        id
    );
    assert!(server.connect("original").await.is_ok());
    root.batch_execute("ALTER DATABASE original OWNER TO owner")
        .await
        .unwrap();
    assert_eq!(
        metadata
            .get_database("original")
            .await
            .unwrap()
            .unwrap()
            .value()
            .owner_user_id,
        owner.id()
    );
    root.batch_execute("ALTER DATABASE original RESET ALL")
        .await
        .unwrap();
    code(
        &root,
        "ALTER DATABASE original SET work_mem = '10MB'",
        "0A000",
    )
    .await;
    code(&root, "CREATE USER ignored", "0A000").await;
    code(&root, "CREATE SCHEMA ignored", "0A000").await;
    code(&root, "CREATE DATABASE original", "42P04").await;
    code(&root, "CREATE DATABASE unknown_owner OWNER absent", "42704").await;
    root.batch_execute("CREATE DATABASE IF NOT EXISTS original; DROP DATABASE IF EXISTS absent")
        .await
        .unwrap();
    root.batch_execute("BEGIN").await.unwrap();
    code(&root, "CREATE DATABASE transaction_db", "25001").await;
    root.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        metadata
            .get_database("transaction_db")
            .await
            .unwrap()
            .is_none()
    );
    let rows = root.query("SHOW DATABASES LIKE 'ori%'", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, String>(0), "original");
    code(&root, "DROP DATABASE lyrasys WITH (FORCE)", "42501").await;
    code(&root, "ALTER DATABASE lyrasys RENAME TO x", "0A000").await;
    drop(root);
    drop(target);
    server.stop().await;
    let server = Server::new(metadata.clone()).await;
    let root = server.connect("original").await.unwrap();
    root.batch_execute("DROP DATABASE public").await.unwrap();
    server.stop().await;
    let server = Server::new(metadata.clone()).await;
    assert!(server.connect("public").await.is_err());
    assert_eq!(metadata.list_databases().await.unwrap().len(), 2);
    server.stop().await;
}

#[tokio::test]
async fn malformed_scram_lengths_return_error_without_panicking() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let server = Server::new(metadata).await;
    for body in [
        vec![],
        b"SCRAM-SHA-256\0".to_vec(),
        b"SCRAM-SHA-256\0\xff\xff\xff\xfe".to_vec(),
        b"SCRAM-SHA-256\0\0\0\0\x10x".to_vec(),
    ] {
        let mut socket = TcpStream::connect(("127.0.0.1", server.port))
            .await
            .unwrap();
        // Deliberately omit database: the startup default must remain public.
        let startup = b"\0\x03\0\0user\0lyrasys\0\0";
        socket
            .write_all(&((startup.len() + 4) as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(startup).await.unwrap();
        let (kind, _) = read_message(&mut socket).await;
        assert_eq!(kind, b'R');
        socket.write_all(b"p").await.unwrap();
        socket
            .write_all(&((body.len() + 4) as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
        let (kind, response) = read_message(&mut socket).await;
        assert_eq!(kind, b'E');
        assert!(response.windows(5).any(|bytes| bytes == b"28P01"));
    }
    assert!(server.connect("public").await.is_ok());
    server.stop().await;
}

async fn read_message(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    timeout(Duration::from_secs(2), async {
        let mut header = [0; 5];
        socket.read_exact(&mut header).await.unwrap();
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        assert!((4..=4096).contains(&length));
        let mut body = vec![0; length - 4];
        socket.read_exact(&mut body).await.unwrap();
        (header[0], body)
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupted_drop_is_terminal_across_restart() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let user = metadata.get_user("lyrasys").await.unwrap().unwrap();
    let database = metadata
        .create_database(Database::new("interrupted", user.id()))
        .await
        .unwrap();
    let mut value = database.value().clone();
    value.state = DatabaseState::Dropping as i32;
    metadata
        .update_database(database.id(), value, database.version())
        .await
        .unwrap();
    let server = Server::new(metadata.clone()).await;
    assert_eq!(
        metadata
            .get_database("interrupted")
            .await
            .unwrap()
            .unwrap()
            .value()
            .state(),
        DatabaseState::DropFailed
    );
    assert!(server.connect("interrupted").await.is_err());
    let root = server.connect("public").await.unwrap();
    code(&root, "DROP DATABASE interrupted", "55000").await;
    code(&root, "DROP DATABASE interrupted WITH (FORCE)", "55000").await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_name_and_admission_checks_are_serialized() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let server = Server::new(metadata.clone()).await;
    let clients = join_all((0..8).map(|_| server.connect("public")))
        .await
        .into_iter()
        .map(Result::unwrap)
        .collect::<Vec<_>>();
    let results = join_all(
        clients
            .iter()
            .map(|client| client.batch_execute("CREATE DATABASE concurrent CONNECTION LIMIT 1")),
    )
    .await;
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    for error in results.into_iter().filter_map(Result::err) {
        assert_eq!(error.code().unwrap().code(), "42P04");
    }
    let connections = join_all((0..8).map(|_| server.connect("concurrent"))).await;
    assert_eq!(
        connections.iter().filter(|result| result.is_ok()).count(),
        1
    );
    for error in connections
        .iter()
        .filter_map(|result| result.as_ref().err())
    {
        assert_eq!(error.code().unwrap().code(), "53300");
    }
    assert_eq!(metadata.list_databases().await.unwrap().len(), 3);
    drop(connections);
    drop(clients);
    server.stop().await;
}

#[tokio::test]
async fn malformed_startup_text_and_short_cancel_do_not_reach_lossy_decoder() {
    let metadata = Arc::new(MemoryMetadata::new());
    metadata
        .initialize(make_verifier("test-password").unwrap())
        .await
        .unwrap();
    let server = Server::new(metadata).await;
    for (payload, expected) in [
        (b"\0\x03\0\0user\0\xff\0\0".to_vec(), "22021"),
        (80877102u32.to_be_bytes().to_vec(), "08P01"),
    ] {
        let mut socket = TcpStream::connect(("127.0.0.1", server.port))
            .await
            .unwrap();
        socket
            .write_all(&((payload.len() + 4) as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&payload).await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(2), socket.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.first(), Some(&b'E'));
        assert!(
            response
                .windows(expected.len())
                .any(|bytes| bytes == expected.as_bytes())
        );
    }
    assert!(server.connect("public").await.is_ok());
    server.stop().await;
}

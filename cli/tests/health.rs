use lyra_catalog::{Cata, options::CataOptions};
use lyra_catalog_cli::health::{self, Health};
use lyra_meta::config::Observability;
use lyra_meta::metadata::{MemoryMetadata, Metadata};
use lyra_meta::observability::Telemetry;
use lyra_meta::utils::verifier::make_verifier;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Handle;
#[cfg(target_os = "linux")]
use tokio::time::sleep;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

async fn request(address: SocketAddr, method: &str, path: &str) -> Vec<u8> {
    timeout(Duration::from_secs(4), async {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                format!("{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).await.unwrap();
        bytes
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_http_routes_and_capture_disconnect() {
    let mut settings = Observability::default().normalize().unwrap();
    settings.prometheus.enabled = false;
    let telemetry = Arc::new(Telemetry::new(settings, true, None, &Handle::current()).unwrap());
    let meta = Arc::new(MemoryMetadata::new());
    meta.initialize(make_verifier("test-only").unwrap())
        .await
        .unwrap();
    let catalog = Arc::new(
        Cata::new(CataOptions::default(), meta.clone())
            .await
            .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let context = CancellationToken::new();
    let cancel = context.clone();
    let profiler = telemetry.profiler();
    let service = tokio::spawn(async move {
        axum::serve(listener, health::router(Health { catalog, profiler }))
            .with_graceful_shutdown(cancel.cancelled_owned())
            .await
            .unwrap();
    });
    assert!(
        request(address, "GET", "/live")
            .await
            .starts_with(b"HTTP/1.1 200")
    );
    assert!(
        request(address, "GET", "/ready")
            .await
            .starts_with(b"HTTP/1.1 503")
    );
    assert!(
        request(address, "GET", "/metrics")
            .await
            .starts_with(b"HTTP/1.1 404")
    );
    assert!(
        request(address, "HEAD", "/debug/pprof/profile")
            .await
            .starts_with(b"HTTP/1.1 405")
    );
    assert!(
        request(address, "GET", "/debug/pprof/profile?seconds=0")
            .await
            .starts_with(b"HTTP/1.1 400")
    );
    #[cfg(target_os = "linux")]
    capture_disconnect(address).await;
    meta.close().await.unwrap();
    assert!(
        request(address, "GET", "/live")
            .await
            .starts_with(b"HTTP/1.1 503")
    );
    assert!(
        request(address, "GET", "/ready")
            .await
            .starts_with(b"HTTP/1.1 503")
    );
    telemetry.close().await;
    context.cancel();
    service.await.unwrap();
}

// CPU profiling is validated on the supported Linux deployment target.
#[cfg(target_os = "linux")]
async fn capture_disconnect(address: SocketAddr) {
    let mut disconnect = TcpStream::connect(address).await.unwrap();
    disconnect
        .write_all(b"GET /debug/pprof/profile?seconds=30 HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    assert!(
        request(address, "GET", "/debug/pprof/profile?seconds=1")
            .await
            .starts_with(b"HTTP/1.1 429")
    );
    drop(disconnect);
    // A disconnected capture must release its sampler/permit, followed by the
    // five-second cooldown. It must not occupy the full original 30 seconds.
    timeout(Duration::from_secs(15), async {
        loop {
            let response = request(address, "GET", "/debug/pprof/profile?seconds=1").await;
            if response.starts_with(b"HTTP/1.1 200") {
                break;
            }
            assert!(response.starts_with(b"HTTP/1.1 429"));
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
}

use axum::Router;
use axum::http::{StatusCode, header};
use axum::routing::get;
use lyra_catalog::Cata;
use prometheus::{Encoder, Registry, TextEncoder};
use std::io;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn serve(
    listener: TcpListener,
    catalog: Arc<Cata>,
    registry: Registry,
) -> io::Result<()> {
    let shutdown = catalog.cancellation();
    axum::serve(listener, router(catalog, registry))
        .with_graceful_shutdown(shutdown.cancelled_owned())
        .await
}

fn router(catalog: Arc<Cata>, registry: Registry) -> Router {
    Router::new()
        .route("/live", get(|| async { StatusCode::OK }))
        .route(
            "/ready",
            get(move || {
                let catalog = Arc::clone(&catalog);
                async move {
                    if catalog.is_ready() {
                        StatusCode::OK
                    } else {
                        StatusCode::SERVICE_UNAVAILABLE
                    }
                }
            }),
        )
        .route(
            "/metrics",
            get(move || {
                let registry = registry.clone();
                async move {
                    let mut body = Vec::new();
                    TextEncoder::new()
                        .encode(&registry.gather(), &mut body)
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                    Ok::<_, StatusCode>((
                        [(
                            header::CONTENT_TYPE,
                            "text/plain; version=0.0.4; charset=utf-8",
                        )],
                        body,
                    ))
                }
            }),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use lyra_catalog::options::CataOptions;
    use lyra_meta::metadata::{MemoryMetadata, Metadata};
    use lyra_meta::utils::verifier::make_verifier;
    use prometheus::IntGauge;
    use tower::ServiceExt;

    #[tokio::test]
    async fn metrics_remain_available_while_catalog_is_not_ready() {
        let metadata = Arc::new(MemoryMetadata::new());
        metadata
            .initialize(make_verifier("test-only-password").unwrap())
            .await
            .unwrap();
        let catalog = Arc::new(Cata::new(CataOptions::default(), metadata).await.unwrap());
        let registry = Registry::new();
        let gauge = IntGauge::new("lyra_catalog_ready", "Catalog readiness").unwrap();
        registry.register(Box::new(gauge)).unwrap();
        let app = router(catalog, registry);
        for (path, expected) in [
            ("/live", StatusCode::OK),
            ("/ready", StatusCode::SERVICE_UNAVAILABLE),
            ("/missing", StatusCode::NOT_FOUND),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/plain; version=0.0.4")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(
            String::from_utf8(body.to_vec())
                .unwrap()
                .contains("lyra_catalog_ready 0")
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}

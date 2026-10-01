use axum::{
    Router,
    extract::State,
    http::{Method, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use lyra_catalog::Cata;
use lyra_meta::observability::{ProfileError, Profiler};
use std::sync::Arc;

#[derive(Clone)]
pub struct Health {
    pub catalog: Arc<Cata>,
    pub profiler: Arc<Profiler>,
}
pub fn router(state: Health) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .route("/debug/pprof/profile", get(profile))
        .with_state(state)
}
async fn live(State(state): State<Health>) -> StatusCode {
    if state.catalog.probe().await.0 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn ready(State(state): State<Health>) -> StatusCode {
    if state.catalog.probe().await.1 {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn profile(State(state): State<Health>, method: Method, uri: Uri) -> Response {
    if method != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    match state.profiler.capture(uri.query()).await {
        Ok(body) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream"),
                (header::CONTENT_ENCODING, "gzip"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            body,
        )
            .into_response(),
        Err(ProfileError::Disabled) => StatusCode::NOT_FOUND.into_response(),
        Err(ProfileError::InvalidQuery) => StatusCode::BAD_REQUEST.into_response(),
        Err(ProfileError::Busy) => StatusCode::TOO_MANY_REQUESTS.into_response(),
        Err(ProfileError::Cancelled | ProfileError::Failed) => {
            StatusCode::SERVICE_UNAVAILABLE.into_response()
        }
    }
}

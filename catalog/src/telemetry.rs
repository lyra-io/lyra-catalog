use lyra_meta::observability::{Gauge, Meter};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) struct Metrics {
    // Immutable state
    ready: Gauge<u64>,
    // Mutable state
    last_auth_failure: Mutex<Option<Instant>>,
}
impl Metrics {
    pub(crate) fn new(meter: Meter) -> Self {
        Self {
            ready: meter.u64_gauge("lyra_catalog_ready").build(),
            last_auth_failure: Mutex::new(None),
        }
    }
    pub(crate) fn ready(&self, ready: bool) {
        self.ready.record(u64::from(ready), &[]);
    }
    pub(crate) fn authentication(&self, reason: &'static str) {
        let mut last = self
            .last_auth_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
            tracing::warn!(
                event = "authentication_failed",
                reason,
                "authentication failed"
            );
            *last = Some(Instant::now());
        }
    }
}

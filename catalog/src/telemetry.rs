use crate::Result;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub(crate) struct Metrics {
    // Immutable state
    operations: Counter<u64>,
    duration: Histogram<f64>,
    active: Gauge<u64>,
    authentication: Counter<u64>,
    ready: Gauge<u64>,
    // Mutable state
    last_failure: Mutex<Option<Instant>>,
    last_auth_failure: Mutex<Option<Instant>>,
}

impl Metrics {
    pub(crate) fn new(meter: Meter) -> Self {
        Self {
            operations: meter
                .u64_counter("lyra_catalog_database_operations_total")
                .build(),
            duration: meter
                .f64_histogram("lyra_catalog_database_operation_duration_seconds")
                .with_unit("s")
                .with_boundaries(vec![
                    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
                    60.0,
                ])
                .build(),
            active: meter.u64_gauge("lyra_catalog_active_connections").build(),
            authentication: meter
                .u64_counter("lyra_catalog_authentication_failures_total")
                .build(),
            ready: meter.u64_gauge("lyra_catalog_ready").build(),
            last_failure: Mutex::new(None),
            last_auth_failure: Mutex::new(None),
        }
    }
    pub(crate) fn operation<T>(
        &self,
        operation: &'static str,
        started: Instant,
        result: &Result<T>,
    ) {
        let outcome = if result.is_ok() { "success" } else { "error" };
        let labels = [
            KeyValue::new("operation", operation),
            KeyValue::new("outcome", outcome),
        ];
        self.operations.add(1, &labels);
        self.duration
            .record(started.elapsed().as_secs_f64(), &labels);
        let duration_seconds = started.elapsed().as_secs_f64();
        if let Err(error) = result {
            let mut last = self.last_failure.lock().unwrap();
            if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(5)) {
                tracing::warn!(
                    event = "database_mutation_failed",
                    operation,
                    outcome,
                    duration_seconds,
                    code = error.code(),
                    "database operation failed"
                );
                *last = Some(Instant::now());
            }
        } else if operation != "list" {
            tracing::info!(
                event = "database_mutation_completed",
                operation,
                outcome,
                duration_seconds,
                "database operation completed"
            );
        }
    }
    pub(crate) fn active(&self, count: usize) {
        self.active.record(count as u64, &[]);
    }
    pub(crate) fn ready(&self, ready: bool) {
        self.ready.record(u64::from(ready), &[]);
    }
    pub(crate) fn authentication(&self, reason: &'static str) {
        self.authentication
            .add(1, &[KeyValue::new("reason", reason)]);
        let mut last = self.last_auth_failure.lock().unwrap();
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

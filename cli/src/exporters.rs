use opentelemetry_otlp::{LogExporter as OtlpLogs, MetricExporter as OtlpMetrics};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::logs::{LogBatch, LogExporter};
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use opentelemetry_sdk::metrics::{Temporality, exporter::PushMetricExporter};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
struct Health {
    // Mutable state
    failure: Mutex<Option<Instant>>,
}
impl Health {
    fn observe(&self, signal: &'static str, result: &OTelSdkResult) {
        let mut last = self.failure.lock().unwrap();
        if result.is_err() {
            if last.is_none_or(|last| last.elapsed() >= Duration::from_secs(30)) {
                // This target only goes to local JSON, never back into its own
                // failing OTLP log exporter. No exporter error strings are used.
                tracing::warn!(target: "lyra_telemetry", event = "export_failed", signal, outcome = "error", "telemetry export failed; this batch may be lost");
                *last = Some(Instant::now());
            }
        } else if last.take().is_some() {
            tracing::info!(target: "lyra_telemetry", event = "export_recovered", signal, outcome = "success", "telemetry export recovered");
        }
    }
}

pub(crate) struct Metrics {
    // Immutable state
    exporter: OtlpMetrics,
    // Mutable state
    health: Health,
}
impl Metrics {
    pub(crate) fn new(exporter: OtlpMetrics) -> Self {
        Self {
            exporter,
            health: Health::default(),
        }
    }
}
impl PushMetricExporter for Metrics {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let result = self.exporter.export(metrics).await;
        self.health.observe("metrics", &result);
        result
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.exporter.force_flush()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.exporter.shutdown_with_timeout(timeout)
    }
    fn temporality(&self) -> Temporality {
        self.exporter.temporality()
    }
}

#[derive(Debug)]
pub(crate) struct Logs {
    // Immutable state
    exporter: OtlpLogs,
    // Mutable state
    health: Health,
}
impl Logs {
    pub(crate) fn new(exporter: OtlpLogs) -> Self {
        Self {
            exporter,
            health: Health::default(),
        }
    }
}
impl LogExporter for Logs {
    async fn export(&self, batch: LogBatch<'_>) -> OTelSdkResult {
        let result = self.exporter.export(batch).await;
        self.health.observe("logs", &result);
        result
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.exporter.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.exporter.set_resource(resource);
    }
}

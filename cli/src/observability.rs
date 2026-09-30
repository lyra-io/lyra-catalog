use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;
use std::io;
use std::time::Duration;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub struct Telemetry {
    // Control state
    _stdout: WorkerGuard,
    // Immutable state
    metrics: SdkMeterProvider,
    registry: Registry,
}

impl Telemetry {
    pub fn new() -> Result<Self, &'static str> {
        let (metrics, registry) = provider()?;
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .finish(io::stdout());
        // Only sanitized Lyra events enter stdout. Third-party SQL/client
        // diagnostics can contain raw queries or authentication messages.
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_filter(EnvFilter::new(
                        "off,lyra_catalog=info,lyra_catalog_cli=info,lyra_meta=info",
                    )),
            )
            .try_init()
            .map_err(|_| "logging could not be initialized")?;
        Ok(Self {
            _stdout: guard,
            metrics,
            registry,
        })
    }
    pub fn meter(&self, component: &'static str) -> Meter {
        self.metrics.meter(component)
    }
    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }
    pub async fn shutdown(self) {
        let metrics = self.metrics.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = metrics.shutdown_with_timeout(Duration::from_secs(3));
        })
        .await;
    }
}

fn provider() -> Result<(SdkMeterProvider, Registry), &'static str> {
    let registry = Registry::new();
    let reader = opentelemetry_prometheus::exporter()
        .with_registry(registry.clone())
        // Instruments already use the reviewed Prometheus names and units.
        .without_units()
        .without_counter_suffixes()
        .without_target_info()
        .build()
        .map_err(|_| "Prometheus exporter could not be initialized")?;
    let metrics = SdkMeterProvider::builder()
        .with_resource(
            Resource::builder_empty()
                .with_service_name("lyra-catalog")
                .build(),
        )
        .with_reader(reader)
        .build();
    Ok((metrics, registry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::KeyValue;
    use prometheus::{Encoder, TextEncoder};

    #[test]
    fn direct_scrapes_preserve_names_types_labels_and_cumulative_values() {
        let (provider, registry) = provider().unwrap();
        let meter = provider.meter("lyra-catalog");
        let counter = meter
            .u64_counter("lyra_catalog_database_operations_total")
            .build();
        let gauge = meter.u64_gauge("lyra_catalog_ready").build();
        let duration = meter
            .f64_histogram("lyra_catalog_database_operation_duration_seconds")
            .with_unit("s")
            .with_boundaries(vec![0.01, 1.0])
            .build();
        counter.add(1, &[KeyValue::new("operation", "create")]);
        gauge.record(1, &[]);
        duration.record(0.5, &[]);
        for _ in 0..2 {
            let mut body = Vec::new();
            TextEncoder::new()
                .encode(&registry.gather(), &mut body)
                .unwrap();
            let body = String::from_utf8(body).unwrap();
            assert!(body.contains("# TYPE lyra_catalog_database_operations_total counter"));
            assert!(body.contains("# TYPE lyra_catalog_ready gauge"));
            assert!(
                body.contains("# TYPE lyra_catalog_database_operation_duration_seconds histogram")
            );
            assert!(body.lines().any(|line| {
                line.starts_with("lyra_catalog_database_operations_total{")
                    && line.contains("operation=\"create\"")
                    && line.ends_with(" 1")
            }));
            assert!(!body.contains("_total_total"));
            assert!(!body.contains("_seconds_seconds"));
            assert!(!body.contains("service_name="));
        }
    }
}

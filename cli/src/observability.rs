use opentelemetry::metrics::{Meter, MeterProvider, ObservableCounter};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use prometheus::Registry;
use std::io;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing_appender::non_blocking::{ErrorCounter, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub struct Telemetry {
    // Control state
    log_monitor: JoinHandle<()>,
    _stdout: WorkerGuard,
    // Immutable state
    metrics: SdkMeterProvider,
    registry: Registry,
    _log_dropped: ObservableCounter<u64>,
}

impl Telemetry {
    pub fn new() -> Result<Self, &'static str> {
        let (metrics, registry) = provider()?;
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .finish(io::stdout());
        let dropped = writer.error_counter();
        let log_dropped = log_drop_counter(&metrics.meter("lyra-catalog"), dropped.clone());
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
        let log_monitor = tokio::spawn(async move {
            let mut previous = 0;
            let mut dropping = false;
            let mut ticks = tokio::time::interval(Duration::from_secs(5));
            loop {
                ticks.tick().await;
                let current = dropped.dropped_lines();
                if current > previous {
                    tracing::warn!(
                        service = "lyra-catalog",
                        event = "log_records_dropped",
                        dropped_records = current - previous,
                        "stdout queue dropped log records"
                    );
                    dropping = true;
                } else if dropping {
                    tracing::info!(
                        service = "lyra-catalog",
                        event = "log_queue_recovered",
                        "no additional log records dropped during the last interval"
                    );
                    dropping = false;
                }
                previous = current;
            }
        });
        Ok(Self {
            log_monitor,
            _stdout: guard,
            metrics,
            registry,
            _log_dropped: log_dropped,
        })
    }
    pub fn meter(&self, component: &'static str) -> Meter {
        self.metrics.meter(component)
    }
    pub fn registry(&self) -> Registry {
        self.registry.clone()
    }
    pub async fn shutdown(self) {
        self.log_monitor.abort();
        let metrics = self.metrics.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = metrics.shutdown_with_timeout(Duration::from_secs(3));
        })
        .await;
    }
}

fn log_drop_counter(meter: &Meter, dropped: ErrorCounter) -> ObservableCounter<u64> {
    meter
        .u64_observable_counter("lyra_catalog_log_dropped_total")
        .with_description("Log records dropped by the bounded stdout queue")
        .with_callback(move |observer| observer.observe(dropped.dropped_lines() as u64, &[]))
        .build()
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
    use std::io::Write;
    use std::sync::mpsc;

    struct StalledWriter {
        entered: mpsc::Sender<()>,
        resume: mpsc::Receiver<()>,
        first: bool,
    }

    impl Write for StalledWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.first {
                self.first = false;
                let _ = self.entered.send(());
                let _ = self.resume.recv_timeout(Duration::from_secs(5));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn blocked_stdout_is_nonblocking_and_dropped_records_remain_observable() {
        let (entered, waiting) = mpsc::channel();
        let (resume, receiver) = mpsc::channel();
        let (mut writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1)
            .lossy(true)
            .finish(StalledWriter {
                entered,
                resume: receiver,
                first: true,
            });
        writer.write_all(b"first\n").unwrap();
        waiting.recv_timeout(Duration::from_secs(2)).unwrap();
        writer.write_all(b"queued\n").unwrap();
        writer.write_all(b"dropped-one\n").unwrap();
        writer.write_all(b"dropped-two\n").unwrap();
        assert_eq!(writer.error_counter().dropped_lines(), 2);
        let (provider, registry) = provider().unwrap();
        let _instrument = log_drop_counter(&provider.meter("test"), writer.error_counter());
        for _ in 0..2 {
            let mut body = Vec::new();
            TextEncoder::new()
                .encode(&registry.gather(), &mut body)
                .unwrap();
            let body = String::from_utf8(body).unwrap();
            assert!(body.contains("# TYPE lyra_catalog_log_dropped_total counter"));
            assert!(
                body.lines()
                    .any(|line| line.starts_with("lyra_catalog_log_dropped_total")
                        && line.ends_with(" 2")),
                "{body}"
            );
        }
        resume.send(()).unwrap();
        drop(guard);
    }

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

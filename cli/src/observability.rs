use crate::exporters::{Logs, Metrics};
use crate::manifest::Observability;
use opentelemetry::metrics::{Meter, MeterProvider};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::{BatchConfigBuilder, BatchLogProcessor, SdkLoggerProvider};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use std::io;
use std::time::Duration;
use tracing_appender::non_blocking::{NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

pub struct Telemetry {
    // Control state
    _stdout: WorkerGuard,
    // Immutable state
    metrics: SdkMeterProvider,
    logs: SdkLoggerProvider,
}

impl Telemetry {
    pub fn new(options: Option<&Observability>) -> Result<Self, &'static str> {
        let resource = Resource::builder_empty()
            .with_service_name("lyra-catalog")
            .build();
        let mut metrics = SdkMeterProvider::builder().with_resource(resource.clone());
        let mut logs = SdkLoggerProvider::builder().with_resource(resource);
        if let Some(options) = options {
            let metric_exporter = MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&options.otlp_endpoint)
                .with_timeout(Duration::from_secs(2))
                .build()
                .map_err(|_| "metric exporter could not be initialized")?;
            metrics = metrics.with_reader(
                PeriodicReader::builder(Metrics::new(metric_exporter))
                    .with_interval(Duration::from_secs(5))
                    .build(),
            );
            let log_exporter = LogExporter::builder()
                .with_tonic()
                .with_endpoint(&options.otlp_endpoint)
                .with_timeout(Duration::from_secs(2))
                .build()
                .map_err(|_| "log exporter could not be initialized")?;
            logs = logs.with_log_processor(
                BatchLogProcessor::builder(Logs::new(log_exporter))
                    .with_batch_config(
                        BatchConfigBuilder::default()
                            .with_max_queue_size(1024)
                            .with_max_export_batch_size(128)
                            .with_scheduled_delay(Duration::from_secs(1))
                            .build(),
                    )
                    .build(),
            );
        }
        let metrics = metrics.build();
        let logs = logs.build();
        let bridge = OpenTelemetryTracingBridge::new(&logs);
        let (writer, guard) = NonBlockingBuilder::default()
            .buffered_lines_limit(1024)
            .lossy(true)
            .finish(io::stdout());
        // Only our sanitized events enter exported logs. Third-party SQL/client
        // diagnostics can contain raw queries or authentication messages.
        let filter =
            || EnvFilter::new("off,lyra_catalog=info,lyra_catalog_cli=info,lyra_meta=info");
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::fmt::layer()
                    .json()
                    .with_writer(writer)
                    .with_filter(EnvFilter::new("off,lyra_catalog=info,lyra_catalog_cli=info,lyra_meta=info,lyra_telemetry=info")),
            )
            .with(bridge.with_filter(filter()))
            .try_init()
            .map_err(|_| "logging could not be initialized")?;
        Ok(Self {
            _stdout: guard,
            metrics,
            logs,
        })
    }
    pub fn meter(&self, component: &'static str) -> Meter {
        self.metrics.meter(component)
    }
    pub async fn shutdown(self) {
        let metrics = self.metrics.clone();
        let logs = self.logs.clone();
        let _ = tokio::task::spawn_blocking(move || {
            let _ = logs.shutdown_with_timeout(Duration::from_secs(3));
            let _ = metrics.shutdown_with_timeout(Duration::from_secs(3));
        })
        .await;
    }
}

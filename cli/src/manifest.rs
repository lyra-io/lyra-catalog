use async_trait::async_trait;
use lyra_meta::config::{Listener, Metadata, Observability, Settings, validate_listeners};
use lyra_meta::observability::{Prepared, Telemetry};
use lyra_meta::toolkit::{ReloadError, ReloadTarget};
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    metadata: Metadata,
    server: Server,
    observability: Option<Observability>,
}
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Server {
    pub public: Listener,
    pub health: Option<Listener>,
    pub tokio_console: Option<Listener>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub metadata: Metadata,
    pub server: Server,
    pub observability: Settings,
}
impl Manifest {
    pub fn parse(text: &str) -> Result<Self, ReloadError> {
        let input: Input = toml::from_str(text).map_err(|_| ReloadError("invalid_toml"))?;
        input.metadata.validate().map_err(|e| ReloadError(e.0))?;
        let settings = input
            .observability
            .unwrap_or_default()
            .normalize()
            .map_err(|e| ReloadError(e.0))?;
        let mut listeners = vec![input.server.public.listen, settings.prometheus.listen];
        listeners.extend(input.server.health.as_ref().map(|l| l.listen));
        listeners.extend(input.server.tokio_console.as_ref().map(|l| l.listen));
        validate_listeners(&listeners).map_err(|e| ReloadError(e.0))?;
        Telemetry::validate(
            &settings,
            input.server.health.is_some(),
            input.server.tokio_console.as_ref().map(|l| l.listen),
        )?;
        Ok(Self {
            metadata: input.metadata,
            server: input.server,
            observability: settings,
        })
    }
    pub fn changes(&self, new: &Self) -> Result<Vec<&'static str>, ReloadError> {
        if self.metadata != new.metadata
            || self.server != new.server
            || self.observability.prometheus.listen != new.observability.prometheus.listen
        {
            return Err(ReloadError("restart_required"));
        }
        let mut fields = Vec::new();
        let a = &self.observability;
        let b = &new.observability;
        macro_rules! changed {
            ($field:expr, $left:expr, $right:expr) => {
                if $left != $right {
                    fields.push($field);
                }
            };
        }
        changed!(
            "observability.metrics.prometheus.enabled",
            a.prometheus.enabled,
            b.prometheus.enabled
        );
        changed!(
            "observability.metrics.prometheus.path",
            a.prometheus.path,
            b.prometheus.path
        );
        changed!("observability.log.level", a.log_level, b.log_level);
        changed!(
            "observability.pprof.enabled",
            a.pprof.enabled,
            b.pprof.enabled
        );
        changed!(
            "observability.pprof.frequency_hz",
            a.pprof.frequency_hz,
            b.pprof.frequency_hz
        );
        changed!(
            "observability.pprof.default_duration_seconds",
            a.pprof.default_duration_seconds,
            b.pprof.default_duration_seconds
        );
        changed!(
            "observability.pprof.max_duration_seconds",
            a.pprof.max_duration_seconds,
            b.pprof.max_duration_seconds
        );
        changed!(
            "observability.tokio_console.enabled",
            a.tokio_console.enabled,
            b.tokio_console.enabled
        );
        changed!(
            "observability.tokio_console.publish_interval_ms",
            a.tokio_console.publish_interval_ms,
            b.tokio_console.publish_interval_ms
        );
        changed!(
            "observability.tokio_console.retention_seconds",
            a.tokio_console.retention_seconds,
            b.tokio_console.retention_seconds
        );
        changed!(
            "observability.tokio_console.event_buffer_capacity",
            a.tokio_console.event_buffer_capacity,
            b.tokio_console.event_buffer_capacity
        );
        Ok(fields)
    }
}
pub struct Reload {
    pub telemetry: Arc<Telemetry>,
}
#[async_trait]
impl ReloadTarget for Reload {
    type Config = Manifest;
    type Prepared = Prepared;
    fn parse(&self, text: &str) -> Result<Manifest, ReloadError> {
        Manifest::parse(text)
    }
    fn changes(&self, old: &Manifest, new: &Manifest) -> Result<Vec<&'static str>, ReloadError> {
        old.changes(new)
    }
    async fn prepare(&self, _: &Manifest, new: &Manifest) -> Result<Prepared, ReloadError> {
        self.telemetry.prepare(new.observability.clone()).await
    }
    async fn commit(&self, prepared: Prepared, _: u64) {
        self.telemetry.commit(prepared).await;
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    const MINIMAL: &str = "[metadata.oxia]\nendpoint='localhost:6648'\nnamespace='default'\n[server.public]\nlisten='127.0.0.1:5432'\n";
    #[test]
    fn strict_toml_and_normalized_reload_policy() {
        let old = Manifest::parse(MINIMAL).unwrap();
        assert!(Manifest::parse("metadata: {}").is_err());
        assert!(Manifest::parse(&format!("{MINIMAL}password='private'")).is_err());
        let explicit = Manifest::parse(&format!("{MINIMAL}[observability.metrics.prometheus]\nenabled=true\nlisten='127.0.0.1:9090'\npath='/metrics'\n")).unwrap();
        assert_eq!(old, explicit);
        let live =
            Manifest::parse(&format!("{MINIMAL}[observability.log]\nlevel='debug'\n")).unwrap();
        assert_eq!(old.changes(&live).unwrap(), vec!["observability.log.level"]);
        let restart = Manifest::parse(&MINIMAL.replace("5432", "5433")).unwrap();
        assert!(old.changes(&restart).is_err());
        assert!(Manifest::parse(&MINIMAL.replace("5432", "9090")).is_err());
        assert!(Manifest::parse(&MINIMAL.replace("default", "")).is_err());
    }
}

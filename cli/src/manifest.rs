use serde::Deserialize;
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;
use std::path::Path;
use url::Url;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub metadata: Metadata,
    pub postgres: Postgres,
    pub observability: Option<Observability>,
    pub health: Option<Health>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub oxia: Oxia,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Oxia {
    pub endpoint: String,
    pub namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Postgres {
    pub listen: SocketAddr,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Health {
    pub listen: SocketAddr,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Observability {
    pub otlp_endpoint: String,
}

impl Manifest {
    pub fn read(path: &Path) -> Result<Self, &'static str> {
        let mut input = String::new();
        File::open(path)
            .map_err(|_| "manifest cannot be read")?
            .take(65_537)
            .read_to_string(&mut input)
            .map_err(|_| "manifest must be UTF-8")?;
        if input.len() > 65_536 {
            return Err("manifest exceeds 64 KiB");
        }
        Self::parse(&input)
    }
    pub fn parse(input: &str) -> Result<Self, &'static str> {
        // Parser diagnostics can contain scalar contents; never return them.
        let manifest: Self =
            serde_yaml_ng::from_str(input).map_err(|_| "invalid manifest or unknown fields")?;
        let oxia = &manifest.metadata.oxia;
        let endpoint = Url::parse(&format!("http://{}", oxia.endpoint))
            .map_err(|_| "invalid Oxia endpoint")?;
        if endpoint.host_str().is_none()
            || oxia
                .endpoint
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse::<u16>().ok())
                .is_none_or(|p| p == 0)
            || endpoint.path() != "/"
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err("Oxia endpoint must be host:port without credentials");
        }
        if oxia.namespace.is_empty()
            || oxia.namespace.len() > 128
            || !oxia
                .namespace
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        {
            return Err("invalid Oxia namespace");
        }
        if manifest.postgres.listen.port() == 0
            || manifest
                .health
                .as_ref()
                .is_some_and(|h| h.listen.port() == 0 || h.listen == manifest.postgres.listen)
        {
            return Err("listener ports must be nonzero and distinct");
        }
        if let Some(observability) = &manifest.observability {
            let endpoint =
                Url::parse(&observability.otlp_endpoint).map_err(|_| "invalid OTLP endpoint")?;
            if endpoint.scheme() != "http"
                || endpoint.host_str().is_none()
                || observability
                    .otlp_endpoint
                    .rsplit_once(':')
                    .and_then(|(_, port)| port.parse::<u16>().ok())
                    .is_none_or(|p| p == 0)
                || endpoint.path() != "/"
                || endpoint.query().is_some()
                || endpoint.fragment().is_some()
                || !endpoint.username().is_empty()
                || endpoint.password().is_some()
            {
                return Err("OTLP endpoint must be an http://host:port local Collector address");
            }
        }
        Ok(manifest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const MINIMAL: &str = "metadata:\n  oxia:\n    endpoint: localhost:6648\n    namespace: lyra-test\npostgres:\n  listen: 127.0.0.1:5432\n";
    #[test]
    fn strict_minimal_manifest() {
        assert!(Manifest::parse(MINIMAL).is_ok());
        assert!(Manifest::parse(&MINIMAL.replace("localhost:6648", "localhost:80")).is_ok());
        assert!(Manifest::parse(&MINIMAL.replace("localhost:6648", "localhost")).is_err());
        assert!(Manifest::parse(&format!("{MINIMAL}password: forbidden\n")).is_err());
        assert!(
            Manifest::parse(&MINIMAL.replace("localhost:6648", "user:password@localhost:6648"))
                .is_err()
        );
        assert!(Manifest::parse(&MINIMAL.replace("lyra-test", "a/b")).is_err());
        assert!(
            Manifest::parse(&format!("{MINIMAL}postgres:\n  listen: 127.0.0.1:5433\n")).is_err()
        );
    }
}

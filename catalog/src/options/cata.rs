#[derive(Clone, Debug)]
pub struct CataOptions {
    pub listen: String,
    pub max_connections: usize,
}

impl CataOptions {
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            listen: format!("{}:{port}", host.into()),
            max_connections: 1024,
        }
    }
}

impl Default for CataOptions {
    fn default() -> Self {
        Self::new("127.0.0.1", 5432)
    }
}

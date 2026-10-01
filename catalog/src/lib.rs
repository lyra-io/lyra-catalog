//! Stateless Catalog foundation and read-only PostgreSQL protocol server.
mod authentication;
mod cata;
mod command;
mod fault;
mod inventory;
pub mod options;
mod query;
mod state;
mod telemetry;
mod wire;

pub use cata::Cata;
pub use fault::{CataError, Result};

//! LIP-0001 database control plane and PostgreSQL protocol server.
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

//! The `comm` module - Communication Protocols and Messages
//! - Between client and scheduler
//! - Between scheduler and sequencer
//! - Between scheduler and dbproxy

mod msql_response;
pub mod scheduler_api;
pub mod scheduler_dbproxy;
pub mod scheduler_sequencer;

pub use msql_response::MsqlResponse;

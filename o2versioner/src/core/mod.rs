//! The `core` module - Algorithm and Data Structures
//! - Algorithm related data structures.
//! - Everything here is written in synchronous style.
//! - Does not use any synchronization primitives.

mod database_version;
mod legality;
mod metadata;
mod msql;
mod operation;
mod transaction_version;

pub use database_version::{DbTableVN, DbVN};
pub use legality::Legality;
pub use metadata::{ClientMeta, RequestMeta};
pub use msql::*;
pub use operation::{AccessPattern, EarlyReleaseTables, RWOperation, TableOp, TableOps};
pub use transaction_version::{DbVNReleaseRequest, TxTableVN, TxVN, VN};

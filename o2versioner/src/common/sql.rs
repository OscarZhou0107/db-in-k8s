use serde::{Deserialize, Serialize};

/// A Sql statement
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlStmt(pub String);

/// Keeps a list of all tables accessed for a Sql transaction
///
/// # Notes
/// 1. `r_tables` and `w_tables` should have no intersection
/// 2. Such intersection should reside in `w_tables` only
///
#[allow(dead_code)]
#[derive(Default)]
pub struct TxTable {
    r_tables: Vec<String>,
    w_tables: Vec<String>,
}

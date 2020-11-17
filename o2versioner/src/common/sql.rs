use serde::{Deserialize, Serialize};

/// Enum representing either W (write) or R (read)
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub enum Operation {
    W,
    R,
}

/// A Sql statement
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlStmt(pub String);

pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

/// Keeps a list of all tables accessed for a Sql transaction
///
/// # Notes
/// 1. `table_ops` should have no duplications in terms of `TableOp::table`
/// 2. Such duplication should only keep the one that `TableOp::op == Operation::W`
///
#[derive(Default)]
pub struct TxTable {
    pub table_ops: Vec<TableOp>,
}

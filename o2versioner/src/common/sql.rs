use serde::{Deserialize, Serialize};

/// Enum representing either W (write) or R (read)
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub enum Operation {
    W,
    R,
}

/// An Sql raw string
///
/// This string can be an invalid Sql statement.
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlRawString(pub String);

impl SqlRawString {
    /// Conversion to a `TxTable` if valid
    /// 
    /// Only the following Sql begin transaction syntax is valid:
    /// 
    /// `BEGIN {TRAN | TRANSACTION} [transaction_name] WITH MARK 'READ table_0 table_1 WRITE table_2' [;]`
    #[allow(dead_code)]
    pub fn to_tx_table(&self) -> Option<TxTable> {
        return None;
    }
}

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

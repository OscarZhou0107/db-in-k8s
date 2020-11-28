use crate::msql::Operation;
use serde::{Deserialize, Serialize};

/// Version number
pub type VN = u64;

/// Version number of a table
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct TableVN {
    pub table: String,
    pub vn: VN,
    pub op: Operation,
}

/// Version numbers of tables declared by a transaction
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxVN {
    pub tx_name: Option<String>,
    /// A single vec storing all W and R `TableVN` for now
    pub table_vns: Vec<TableVN>,
}

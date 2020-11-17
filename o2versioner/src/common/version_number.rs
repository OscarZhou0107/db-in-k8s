use super::sql::Operation;

/// Version number
pub type VN = u64;

/// Version number of a table
#[derive(Debug, Eq, PartialEq)]
pub struct TableVN {
    pub table: String,
    pub vn: VN,
    pub op: Operation,
}

/// Version numbers of tables declared by a transaction
#[derive(Default, Debug, Eq, PartialEq)]
pub struct TxVN {
    // A single vec storing all W and R `TableVN` for now
    pub table_vns: Vec<TableVN>,
}

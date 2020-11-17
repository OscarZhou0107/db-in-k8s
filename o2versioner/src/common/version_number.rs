/// Version number
pub type VN = u64;

/// Enum representing either W (write) or R (read)
pub enum Operation {
    W,
    R,
}

/// Version number of a table
pub struct TableVN {
    pub table: String,
    pub vn: VN,
    pub op: Operation,
}

/// Version numbers of tables declared by a transaction
#[derive(Default)]
pub struct TxVN {
    // A single vec storing all W and R `TableVN` for now
    pub table_vns: Vec<TableVN>,
}

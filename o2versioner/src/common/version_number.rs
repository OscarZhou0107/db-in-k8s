/// Version number
pub type VN = u64;

/// Enum representing either W (write) or R (read)
#[allow(dead_code)]
pub enum Operation {
    W,
    R,
}

/// Version number of a table
#[allow(dead_code)]
pub struct TableVN {
    table: String,
    vn: VN,
    op: Operation,
}

/// Version numbers of tables declared by a transaction
#[allow(dead_code)]
#[derive(Default)]
pub struct TxVN {
    // A single vec storing all W and R `TableVN` for now
    table_vns: Vec<TableVN>,
}

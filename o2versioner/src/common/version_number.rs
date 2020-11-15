/// Enum representing either the W (write) version number or the R (read) version number.
#[allow(dead_code)]
pub enum VN {
    W(u32),
    R(u32),
}

/// Version number of a table.
#[allow(dead_code)]
pub struct TableVN {
    table: String,
    vn: VN,
}

/// Version numbers of tables declared by a transaction.
#[allow(dead_code)]
pub struct TxVN {
    // A single vec storing all W and R `TableVN` for now
    table_vns: Vec<TableVN>,
}

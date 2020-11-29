use crate::core::msql::{Operation, TableOp};
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

impl TableVN {
    pub fn new<S: Into<String>>(table: S, vn: VN, op: Operation) -> Self {
        Self {
            table: table.into(),
            vn,
            op,
        }
    }

    /// Check whether the current `TableVN` matches with the argument `TableOp`
    ///
    /// If `TableOp` is of `Operation::R`, then only need to match the name with `TableVN`;
    /// If `TableOp` is of `Operation::W`, then need to match both the name and also the operation (ie., `Operation::W`) with `TableVN`
    pub fn match_with(&self, tableop: &TableOp) -> bool {
        match tableop.op {
            Operation::R => self.table == tableop.table,
            Operation::W => self.table == tableop.table && self.op == tableop.op,
        }
    }
}

/// Version numbers of tables declared by a transaction
///
/// TODO: For table being early-released, remove them from `TxVN`
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxVN {
    pub tx: Option<String>,
    /// A single vec storing all W and R `TableVN` for now
    pub tablevns: Vec<TableVN>,
}

impl Default for TxVN {
    fn default() -> Self {
        Self {
            tx: None,
            tablevns: Vec::new(),
        }
    }
}

impl TxVN {
    /// Find the `TableVN` that matches with the argument `TableOp` from the `TxVN`
    ///
    /// If `TableOp` is of `Operation::R`, then only need to match the name with `TableVN`;
    /// If `TableOp` is of `Operation::W`, then need to match both the name and also the operation (ie., `Operation::W`) with `TableVN`
    pub fn find(&self, tableop: &TableOp) -> Option<&TableVN> {
        self.tablevns.iter().find(|tablevn| tablevn.match_with(tableop))
    }
}

#[cfg(test)]
mod tests_txvn {
    use super::*;

    #[test]
    fn test_find() {
        let txvn = TxVN {
            tx: None,
            tablevns: vec![TableVN::new("t0", 0, Operation::R), TableVN::new("t1", 2, Operation::W)],
        };

        assert_eq!(
            txvn.find(&TableOp::new("t0", Operation::R)),
            Some(&TableVN::new("t0", 0, Operation::R))
        );
        assert_eq!(txvn.find(&TableOp::new("t0", Operation::W)), None);
        assert_eq!(
            txvn.find(&TableOp::new("t1", Operation::R)),
            Some(&TableVN::new("t1", 2, Operation::W))
        );
        assert_eq!(
            txvn.find(&TableOp::new("t1", Operation::W)),
            Some(&TableVN::new("t1", 2, Operation::W))
        );
    }
}

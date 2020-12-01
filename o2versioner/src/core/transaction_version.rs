use crate::core::operation::*;
use serde::{Deserialize, Serialize};

/// Version number
pub type VN = u64;

/// Version number of a table of the transaction
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxTableVN {
    pub table: String,
    pub vn: VN,
    pub op: Operation,
}

impl TxTableVN {
    pub fn new<S: Into<String>>(table: S, vn: VN, op: Operation) -> Self {
        Self {
            table: table.into(),
            vn,
            op,
        }
    }

    /// Check whether the current `TxTableVN` matches with the argument `TableOp`
    ///
    /// If `TableOp` is of `Operation::R`, then only need to match the name with `TxTableVN`;
    /// If `TableOp` is of `Operation::W`, then need to match both the name and also the operation (ie., `Operation::W`) with `TxTableVN`
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxVN {
    pub tx: Option<String>,
    /// A single vec storing all W and R `TxTableVN` for now
    pub txtablevns: Vec<TxTableVN>,
}

impl Default for TxVN {
    fn default() -> Self {
        Self {
            tx: None,
            txtablevns: Vec::new(),
        }
    }
}

impl TxVN {
    /// Find the `TxTableVN` that matches with the argument `TableOp` from the `TxVN`
    ///
    /// If `TableOp` is of `Operation::R`, then only need to match the name with `TxTableVN`;
    /// If `TableOp` is of `Operation::W`, then need to match both the name and also the operation (ie., `Operation::W`) with `TxTableVN`
    pub fn get_from_tableop(&self, tableop: &TableOp) -> Option<TxTableVN> {
        self.txtablevns
            .iter()
            .find(|txtablevn| txtablevn.match_with(tableop))
            .cloned()
    }

    /// Find a Vec<&TxTableVN> of the argument `TableOps` from the `TxVN` if they *ALL* match
    pub fn get_from_tableops(&self, tableops: &TableOps) -> Result<Vec<TxTableVN>, &'static str> {
        let res: Vec<_> = tableops
            .get()
            .iter()
            .filter_map(|tableop| self.get_from_tableop(tableop))
            .collect();
        if res.len() != tableops.get().len() {
            Err("Some tables in the TableOps are not assigned a VN in TxVN")
        } else {
            Ok(res)
        }
    }
}

#[cfg(test)]
mod tests_txvn {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_get_tableop() {
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W),
            ],
        };

        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t0", Operation::R)),
            Some(TxTableVN::new("t0", 0, Operation::R))
        );
        assert_eq!(txvn.get_from_tableop(&TableOp::new("t0", Operation::W)), None);
        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t1", Operation::R)),
            Some(TxTableVN::new("t1", 2, Operation::W))
        );
        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t1", Operation::W)),
            Some(TxTableVN::new("t1", 2, Operation::W))
        );
    }

    #[test]
    fn test_get_tableops() {
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W),
            ],
        };

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", Operation::R),
                TableOp::new("t1", Operation::R)
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W)
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t1", Operation::R),
                TableOp::new("t0", Operation::R),
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W),
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", Operation::R),
                TableOp::new("t1", Operation::W)
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W)
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t1", Operation::W),
                TableOp::new("t0", Operation::R),
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, Operation::R),
                TxTableVN::new("t1", 2, Operation::W),
            ])
        );

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", Operation::W),
                TableOp::new("t1", Operation::R)
            ]))
            .is_err());

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", Operation::W),
                TableOp::new("t1", Operation::W)
            ]))
            .is_err());

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t0", Operation::R),])),
            Ok(vec![TxTableVN::new("t0", 0, Operation::R),])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t1", Operation::R)])),
            Ok(vec![TxTableVN::new("t1", 2, Operation::W)])
        );

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t2", Operation::R)]))
            .is_err());

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t2", Operation::R),
                TableOp::new("t0", Operation::R)
            ]))
            .is_err());
    }
}

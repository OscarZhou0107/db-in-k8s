use crate::core::operation::*;
use serde::{Deserialize, Serialize};

/// Version number
pub type VN = u64;

/// Version number of a table of the transaction
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxTableVN {
    pub table: String,
    pub vn: VN,
    pub op: RWOperation,
}

impl TxTableVN {
    pub fn new<S: Into<String>>(table: S, vn: VN, op: RWOperation) -> Self {
        Self {
            table: table.into(),
            vn,
            op,
        }
    }

    /// Check whether the current `TxTableVN` matches with the argument `TableOp`
    ///
    /// If `TableOp` is of `RWOperation::R`, then only need to match the name with `TxTableVN`;
    /// If `TableOp` is of `RWOperation::W`, then need to match both the name and also the operation (ie., `RWOperation::W`) with `TxTableVN`
    pub fn match_with(&self, tableop: &TableOp) -> bool {
        match tableop.op() {
            RWOperation::R => self.table == tableop.table(),
            RWOperation::W => self.table == tableop.table() && self.op == tableop.op(),
        }
    }
}

/// Version numbers of tables declared by a transaction
///
/// TODO: For table being early-released, pop them from `TxVN`
/// into `DbVNReleaseRequest`.
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
    /// If `TableOp` is of `RWOperation::R`, then only need to match the name with `TxTableVN`;
    /// If `TableOp` is of `RWOperation::W`, then need to match both the name and also the operation (ie., `RWOperation::W`) with `TxTableVN`
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

    /// Translate `TxVN` into multiple `DbVNReleaseRequest`
    pub fn into_dbvn_release_request(self) -> DbVNReleaseRequest {
        DbVNReleaseRequest(
            self.txtablevns
                .iter()
                .cloned()
                .map(|txtablevn| txtablevn.table)
                .collect(),
        )
    }
}

/// A single-use request for releasing version on `DbVN`
///
/// Do not derive `Clone`, acquire it through `TxVN::into_dbvn_release_request`
#[derive(Debug, Eq, PartialEq)]
pub struct DbVNReleaseRequest(Vec<String>);

impl IntoIterator for DbVNReleaseRequest {
    type Item = String;
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl DbVNReleaseRequest {
    pub fn into_inner(self) -> Vec<String> {
        self.0
    }

    pub fn inner(&self) -> &[String] {
        &self.0
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
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ],
        };

        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t0", RWOperation::R)),
            Some(TxTableVN::new("t0", 0, RWOperation::R))
        );
        assert_eq!(txvn.get_from_tableop(&TableOp::new("t0", RWOperation::W)), None);
        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t1", RWOperation::R)),
            Some(TxTableVN::new("t1", 2, RWOperation::W))
        );
        assert_eq!(
            txvn.get_from_tableop(&TableOp::new("t1", RWOperation::W)),
            Some(TxTableVN::new("t1", 2, RWOperation::W))
        );
    }

    #[test]
    fn test_get_tableops() {
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ],
        };

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::R)
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W)
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t1", RWOperation::R),
                TableOp::new("t0", RWOperation::R),
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::W)
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W)
            ])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t1", RWOperation::W),
                TableOp::new("t0", RWOperation::R),
            ])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ])
        );

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::W),
                TableOp::new("t1", RWOperation::R)
            ]))
            .is_err());

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::W),
                TableOp::new("t1", RWOperation::W)
            ]))
            .is_err());

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R),])),
            Ok(vec![TxTableVN::new("t0", 0, RWOperation::R),])
        );

        assert_eq!(
            txvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t1", RWOperation::R)])),
            Ok(vec![TxTableVN::new("t1", 2, RWOperation::W)])
        );

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t2", RWOperation::R)]))
            .is_err());

        assert!(txvn
            .get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t2", RWOperation::R),
                TableOp::new("t0", RWOperation::R)
            ]))
            .is_err());
    }

    #[test]
    fn test_into_dbvn_release_request() {
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ],
        };

        assert_eq!(
            txvn.into_dbvn_release_request(),
            DbVNReleaseRequest(vec![String::from("t0"), String::from("t1")]),
        );
    }
}

use crate::core::operation::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TxVN {
    tx: Option<String>,
    /// A single vec storing all W and R `TxTableVN` for now
    txtablevns: Vec<TxTableVN>,
    uuid: Uuid,
}

impl TxVN {
    /// Create a new `TxVN` with a unique id assigned to the transaction
    pub fn new() -> Self {
        Self {
            tx: None,
            txtablevns: Vec::new(),
            uuid: Uuid::new_v4(),
        }
    }

    /// Set `uuid` to nil, for testing, so that `TxVN` can be properly
    /// compared for equality
    pub fn erase_uuid(mut self) -> Self {
        self.uuid = Uuid::nil();
        self
    }

    /// Builder style setting transaction name
    pub fn set_tx<S: Into<String>>(mut self, tx: Option<S>) -> Self {
        self.tx = tx.map(|tx| tx.into());
        self
    }

    /// Builder style setting the `txtablevns`
    pub fn set_txtablevns<I: IntoIterator<Item = TxTableVN>>(mut self, txtablevns: I) -> Self {
        self.txtablevns = txtablevns.into_iter().collect();
        self
    }

    /// A `ref` to transaction name `tx`
    pub fn tx(&self) -> &Option<String> {
        &self.tx
    }

    /// A `ref` to the `txtablevns`
    pub fn txtablevns(&self) -> &[TxTableVN] {
        &self.txtablevns
    }

    /// A `ref` to the `uuid`
    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

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

    /// Find a Vec<&TxTableVN> of the argument `EarlyReleaseTables` from the `TxVN` if they *ALL* match
    pub fn get_from_ertables(&self, ertables: &EarlyReleaseTables) -> Result<Vec<TxTableVN>, &'static str> {
        let res: Vec<_> = ertables
            .get()
            .iter()
            .filter_map(|ertable| self.txtablevns.iter().find(|txtablevn| txtablevn.table == *ertable))
            .cloned()
            .collect();
        if res.len() != ertables.get().len() {
            Err("Some tables in the EarlyReleaseTables are not assigned a VN in TxVN")
        } else {
            Ok(res)
        }
    }

    /// Translate `TxVN` into `DbVNReleaseRequest`
    pub fn into_dbvn_release_request(self) -> DbVNReleaseRequest {
        DbVNReleaseRequest(
            self.txtablevns
                .iter()
                .cloned()
                .map(|txtablevn| txtablevn.table)
                .collect(),
        )
    }

    /// Remove tables from `EarlyReleaseTables` from `TxVN`, and translate those tables into `DbVNReleaseRequest`
    /// If any table from `EarlyReleaseTables` is not found in `TxVN`, `Err(reason: &'static str)` is returned
    pub fn early_release_request(&mut self, ertables: EarlyReleaseTables) -> Result<DbVNReleaseRequest, &'static str> {
        self.get_from_ertables(&ertables)?;
        self.txtablevns = self
            .txtablevns
            .iter()
            .filter(|txtablevn| !ertables.get().contains(&txtablevn.table))
            .cloned()
            .collect();
        Ok(DbVNReleaseRequest(ertables.into_vec()))
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
    fn test_get_from_tableop() {
        let txvn = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 2, RWOperation::W),
        ]);

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
    fn test_get_from_tableops() {
        let txvn = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 2, RWOperation::W),
        ]);

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
            Ok(vec![TxTableVN::new("t0", 0, RWOperation::R)])
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
    fn test_get_from_ertables() {
        let txvn = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 2, RWOperation::W),
        ]);

        assert_eq!(
            txvn.get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t0", "t1"])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W)
            ])
        );

        assert_eq!(
            txvn.get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t1", "t0"])),
            Ok(vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::W),
            ])
        );

        assert_eq!(
            txvn.get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t0"])),
            Ok(vec![TxTableVN::new("t0", 0, RWOperation::R)])
        );

        assert_eq!(
            txvn.get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t1"])),
            Ok(vec![TxTableVN::new("t1", 2, RWOperation::W)])
        );

        assert!(txvn
            .get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t3"]))
            .is_err());

        assert!(txvn
            .get_from_ertables(&EarlyReleaseTables::from_iter(vec!["t2", "t0"]))
            .is_err());
    }

    #[test]
    fn test_into_dbvn_release_request() {
        let txvn = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 2, RWOperation::W),
        ]);

        assert_eq!(
            txvn.into_dbvn_release_request(),
            DbVNReleaseRequest(vec![String::from("t0"), String::from("t1")]),
        );
    }

    #[test]
    fn test_early_release_request() {
        let mut txvn = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 2, RWOperation::W),
            TxTableVN::new("t2", 5, RWOperation::R),
        ]);

        assert!(txvn.early_release_request(EarlyReleaseTables::from("t0 t3")).is_err());

        assert_eq!(
            txvn.early_release_request(EarlyReleaseTables::from("t0 t2")),
            Ok(DbVNReleaseRequest(vec![String::from("t0"), String::from("t2")]))
        );
        assert_eq!(txvn.txtablevns, vec![TxTableVN::new("t1", 2, RWOperation::W)]);
    }
}

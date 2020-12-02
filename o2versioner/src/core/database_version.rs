use super::operation::*;
use super::transaction_version::*;
use std::collections::HashMap;

/// Version number of a table of on a single database instance
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DbTableVN {
    pub table: String,
    pub vn: VN,
}

impl DbTableVN {
    pub fn new<S: Into<String>>(table: S, vn: VN) -> Self {
        Self {
            table: table.into(),
            vn,
        }
    }
}

/// Version number of all tables on a single database instance
pub struct DbVN(HashMap<String, VN>);

impl Default for DbVN {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl DbVN {
    pub fn get_from_tableop(&self, tableop: &TableOp) -> DbTableVN {
        DbTableVN::new(&tableop.table, self.0.get(&tableop.table).cloned().unwrap_or_default())
    }

    pub fn get_from_tableops(&self, tableops: &TableOps) -> Vec<DbTableVN> {
        tableops
            .get()
            .iter()
            .map(|tableop| self.get_from_tableop(tableop))
            .collect()
    }

    fn can_execute_query_on_table(&self, txtablevn: &TxTableVN) -> bool {
        match &txtablevn.op {
            RWOperation::R => self.0.get(&txtablevn.table).cloned().unwrap_or_default() >= txtablevn.vn,
            RWOperation::W => self.0.get(&txtablevn.table).cloned().unwrap_or_default() == txtablevn.vn,
        }
    }

    /// Check whether the query with the `TableOps` and belongs to transaction with `TxVN`
    /// is allowed to execute
    ///
    /// # Important:
    /// 1. You must check the original `TableOps` that is used to yield `txtablevns`
    /// for any potential mixing of R and W.
    /// 2. Use `TxVN::get_from_tableops(&self, tableops: &TableOps)` to get a list of `TxTableVN`
    /// for checking
    ///
    /// # Examples:
    /// ```
    /// use o2versioner::core::*;
    /// use std::iter::FromIterator;
    ///
    /// let dbvn = DbVN::default();
    ///
    /// let txvn = TxVN {
    ///     tx: None,
    ///     txtablevns: vec![
    ///         TxTableVN::new("t0", 0, RWOperation::W),
    ///         TxTableVN::new("t1", 0, RWOperation::W),
    ///         TxTableVN::new("t2", 6, RWOperation::R),
    ///     ],
    /// };
    ///
    /// let can_execute_list = txvn
    ///     .get_from_tableops(&TableOps::from_iter(vec![
    ///         TableOp::new("t0", RWOperation::R),
    ///         TableOp::new("t1", RWOperation::R),
    ///     ]))
    ///     .unwrap();
    ///
    /// assert!(dbvn.can_execute_query(&can_execute_list));
    /// ```
    ///
    /// # Notes:
    /// 1. A write query is executed only when the version numbers for each table at the
    /// database match the version numbers in the query.
    /// 2. A read query is executed only when the version numbers for each table at the
    /// database are greater than or equal to the version numbers in the query.
    /// 3. As `TxVN` removes duplicated tables by merging R into W operations,
    /// a Read-only query can sometimes be associated with a W VN since the table may be
    /// updated in other queries in the same transaction, and this is totally allowed and supported.
    /// 3a. Firstly, the table with both W and R within a transaction will be assigned a W VN.
    /// 3b. Secondly, if a W VN is assigned, all W and R queries will be adhered to the W VN. On
    /// the other hand, if a R VN is assigned, when queries with that table performs a W will raise an error,
    /// this is not allow!
    /// 3c. W VN is must more strict than R VN, so database version will never > the assigned VN,
    /// until the transaction owning that VN is ended. In such case, although the read-only
    /// query is still executed based on the read-only VN rule, tables using W VN are still implicitly
    /// blocked until the database version == the assigned VN.
    pub fn can_execute_query(&self, txtablevns: &[TxTableVN]) -> bool {
        txtablevns
            .iter()
            .all(|txtablevn| self.can_execute_query_on_table(txtablevn))
    }

    /// Increment all database versions by 1 for tables listed in `DbVNReleaseRequest`
    ///
    /// # Examples
    /// You can acquire `DbVNReleaseRequest` by:
    /// ```
    /// use o2versioner::core::*;
    /// let mut dbvn = DbVN::default();
    /// let txvn = TxVN {
    ///     tx: None,
    ///     txtablevns: vec![
    ///         TxTableVN::new("t0", 5, RWOperation::W),
    ///         TxTableVN::new("t1", 6, RWOperation::W),
    ///         TxTableVN::new("t2", 6, RWOperation::R),
    ///     ],
    /// };
    /// dbvn.release_version(txvn.into_dbvn_release_request());
    /// ```
    pub fn release_version(&mut self, release_request: DbVNReleaseRequest) {
        release_request.into_iter().for_each(|table| {
            *self.0.entry(table).or_default() += 1;
        });
    }
}

#[cfg(test)]
mod tests_dbvn {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_get_from_tableop() {
        let dbvn = DbVN(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );

        assert_eq!(
            dbvn.get_from_tableop(&TableOp::new("t0", RWOperation::R)),
            DbTableVN::new("t0", 5)
        );
        assert_eq!(
            dbvn.get_from_tableop(&TableOp::new("t1", RWOperation::R)),
            DbTableVN::new("t1", 6)
        );
        assert_eq!(
            dbvn.get_from_tableop(&TableOp::new("t2", RWOperation::R)),
            DbTableVN::new("t2", 0)
        );
    }

    #[test]
    fn test_get_from_tableops() {
        let dbvn = DbVN(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );

        assert_eq!(
            dbvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R)])),
            vec![DbTableVN::new("t0", 5)]
        );
        assert_eq!(
            dbvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t1", RWOperation::R)])),
            vec![DbTableVN::new("t1", 6)]
        );
        assert_eq!(
            dbvn.get_from_tableops(&TableOps::from_iter(vec![TableOp::new("t2", RWOperation::R)])),
            vec![DbTableVN::new("t2", 0)]
        );

        assert_eq!(
            dbvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::R)
            ])),
            vec![DbTableVN::new("t0", 5), DbTableVN::new("t1", 6)]
        );

        assert_eq!(
            dbvn.get_from_tableops(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::R),
                TableOp::new("t2", RWOperation::R)
            ])),
            vec![
                DbTableVN::new("t0", 5),
                DbTableVN::new("t1", 6),
                DbTableVN::new("t2", 0)
            ]
        );
    }

    #[test]
    fn test_can_execute_query_on_table() {
        let dbvn = DbVN::default();
        assert!(!dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 5, RWOperation::W)));
        assert!(!dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 5, RWOperation::R)));
        assert!(dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 0, RWOperation::R)));
        assert!(dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 0, RWOperation::W)));

        let dbvn = DbVN(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );
        assert!(dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 5, RWOperation::W)));
        assert!(dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 5, RWOperation::R)));
        assert!(dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 0, RWOperation::R)));
        assert!(!dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 6, RWOperation::W)));
        assert!(!dbvn.can_execute_query_on_table(&TxTableVN::new("t0", 6, RWOperation::R)));
    }

    #[test]
    fn test_can_execute_query() {
        let tableops0 = TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R), TableOp::new("t1", RWOperation::R)]);
        let tableops1 = TableOps::from_iter(vec![TableOp::new("t0", RWOperation::W), TableOp::new("t1", RWOperation::W)]);
        let tableops2 = TableOps::from_iter(vec![TableOp::new("t0", RWOperation::W)]);
        let tableops3 = TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R)]);

        let txvn0 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, RWOperation::W),
                TxTableVN::new("t1", 5, RWOperation::R),
            ],
        };
        let txvn1 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 6, RWOperation::W),
                TxTableVN::new("t1", 7, RWOperation::W),
            ],
        };
        let txvn2 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, RWOperation::W),
                TxTableVN::new("t1", 6, RWOperation::W),
            ],
        };
        let txvn3 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 3, RWOperation::R),
                TxTableVN::new("t1", 2, RWOperation::R),
            ],
        };

        let dbvn = DbVN::default();
        assert!(!dbvn.can_execute_query(&txvn0.get_from_tableops(&tableops0).unwrap()));

        let dbvn = DbVN(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );
        assert!(dbvn.can_execute_query(&txvn0.get_from_tableops(&tableops0).unwrap()));
        assert!(!dbvn.can_execute_query(&txvn1.get_from_tableops(&tableops0).unwrap()));
        assert!(dbvn.can_execute_query(&txvn2.get_from_tableops(&tableops0).unwrap()));

        assert!(!dbvn.can_execute_query(&txvn1.get_from_tableops(&tableops1).unwrap()));
        assert!(dbvn.can_execute_query(&txvn2.get_from_tableops(&tableops1,).unwrap()));

        assert!(dbvn.can_execute_query(&txvn0.get_from_tableops(&tableops2,).unwrap()));
        assert!(!dbvn.can_execute_query(&txvn1.get_from_tableops(&tableops2,).unwrap()));
        assert!(dbvn.can_execute_query(&txvn2.get_from_tableops(&tableops2,).unwrap()));

        assert!(dbvn.can_execute_query(&txvn3.get_from_tableops(&tableops3,).unwrap()));
    }

    #[test]
    fn test_release_version() {
        let mut dbvn = DbVN(
            [("t0", 5), ("t1", 6), ("t2", 7), ("t3", 8)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );

        let txvn0 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, RWOperation::W),
                TxTableVN::new("t1", 6, RWOperation::W),
                TxTableVN::new("t2", 6, RWOperation::R),
            ],
        };

        let txvn1 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 6, RWOperation::W),
                TxTableVN::new("t1", 7, RWOperation::R),
            ],
        };

        let tableops = TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R), TableOp::new("t1", RWOperation::R)]);

        assert!(dbvn.can_execute_query(&txvn0.get_from_tableops(&tableops,).unwrap()));
        assert!(!dbvn.can_execute_query(&txvn1.get_from_tableops(&tableops,).unwrap()));
        dbvn.release_version(txvn0.into_dbvn_release_request());
        assert!(dbvn.can_execute_query(&txvn1.get_from_tableops(&tableops,).unwrap()));
    }
}

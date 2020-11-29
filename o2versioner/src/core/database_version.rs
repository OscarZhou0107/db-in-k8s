use super::msql::*;
use super::transaction_version::*;
use std::collections::HashMap;

/// Version number of a table of on a single database instance
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct DbTableVN {
    pub table: String,
    pub vn: VN,
}

/// Version number of all tables on a single database instance
pub struct DbVN(HashMap<String, VN>);

impl Default for DbVN {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl DbVN {
    /// Check whether the query with the `TableOps` and belongs to transaction with `TxVN`
    /// is allowed to execute
    ///
    /// Notes:
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
    pub fn can_execute_query(&self, tableops: &TableOps, txvn: &TxVN) -> bool {
        let rule = match tableops.access_pattern() {
            AccessPattern::WriteOnly => |db_vn, assigned_vn| db_vn == assigned_vn,
            AccessPattern::ReadOnly => |db_vn, assigned_vn| db_vn >= assigned_vn,
            AccessPattern::Mixed => panic!("Queries involving both read and write are not supported!"),
        };

        tableops.get().iter().all(|tableop| {
            let txtablevn = txvn
                .find_tableop(tableop)
                .expect(&format!("TableOp {:?} does not match with TxVN {:?}", tableop, txvn));
            rule(self.0.get(&tableop.table).cloned().unwrap_or_default(), txtablevn.vn)
        })
    }

    /// Increment all database versions by 1 for tables listed in `TxVN`
    pub fn release_version(&mut self, txvn: &TxVN) {
        txvn.txtablevns.iter().for_each(|txtablevn| {
            *self.0.entry(txtablevn.table.to_owned()).or_default() += 1;
        });
    }
}

#[cfg(test)]
mod tests_dbvn {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_can_execute_query() {
        let tableops0 = TableOps::from_iter(vec![TableOp::new("t0", Operation::R), TableOp::new("t1", Operation::R)]);
        let tableops1 = TableOps::from_iter(vec![TableOp::new("t0", Operation::W), TableOp::new("t1", Operation::W)]);
        let tableops2 = TableOps::from_iter(vec![TableOp::new("t0", Operation::W)]);
        let tableops3 = TableOps::from_iter(vec![TableOp::new("t0", Operation::R)]);

        let txvn0 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, Operation::W),
                TxTableVN::new("t1", 5, Operation::R),
            ],
        };
        let txvn1 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 6, Operation::W),
                TxTableVN::new("t1", 7, Operation::W),
            ],
        };
        let txvn2 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, Operation::W),
                TxTableVN::new("t1", 6, Operation::W),
            ],
        };
        let txvn3 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 3, Operation::R),
                TxTableVN::new("t1", 2, Operation::R),
            ],
        };

        let vndb = DbVN::default();
        assert!(!vndb.can_execute_query(&tableops0, &txvn0));

        let vndb = DbVN(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );
        assert!(vndb.can_execute_query(&tableops0, &txvn0));
        assert!(!vndb.can_execute_query(&tableops0, &txvn1));
        assert!(vndb.can_execute_query(&tableops0, &txvn2));

        assert!(!vndb.can_execute_query(&tableops1, &txvn1));
        assert!(vndb.can_execute_query(&tableops1, &txvn2));

        assert!(vndb.can_execute_query(&tableops2, &txvn0));
        assert!(!vndb.can_execute_query(&tableops2, &txvn1));
        assert!(vndb.can_execute_query(&tableops2, &txvn2));

        assert!(vndb.can_execute_query(&tableops3, &txvn3));
    }

    #[test]
    #[should_panic]
    fn test_can_execute_queyr_panic_0() {
        let vndb = DbVN::default();
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, Operation::W),
                TxTableVN::new("t1", 5, Operation::R),
            ],
        };
        let tableops = TableOps::from_iter(vec![TableOp::new("t0", Operation::W), TableOp::new("t1", Operation::R)]);
        vndb.can_execute_query(&tableops, &txvn);
    }

    #[test]
    #[should_panic]
    fn test_can_execute_query_panic_1() {
        let vndb = DbVN::default();
        let txvn = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, Operation::W),
                TxTableVN::new("t1", 5, Operation::R),
            ],
        };

        let tableops = TableOps::from_iter(vec![TableOp::new("t1", Operation::W)]);
        vndb.can_execute_query(&tableops, &txvn);
    }

    #[test]
    fn test_release_version() {
        let mut vndb = DbVN(
            [("t0", 5), ("t1", 6), ("t2", 7), ("t3", 8)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );

        let txvn0 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 5, Operation::W),
                TxTableVN::new("t1", 6, Operation::W),
                TxTableVN::new("t2", 6, Operation::R),
            ],
        };

        let txvn1 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 6, Operation::W),
                TxTableVN::new("t1", 7, Operation::R),
            ],
        };

        let tableops = TableOps::from_iter(vec![TableOp::new("t0", Operation::R), TableOp::new("t1", Operation::R)]);

        assert!(vndb.can_execute_query(&tableops, &txvn0));
        assert!(!vndb.can_execute_query(&tableops, &txvn1));
        vndb.release_version(&txvn0);
        assert!(vndb.can_execute_query(&tableops, &txvn1));
    }
}

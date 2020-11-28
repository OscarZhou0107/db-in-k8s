use super::msql::*;
use super::version_number::*;
use std::collections::HashMap;

/// Version number database for a single Sql database instance
pub struct VNDatabase(HashMap<String, VN>);

impl Default for VNDatabase {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl VNDatabase {
    pub fn can_execute(&self, tableops: &TableOps, txvn: &TxVN) -> bool {
        // A write query is executed only when the version numbers for each table at the
        // database match the version numbers in the query.
        // A read query is executed only when the version numbers for each table at the
        // database are greater than or equal to the version numbers in the query.
        let rule = match tableops.access_pattern() {
            AccessPattern::WriteOnly => |db_vn, assigned_vn| db_vn == assigned_vn,
            AccessPattern::ReadOnly => |db_vn, assigned_vn| db_vn >= assigned_vn,
            AccessPattern::Mixed => panic!("Queries involving both read and write are not supported!"),
        };

        tableops.get().iter().all(|tableop| {
            let tablevn = txvn
                .find(tableop)
                .expect(&format!("TableOp {:?} does not match with TxVN {:?}", tableop, txvn));
            rule(self.0.get(&tableop.table).cloned().unwrap_or_default(), tablevn.vn)
        })
    }
}

#[cfg(test)]
mod tests_vndatabase {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_can_execute() {
        let tableops0 = TableOps::from_iter(vec![TableOp::new("t0", Operation::R), TableOp::new("t1", Operation::R)]);
        let tableops1 = TableOps::from_iter(vec![TableOp::new("t0", Operation::W), TableOp::new("t1", Operation::W)]);
        let tableops2 = TableOps::from_iter(vec![TableOp::new("t0", Operation::W)]);
        let tableops3 = TableOps::from_iter(vec![TableOp::new("t0", Operation::R)]);

        let txvn0 = TxVN {
            tx: None,
            tablevns: vec![TableVN::new("t0", 5, Operation::W), TableVN::new("t1", 5, Operation::R)],
        };
        let txvn1 = TxVN {
            tx: None,
            tablevns: vec![TableVN::new("t0", 6, Operation::W), TableVN::new("t1", 7, Operation::W)],
        };
        let txvn2 = TxVN {
            tx: None,
            tablevns: vec![TableVN::new("t0", 5, Operation::W), TableVN::new("t1", 6, Operation::W)],
        };
        let txvn3 = TxVN {
            tx: None,
            tablevns: vec![TableVN::new("t0", 3, Operation::R), TableVN::new("t1", 2, Operation::R)],
        };

        let vndb = VNDatabase::default();
        assert!(!vndb.can_execute(&tableops0, &txvn0));

        let vndb = VNDatabase(
            [("t0", 5), ("t1", 6)]
                .iter()
                .cloned()
                .map(|(s, vn)| (s.to_owned(), vn as VN))
                .collect(),
        );
        assert!(vndb.can_execute(&tableops0, &txvn0));
        assert!(!vndb.can_execute(&tableops0, &txvn1));
        assert!(vndb.can_execute(&tableops0, &txvn2));

        assert!(!vndb.can_execute(&tableops1, &txvn1));
        assert!(vndb.can_execute(&tableops1, &txvn2));

        assert!(vndb.can_execute(&tableops2, &txvn0));
        assert!(!vndb.can_execute(&tableops2, &txvn1));
        assert!(vndb.can_execute(&tableops2, &txvn2));

        assert!(vndb.can_execute(&tableops3, &txvn3));
    }
}

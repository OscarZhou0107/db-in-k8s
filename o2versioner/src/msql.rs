use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::iter::FromIterator;

/// Enum representing either a W (write) or R (read) for a table
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operation {
    W,
    R,
}

/// Enum representing the end transaction mode, can be either `Abort` or `Commit`
pub enum EndMode {
    Abort,
    Commit,
}

/// Representing the access mode for `self.table`, can be either `Operation::R` or `Operation::W`
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

impl TableOp {
    pub fn new<S: Into<String>>(table: S, op: Operation) -> Self {
        Self {
            table: table.into(),
            op,
        }
    }
}

/// Representing a collection of TableOp
///
/// Is automatically sorted in ascending order by `TableOp::table` and by `TableOp::op`
/// (`Operation`s with same `String` are ordered such that `Operation::W` comes before `Operation::R`)
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableOps(Vec<TableOp>);

impl Default for TableOps {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl TableOps {
    /// Get a ref to the internal storage
    pub fn get(&self) -> &[TableOp] {
        &self.0[..]
    }

    /// Convert to a `Vec<TableOp>`
    pub fn into_vec(self) -> Vec<TableOp> {
        self.0
    }

    /// Append a `TableOp`, will validate all entires again
    pub fn add_tableop(mut self, tableop: TableOp) -> Self {
        self.0.push(tableop);
        Self::from_iter(self)
    }
}

impl IntoIterator for TableOps {
    type Item = TableOp;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl FromIterator<TableOp> for TableOps {
    fn from_iter<I: IntoIterator<Item = TableOp>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .sorted_by(|left, right| {
                    if left.table != right.table {
                        Ord::cmp(&left.table, &right.table)
                    } else {
                        if left.op == right.op {
                            Ordering::Equal
                        } else {
                            if left.op == Operation::W {
                                Ordering::Less
                            } else {
                                Ordering::Greater
                            }
                        }
                    }
                })
                .dedup_by(|left, right| left.table == right.table)
                .collect(),
        )
    }
}

/// Begin a Msql transaction
pub struct MsqlBeginTx {
    tx: Option<String>,
    table_ops: TableOps,
}

impl Default for MsqlBeginTx {
    fn default() -> Self {
        Self {
            tx: None,
            table_ops: TableOps::default(),
        }
    }
}

impl MsqlBeginTx {
    /// Set an optional name for the transacstion, will overwrite previous value
    pub fn set_name<S: Into<String>>(mut self, name: Option<S>) -> Self {
        self.tx = name.map(|s| s.into());
        self
    }

    /// Set the `TableOps` for the transaction, will overwrite previous value
    pub fn set_table_ops(mut self, table_ops: TableOps) -> Self {
        self.table_ops = table_ops;
        self
    }

    /// Get a ref to the optional transaction name
    pub fn name(&self) -> Option<&str> {
        self.tx.as_ref().map(|s| &s[..])
    }

    /// Get a ref to the `TableOps` of the transaction
    pub fn table_ops(&self) -> &TableOps {
        &self.table_ops
    }

    /// Unwrap into (name: Option<String>, table_ops: TableOps)
    pub fn unwrap(self) -> (Option<String>, TableOps) {
        (self.tx, self.table_ops)
    }
}

/// A Msql query statement
pub struct MsqlQuery {
    query: String,
    table_ops: TableOps,
}

impl MsqlQuery {
    /// Create a new query, `table_ops` must correctly annotate the `query`
    pub fn new<S: Into<String>>(query: S, table_ops: TableOps) -> Self {
        Self {
            query: query.into(),
            table_ops,
        }
    }

    /// Get a ref to the query
    pub fn query(&self) -> &str {
        &self.query[..]
    }

    /// Get a ref to the `TableOps` of the query
    pub fn table_ops(&self) -> &TableOps {
        &self.table_ops
    }

    /// Unwrap into (query: String, table_ops: TableOps)
    pub fn unwrap(self) -> (String, TableOps) {
        (self.query, self.table_ops)
    }
}

/// End a Msql transaction
pub struct MsqlEndTx(pub EndMode);

/// Unit test for `TableOps`
#[cfg(test)]
mod tests_tableops {
    use super::*;

    #[test]
    fn test_from_iter() {
        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_0", Operation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_r_0".to_owned(), Operation::R),
                TableOp::new("table_r_1".to_owned(), Operation::R),
                TableOp::new("table_w_0".to_owned(), Operation::W),
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R)
            ])
            .into_iter()
            .collect::<Vec<_>>(),
            vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R)
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_w_0", Operation::W),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_1", Operation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_0", Operation::W),
                TableOp::new("table_w_1", Operation::W),
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::R)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", Operation::R)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::W),
                TableOp::new("table_0", Operation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", Operation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", Operation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", Operation::W,)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_0", Operation::W),
                TableOp::new("table_0", Operation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", Operation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_1", Operation::R),
                TableOp::new("table_0", Operation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_0", Operation::W,),
                TableOp::new("table_1", Operation::R)
            ]
        );
    }

    #[test]
    fn test_add_tableop() {
        assert_eq!(
            TableOps::default()
                .add_tableop(TableOp::new("table_0", Operation::R))
                .add_tableop(TableOp::new("table_0", Operation::R))
                .add_tableop(TableOp::new("table_1", Operation::R))
                .add_tableop(TableOp::new("table_0", Operation::W))
                .into_vec(),
            vec![
                TableOp::new("table_0".to_owned(), Operation::W),
                TableOp::new("table_1".to_owned(), Operation::R)
            ]
        );
    }
}

// /// Unit test for `SqlBeginTx`
// #[cfg(test)]
// mod tests_sqlbegintx {
//     use super::*;
//     use std::convert::TryInto;

//     #[test]
//     fn test_try_from_sqlstring_helper() {
//         let tests = vec![
//             (
//                 SqlString::from("BEGIN TRAN trans1 WITH MARK 'READ table_0 table_1 WRITE table_2';"),
//                 Some(("trans1", "READ table_0 table_1 WRITE table_2")),
//             ),
//             (
//                 SqlString::from("BEGIN TRAN WITH MARK 'READ table_0 table_1 WRITE table_2';"),
//                 Some(("", "READ table_0 table_1 WRITE table_2")),
//             ),
//             (SqlString::from("BEGIN TRAN WITH MARK '';"), Some(("", ""))),
//             (SqlString::from("BEGIN TRAN WITH MARK ''"), Some(("", ""))),
//             (
//                 SqlString::from("BEGIN TRANSACTION trans1 WITH MARK 'WRITE table_2 READ table_0 table_1';"),
//                 Some(("trans1", "WRITE table_2 READ table_0 table_1")),
//             ),
//             (
//                 SqlString::from("BEGIN TRANSACTION WITH MARK 'WRITE table_2 READ table_0 table_1';"),
//                 Some(("", "WRITE table_2 READ table_0 table_1")),
//             ),
//             (SqlString::from("BEGIN TRANSACTION WITH MARK '';"), Some(("", ""))),
//             (SqlString::from("BEGIN TRANSACTION WITH MARK ''"), Some(("", ""))),
//             (
//                 SqlString::from("     BEGIN  TRANSACTION   WITH MARK '   READ   table_0'   ;  "),
//                 Some(("", "   READ   table_0")),
//             ),
//             (SqlString::from("SELECT * FROM table_0;"), None),
//             (SqlString::from("BGIN TRANSACTION WITH MARK ''"), None),
//             (SqlString::from("BEGIN TRENSACTION WITH MARK ''"), None),
//             (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
//             (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
//             (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK'';"), None),
//             (
//                 SqlString::from("BEGIN TRANSACTION trans0 WITH MARK 'read table_2 write    table_0';"),
//                 Some(("trans0", "read table_2 write    table_0")),
//             ),
//         ];

//         tests.into_iter().for_each(|(sqlstring, res)| {
//             assert_eq!(
//                 SqlBeginTx::try_from_sqlstring_helper(&sqlstring),
//                 res.map(|res_ref| (res_ref.0, res_ref.1)).ok_or(())
//             )
//         });
//     }

//     #[test]
//     fn test_try_from_sqlstring() {
//         assert_eq!(
//             SqlString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';").try_into(),
//             Ok(SqlBeginTx {
//                 tx_name: String::from("tx0"),
//                 table_ops: vec![
//                     TableOp {
//                         table: String::from("table1"),
//                         op: Operation::R
//                     },
//                     TableOp {
//                         table: String::from("table2"),
//                         op: Operation::W
//                     },
//                     TableOp {
//                         table: String::from("table3"),
//                         op: Operation::W
//                     },
//                 ]
//             })
//         );

//         assert_eq!(
//             SqlString::from("BeGin TraN with MarK 'table0 read table1 read write table2 table3 read';").try_into(),
//             Ok(SqlBeginTx {
//                 tx_name: String::from(""),
//                 table_ops: vec![
//                     TableOp {
//                         table: String::from("table1"),
//                         op: Operation::R
//                     },
//                     TableOp {
//                         table: String::from("table2"),
//                         op: Operation::W
//                     },
//                     TableOp {
//                         table: String::from("table3"),
//                         op: Operation::W
//                     },
//                 ]
//             })
//         );

//         assert_eq!(
//             SqlString::from("BeGin TraNsaction with MarK 'table0 read table1 read write table2 table3 read'")
//                 .try_into(),
//             Ok(SqlBeginTx {
//                 tx_name: String::from(""),
//                 table_ops: vec![
//                     TableOp {
//                         table: String::from("table1"),
//                         op: Operation::R
//                     },
//                     TableOp {
//                         table: String::from("table2"),
//                         op: Operation::W
//                     },
//                     TableOp {
//                         table: String::from("table3"),
//                         op: Operation::W
//                     },
//                 ]
//             })
//         );

//         assert_eq!(
//             SqlString::from("BeGin TraNsaction with MarK ''").try_into(),
//             Ok(SqlBeginTx {
//                 tx_name: String::from(""),
//                 table_ops: vec![]
//             })
//         );

//         assert_eq!(
//             SqlBeginTx::try_from(SqlString::from(
//                 "BeGin TraNssaction with MarK 'read table1 table2 table3'"
//             )),
//             Err(())
//         );

//         assert_eq!(
//             SqlBeginTx::try_from(SqlString::from("BeGin TraNsaction with MarK")),
//             Err(())
//         );

//         assert_eq!(SqlBeginTx::try_from(SqlString::from("select * from table0;")), Err(()));

//         assert_eq!(SqlBeginTx::try_from(SqlString::from("begin")), Err(()));

//         assert_eq!(SqlBeginTx::try_from(SqlString::from("")), Err(()));
//     }

//     #[test]
//     fn test_process_tx_name() {
//         assert_eq!(SqlBeginTx::process_tx_name("  tX_name  "), *"tX_name");

//         assert_eq!(SqlBeginTx::process_tx_name("  tx_nAMe 1 2 3  "), *"tx_nAMe123");

//         assert_eq!(SqlBeginTx::process_tx_name("   "), *"");

//         assert_ne!(SqlBeginTx::process_tx_name("tXX_name"), *"txx_name");
//         assert_eq!(SqlBeginTx::process_tx_name("tXX_name"), *"tXX_name");
//     }

//     #[test]
//     fn test_process_table_ops() {
//         assert_eq!(
//             SqlBeginTx::process_table_ops("read table_R_0 table_r_1 write table_w_0"),
//             vec![
//                 TableOp {
//                     table: String::from("table_R_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_r_1"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_w_0"),
//                     op: Operation::W,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops(" read     table_r_0   table_r_1    write  table_w_0 "),
//             vec![
//                 TableOp {
//                     table: String::from("table_r_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_r_1"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_w_0"),
//                     op: Operation::W,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("WrItE Table_w_0 read table_r_0 table_r_1"),
//             vec![
//                 TableOp {
//                     table: String::from("Table_w_0"),
//                     op: Operation::W,
//                 },
//                 TableOp {
//                     table: String::from("table_r_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_r_1"),
//                     op: Operation::R,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("write table_w_0writE read table_r_0read writetable_r_1reAd"),
//             vec![
//                 TableOp {
//                     table: String::from("table_r_0read"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_w_0writE"),
//                     op: Operation::W,
//                 },
//                 TableOp {
//                     table: String::from("writetable_r_1reAd"),
//                     op: Operation::R,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("write read Table_r_0 Table_r_1"),
//             vec![
//                 TableOp {
//                     table: String::from("Table_r_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("Table_r_1"),
//                     op: Operation::R,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("read table_r_0 Table_r_1 write"),
//             vec![
//                 TableOp {
//                     table: String::from("Table_r_1"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_r_0"),
//                     op: Operation::R,
//                 },
//             ]
//         );

//         assert_eq!(SqlBeginTx::process_table_ops("ReaD WriTE"), vec![]);

//         assert_eq!(SqlBeginTx::process_table_ops(""), vec![]);

//         assert_eq!(
//             SqlBeginTx::process_table_ops("ReAd table_R_0 WrItE table_W_0 rEad table_R_1 "),
//             vec![
//                 TableOp {
//                     table: String::from("table_R_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_R_1"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_W_0"),
//                     op: Operation::W,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("read Table_r_0 WRITE REAd"),
//             vec![TableOp {
//                 table: String::from("Table_r_0"),
//                 op: Operation::R,
//             },]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("Read table_r_0 writereAD"),
//             vec![
//                 TableOp {
//                     table: String::from("table_r_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("writereAD"),
//                     op: Operation::R,
//                 },
//             ]
//         );

//         assert_eq!(
//             SqlBeginTx::process_table_ops("table0 table1 table2 read table_r_0 table_r_1 Write table_w_0"),
//             vec![
//                 TableOp {
//                     table: String::from("table_r_0"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_r_1"),
//                     op: Operation::R,
//                 },
//                 TableOp {
//                     table: String::from("table_w_0"),
//                     op: Operation::W,
//                 },
//             ]
//         );

//         assert_eq!(SqlBeginTx::process_table_ops("tAble0 taBle1 tabLe2"), vec![]);
//     }
// }

// /// Unit test for `SqlQuery`
// #[cfg(test)]
// mod tests_sqlquery {
//     #![allow(warnings)]
//     use super::*;
//     use std::convert::TryInto;

//     #[test]
//     fn test_try_from_sqlstring_helper() {}
// }

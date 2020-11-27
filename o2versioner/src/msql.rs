use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::iter::FromIterator;
use uuid::Uuid;

/// Enum representing either a W (write) or R (read) for a table
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operation {
    W,
    R,
}

/// Enum representing the access pattern of a `TableOps`
#[derive(Debug, Eq, PartialEq)]
pub enum AccessPattern {
    WriteOnly,
    ReadOnly,
    Mixed,
}

/// Enum representing the end transaction mode, can be either `Abort` or `Commit`
#[derive(Debug)]
pub enum EndMode {
    Abort,
    Commit,
}

/// Representing the access mode for `Self::table`, can be either `Operation::R` or `Operation::W`
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

    /// The access pattern of this `TableOps`
    pub fn access_pattern(&self) -> AccessPattern {
        if self.0.iter().all(|tableop| tableop.op == Operation::R) {
            AccessPattern::ReadOnly
        } else if self.0.iter().all(|tableop| tableop.op == Operation::W) {
            AccessPattern::WriteOnly
        } else {
            AccessPattern::Mixed
        }
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

/// Representing the final form of all `M: IntoMsqlEndString`
///
/// The process of converting into `MsqlEndString` is not revertible.
pub struct MsqlEndString(String);

impl<M> From<M> for MsqlEndString
where
    M: IntoMsqlEndString,
{
    fn from(m: M) -> Self {
        m.into_msqlendstring()
    }
}

/// Traits for all Msql pieces to convert into `MsqlEndString`
/// TODO
pub trait IntoMsqlEndString {
    fn into_msqlendstring(self) -> MsqlEndString;
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

    /// Append a uuid to the end of the transaction name
    pub fn add_uuid(mut self) -> Self {
        self.tx = self.tx.map(|mut tx| {
            tx.push_str("_");
            tx.push_str(&Uuid::new_v4().to_string());
            tx
        });
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
    pub fn new<S: Into<String>>(query: S, table_ops: TableOps) -> Result<Self, &'static str> {
        if let AccessPattern::Mixed = table_ops.access_pattern() {
            Err("Only read-only or write-only Msql query is supported, but not the mixed!")
        } else {
            Ok(Self {
                query: query.into(),
                table_ops,
            })
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
        let tableops = TableOps::default();

        let tableops = tableops.add_tableop(TableOp::new("table_0", Operation::R));
        assert_eq!(tableops.get(), vec![TableOp::new("table_0", Operation::R)].as_slice());

        let tableops = tableops.add_tableop(TableOp::new("table_0", Operation::R));
        assert_eq!(tableops.get(), vec![TableOp::new("table_0", Operation::R)].as_slice());

        let tableops = tableops.add_tableop(TableOp::new("table_1", Operation::R));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", Operation::R),
                TableOp::new("table_1", Operation::R)
            ]
            .as_slice()
        );

        let tableops = tableops.add_tableop(TableOp::new("table_0", Operation::W));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", Operation::W),
                TableOp::new("table_1", Operation::R)
            ]
            .as_slice()
        );

        let tableops = tableops.add_tableop(TableOp::new("table_1", Operation::W));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", Operation::W),
                TableOp::new("table_1", Operation::W)
            ]
            .as_slice()
        );
    }

    #[test]
    fn test_access_pattern() {
        assert_eq!(
            TableOps::from_iter(vec![TableOp::new("table_r_0", Operation::R),]).access_pattern(),
            AccessPattern::ReadOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![TableOp::new("table_w_0", Operation::W),]).access_pattern(),
            AccessPattern::WriteOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_0", Operation::W)
            ])
            .access_pattern(),
            AccessPattern::Mixed
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_r_2", Operation::R),
            ])
            .access_pattern(),
            AccessPattern::ReadOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_w_0", Operation::W),
                TableOp::new("table_w_1", Operation::W),
                TableOp::new("table_w_2", Operation::W),
            ])
            .access_pattern(),
            AccessPattern::WriteOnly
        );
    }
}

/// Unit test for `MsqlQuery`
#[cfg(test)]
mod tests_msqlquery {
    use super::*;

    #[test]
    fn test_new() {
        assert!(MsqlQuery::new(
            "Select * from table0;",
            TableOps::from_iter(vec![TableOp::new("table0", Operation::R)])
        )
        .is_ok());

        assert!(MsqlQuery::new(
            "Update table1 set name=\"ray\" where id = 20;",
            TableOps::from_iter(vec![TableOp::new("table1", Operation::W)])
        )
        .is_ok());

        assert!(MsqlQuery::new(
            "some_black_magic;",
            TableOps::from_iter(vec![
                TableOp::new("table0", Operation::W),
                TableOp::new("table1", Operation::R)
            ])
        )
        .is_err());
    }
}

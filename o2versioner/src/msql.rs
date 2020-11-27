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
pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

/// Representing a collection of TableOp
///
/// Is automatically sorted in ascending order by `TableOp::table` and by `TableOp::op`
/// (`Operation`s with same `String` are ordered such that `Operation::W` comes before `Operation::R`)
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
    pub fn set_name(mut self, name: Option<String>) -> Self {
        self.tx = name;
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
    pub fn new(query: String, table_ops: TableOps) -> Self {
        Self { query, table_ops }
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

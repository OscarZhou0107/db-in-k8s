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

/// Representing the final form of all `M: IntoMsqlFinalString`
///
/// The process of converting into `MsqlFinalString` is not reversible.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MsqlFinalString(String);

impl<M> From<M> for MsqlFinalString
where
    M: IntoMsqlFinalString,
{
    fn from(m: M) -> Self {
        m.into_msqlfinalstring()
    }
}

/// Traits for all Msql pieces to convert into `MsqlFinalString`
pub trait IntoMsqlFinalString {
    fn into_msqlfinalstring(self) -> MsqlFinalString;
}

/// Begin a Msql transaction
#[derive(Debug, Serialize, Deserialize)]
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

impl IntoMsqlFinalString for MsqlBeginTx {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        let mut sql = String::from("BEGIN TRAN");
        if let Some(txname) = self.unwrap().0 {
            sql.push_str(" ");
            sql.push_str(&txname);
        }
        sql.push_str(";");
        MsqlFinalString(sql)
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
#[derive(Debug, Serialize, Deserialize)]
pub struct MsqlQuery {
    query: String,
    table_ops: TableOps,
}

impl IntoMsqlFinalString for MsqlQuery {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        MsqlFinalString(self.unwrap().0)
    }
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

/// Enum representing the end transaction mode, can be either `Rollback` or `Commit`
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MsqlEndTxMode {
    Commit,
    Rollback,
}

/// End a Msql transaction
#[derive(Debug, Serialize, Deserialize)]
pub struct MsqlEndTx {
    tx: Option<String>,
    mode: MsqlEndTxMode,
}

impl IntoMsqlFinalString for MsqlEndTx {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        let mut sql = match self.mode() {
            MsqlEndTxMode::Rollback => String::from("ROLLBACK TRAN"),
            MsqlEndTxMode::Commit => String::from("COMMIT TRAN"),
        };

        if let Some(txname) = self.name() {
            sql.push_str(" ");
            sql.push_str(txname);
        }
        sql.push_str(";");
        MsqlFinalString(sql)
    }
}

impl MsqlEndTx {
    /// Returns a `MsqlEndTx` that represents commit
    pub fn commit() -> Self {
        Self {
            tx: None,
            mode: MsqlEndTxMode::Commit,
        }
    }

    /// Returns a `MsqlEndTx` that represents rollback
    pub fn rollback() -> Self {
        Self {
            tx: None,
            mode: MsqlEndTxMode::Rollback,
        }
    }

    /// Set an optional name for transacstion, will overwrite previous value
    pub fn set_name<S: Into<String>>(mut self, name: Option<S>) -> Self {
        self.tx = name.map(|s| s.into());
        self
    }

    /// Set the mode for ending the transaction, will overwrite previous value
    pub fn set_mode(mut self, mode: MsqlEndTxMode) -> Self {
        self.mode = mode;
        self
    }

    /// Get a ref to the optional transaction name
    pub fn name(&self) -> Option<&str> {
        self.tx.as_ref().map(|s| &s[..])
    }

    /// Get the ending transaction mode
    pub fn mode(&self) -> MsqlEndTxMode {
        self.mode
    }

    /// Unwrap into (name: Option<String>, mode: MsqlEndTxMode)
    pub fn unwrap(self) -> (Option<String>, MsqlEndTxMode) {
        (self.tx, self.mode)
    }
}

/// Represents a Msql command variant.
/// The main user interface for Msql.
///
/// `Msql` can be constructed directly, or converted from `MsqlText`
#[derive(Debug, Serialize, Deserialize)]
pub enum Msql {
    BeginTx(MsqlBeginTx),
    Query(MsqlQuery),
    EndTx(MsqlEndTx),
}

impl IntoMsqlFinalString for Msql {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        match self {
            Msql::BeginTx(msqlbegintx) => msqlbegintx.into_msqlfinalstring(),
            Msql::Query(msqlquery) => msqlquery.into_msqlfinalstring(),
            Msql::EndTx(msqlendtx) => msqlendtx.into_msqlfinalstring(),
        }
    }
}

/// Represents a text formatted Msql command variant.
/// The main user interface for Msql with maximum compatibility.
/// This is a text version of `Msql`.
///
/// `MsqlText` needs to be converted into `Msql` first
#[derive(Debug, Serialize, Deserialize)]
pub enum MsqlText {
    BeginTx {
        #[serde(default)]
        tx: Option<String>,
        table_ops: String,
    },
    Query {
        query: String,
        table_ops: String,
    },
    EndTx {
        #[serde(default)]
        tx: Option<String>,
        mode: MsqlEndTxMode,
    },
}

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

/// Unit test for `IntoMsqlFinalString`
#[cfg(test)]
mod tests_into_msqlfinalstring {
    use super::*;
    use std::convert::Into;

    #[test]
    fn test_from_msqlbegintx() {
        assert_eq!(
            MsqlFinalString::from(MsqlBeginTx::default().set_table_ops(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ]))),
            MsqlFinalString(String::from("BEGIN TRAN;"))
        );

        let mfs: MsqlFinalString = MsqlBeginTx::default()
            .set_table_ops(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ]))
            .set_name(Some("tx0"))
            .into();
        assert_eq!(mfs, MsqlFinalString(String::from("BEGIN TRAN tx0;")));
    }

    #[test]
    fn test_from_msqlquery() {
        assert_eq!(
            MsqlFinalString::from(
                MsqlQuery::new(
                    "select * from table0 where true;",
                    TableOps::from_iter(vec![TableOp::new("table0", Operation::R)])
                )
                .unwrap()
            ),
            MsqlFinalString(String::from("select * from table0 where true;"))
        );

        let mfs: MsqlFinalString = MsqlQuery::new(
            "update table1 set name=\"ray\" where id = 20;",
            TableOps::from_iter(vec![TableOp::new("table1", Operation::W)]),
        )
        .unwrap()
        .into();
        assert_eq!(
            mfs,
            MsqlFinalString(String::from("update table1 set name=\"ray\" where id = 20;"))
        );
    }

    #[test]
    fn test_from_msqlendtx() {
        assert_eq!(
            MsqlFinalString::from(MsqlEndTx::commit()),
            MsqlFinalString(String::from("COMMIT TRAN;"))
        );

        let mfs: MsqlFinalString = MsqlEndTx::rollback().set_name(Some("tx1")).into();
        assert_eq!(mfs, MsqlFinalString(String::from("ROLLBACK TRAN tx1;")));
    }

    #[test]
    fn test_from_msql() {
        assert_eq!(
            MsqlFinalString::from(Msql::BeginTx(MsqlBeginTx::default().set_table_ops(
                TableOps::from_iter(vec![
                    TableOp::new("table0", Operation::R),
                    TableOp::new("table0", Operation::W),
                ])
            ))),
            MsqlFinalString(String::from("BEGIN TRAN;"))
        );

        assert_eq!(
            MsqlFinalString::from(Msql::Query(
                MsqlQuery::new(
                    "select * from table0 where true;",
                    TableOps::from_iter(vec![TableOp::new("table0", Operation::R)])
                )
                .unwrap()
            )),
            MsqlFinalString(String::from("select * from table0 where true;"))
        );

        let mfs: MsqlFinalString = Msql::EndTx(MsqlEndTx::rollback().set_name(Some("tx1"))).into();
        assert_eq!(mfs, MsqlFinalString(String::from("ROLLBACK TRAN tx1;")));
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

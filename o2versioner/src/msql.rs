use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use std::iter::FromIterator;
use unicase::UniCase;

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
/// Automatically sorted in ascending order by `TableOp::table` and by `TableOp::op`
/// (`Operation`s with same `String` are ordered such that `Operation::W` comes before `Operation::R`)
///
/// # Examples
/// ```
/// use o2versioner::msql::{Operation, TableOp, TableOps};
/// use std::iter::FromIterator;
///
/// let tableops0 = TableOps::from_iter(vec![
///     TableOp::new("table0", Operation::R),
///     TableOp::new("table1", Operation::W),
/// ]);
///
/// let tableops1 = TableOps::from("read table0 write  table1");
///
/// let tableops2 = TableOps::default()
///     .add_tableop(TableOp::new("table1", Operation::W))
///     .add_tableop(TableOp::new("table0", Operation::R));
///
/// assert_eq!(tableops0, tableops1);
/// assert_eq!(tableops1, tableops2);
/// ```
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
    /// Convert the input `IntoIterator<Item = TableOp>` into `TableOps`
    ///
    /// Sort input `Iterator<Item = TableOp>` in ascending order by `TableOp::name` and by `TableOp::op`
    /// (`TableOp`s with same `TableOp::name` are ordered such that `Operation::W` comes before `Operation::R`)
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

impl<S> From<S> for TableOps
where
    S: Into<String>,
{
    /// Convert the input `Into<String>` into `TableOps`
    ///
    /// # Examples
    /// ```
    /// use o2versioner::msql::TableOps;
    /// let tableops = TableOps::from("read t0 write t1 read t2");
    /// ```
    ///
    /// # Note
    /// 1. Keywords, such as "read" and "write", are case insensitive.
    /// 2. It supports multiple occurance for read or write keyword
    /// 3. Any space-separated identifier are associated with the previous keyword if existed; else, will be discarded
    /// 4. If no identifier is followed after a keyword, the keyword will be ignored
    /// 5. See comment in `TableOps::from_iter`
    fn from(str_like: S) -> Self {
        let read_ci = UniCase::new("read");
        let write_ci = UniCase::new("write");
        Self::from_iter(
            str_like
                .into()
                .split_whitespace()
                .map(|token| UniCase::new(token))
                .skip_while(|token_ci| *token_ci != read_ci && *token_ci != write_ci)
                .scan(None, |is_read_st, token_ci| {
                    if token_ci == read_ci {
                        *is_read_st = Some(true);
                        return Some(None);
                    } else if token_ci == write_ci {
                        *is_read_st = Some(false);
                        return Some(None);
                    } else {
                        // is_read_st can't be None here because
                        // we should have met at least one key word
                        if is_read_st.unwrap() {
                            return Some(Some(TableOp::new(token_ci.into_inner(), Operation::R)));
                        } else {
                            return Some(Some(TableOp::new(token_ci.into_inner(), Operation::W)));
                        }
                    }
                })
                .filter_map(|token_op| token_op),
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
///
/// # Examples
/// ```
/// use o2versioner::msql::{MsqlBeginTx, TableOps};
///
/// MsqlBeginTx::default()
///     .set_name(Some("tx0"))
///     .set_tableops(TableOps::from("READ table0 WRITE table1 table2 read table3"));
/// ```
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MsqlBeginTx {
    tx: Option<String>,
    tableops: TableOps,
}

impl Default for MsqlBeginTx {
    fn default() -> Self {
        Self {
            tx: None,
            tableops: TableOps::default(),
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

impl From<TableOps> for MsqlBeginTx {
    fn from(tableops: TableOps) -> Self {
        Self::default().set_tableops(tableops)
    }
}

impl MsqlBeginTx {
    /// Set an optional name for the transacstion, will overwrite previous value
    pub fn set_name<S: Into<String>>(mut self, name: Option<S>) -> Self {
        self.tx = name.map(|s| s.into());
        self
    }

    /// Set the `TableOps` for the transaction, will overwrite previous value
    pub fn set_tableops(mut self, tableops: TableOps) -> Self {
        self.tableops = tableops;
        self
    }

    /// Get a ref to the optional transaction name
    pub fn name(&self) -> Option<&str> {
        self.tx.as_ref().map(|s| &s[..])
    }

    /// Get a ref to the `TableOps` of the transaction
    pub fn tableops(&self) -> &TableOps {
        &self.tableops
    }

    /// Unwrap into (name: Option<String>, tableops: TableOps)
    pub fn unwrap(self) -> (Option<String>, TableOps) {
        (self.tx, self.tableops)
    }
}

/// A Msql query statement
///
/// # Examples
/// ```
/// use o2versioner::msql::{MsqlQuery, TableOps};
///
/// MsqlQuery::new("SELECT * FROM table0, table1;", TableOps::from("READ table0 table1"))
///     .unwrap();
/// ```
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MsqlQuery {
    query: String,
    tableops: TableOps,
}

impl IntoMsqlFinalString for MsqlQuery {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        MsqlFinalString(self.unwrap().0)
    }
}

impl MsqlQuery {
    /// Create a new query, `tableops` must correctly annotate the `query`
    pub fn new<S: Into<String>>(query: S, tableops: TableOps) -> Result<Self, &'static str> {
        if let AccessPattern::Mixed = tableops.access_pattern() {
            Err("Only read-only or write-only Msql query is supported, but not the mixed!")
        } else {
            Ok(Self {
                query: query.into(),
                tableops,
            })
        }
    }

    /// Get a ref to the query
    pub fn query(&self) -> &str {
        &self.query[..]
    }

    /// Get a ref to the `TableOps` of the query
    pub fn tableops(&self) -> &TableOps {
        &self.tableops
    }

    /// Unwrap into (query: String, tableops: TableOps)
    pub fn unwrap(self) -> (String, TableOps) {
        (self.query, self.tableops)
    }
}

/// Enum representing the end transaction mode, can be either `Rollback` or `Commit`
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MsqlEndTxMode {
    Commit,
    Rollback,
}

/// End a Msql transaction
///
/// # Examples
/// ```
/// use o2versioner::msql::MsqlEndTx;
///
/// MsqlEndTx::commit();
/// MsqlEndTx::rollback().set_name(Some("tx1"));
/// ```
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl From<MsqlEndTxMode> for MsqlEndTx {
    fn from(mode: MsqlEndTxMode) -> Self {
        Self { tx: None, mode }
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
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl TryFrom<MsqlText> for Msql {
    type Error = &'static str;
    fn try_from(msqltext: MsqlText) -> Result<Self, Self::Error> {
        match msqltext {
            MsqlText::BeginTx { tx, tableops } => Ok(Msql::BeginTx(
                MsqlBeginTx::default()
                    .set_name(tx)
                    .set_tableops(TableOps::from(tableops)),
            )),
            MsqlText::Query { query, tableops } => {
                MsqlQuery::new(query, TableOps::from(tableops)).map(|mq| Msql::Query(mq))
            }
            MsqlText::EndTx { tx, mode } => Ok(Msql::EndTx(MsqlEndTx::from(mode).set_name(tx))),
        }
    }
}

/// Represents a text formatted Msql command variant.
/// The main user interface for Msql with maximum compatibility.
/// This is a text version of `Msql`.
///
/// `MsqlText` needs to be converted into `Msql` first.
///
/// # Examples - Json conversion
/// ```
/// use o2versioner::msql::{MsqlEndTxMode, MsqlText};
///
/// // "type":"query" suggests MsqlText::Query
/// let query_str = r#"
/// {
///     "type":"query",
///     "query":"select * from t;",
///     "tableops":"read t"
/// }"#;
/// let query: MsqlText = serde_json::from_str(query_str).unwrap();
/// assert_eq!(
///     query,
///     MsqlText::Query {
///         query: String::from("select * from t;"),
///         tableops: String::from("read t")
///     }
/// );
///
/// // "type":"begintx" suggests MsqlText::BeginTx
/// // Use null for Option<String>::None
/// let begintx_str = r#"
/// {
///     "type":"begintx",
///     "tx":null,
///     "tableops":"read table0 write table1 read table2"
/// }"#;
/// let begintx: MsqlText = serde_json::from_str(begintx_str).unwrap();
/// assert_eq!(
///     begintx,
///     MsqlText::BeginTx {
///         tx: None,
///         tableops: String::from("read table0 write table1 read table2")
///     }
/// );
///
/// // Can also skip the value for Option<String>::None
/// let begintx_str = r#"
/// {
///     "type":"begintx",
///     "tableops":"read table0 write table1 read table2"
/// }"#;
/// let begintx: MsqlText = serde_json::from_str(begintx_str).unwrap();
/// assert_eq!(
///     begintx,
///     MsqlText::BeginTx {
///         tx: None,
///         tableops: String::from("read table0 write table1 read table2")
///     }
/// );
///
/// // "type":"endtx" suggests MsqlText::EndTx
/// // Simply enter the value for Option<String>::Some(String)
/// // Use "commit" for MsqlEndTxMode::Commit
/// // Use "rollback" for MsqlEndTxMode::Rollback
/// let endtx_str = r#"
/// {
///     "type":"endtx",
///     "mode":"commit",
///     "tx":"tx2"
/// }"#;
/// let endtx: MsqlText = serde_json::from_str(endtx_str).unwrap();
/// assert_eq!(
///     endtx,
///     MsqlText::EndTx {
///         tx: Some(String::from("tx2")),
///         mode: MsqlEndTxMode::Commit
///     }
/// );
/// ```
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MsqlText {
    BeginTx {
        #[serde(default)]
        tx: Option<String>,
        tableops: String,
    },
    Query {
        query: String,
        tableops: String,
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
    fn test_from_string() {
        assert_eq!(
            TableOps::from("ReAd table_R_0 table_r_1 WRite table_w_0").into_vec(),
            vec![
                TableOp::new("table_R_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_0", Operation::W)
            ]
        );

        assert_eq!(
            TableOps::from(" read     tAble_r_0   tAble_r_1    WRITE  tAble_w_0 ").into_vec(),
            vec![
                TableOp::new("tAble_r_0", Operation::R),
                TableOp::new("tAble_r_1", Operation::R),
                TableOp::new("tAble_w_0", Operation::W)
            ]
        );

        assert_eq!(
            TableOps::from("WrItE Table_w_0 read table_r_0 table_r_1").into_vec(),
            vec![
                TableOp::new("Table_w_0", Operation::W),
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R)
            ]
        );

        assert_eq!(
            TableOps::from("write table_w_0writE read table_r_0read writetable_r_1reAd").into_vec(),
            vec![
                TableOp::new("table_r_0read", Operation::R),
                TableOp::new("table_w_0writE", Operation::W),
                TableOp::new("writetable_r_1reAd", Operation::R),
            ]
        );

        assert_eq!(
            TableOps::from("write read Table_r_0 Table_r_1").into_vec(),
            vec![
                TableOp::new("Table_r_0", Operation::R),
                TableOp::new("Table_r_1", Operation::R),
            ]
        );

        assert_eq!(
            TableOps::from("read table_r_0 Table_r_1 write").into_vec(),
            vec![
                TableOp::new("Table_r_1", Operation::R),
                TableOp::new("table_r_0", Operation::R)
            ]
        );

        assert_eq!(TableOps::from("ReaD WriTE").into_vec(), vec![]);

        assert_eq!(TableOps::from("").into_vec(), vec![]);

        assert_eq!(
            TableOps::from("ReAd table_R_0 WrItE table_W_0 rEad table_R_1 ").into_vec(),
            vec![
                TableOp::new("table_R_0", Operation::R),
                TableOp::new("table_R_1", Operation::R),
                TableOp::new("table_W_0", Operation::W)
            ]
        );

        assert_eq!(
            TableOps::from("read Table_r_0 WRITE REAd").into_vec(),
            vec![TableOp::new("Table_r_0", Operation::R)]
        );

        assert_eq!(
            TableOps::from("Read table_r_0 writereAD").into_vec(),
            vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("writereAD", Operation::R)
            ]
        );

        assert_eq!(
            TableOps::from("table0 table1 table2 read table_r_0 table_r_1 Write table_w_0").into_vec(),
            vec![
                TableOp::new("table_r_0", Operation::R),
                TableOp::new("table_r_1", Operation::R),
                TableOp::new("table_w_0", Operation::W)
            ]
        );

        assert_eq!(TableOps::from("tAble0 taBle1 tabLe2").into_vec(), vec![]);
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

    #[test]
    fn test_from_msqlbegintx() {
        assert_eq!(
            MsqlFinalString::from(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ]))),
            MsqlFinalString(String::from("BEGIN TRAN;"))
        );

        let mfs: MsqlFinalString = MsqlBeginTx::from(TableOps::from_iter(vec![
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
            MsqlFinalString::from(Msql::BeginTx(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ])))),
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

/// Unit test for `Msql`
#[cfg(test)]
mod tests_msql {
    use super::*;
    #[test]
    fn test_from_msqltext() {
        assert_eq!(
            Msql::try_from(MsqlText::BeginTx {
                tx: None,
                tableops: String::from("read table0 read table1 write table2 table3")
            }),
            Ok(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "read table0 read table1 write table2 table3"
            ))))
        );

        assert_eq!(
            Msql::try_from(MsqlText::Query {
                query: String::from("select * from table0;"),
                tableops: String::from("read table0")
            }),
            MsqlQuery::new(
                "select * from table0;",
                TableOps::from_iter(vec![TableOp::new("table0", Operation::R)])
            )
            .map(|q| Msql::Query(q))
        );

        assert_eq!(
            Msql::try_from(MsqlText::EndTx {
                tx: Some(String::from("t3")),
                mode: MsqlEndTxMode::Rollback,
            }),
            Ok(Msql::EndTx(MsqlEndTx::rollback().set_name(Some("t3"))))
        )
    }
}

#[cfg(test)]
mod tests_msqltext {
    use crate::msql::*;

    #[test]
    fn test_msqltext_endtx_json() {
        println!("SERIALIZE");

        let a = MsqlText::EndTx {
            tx: Some(String::from("tx0")),
            mode: MsqlEndTxMode::Commit,
        };
        println!("{}", serde_json::to_string(&a).unwrap());

        let a = MsqlText::EndTx {
            tx: None,
            mode: MsqlEndTxMode::Rollback,
        };
        println!("{}", serde_json::to_string(&a).unwrap());

        println!("DESERIALIZE");

        let a = r#"
        {
            "type":"endtx",
            "tx":"tx0",
            "mode":"commit"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);

        let a = r#"
        {
            "type":"endtx",
            "tx":null,
            "mode":"rollback"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);

        let a = r#"
        {
            "type":"endtx",
            "mode":"rollback"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);
    }
}

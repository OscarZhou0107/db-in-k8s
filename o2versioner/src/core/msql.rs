use super::operation::*;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Representing the final form of all `M: IntoMsqlFinalString`
///
/// The process of converting into `MsqlFinalString` is not reversible.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MsqlFinalString(String);

impl MsqlFinalString {
    pub fn new<S: Into<String>>(s: S) -> Self {
        Self(s.into())
    }

    pub fn inner(&self) -> &str {
        &self.0[..]
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

/// For conversion into `String`
impl From<MsqlFinalString> for String {
    fn from(m: MsqlFinalString) -> Self {
        m.into_inner()
    }
}

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
/// use o2versioner::core::msql::MsqlBeginTx;
/// use o2versioner::core::operation::TableOps;
///
/// MsqlBeginTx::default()
///     .set_name(Some("tx0"))
///     .set_tableops(TableOps::from("READ table0 WRITE table1 table2 read table3"));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
/// use o2versioner::core::msql::MsqlQuery;
/// use o2versioner::core::operation::TableOps;
///
/// MsqlQuery::new("SELECT * FROM table0, table1;", TableOps::from("READ table0 table1"))
///     .unwrap();
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
#[serde(rename_all = "snake_case")]
pub enum MsqlEndTxMode {
    Commit,
    Rollback,
}

/// End a Msql transaction
///
/// # Examples
/// ```
/// use o2versioner::core::msql::MsqlEndTx;
///
/// MsqlEndTx::commit();
/// MsqlEndTx::rollback().set_name(Some("tx1"));
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum Msql {
    BeginTx(MsqlBeginTx),
    Query(MsqlQuery),
    EndTx(MsqlEndTx),
}

impl Msql {
    pub fn is_begintx(&self) -> bool {
        match self {
            Self::BeginTx(_) => true,
            _ => false,
        }
    }

    pub fn is_query(&self) -> bool {
        match self {
            Self::Query(_) => true,
            _ => false,
        }
    }

    pub fn is_endtx(&self) -> bool {
        match self {
            Self::EndTx(_) => true,
            _ => false,
        }
    }
}

impl IntoMsqlFinalString for Msql {
    fn into_msqlfinalstring(self) -> MsqlFinalString {
        match self {
            Self::BeginTx(msqlbegintx) => msqlbegintx.into_msqlfinalstring(),
            Self::Query(msqlquery) => msqlquery.into_msqlfinalstring(),
            Self::EndTx(msqlendtx) => msqlendtx.into_msqlfinalstring(),
        }
    }
}

impl TryFrom<MsqlText> for Msql {
    type Error = &'static str;
    fn try_from(msqltext: MsqlText) -> Result<Self, Self::Error> {
        match msqltext {
            MsqlText::BeginTx { tx, tableops } => Ok(Self::BeginTx(
                MsqlBeginTx::default()
                    .set_name(tx)
                    .set_tableops(TableOps::from(tableops)),
            )),
            MsqlText::Query { query, tableops } => {
                MsqlQuery::new(query, TableOps::from(tableops)).map(|mq| Self::Query(mq))
            }
            MsqlText::EndTx { tx, mode } => Ok(Self::EndTx(MsqlEndTx::from(mode).set_name(tx))),
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
/// use o2versioner::core::msql::{MsqlEndTxMode, MsqlText};
///
/// // "op":"query" suggests MsqlText::Query
/// let query_str = r#"
/// {
///     "op":"query",
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
/// // "op":"begin_tx" suggests MsqlText::BeginTx
/// // Use null for Option<String>::None
/// let begintx_str = r#"
/// {
///     "op":"begin_tx",
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
///     "op":"begin_tx",
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
/// // "op":"end_tx" suggests MsqlText::EndTx
/// // Simply enter the value for Option<String>::Some(String)
/// // Use "commit" for MsqlEndTxMode::Commit
/// // Use "rollback" for MsqlEndTxMode::Rollback
/// let endtx_str = r#"
/// {
///     "op":"end_tx",
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
#[serde(tag = "op", rename_all = "snake_case")]
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

/// Unit test for `IntoMsqlFinalString`
#[cfg(test)]
mod tests_into_msqlfinalstring {
    use super::*;
    use std::iter::FromIterator;

    #[test]
    fn test_from_msqlbegintx() {
        assert_eq!(
            MsqlFinalString::from(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ]))),
            MsqlFinalString::new("BEGIN TRAN;")
        );

        let mfs: MsqlFinalString = MsqlBeginTx::from(TableOps::from_iter(vec![
            TableOp::new("table0", Operation::R),
            TableOp::new("table0", Operation::W),
        ]))
        .set_name(Some("tx0"))
        .into();
        assert_eq!(mfs, MsqlFinalString::new("BEGIN TRAN tx0;"));
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
            MsqlFinalString::new("select * from table0 where true;")
        );

        let mfs: MsqlFinalString = MsqlQuery::new(
            "update table1 set name=\"ray\" where id = 20;",
            TableOps::from_iter(vec![TableOp::new("table1", Operation::W)]),
        )
        .unwrap()
        .into();
        assert_eq!(
            mfs,
            MsqlFinalString::new("update table1 set name=\"ray\" where id = 20;")
        );
    }

    #[test]
    fn test_from_msqlendtx() {
        assert_eq!(
            MsqlFinalString::from(MsqlEndTx::commit()),
            MsqlFinalString::new("COMMIT TRAN;")
        );

        let mfs: MsqlFinalString = MsqlEndTx::rollback().set_name(Some("tx1")).into();
        assert_eq!(mfs, MsqlFinalString::new("ROLLBACK TRAN tx1;"));
    }

    #[test]
    fn test_from_msql() {
        assert_eq!(
            MsqlFinalString::from(Msql::BeginTx(MsqlBeginTx::from(TableOps::from_iter(vec![
                TableOp::new("table0", Operation::R),
                TableOp::new("table0", Operation::W),
            ])))),
            MsqlFinalString::new("BEGIN TRAN;")
        );

        assert_eq!(
            MsqlFinalString::from(Msql::Query(
                MsqlQuery::new(
                    "select * from table0 where true;",
                    TableOps::from_iter(vec![TableOp::new("table0", Operation::R)])
                )
                .unwrap()
            )),
            MsqlFinalString::new("select * from table0 where true;")
        );

        let mfs: MsqlFinalString = Msql::EndTx(MsqlEndTx::rollback().set_name(Some("tx1"))).into();
        assert_eq!(mfs, MsqlFinalString::new("ROLLBACK TRAN tx1;"));
    }
}

/// Unit test for `MsqlQuery`
#[cfg(test)]
mod tests_msqlquery {
    use super::*;
    use std::iter::FromIterator;

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
    use std::iter::FromIterator;

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

    #[test]
    fn test_is_begintx() {
        assert!(Msql::BeginTx(MsqlBeginTx::default()).is_begintx());
        assert!(!Msql::Query(MsqlQuery::new("selet * from ray;", TableOps::default()).unwrap()).is_begintx());
        assert!(!Msql::EndTx(MsqlEndTx::rollback()).is_begintx());
    }

    #[test]
    fn test_is_query() {
        assert!(!Msql::BeginTx(MsqlBeginTx::default()).is_query());
        assert!(Msql::Query(MsqlQuery::new("selet * from ray;", TableOps::default()).unwrap()).is_query());
        assert!(!Msql::EndTx(MsqlEndTx::rollback()).is_query());
    }

    #[test]
    fn test_is_endtx() {
        assert!(!Msql::BeginTx(MsqlBeginTx::default()).is_endtx());
        assert!(!Msql::Query(MsqlQuery::new("selet * from ray;", TableOps::default()).unwrap()).is_endtx());
        assert!(Msql::EndTx(MsqlEndTx::rollback()).is_endtx());
    }
}

#[cfg(test)]
mod tests_msqltext {
    use crate::core::msql::*;

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
            "op":"end_tx",
            "tx":"tx0",
            "mode":"commit"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);

        let a = r#"
        {
            "op":"end_tx",
            "tx":null,
            "mode":"rollback"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);

        let a = r#"
        {
            "op":"end_tx",
            "mode":"rollback"
        }"#;
        let b: MsqlText = serde_json::from_str(a).unwrap();
        println!("{:?}", b);
    }
}

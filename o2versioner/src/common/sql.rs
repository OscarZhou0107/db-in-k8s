use super::utility;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Enum representing either W (write) or R (read)
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, Copy, Clone)]
pub enum Operation {
    W,
    R,
}

/// An Sql raw string
///
/// This string can be an invalid Sql statement.
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlRawString(pub String);

impl SqlRawString {
    /// Convenience constructor to build `SqlRawString` from `&str`
    #[allow(dead_code)]
    fn from(raw_str: &str) -> SqlRawString {
        SqlRawString(raw_str.to_owned())
    }

    /// Parse the transaction name and mark if valid
    ///
    /// Return `Some((transaction_name, mark))` or None
    fn get_tx_data(&self) -> Option<(String, String)> {
        let sql_candidate = self.0.to_ascii_lowercase();
        let re =
            Regex::new(r"^\s*begin\s+(?:tran|transaction)\s+(\S*)\s*with\s+mark\s+'(.*)'\s*;?\s*$")
                .unwrap();

        re.captures(&sql_candidate).map(|caps| {
            (
                caps.get(1).unwrap().as_str().to_owned(),
                caps.get(2).unwrap().as_str().to_owned(),
            )
        })
    }

    /// Conversion to a `TxTable` if valid
    ///
    /// Only the following Sql begin transaction syntax is valid:
    ///
    /// `BEGIN {TRAN | TRANSACTION} [transaction_name] WITH MARK 'READ table_0 table_1 WRITE table_2' [;]`
    ///
    /// # Note
    /// 1. If `add_uuid == True`, will append uuid to the end of `TxTable::tx_name`
    /// 2. `add_uuid == False` should only be used for unit testing
    #[allow(dead_code)]
    pub fn to_tx_table(&self, add_uuid: bool) -> Option<TxTable> {
        self.get_tx_data()
            .map(|(tx_name, mark)| TxTable::from_string(tx_name, mark, add_uuid))
    }
}

pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

/// Keeps a list of all tables accessed for a Sql transaction
///
/// # Notes
/// 1. `table_ops` should have no duplications in terms of `TableOp::table`
/// 2. Such duplication should only keep the one that `TableOp::op == Operation::W`
///
#[derive(Default)]
pub struct TxTable {
    pub tx_name: String,
    pub table_ops: Vec<TableOp>,
}

impl TxTable {
    /// Process the argument `tx_name` for use as the `TxTable.tx_name` field
    ///
    /// # Note
    /// 1. If `add_uuid == True`, will append uuid to the end of the argument `tx_name`
    /// 2. `add_uuid == False` should only be used for unit testing
    /// 3. Will remove any whitespace from the argument `tx_name`
    fn process_tx_name(mut tx_name: String, add_uuid: bool) -> String {
        utility::remove_whitespace(&mut tx_name);
        if add_uuid {
            tx_name.push_str("_");
            tx_name.push_str(&Uuid::new_v4().to_string());
        }
        tx_name
    }

    fn process_table_ops(_mark: String) -> Vec<TableOp> {
        Vec::new()
    }

    fn from_string(tx_name: String, mark: String, add_uuid: bool) -> TxTable {
        TxTable {
            tx_name: TxTable::process_tx_name(tx_name, add_uuid),
            table_ops: TxTable::process_table_ops(mark),
        }
    }
}

/// Unit test for `SqlRawString`
#[cfg(test)]
mod tests_sql_raw_string {
    use super::SqlRawString;

    #[test]
    fn test_get_tx_data() {
        let tests = vec![
            (
                SqlRawString::from(
                    "BEGIN TRAN trans1 WITH MARK 'READ table_0 table_1 WRITE table_2';",
                ),
                Some(("trans1", "READ table_0 table_1 WRITE table_2")),
            ),
            (
                SqlRawString::from("BEGIN TRAN WITH MARK 'READ table_0 table_1 WRITE table_2';"),
                Some(("", "READ table_0 table_1 WRITE table_2")),
            ),
            (
                SqlRawString::from("BEGIN TRAN WITH MARK '';"),
                Some(("", "")),
            ),
            (
                SqlRawString::from("BEGIN TRAN WITH MARK ''"),
                Some(("", "")),
            ),
            (
                SqlRawString::from(
                    "BEGIN TRANSACTION trans1 WITH MARK 'WRITE table_2 READ table_0 table_1';",
                ),
                Some(("trans1", "WRITE table_2 READ table_0 table_1")),
            ),
            (
                SqlRawString::from(
                    "BEGIN TRANSACTION WITH MARK 'WRITE table_2 READ table_0 table_1';",
                ),
                Some(("", "WRITE table_2 READ table_0 table_1")),
            ),
            (
                SqlRawString::from("BEGIN TRANSACTION WITH MARK '';"),
                Some(("", "")),
            ),
            (
                SqlRawString::from("BEGIN TRANSACTION WITH MARK ''"),
                Some(("", "")),
            ),
            (
                SqlRawString::from("     BEGIN  TRANSACTION   WITH MARK '   READ   table_0'   ;  "),
                Some(("", "   READ   table_0")),
            ),
            (SqlRawString::from("SELECT * FROM table_0;"), None),
            (SqlRawString::from("BGIN TRANSACTION WITH MARK ''"), None),
            (SqlRawString::from("BEGIN TRENSACTION WITH MARK ''"), None),
            (
                SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK;"),
                None,
            ),
            (
                SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK;"),
                None,
            ),
            (
                SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK'';"),
                None,
            ),
            (
                SqlRawString::from(
                    "BEGIN TRANSACTION trans0 WITH MARK 'read table_2 write    table_0';",
                ),
                Some(("trans0", "read table_2 write    table_0")),
            ),
        ];

        tests.into_iter().for_each(|(sql_raw_string, res)| {
            assert_eq!(
                sql_raw_string.get_tx_data(),
                res.map(|res_ref| (
                    res_ref.0.to_ascii_lowercase(),
                    res_ref.1.to_ascii_lowercase()
                ))
            )
        });
    }
}

/// Unit test for `TxTable`
#[cfg(test)]
mod tests_tx_table {
    use super::TxTable;

    #[test]
    fn test_process_tx_name() {
        assert_eq!(
            TxTable::process_tx_name(String::from("  tx_name  "), false),
            String::from("tx_name")
        );

        assert_eq!(
            TxTable::process_tx_name(String::from("  tx_name 1 2 3  "), false),
            String::from("tx_name123")
        );

        assert_eq!(
            TxTable::process_tx_name(String::from("   "), false),
            String::from("")
        );

        assert_eq!(
            TxTable::process_tx_name(String::from("tx_name"), false),
            String::from("tx_name")
        );
    }

    #[test]
    fn test_process_table_ops() {}

    #[test]
    fn test_from_string() {}
}

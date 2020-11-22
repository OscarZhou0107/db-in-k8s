use crate::util::common;
use regex::Regex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Enum representing either W (write) or R (read)
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
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
    pub fn from(raw_str: &str) -> SqlRawString {
        SqlRawString(raw_str.to_owned())
    }

    /// Parse the transaction name and mark if valid
    ///
    /// Return `Some((transaction_name, mark))` or None
    fn get_tx_data(&self) -> Option<(String, String)> {
        let re = Regex::new(r"^\s*begin\s+(?:tran|transaction)\s+(\S*)\s*with\s+mark\s+'(.*)'\s*;?\s*$").unwrap();

        re.captures(&self.0.to_ascii_lowercase()).map(|caps| {
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
            .map(|(tx_name, mark)| TxTable::from_str(&tx_name, &mark, add_uuid))
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

/// Keeps a list of all tables accessed for a Sql transaction
///
/// # Notes
/// 1. `table_ops` should have no duplications in terms of `TableOp::table`
/// 2. Such duplication should only keep the one that `TableOp::op == Operation::W`
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
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
    fn process_tx_name(tx_name: &str, add_uuid: bool) -> String {
        let mut tx_name = tx_name.to_ascii_lowercase();
        common::remove_whitespace(&mut tx_name);
        if add_uuid {
            tx_name.push_str("_");
            tx_name.push_str(&Uuid::new_v4().to_string());
        }
        tx_name
    }

    /// Process the argument `mark` for use as the `TxTable.table_ops` field
    ///
    /// # Note
    /// 1. It supports multiple occurance for read or write keyword
    /// 2. Any space-separated identifier are associated with the previous keyword if existed; else, will be discarded
    /// 3. If no identifier is followed after a keyword, the keyword will be ignored
    fn process_table_ops(mark: &str) -> Vec<TableOp> {
        mark.to_ascii_lowercase()
            .split_whitespace()
            .skip_while(|token| *token != "read" && *token != "write")
            .scan(None, |is_read_st, token| {
                if token == "read" {
                    *is_read_st = Some(true);
                    return Some(None);
                } else if token == "write" {
                    *is_read_st = Some(false);
                    return Some(None);
                } else {
                    // is_read_st can't be None here because
                    // we should have met at least one key word
                    if is_read_st.unwrap() {
                        return Some(Some((token, Operation::R)));
                    } else {
                        return Some(Some((token, Operation::W)));
                    }
                }
            })
            .filter_map(|token_op| token_op)
            .map(|(token, op)| TableOp {
                table: token.to_owned(),
                op,
            })
            .collect()
    }

    /// Construct a `TxTable` with raw string arguments `tx_name` and `mark`
    /// which are then processed to form the `TxTable.tx_name` and `TxTable.table_ops` fields
    ///
    /// # Note
    /// Refer to `TxTable::process_tx_name` and `TxTable::process_table_ops` for processing details
    fn from_str(tx_name: &str, mark: &str, add_uuid: bool) -> TxTable {
        TxTable {
            tx_name: TxTable::process_tx_name(tx_name, add_uuid),
            table_ops: TxTable::process_table_ops(mark),
        }
    }
}

/// Unit test for `SqlRawString`
#[cfg(test)]
mod tests_sql_raw_string {
    use super::Operation;
    use super::SqlRawString;
    use super::TableOp;
    use super::TxTable;

    #[test]
    fn test_get_tx_data() {
        let tests = vec![
            (
                SqlRawString::from("BEGIN TRAN trans1 WITH MARK 'READ table_0 table_1 WRITE table_2';"),
                Some(("trans1", "READ table_0 table_1 WRITE table_2")),
            ),
            (
                SqlRawString::from("BEGIN TRAN WITH MARK 'READ table_0 table_1 WRITE table_2';"),
                Some(("", "READ table_0 table_1 WRITE table_2")),
            ),
            (SqlRawString::from("BEGIN TRAN WITH MARK '';"), Some(("", ""))),
            (SqlRawString::from("BEGIN TRAN WITH MARK ''"), Some(("", ""))),
            (
                SqlRawString::from("BEGIN TRANSACTION trans1 WITH MARK 'WRITE table_2 READ table_0 table_1';"),
                Some(("trans1", "WRITE table_2 READ table_0 table_1")),
            ),
            (
                SqlRawString::from("BEGIN TRANSACTION WITH MARK 'WRITE table_2 READ table_0 table_1';"),
                Some(("", "WRITE table_2 READ table_0 table_1")),
            ),
            (SqlRawString::from("BEGIN TRANSACTION WITH MARK '';"), Some(("", ""))),
            (SqlRawString::from("BEGIN TRANSACTION WITH MARK ''"), Some(("", ""))),
            (
                SqlRawString::from("     BEGIN  TRANSACTION   WITH MARK '   READ   table_0'   ;  "),
                Some(("", "   READ   table_0")),
            ),
            (SqlRawString::from("SELECT * FROM table_0;"), None),
            (SqlRawString::from("BGIN TRANSACTION WITH MARK ''"), None),
            (SqlRawString::from("BEGIN TRENSACTION WITH MARK ''"), None),
            (SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
            (SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
            (SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK'';"), None),
            (
                SqlRawString::from("BEGIN TRANSACTION trans0 WITH MARK 'read table_2 write    table_0';"),
                Some(("trans0", "read table_2 write    table_0")),
            ),
        ];

        tests.into_iter().for_each(|(sql_raw_string, res)| {
            assert_eq!(
                sql_raw_string.get_tx_data(),
                res.map(|res_ref| (res_ref.0.to_ascii_lowercase(), res_ref.1.to_ascii_lowercase()))
            )
        });
    }

    #[test]
    fn test_to_tx_table() {
        assert_eq!(
            SqlRawString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';")
                .to_tx_table(false),
            Some(TxTable {
                tx_name: String::from("tx0"),
                table_ops: vec![
                    TableOp {
                        table: String::from("table1"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("table2"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("table3"),
                        op: Operation::W
                    },
                ]
            })
        );

        assert_eq!(
            SqlRawString::from("BeGin TraN with MarK 'table0 read table1 read write table2 table3 read';")
                .to_tx_table(false),
            Some(TxTable {
                tx_name: String::from(""),
                table_ops: vec![
                    TableOp {
                        table: String::from("table1"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("table2"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("table3"),
                        op: Operation::W
                    },
                ]
            })
        );

        assert_eq!(
            SqlRawString::from("BeGin TraNsaction with MarK 'table0 read table1 read write table2 table3 read'")
                .to_tx_table(false),
            Some(TxTable {
                tx_name: String::from(""),
                table_ops: vec![
                    TableOp {
                        table: String::from("table1"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("table2"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("table3"),
                        op: Operation::W
                    },
                ]
            })
        );

        assert_eq!(
            SqlRawString::from("BeGin TraNsaction with MarK ''").to_tx_table(false),
            Some(TxTable {
                tx_name: String::from(""),
                table_ops: vec![]
            })
        );

        assert_eq!(
            SqlRawString::from("BeGin TraNssaction with MarK 'read table1 table2 table3'").to_tx_table(false),
            None
        );

        assert_eq!(
            SqlRawString::from("BeGin TraNsaction with MarK").to_tx_table(false),
            None
        );

        assert_eq!(SqlRawString::from("select * from table0;").to_tx_table(false), None);

        assert_eq!(SqlRawString::from("begin").to_tx_table(false), None);

        assert_eq!(SqlRawString::from("").to_tx_table(false), None);
    }
}

/// Unit test for `TxTable`
#[cfg(test)]
mod tests_tx_table {
    use super::Operation;
    use super::TableOp;
    use super::TxTable;

    #[test]
    fn test_process_tx_name() {
        assert_eq!(TxTable::process_tx_name("  tx_name  ", false), *"tx_name");

        assert_eq!(TxTable::process_tx_name("  tx_name 1 2 3  ", false), *"tx_name123");

        assert_eq!(TxTable::process_tx_name("   ", false), *"");

        assert_eq!(TxTable::process_tx_name("tx_name", false), *"tx_name");
    }

    #[test]
    fn test_process_table_ops() {
        assert_eq!(
            TxTable::process_table_ops("read table_r_0 table_r_1 write table_w_0"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops(" read     table_r_0   table_r_1    write  table_w_0 "),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("write table_w_0 read table_r_0 table_r_1"),
            vec![
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("write table_w_0write read table_r_0read writetable_r_1read"),
            vec![
                TableOp {
                    table: String::from("table_w_0write"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("table_r_0read"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("writetable_r_1read"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("write read table_r_0 table_r_1"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 table_r_1 write"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(TxTable::process_table_ops("read write"), vec![]);

        assert_eq!(TxTable::process_table_ops(""), vec![]);

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 read table_r_1 write table_w_0"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 write table_w_0 read table_r_1 "),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 write read table_r_1 "),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 write read"),
            vec![TableOp {
                table: String::from("table_r_0"),
                op: Operation::R,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 writeread"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("writeread"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_r_0 write table_w_0 read table_r_1 write table_w_1"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_1"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(
            TxTable::process_table_ops("table0 table1 table2 read table_r_0 table_r_1 write table_w_0"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(TxTable::process_table_ops("table0 table1 table2"), vec![]);
    }

    #[test]
    fn test_from_str() {
        assert_eq!(
            TxTable::from_str("tx0", "read tr0 tr1 write tw0 tw1", false),
            TxTable {
                tx_name: String::from("tx0"),
                table_ops: vec![
                    TableOp {
                        table: String::from("tr0"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("tr1"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("tw0"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("tw1"),
                        op: Operation::W
                    },
                ]
            }
        );

        assert_eq!(
            TxTable::from_str("tx1", "read tr0 write tw0 read tr1 write", false),
            TxTable {
                tx_name: String::from("tx1"),
                table_ops: vec![
                    TableOp {
                        table: String::from("tr0"),
                        op: Operation::R
                    },
                    TableOp {
                        table: String::from("tw0"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("tr1"),
                        op: Operation::R
                    },
                ]
            }
        );

        assert_eq!(
            TxTable::from_str("", "", false),
            TxTable {
                tx_name: String::from(""),
                table_ops: vec![]
            }
        );
    }
}

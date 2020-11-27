use crate::util::common;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use uuid::Uuid;

/// Enum representing either W (write) or R (read)
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operation {
    W,
    R,
}

/// Represents a raw Sql string
///
/// This string can be an invalid Sql statement.
#[derive(Debug, Serialize, Deserialize)]
pub struct SqlString(pub String);

impl<'a> From<&'a str> for SqlString {
    fn from(s: &'a str) -> Self {
        Self(s.to_owned())
    }
}

impl SqlString {
    /// Parse the transaction name and mark if valid
    ///
    /// Return `Option<SqlBeginTx>`
    fn get_sqlbegintx(&self) -> Option<SqlBeginTx> {
        let re = Regex::new(r"^\s*begin\s+(?:tran|transaction)\s+(\S*)\s*with\s+mark\s+'(.*)'\s*;?\s*$").unwrap();

        re.captures(&self.0.to_ascii_lowercase())
            .map(|caps| SqlBeginTx::new(caps.get(1).unwrap().as_str(), caps.get(2).unwrap().as_str()))
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
    pub fn to_txtable(&self, add_uuid: bool) -> Option<TxTable> {
        self.get_sqlbegintx()
            .map(|sqlbegintx| TxTable::new(&sqlbegintx, add_uuid))
    }
}

/// Represents a raw Sql begin transaction
#[derive(Debug, Eq, PartialEq)]
struct SqlBeginTx {
    tx_name: String,
    mark: String,
}

impl SqlBeginTx {
    fn new(tx_name: &str, mark: &str) -> Self {
        Self {
            tx_name: tx_name.to_owned(),
            mark: mark.to_owned(),
        }
    }
}

///

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
    /// Construct a `TxTable` with `SqlBeginTx`
    /// which is then processed to form the `TxTable.tx_name` and `TxTable.table_ops` fields
    ///
    /// # Note
    /// Refer to `TxTable::process_tx_name` and `TxTable::process_table_ops` for processing details
    fn new(sqlbegintx: &SqlBeginTx, add_uuid: bool) -> Self {
        Self {
            tx_name: Self::process_tx_name(&sqlbegintx.tx_name, add_uuid),
            table_ops: Self::process_table_ops(&sqlbegintx.mark),
        }
    }

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
    /// 4. `Vec<TableOp>` is sorted in ascending order by `TableOp.name` and by `TableOp.op`
    /// (`TableOp`s with same `TableOp.name` are ordered such that `Operation::W` comes before `Operation::R`)
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
            .sorted_by(|(left_token, left_op), (right_token, right_op)| {
                if left_token != right_token {
                    Ord::cmp(&left_token, &right_token)
                } else {
                    if left_op == right_op {
                        Ordering::Equal
                    } else {
                        if *left_op == Operation::W {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        }
                    }
                }
            })
            .dedup_by(|(left_token, _), (right_token, _)| left_token == right_token)
            .map(|(token, op)| TableOp {
                table: token.to_owned(),
                op,
            })
            .collect()
    }
}

/// Unit test for `SqlString`
#[cfg(test)]
mod tests_sqlstring {
    use super::Operation;
    use super::SqlBeginTx;
    use super::SqlString;
    use super::TableOp;
    use super::TxTable;

    #[test]
    fn test_get_sqlbegintx() {
        let tests = vec![
            (
                SqlString::from("BEGIN TRAN trans1 WITH MARK 'READ table_0 table_1 WRITE table_2';"),
                Some(("trans1", "READ table_0 table_1 WRITE table_2")),
            ),
            (
                SqlString::from("BEGIN TRAN WITH MARK 'READ table_0 table_1 WRITE table_2';"),
                Some(("", "READ table_0 table_1 WRITE table_2")),
            ),
            (SqlString::from("BEGIN TRAN WITH MARK '';"), Some(("", ""))),
            (SqlString::from("BEGIN TRAN WITH MARK ''"), Some(("", ""))),
            (
                SqlString::from("BEGIN TRANSACTION trans1 WITH MARK 'WRITE table_2 READ table_0 table_1';"),
                Some(("trans1", "WRITE table_2 READ table_0 table_1")),
            ),
            (
                SqlString::from("BEGIN TRANSACTION WITH MARK 'WRITE table_2 READ table_0 table_1';"),
                Some(("", "WRITE table_2 READ table_0 table_1")),
            ),
            (SqlString::from("BEGIN TRANSACTION WITH MARK '';"), Some(("", ""))),
            (SqlString::from("BEGIN TRANSACTION WITH MARK ''"), Some(("", ""))),
            (
                SqlString::from("     BEGIN  TRANSACTION   WITH MARK '   READ   table_0'   ;  "),
                Some(("", "   READ   table_0")),
            ),
            (SqlString::from("SELECT * FROM table_0;"), None),
            (SqlString::from("BGIN TRANSACTION WITH MARK ''"), None),
            (SqlString::from("BEGIN TRENSACTION WITH MARK ''"), None),
            (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
            (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK;"), None),
            (SqlString::from("BEGIN TRANSACTION trans0 WITH MARK'';"), None),
            (
                SqlString::from("BEGIN TRANSACTION trans0 WITH MARK 'read table_2 write    table_0';"),
                Some(("trans0", "read table_2 write    table_0")),
            ),
        ];

        tests.into_iter().for_each(|(sqlstring, res)| {
            assert_eq!(
                sqlstring.get_sqlbegintx(),
                res.map(|res_ref| SqlBeginTx::new(&res_ref.0.to_ascii_lowercase(), &res_ref.1.to_ascii_lowercase()))
            )
        });
    }

    #[test]
    fn test_to_txtable() {
        assert_eq!(
            SqlString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';")
                .to_txtable(false),
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
            SqlString::from("BeGin TraN with MarK 'table0 read table1 read write table2 table3 read';")
                .to_txtable(false),
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
            SqlString::from("BeGin TraNsaction with MarK 'table0 read table1 read write table2 table3 read'")
                .to_txtable(false),
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
            SqlString::from("BeGin TraNsaction with MarK ''").to_txtable(false),
            Some(TxTable {
                tx_name: String::from(""),
                table_ops: vec![]
            })
        );

        assert_eq!(
            SqlString::from("BeGin TraNssaction with MarK 'read table1 table2 table3'").to_txtable(false),
            None
        );

        assert_eq!(SqlString::from("BeGin TraNsaction with MarK").to_txtable(false), None);

        assert_eq!(SqlString::from("select * from table0;").to_txtable(false), None);

        assert_eq!(SqlString::from("begin").to_txtable(false), None);

        assert_eq!(SqlString::from("").to_txtable(false), None);
    }
}

/// Unit test for `TxTable`
#[cfg(test)]
mod tests_txtable {
    use super::Operation;
    use super::SqlBeginTx;
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
            TxTable::process_table_ops("write table_w_0write read table_r_0read writetable_r_1read"),
            vec![
                TableOp {
                    table: String::from("table_r_0read"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0write"),
                    op: Operation::W,
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
                    table: String::from("table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0"),
                    op: Operation::W,
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

        assert_eq!(
            TxTable::process_table_ops("read table_0 table_0"),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::R,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("write table_0 table_0"),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_0 write table_0"),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_0 table_0 write table_0"),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_0 table_0 write table_0 table_0"),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            TxTable::process_table_ops("read table_0 table_1 write table_0"),
            vec![
                TableOp {
                    table: String::from("table_0"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("table_1"),
                    op: Operation::R,
                },
            ]
        );
    }

    #[test]
    fn test_new() {
        assert_eq!(
            TxTable::new(&SqlBeginTx::new("tx0", "read tr0 tr1 write tw0 tw1"), false),
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
            TxTable::new(&SqlBeginTx::new("tx1", "read tr0 write tw0 read tr1 write"), false),
            TxTable {
                tx_name: String::from("tx1"),
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
                ]
            }
        );

        assert_eq!(
            TxTable::new(&SqlBeginTx::new("tx1", "read t0 write t0 read t1 write"), false),
            TxTable {
                tx_name: String::from("tx1"),
                table_ops: vec![
                    TableOp {
                        table: String::from("t0"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("t1"),
                        op: Operation::R
                    },
                ]
            }
        );

        assert_eq!(
            TxTable::new(&SqlBeginTx::new("tx1", "read t1 write t0 write t1 write t0"), false),
            TxTable {
                tx_name: String::from("tx1"),
                table_ops: vec![
                    TableOp {
                        table: String::from("t0"),
                        op: Operation::W
                    },
                    TableOp {
                        table: String::from("t1"),
                        op: Operation::W
                    },
                ]
            }
        );

        assert_eq!(
            TxTable::new(&SqlBeginTx::new("", ""), false),
            TxTable {
                tx_name: String::from(""),
                table_ops: vec![]
            }
        );
    }
}

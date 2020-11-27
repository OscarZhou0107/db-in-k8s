use crate::util::common;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use uuid::Uuid;

/// Enum representing either W (write) or R (read)
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum Operation {
    W,
    R,
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableOp {
    pub table: String,
    pub op: Operation,
}

trait IntoLegalTableOps {
    fn into_legal_table_ops(self) -> Vec<TableOp>;
}

/// Automatically converts any `Iterator<Item=(String, Operation)>` to a well-formed `Vec<TableOp>`
///
/// # Note
/// 1. It is expected `String` is already processed
/// 2. `Vec<TableOp>` is sorted in ascending order by `String` and by `Operation`
/// (`Operation`s with same `String` are ordered such that `Operation::W` comes before `Operation::R`)
impl<T, S> IntoLegalTableOps for T
where
    T: IntoIterator<Item = (S, Operation)>,
    S: ToString + Ord,
{
    fn into_legal_table_ops(self) -> Vec<TableOp> {
        self.into_iter()
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
                table: token.to_string(),
                op: op.into(),
            })
            .collect()
    }
}

/// Represents a raw Sql string
///
/// This string can be an invalid Sql statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlString(pub String);

impl<'a> From<&'a str> for SqlString {
    fn from(s: &'a str) -> Self {
        Self(s.to_owned())
    }
}

impl From<String> for SqlString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// Keeps a list of all tables accessed for a Sql transaction
///
/// # Notes
/// 1. `table_ops` should have no duplications in terms of `TableOp::table`
/// 2. Such duplication should only keep the one that `TableOp::op == Operation::W`
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SqlBeginTx {
    pub tx_name: String,
    pub table_ops: Vec<TableOp>,
}

impl SqlBeginTx {
    /// Parse the transaction name and mark if valid
    ///
    /// Return `Result<(tx_name: String, mark: String), ())>`
    fn try_from_sqlstring_helper(sqlstring: &SqlString) -> Result<(String, String), ()> {
        let re = Regex::new(r"^\s*begin\s+(?:tran|transaction)\s+(\S*)\s*with\s+mark\s+'(.*)'\s*;?\s*$").unwrap();

        re.captures(&sqlstring.0.to_ascii_lowercase())
            .map(|caps| {
                (
                    caps.get(1).unwrap().as_str().to_owned(),
                    caps.get(2).unwrap().as_str().to_owned(),
                )
            })
            .ok_or(())
    }

    /// Process the argument `tx_name` for use as the `SqlBeginTx.tx_name` field
    ///
    /// # Note
    /// Will remove any whitespace from the argument `tx_name`
    fn process_tx_name(tx_name: &str) -> String {
        let mut tx_name = tx_name.to_ascii_lowercase();
        common::remove_whitespace(&mut tx_name);
        tx_name
    }

    /// Process the argument `mark` for use as the `SqlBeginTx.table_ops` field
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
            .into_legal_table_ops()
    }

    pub fn add_uuid(mut self) -> Self {
        self.tx_name.push_str("_");
        self.tx_name.push_str(&Uuid::new_v4().to_string());
        self
    }
}

impl TryFrom<SqlString> for SqlBeginTx {
    type Error = ();

    /// Try to construct a `SqlBeginTx` from `SqlString`
    ///
    /// The conversion tries to parse for the expecting Sql syntax and then prepares
    /// for the `SqlBeginTx.tx_name` and `SqlBeginTx.table_ops` fields
    ///
    /// # Note
    /// Refer to `SqlBeginTx::process_tx_name` and `SqlBeginTx::process_table_ops` for processing details
    fn try_from(sqlstring: SqlString) -> Result<Self, Self::Error> {
        Self::try_from_sqlstring_helper(&sqlstring).map(|(tx_name, mark)| Self {
            tx_name: Self::process_tx_name(&tx_name),
            table_ops: Self::process_table_ops(&mark),
        })
    }
}

#[allow(warnings)]
pub struct SqlQuery {
    pub query: String,
    pub table_ops: Vec<TableOp>,
}

#[cfg(test)]
mod tests_into_legal_table_ops {
    use super::*;

    #[test]
    fn test_into_legal_table_ops() {
        assert_eq!(
            vec![
                ("table_r_0", Operation::R),
                ("table_r_1", Operation::R),
                ("table_w_0", Operation::W)
            ]
            .into_legal_table_ops(),
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
            vec![("table_r_0", Operation::R), ("table_r_1", Operation::R)].into_legal_table_ops(),
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
            vec![
                ("table_r_0", Operation::R),
                ("table_w_0", Operation::W),
                ("table_r_1", Operation::R),
                ("table_w_1", Operation::W)
            ]
            .into_legal_table_ops(),
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
            vec![("table_0", Operation::R), ("table_0", Operation::R)].into_legal_table_ops(),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::R,
            },]
        );

        assert_eq!(
            vec![("table_0", Operation::W), ("table_0", Operation::W)].into_legal_table_ops(),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            vec![("table_0", Operation::R), ("table_0", Operation::W)].into_legal_table_ops(),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            vec![
                ("table_0", Operation::R),
                ("table_0", Operation::R),
                ("table_0", Operation::W)
            ]
            .into_legal_table_ops(),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            vec![
                ("table_0", Operation::R),
                ("table_0", Operation::R),
                ("table_0", Operation::W),
                ("table_0", Operation::W)
            ]
            .into_legal_table_ops(),
            vec![TableOp {
                table: String::from("table_0"),
                op: Operation::W,
            },]
        );

        assert_eq!(
            vec![
                ("table_0", Operation::R),
                ("table_1", Operation::R),
                ("table_0", Operation::W)
            ]
            .into_legal_table_ops(),
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
}

/// Unit test for `SqlBeginTx`
#[cfg(test)]
mod tests_sqlbegintx {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_try_from_sqlstring_helper() {
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
                SqlBeginTx::try_from_sqlstring_helper(&sqlstring),
                res.map(|res_ref| (res_ref.0.to_ascii_lowercase(), res_ref.1.to_ascii_lowercase()))
                    .ok_or(())
            )
        });
    }

    #[test]
    fn test_try_from_sqlstring() {
        assert_eq!(
            SqlString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';").try_into(),
            Ok(SqlBeginTx {
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
            SqlString::from("BeGin TraN with MarK 'table0 read table1 read write table2 table3 read';").try_into(),
            Ok(SqlBeginTx {
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
                .try_into(),
            Ok(SqlBeginTx {
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
            SqlString::from("BeGin TraNsaction with MarK ''").try_into(),
            Ok(SqlBeginTx {
                tx_name: String::from(""),
                table_ops: vec![]
            })
        );

        assert_eq!(
            SqlBeginTx::try_from(SqlString::from(
                "BeGin TraNssaction with MarK 'read table1 table2 table3'"
            )),
            Err(())
        );

        assert_eq!(
            SqlBeginTx::try_from(SqlString::from("BeGin TraNsaction with MarK")),
            Err(())
        );

        assert_eq!(SqlBeginTx::try_from(SqlString::from("select * from table0;")), Err(()));

        assert_eq!(SqlBeginTx::try_from(SqlString::from("begin")), Err(()));

        assert_eq!(SqlBeginTx::try_from(SqlString::from("")), Err(()));
    }

    #[test]
    fn test_process_tx_name() {
        assert_eq!(SqlBeginTx::process_tx_name("  tx_name  "), *"tx_name");

        assert_eq!(SqlBeginTx::process_tx_name("  tx_name 1 2 3  "), *"tx_name123");

        assert_eq!(SqlBeginTx::process_tx_name("   "), *"");

        assert_eq!(SqlBeginTx::process_tx_name("tx_name"), *"tx_name");
    }

    #[test]
    fn test_process_table_ops() {
        assert_eq!(
            SqlBeginTx::process_table_ops("read table_r_0 table_r_1 write table_w_0"),
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
            SqlBeginTx::process_table_ops(" read     table_r_0   table_r_1    write  table_w_0 "),
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
            SqlBeginTx::process_table_ops("write table_w_0 read table_r_0 table_r_1"),
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
            SqlBeginTx::process_table_ops("write table_w_0write read table_r_0read writetable_r_1read"),
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
            SqlBeginTx::process_table_ops("write read table_r_0 table_r_1"),
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
            SqlBeginTx::process_table_ops("read table_r_0 table_r_1 write"),
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

        assert_eq!(SqlBeginTx::process_table_ops("read write"), vec![]);

        assert_eq!(SqlBeginTx::process_table_ops(""), vec![]);

        assert_eq!(
            SqlBeginTx::process_table_ops("read table_r_0 write table_w_0 read table_r_1 "),
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
            SqlBeginTx::process_table_ops("read table_r_0 write read"),
            vec![TableOp {
                table: String::from("table_r_0"),
                op: Operation::R,
            },]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("read table_r_0 writeread"),
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
            SqlBeginTx::process_table_ops("table0 table1 table2 read table_r_0 table_r_1 write table_w_0"),
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

        assert_eq!(SqlBeginTx::process_table_ops("table0 table1 table2"), vec![]);
    }
}

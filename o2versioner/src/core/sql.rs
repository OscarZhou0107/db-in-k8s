use crate::util::common;
use itertools::Itertools;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::convert::TryFrom;
use unicase::UniCase;
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
/// # Notes
/// 1. This string can be an invalid Sql statement.
/// 2. Sql keyword are case insensitive, other things are case sensitive.
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
    /// # Notes
    /// 1. Assume Sql keywords are case insensitive.
    /// 2. Others are case sensitive, including transaction name and table name
    /// Return `Result<(tx_name: String, mark: String), ())>`
    fn try_from_sqlstring_helper(sqlstring: &SqlString) -> Result<(&str, &str), ()> {
        let re = Regex::new(
            r"^\s*(?i)begin(?-i)\s+(?i)(?:tran|transaction)(?-i)\s+(\S*)\s*(?i)with\s+mark(?-i)\s+'(.*)'\s*;?\s*$",
        )
        .unwrap();

        re.captures(&sqlstring.0)
            .map(|caps| (caps.get(1).unwrap().as_str(), caps.get(2).unwrap().as_str()))
            .ok_or(())
    }

    /// Process the argument `tx_name` for use as the `SqlBeginTx.tx_name` field
    ///
    /// # Notes
    /// 1. Will remove any whitespace from the argument `tx_name`
    fn process_tx_name(tx_name: &str) -> String {
        let mut tx_name = tx_name.to_owned();
        common::remove_whitespace(&mut tx_name);
        tx_name
    }

    /// Process the argument `mark` for use as the `SqlBeginTx.table_ops` field
    ///
    /// # Note
    /// 1. Assume keywords, such as "read" and "write", are case insensitive.
    /// 2. It supports multiple occurance for read or write keyword
    /// 3. Any space-separated identifier are associated with the previous keyword if existed; else, will be discarded
    /// 4. If no identifier is followed after a keyword, the keyword will be ignored
    /// 5. `Vec<TableOp>` is sorted in ascending order by `TableOp.name` and by `TableOp.op`
    /// (`TableOp`s with same `TableOp.name` are ordered such that `Operation::W` comes before `Operation::R`)
    fn process_table_ops(mark: &str) -> Vec<TableOp> {
        let read_ci = UniCase::new("read");
        let write_ci = UniCase::new("write");
        mark.split_whitespace()
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
                        return Some(Some((token_ci.into_inner(), Operation::R)));
                    } else {
                        return Some(Some((token_ci.into_inner(), Operation::W)));
                    }
                }
            })
            .filter_map(|token_op| token_op)
            .into_legal_table_ops()
    }

    /// Append a uuid to the end of `SqlBeginTx::tx_name`
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
            tx_name: Self::process_tx_name(tx_name),
            table_ops: Self::process_table_ops(mark),
        })
    }
}

#[allow(warnings)]
pub struct SqlQuery {
    pub query: String,
    pub table_ops: Vec<TableOp>,
}

/// Unit test for `IntoLegalTableOps`
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
                res.map(|res_ref| (res_ref.0, res_ref.1)).ok_or(())
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
        assert_eq!(SqlBeginTx::process_tx_name("  tX_name  "), *"tX_name");

        assert_eq!(SqlBeginTx::process_tx_name("  tx_nAMe 1 2 3  "), *"tx_nAMe123");

        assert_eq!(SqlBeginTx::process_tx_name("   "), *"");

        assert_ne!(SqlBeginTx::process_tx_name("tXX_name"), *"txx_name");
        assert_eq!(SqlBeginTx::process_tx_name("tXX_name"), *"tXX_name");
    }

    #[test]
    fn test_process_table_ops() {
        assert_eq!(
            SqlBeginTx::process_table_ops("read table_R_0 table_r_1 write table_w_0"),
            vec![
                TableOp {
                    table: String::from("table_R_0"),
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
            SqlBeginTx::process_table_ops("WrItE Table_w_0 read table_r_0 table_r_1"),
            vec![
                TableOp {
                    table: String::from("Table_w_0"),
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
            SqlBeginTx::process_table_ops("write table_w_0writE read table_r_0read writetable_r_1reAd"),
            vec![
                TableOp {
                    table: String::from("table_r_0read"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_w_0writE"),
                    op: Operation::W,
                },
                TableOp {
                    table: String::from("writetable_r_1reAd"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("write read Table_r_0 Table_r_1"),
            vec![
                TableOp {
                    table: String::from("Table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("Table_r_1"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("read table_r_0 Table_r_1 write"),
            vec![
                TableOp {
                    table: String::from("Table_r_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(SqlBeginTx::process_table_ops("ReaD WriTE"), vec![]);

        assert_eq!(SqlBeginTx::process_table_ops(""), vec![]);

        assert_eq!(
            SqlBeginTx::process_table_ops("ReAd table_R_0 WrItE table_W_0 rEad table_R_1 "),
            vec![
                TableOp {
                    table: String::from("table_R_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_R_1"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("table_W_0"),
                    op: Operation::W,
                },
            ]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("read Table_r_0 WRITE REAd"),
            vec![TableOp {
                table: String::from("Table_r_0"),
                op: Operation::R,
            },]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("Read table_r_0 writereAD"),
            vec![
                TableOp {
                    table: String::from("table_r_0"),
                    op: Operation::R,
                },
                TableOp {
                    table: String::from("writereAD"),
                    op: Operation::R,
                },
            ]
        );

        assert_eq!(
            SqlBeginTx::process_table_ops("table0 table1 table2 read table_r_0 table_r_1 Write table_w_0"),
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

        assert_eq!(SqlBeginTx::process_table_ops("tAble0 taBle1 tabLe2"), vec![]);
    }
}

/// Unit test for `SqlQuery`
#[cfg(test)]
mod tests_sqlquery {
    #![allow(warnings)]
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_try_from_sqlstring_helper() {}
}

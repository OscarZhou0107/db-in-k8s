use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
/// use o2versioner::core::operation::*;
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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
    /// use o2versioner::core::operation::TableOps;
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

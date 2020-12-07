use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::iter::FromIterator;
use unicase::UniCase;

/// Enum representing either a W (write) or R (read) for a table
#[derive(Debug, Eq, PartialEq, Clone, Copy, Serialize, Deserialize)]
pub enum RWOperation {
    R,
    W,
}

/// Enum representing the access pattern of a `TableOps`
#[derive(Debug, Eq, PartialEq, Clone, strum::AsRefStr)]
pub enum AccessPattern {
    WriteOnly,
    ReadOnly,
    Mixed,
}

impl AccessPattern {
    pub fn is_write_only(&self) -> bool {
        match &self {
            Self::WriteOnly => true,
            _ => false,
        }
    }

    pub fn is_read_only(&self) -> bool {
        match &self {
            Self::ReadOnly => true,
            _ => false,
        }
    }

    pub fn is_mixed(&self) -> bool {
        match &self {
            Self::Mixed => true,
            _ => false,
        }
    }
}

/// Helper function
fn remove_whitespace(s: &mut String) {
    s.retain(|c| !c.is_whitespace());
}

/// Representing the access mode for `Self::table`, can be either `RWOperation::R` or `RWOperation::W`
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TableOp {
    table: String,
    op: RWOperation,
}

impl TableOp {
    pub fn new<S: Into<String>>(table: S, op: RWOperation) -> Self {
        let mut table = table.into();
        remove_whitespace(&mut table);
        Self { table, op }
    }

    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn op(&self) -> RWOperation {
        self.op
    }

    pub fn unwrap(self) -> (String, RWOperation) {
        (self.table, self.op)
    }
}

/// Representing a collection of TableOp
///
/// Automatically sorted in ascending order by `TableOp::table` and by `TableOp::op`
/// (`RWOperation`s with same `String` are ordered such that `RWOperation::W` comes before `RWOperation::R`)
///
/// # Examples
/// ```
/// use o2versioner::core::*;
/// use std::iter::FromIterator;
///
/// let tableops0 = TableOps::from_iter(vec![
///     TableOp::new("table0", RWOperation::R),
///     TableOp::new("table1", RWOperation::W),
/// ]);
///
/// let tableops1 = TableOps::from("read table0 write  table1");
///
/// let tableops2 = TableOps::default()
///     .add_tableop(TableOp::new("table1", RWOperation::W))
///     .add_tableop(TableOp::new("table0", RWOperation::R));
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
        if self.0.iter().all(|tableop| tableop.op == RWOperation::R) {
            AccessPattern::ReadOnly
        } else if self.0.iter().all(|tableop| tableop.op == RWOperation::W) {
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
    /// (`TableOp`s with same `TableOp::name` are ordered such that `RWOperation::W` comes before `RWOperation::R`)
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
                            if left.op == RWOperation::W {
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
    /// use o2versioner::core::TableOps;
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
                            return Some(Some(TableOp::new(token_ci.into_inner(), RWOperation::R)));
                        } else {
                            return Some(Some(TableOp::new(token_ci.into_inner(), RWOperation::W)));
                        }
                    }
                })
                .filter_map(|token_op| token_op),
        )
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct EarlyReleaseTables(Vec<String>);

impl Default for EarlyReleaseTables {
    fn default() -> Self {
        Self(Vec::new())
    }
}

impl EarlyReleaseTables {
    /// Get a ref to the internal storage
    pub fn get(&self) -> &[String] {
        &self.0[..]
    }

    /// Convert to a `Vec<String>`
    pub fn into_vec(self) -> Vec<String> {
        self.0
    }

    /// Append a table, will validate all entires again
    pub fn add_table<S: Into<String>>(mut self, table: S) -> Self {
        let mut table = table.into();
        remove_whitespace(&mut table);
        self.0.push(table);
        Self::from_iter(self)
    }

    pub fn is_empty(&self) -> bool {
        return self.0.is_empty();
    }
}

impl IntoIterator for EarlyReleaseTables {
    type Item = String;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<S> FromIterator<S> for EarlyReleaseTables
where
    S: Into<String>,
{
    /// Convert a `String` like `IntoIterator` object into `EarlyReleaseTables`
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self(
            iter.into_iter()
                .map(|s| {
                    let mut s = s.into();
                    remove_whitespace(&mut s);
                    s
                })
                .filter(|s| !s.is_empty())
                .sorted()
                .dedup()
                .collect(),
        )
    }
}

impl<S> From<S> for EarlyReleaseTables
where
    S: Into<String>,
{
    /// Convert the input `Into<String>` into `TableOps`
    ///
    /// # Examples
    /// ```
    /// use o2versioner::core::EarlyReleaseTables;
    /// assert_eq!(
    ///     EarlyReleaseTables::from("  TABLE_0   TABLE_1 ".to_owned()).into_vec(),
    ///     vec!["TABLE_0".to_owned(), "TABLE_1".to_owned()]
    /// );
    /// ```
    fn from(str_like: S) -> Self {
        Self::from_iter(str_like.into().split_whitespace())
    }
}

/// Unit test fot `TableOp`
#[cfg(test)]
mod tests_tableop {
    use super::*;

    #[test]
    fn test_new() {
        assert_eq!(
            TableOp::new("table0", RWOperation::R).unwrap(),
            (String::from("table0"), RWOperation::R)
        );
        assert_eq!(
            TableOp::new("table0 table1", RWOperation::R).unwrap(),
            (String::from("table0table1"), RWOperation::R)
        );
        assert_eq!(
            TableOp::new(" table0 table1 ", RWOperation::R).unwrap(),
            (String::from("table0table1"), RWOperation::R)
        );
    }
}

/// Unit test for `AccessPattern`
#[cfg(test)]
mod tests_access_pattern {
    use super::*;

    #[test]
    fn test_is_mixed() {
        assert!(AccessPattern::Mixed.is_mixed());
        assert!(!AccessPattern::Mixed.is_read_only());
        assert!(!AccessPattern::Mixed.is_write_only());
    }

    #[test]
    fn test_is_read_only() {
        assert!(!AccessPattern::ReadOnly.is_mixed());
        assert!(AccessPattern::ReadOnly.is_read_only());
        assert!(!AccessPattern::ReadOnly.is_write_only());
    }

    #[test]
    fn test_is_write_only() {
        assert!(!AccessPattern::WriteOnly.is_mixed());
        assert!(!AccessPattern::WriteOnly.is_read_only());
        assert!(AccessPattern::WriteOnly.is_write_only());
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
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_r_0".to_owned(), RWOperation::R),
                TableOp::new("table_r_1".to_owned(), RWOperation::R),
                TableOp::new("table_w_0".to_owned(), RWOperation::W),
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R)
            ])
            .into_iter()
            .collect::<Vec<_>>(),
            vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R)
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_1", RWOperation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W),
                TableOp::new("table_w_1", RWOperation::W),
            ]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::R)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", RWOperation::R)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::W),
                TableOp::new("table_0", RWOperation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", RWOperation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", RWOperation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", RWOperation::W,)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_0", RWOperation::W),
                TableOp::new("table_0", RWOperation::W)
            ])
            .into_vec(),
            vec![TableOp::new("table_0", RWOperation::W)]
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_1", RWOperation::R),
                TableOp::new("table_0", RWOperation::W)
            ])
            .into_vec(),
            vec![
                TableOp::new("table_0", RWOperation::W,),
                TableOp::new("table_1", RWOperation::R)
            ]
        );
    }

    #[test]
    fn test_from_string() {
        assert_eq!(
            TableOps::from("ReAd table_R_0 table_r_1 WRite table_w_0").into_vec(),
            vec![
                TableOp::new("table_R_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W)
            ]
        );

        assert_eq!(
            TableOps::from(" read     tAble_r_0   tAble_r_1    WRITE  tAble_w_0 ").into_vec(),
            vec![
                TableOp::new("tAble_r_0", RWOperation::R),
                TableOp::new("tAble_r_1", RWOperation::R),
                TableOp::new("tAble_w_0", RWOperation::W)
            ]
        );

        assert_eq!(
            TableOps::from("WrItE Table_w_0 read table_r_0 table_r_1").into_vec(),
            vec![
                TableOp::new("Table_w_0", RWOperation::W),
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R)
            ]
        );

        assert_eq!(
            TableOps::from("write table_w_0writE read table_r_0read writetable_r_1reAd").into_vec(),
            vec![
                TableOp::new("table_r_0read", RWOperation::R),
                TableOp::new("table_w_0writE", RWOperation::W),
                TableOp::new("writetable_r_1reAd", RWOperation::R),
            ]
        );

        assert_eq!(
            TableOps::from("write read Table_r_0 Table_r_1").into_vec(),
            vec![
                TableOp::new("Table_r_0", RWOperation::R),
                TableOp::new("Table_r_1", RWOperation::R),
            ]
        );

        assert_eq!(
            TableOps::from("read table_r_0 Table_r_1 write").into_vec(),
            vec![
                TableOp::new("Table_r_1", RWOperation::R),
                TableOp::new("table_r_0", RWOperation::R)
            ]
        );

        assert_eq!(TableOps::from("ReaD WriTE").into_vec(), vec![]);

        assert_eq!(TableOps::from("").into_vec(), vec![]);

        assert_eq!(
            TableOps::from("ReAd table_R_0 WrItE table_W_0 rEad table_R_1 ").into_vec(),
            vec![
                TableOp::new("table_R_0", RWOperation::R),
                TableOp::new("table_R_1", RWOperation::R),
                TableOp::new("table_W_0", RWOperation::W)
            ]
        );

        assert_eq!(
            TableOps::from("read Table_r_0 WRITE REAd").into_vec(),
            vec![TableOp::new("Table_r_0", RWOperation::R)]
        );

        assert_eq!(
            TableOps::from("Read table_r_0 writereAD").into_vec(),
            vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("writereAD", RWOperation::R)
            ]
        );

        assert_eq!(
            TableOps::from("table0 table1 table2 read table_r_0 table_r_1 Write table_w_0").into_vec(),
            vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W)
            ]
        );

        assert_eq!(TableOps::from("tAble0 taBle1 tabLe2").into_vec(), vec![]);
    }

    #[test]
    fn test_add_tableop() {
        let tableops = TableOps::default();

        let tableops = tableops.add_tableop(TableOp::new("table_0", RWOperation::R));
        assert_eq!(tableops.get(), vec![TableOp::new("table_0", RWOperation::R)].as_slice());

        let tableops = tableops.add_tableop(TableOp::new("table_0", RWOperation::R));
        assert_eq!(tableops.get(), vec![TableOp::new("table_0", RWOperation::R)].as_slice());

        let tableops = tableops.add_tableop(TableOp::new("table_1", RWOperation::R));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", RWOperation::R),
                TableOp::new("table_1", RWOperation::R)
            ]
            .as_slice()
        );

        let tableops = tableops.add_tableop(TableOp::new("table_0", RWOperation::W));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", RWOperation::W),
                TableOp::new("table_1", RWOperation::R)
            ]
            .as_slice()
        );

        let tableops = tableops.add_tableop(TableOp::new("table_1", RWOperation::W));
        assert_eq!(
            tableops.get(),
            vec![
                TableOp::new("table_0", RWOperation::W),
                TableOp::new("table_1", RWOperation::W)
            ]
            .as_slice()
        );
    }

    #[test]
    fn test_access_pattern() {
        assert_eq!(
            TableOps::from_iter(vec![TableOp::new("table_r_0", RWOperation::R),]).access_pattern(),
            AccessPattern::ReadOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![TableOp::new("table_w_0", RWOperation::W),]).access_pattern(),
            AccessPattern::WriteOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_w_0", RWOperation::W)
            ])
            .access_pattern(),
            AccessPattern::Mixed
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_r_0", RWOperation::R),
                TableOp::new("table_r_1", RWOperation::R),
                TableOp::new("table_r_2", RWOperation::R),
            ])
            .access_pattern(),
            AccessPattern::ReadOnly
        );

        assert_eq!(
            TableOps::from_iter(vec![
                TableOp::new("table_w_0", RWOperation::W),
                TableOp::new("table_w_1", RWOperation::W),
                TableOp::new("table_w_2", RWOperation::W),
            ])
            .access_pattern(),
            AccessPattern::WriteOnly
        );
    }
}

/// Unit test for `EarlyReleaseTables`
#[cfg(test)]
mod tests_early_release_tables {
    use super::*;

    #[test]
    fn test_from_iter() {
        assert_eq!(
            EarlyReleaseTables::from_iter(vec!["table_0", "table_1", "table_0"]).into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::from_iter(vec![String::from("table_1"), String::from("table_0")]).into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::from_iter(vec![String::from("table_0"), String::from("table_1")]).into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::from_iter(vec![String::from("table_0 aaa"), String::from("table_1")]).into_vec(),
            vec!["table_0aaa".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::from_iter(Vec::<String>::new()).into_vec(),
            Vec::<String>::new()
        );
        assert_eq!(EarlyReleaseTables::from_iter(vec![""]).into_vec(), Vec::<String>::new());
    }

    #[test]
    fn test_from_string() {
        assert_eq!(
            EarlyReleaseTables::from("table_0  table_1 table_0 ").into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::from("  table_1  table_0".to_owned()).into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );

        assert_eq!(
            EarlyReleaseTables::from("  TABLE_0   TABLE_1 ".to_owned()).into_vec(),
            vec!["TABLE_0".to_owned(), "TABLE_1".to_owned()]
        );

        assert_eq!(
            EarlyReleaseTables::from("   ".to_owned()).into_vec(),
            Vec::<String>::new()
        );
        assert_eq!(EarlyReleaseTables::from("".to_owned()).into_vec(), Vec::<String>::new());
    }

    #[test]
    fn test_add_table() {
        assert_eq!(
            EarlyReleaseTables::default()
                .add_table("table_0")
                .add_table("table_1")
                .into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::default()
                .add_table("table_0")
                .add_table("table_1")
                .add_table("table_0")
                .into_vec(),
            vec!["table_0".to_owned(), "table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::default()
                .add_table("Table_1")
                .add_table("Table_0")
                .into_vec(),
            vec!["Table_0".to_owned(), "Table_1".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::default()
                .add_table("Table_1 bbb")
                .add_table("Table_0 aaa")
                .into_vec(),
            vec!["Table_0aaa".to_owned(), "Table_1bbb".to_owned()]
        );
        assert_eq!(
            EarlyReleaseTables::default().add_table("  ").into_vec(),
            Vec::<String>::new()
        );
        assert_eq!(
            EarlyReleaseTables::default().add_table("").into_vec(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn test_is_empty() {
        assert!(!EarlyReleaseTables::default().add_table("Table_1 bbb").is_empty());
        assert!(EarlyReleaseTables::default().is_empty());
        assert!(EarlyReleaseTables::default().add_table("").is_empty());
        assert!(EarlyReleaseTables::from("").is_empty());
        assert!(EarlyReleaseTables::from_iter(vec!["", " "]).is_empty());
    }
}

use super::*;

/// The legality level of a given request.
#[derive(Debug)]
pub enum Legality {
    /// Legal
    Legal,
    /// The operation must be rejected, but following operations are still accepted
    Critical(String),
    /// The operation must be rejected, and the server should panic because the error cannot be handled as for now
    Panic(String),
}

/// Use this function as a single point for query diagnostics,
/// all legalization must be done before this function call.
/// Code after this function call in later stages can simply panic.
impl Legality {
    pub fn legal() -> Self {
        Self::Legal
    }

    pub fn critical<S: Into<String>>(s: S) -> Self {
        Self::Critical(s.into())
    }

    pub fn panic<S: Into<String>>(s: S) -> Self {
        Self::Panic(s.into())
    }

    fn check_tableops_match_txvn(query: &MsqlQuery, txvn: &TxVN) -> Option<Self> {
        if txvn.get_from_tableops(&query.tableops()).is_err() {
            let missing_tableops: Vec<_> = query
                .tableops()
                .get()
                .iter()
                .filter(|tableop| txvn.get_from_tableop(tableop).is_none())
                .map(|tableop| tableop.table())
                .collect();
            Some(Self::critical(format!(
                "Query is using tables not declared in the BeginTx: {:?}",
                missing_tableops
            )))
        } else {
            None
        }
    }

    pub fn final_check(msql: &Msql, txvn_opt: &Option<TxVN>) -> Self {
        match msql {
            Msql::BeginTx(_begintx) => {
                if txvn_opt.is_some() {
                    Self::critical("Cannot begin new transaction because previous transaction not finished yet.")
                } else {
                    Self::legal()
                }
            }
            Msql::Query(query) => {
                if let Some(txvn) = txvn_opt.as_ref() {
                    match &query.tableops().access_pattern() {
                        AccessPattern::Mixed => Self::critical("Does not support query with mixed R and W"),
                        AccessPattern::ReadOnly => {
                            if let Some(err) = Self::check_tableops_match_txvn(query, txvn) {
                                err
                            } else if query.has_early_release() {
                                Self::critical("Does not support early release on R queries")
                            } else {
                                Self::legal()
                            }
                        }
                        AccessPattern::WriteOnly => {
                            if let Some(err) = Self::check_tableops_match_txvn(query, txvn) {
                                err
                            } else if txvn.get_from_ertables(&query.early_release_tables()).is_err() {
                                Self::critical(
                                        "Tables marked for early release was not declared in the BeginTx or has already been released",
                                    )
                            } else {
                                Self::legal()
                            }
                        }
                    }
                } else {
                    match &query.tableops().access_pattern() {
                        AccessPattern::Mixed => Self::critical("Does not support query with mixed R and W"),
                        AccessPattern::ReadOnly => Self::panic("Does not support fast single R query"),
                        AccessPattern::WriteOnly => Self::panic("Query does not have a valid BeginTx"),
                    }
                }
            }
            Msql::EndTx(_endtx) => {
                if txvn_opt.is_none() {
                    Self::critical("There is not transaction to end")
                } else {
                    Self::legal()
                }
            }
        }
    }
}

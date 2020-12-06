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

    pub fn final_check(msql: &Msql, txvn_opt: &Option<TxVN>) -> Self {
        match msql {
            Msql::BeginTx(_begintx) => {
                if txvn_opt.is_some() {
                    Self::critical("Cannot begin new transaction because previous transaction not finished yet.")
                } else {
                    Self::Legal
                }
            }
            Msql::Query(query) => {
                if let Some(txvn) = txvn_opt.as_ref() {
                    if query.tableops().access_pattern() == AccessPattern::Mixed {
                        Self::critical("Does not support query with mixed R and W")
                    } else if txvn.get_from_tableops(&query.tableops()).is_err() {
                        let missing_tableops: Vec<_> = query
                            .tableops()
                            .get()
                            .iter()
                            .filter(|tableop| txvn.get_from_tableop(tableop).is_none())
                            .map(|tableop| tableop.table())
                            .collect();
                        Self::critical(format!(
                            "Query is using tables not declared in the BeginTx: {:?}",
                            missing_tableops
                        ))
                    } else if txvn.get_from_ertables(&query.early_release_tables()).is_err() {
                        Self::critical(
                            "Tables marked for early release was not declared in the BeginTx or has already been released",
                        )
                    } else {
                        Self::Legal
                    }
                } else {
                    if query.tableops().access_pattern() == AccessPattern::ReadOnly {
                        if query.has_early_release() {
                            // TODO: change it to critical after supporting fast single R query
                            Self::panic("Does not support early release release for fast single R query")
                        } else {
                            Self::panic("Does not support fast single R query yet")
                        }
                    } else {
                        Self::panic("Query does not have a valid BeginTx")
                    }
                }
            }
            Msql::EndTx(_endtx) => {
                if txvn_opt.is_none() {
                    Self::critical("There is not transaction to end")
                } else {
                    Self::Legal
                }
            }
        }
    }
}

use super::*;

/// The legality level of a given request.
#[derive(Debug)]
pub enum Legality {
    /// Legal
    Legal,
    /// The operation must be rejected, but following operations are still accepted
    Critical(&'static str),
    /// The operation must be rejected, and the server should panic because the error cannot be handled as for now
    Panic(&'static str),
}

/// Use this function as a single point for query diagnostics,
/// all legalization must be done before this function call.
/// Code after this function call in later stages can simply panic.
impl Legality {
    pub fn final_check(msql: &Msql, txvn_opt: &Option<TxVN>) -> Self {
        match msql {
            Msql::BeginTx(_begintx) => {
                if txvn_opt.is_some() {
                    Self::Critical("Cannot begin new transaction because previous transaction not finished yet.")
                } else {
                    Self::Legal
                }
            }
            Msql::Query(query) => {
                if let Some(txvn) = txvn_opt.as_ref() {
                    if query.tableops().access_pattern() == AccessPattern::Mixed {
                        Self::Critical("Does not support query with mixed R and W")
                    } else if txvn.get_from_tableops(&query.tableops()).is_err() {
                        Self::Critical("Query is using tables not declared in the BeginTx")
                    } else if txvn.get_from_ertables(&query.early_release_tables()).is_err() {
                        Self::Critical(
                            "Tables marked for early release was not declared in the BeginTx or has already been released",
                        )
                    } else {
                        Self::Legal
                    }
                } else {
                    if query.tableops().access_pattern() == AccessPattern::ReadOnly {
                        if query.has_early_release() {
                            // TODO: change it to critical after supporting fast single R query
                            Self::Panic("Does not support early release release for fast single R query")
                        } else {
                            Self::Panic("Does not support fast single R query yet")
                        }
                    } else {
                        Self::Panic("Query does not have a valid BeginTx")
                    }
                }
            }
            Msql::EndTx(_endtx) => {
                if txvn_opt.is_none() {
                    Self::Critical("There is not transaction to end")
                } else {
                    Self::Legal
                }
            }
        }
    }
}

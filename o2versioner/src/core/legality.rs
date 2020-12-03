#![allow(warnings)]
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

/// Use this function as a single point for query diagnostics at the
/// very beginning stage of request handling, so that the later stages can simply
/// panic
impl Legality {
    pub fn check(msql: &Msql, txvn_opt: &Option<TxVN>) -> Self {
        match msql {
            Msql::BeginTx(begintx) => {
                if txvn_opt.is_some() {
                    Self::Critical("Cannot begin new transaction because previous transaction not finished yet.")
                } else {
                    Self::Legal
                }
            }
            Msql::Query(query) => {
                if txvn_opt.is_none() {
                    Self::Panic("Does not support single un-transactioned query for now")
                } else {
                    let txvn = txvn_opt.as_ref().unwrap();
                    if query.tableops().access_pattern() == AccessPattern::Mixed {
                        Self::Critical("Does not support query with mixed R and W")
                    } else if let Err(_) = txvn.get_from_tableops(&query.tableops()) {
                        Self::Critical("Query is using tables not declared in the BeginTx")
                    } else {
                        Self::Legal
                    }
                }
            }
            Msql::EndTx(endtx) => {
                if txvn_opt.is_none() {
                    Self::Critical("There is not transaction to end")
                } else {
                    Self::Legal
                }
            }
        }
    }
}

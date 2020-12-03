#![allow(warnings)]
use super::*;

/// The legality of a given request.
///
/// - `Legality::Legal` denotes it is legal
/// - `Legality::Warning` denotes the operation is illegal but can be fixed
/// and proceeded.
/// - `Legality::Critical` denotes the operation must be rejected,
/// but following operations are still accepted.
/// - `Legality::Panic` denotes the operation must be rejected,
/// and the server should panic because the error cannot be handled as for now.
#[derive(Debug)]
pub enum Legality {
    Legal,
    Warning(&'static str),
    Critical(&'static str),
    Panic(&'static str),
}

/// Use this function as a single point for query diagnostics at the
/// very beginning stage of request handling, so that the later stages can simply
/// panic
impl Legality {
    pub fn check(msql: &Msql, txvn_opt: &Option<TxVN>) -> Self {
        todo!()
    }
}

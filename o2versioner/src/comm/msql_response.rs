/// This module contains everything about the response
/// to a Msql query.
use crate::dbproxy::core::QueryResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MsqlResponse {
    BeginTx(Result<(), String>),
    Query(Result<String, String>),
    EndTx(Result<String, String>),
}

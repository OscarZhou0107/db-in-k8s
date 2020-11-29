use super::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use crate::core::transaction_version::*;
use crate::dbproxy::core::{Operation, QueryResult};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
/// TODO: this should be replaced by the implementation below
pub enum Message {
    Invalid,
    SqlRequest(Operation),
    SqlResponse(QueryResult),
}

pub enum EndTx {
    Commit,
    Abort,
}

/// Errors representing why a `Message` request can fail
pub enum Error {
    MissingTxBegin,
    Invalid,
}

/// Expecting every request will have a response replied back via the same tcp stream
pub enum NewMessage {
    /// A `Msql` request to dbproxy. `Option<TxVN> == None` for single-read transaction
    MsqlRequest(Msql, Option<TxVN>),
    /// The repsone to the `MsqlRequest`
    MsqlResponse(MsqlResponse),
    /// Response to an invalid request, for exmample, sending `MsqlResponse(MsqlResponse)` to the dbproxy
    Invalid,
}

use super::msql_response::MsqlResponse;
use crate::core::msql::*;
use crate::core::transaction_version::*;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A `Msql` request to dbproxy. `Option<TxVN> == None` for single-read transaction
    MsqlRequest(SocketAddr, Msql, Option<TxVN>),
    /// The repsone to the `MsqlRequest`
    MsqlResponse(MsqlResponse),
    /// Response to an invalid request, for exmample, sending `MsqlResponse(MsqlResponse)` to the dbproxy
    Invalid,
}

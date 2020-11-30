use super::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use crate::core::transaction_version::*;
use crate::dbproxy::core::{QueryResult};
use serde::{Deserialize, Serialize};


pub enum EndTx {
    Commit,
    Abort,
}

/// Errors representing why a `Message` request can fail
pub enum Error {
    MissingTxBegin,
    Invalid,
}

type Addr = String;
/// Expecting every request will have a response replied back via the same tcp stream
#[derive(Serialize, Deserialize, Clone)]
pub enum NewMessage {
    /// A `Msql` request to dbproxy. `Option<TxVN> == None` for single-read transaction
    MsqlRequest(Msql, Addr, Option<TxVN>),
    /// The repsone to the `MsqlRequest`
    MsqlResponse(MsqlResponse),
    /// Response to an invalid request, for exmample, sending `MsqlResponse(MsqlResponse)` to the dbproxy
    Invalid,
}

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

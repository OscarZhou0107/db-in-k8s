use super::mysql::Packet;
//use crate::core::sql::SqlBeginTx;
use crate::core::version_number::TxVN;
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

pub enum RealMessage {
    Invalid,
    // Request from Scheduler to Dbproxy
    RequestBypass(Packet),
    RequestQuery(Packet, TxVN),
    RequestEndTx(EndTx, TxVN),
    // Response from Dbproxy to Scheduler
    ResponseBypass(Result<Packet, Error>),
    ResponseQuery(Result<Packet, Error>),
    ResponseEndTx(Result<Packet, Error>),
}

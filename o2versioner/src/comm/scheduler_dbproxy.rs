use crate::core::version_number::TxVN;

/// Represents a MySQL client/server protocol packet
/// 
/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/PAGE_PROTOCOL.html
///
/// Supports:
/// 1. Connection Phase, SSL off, compression off. Bypassed
/// 2. Command Phase, Text Protocol. Deserialized and handled
pub struct Packet();

pub enum EndTx {
    Commit,
    Abort,
}

/// Errors representing why a `Message` request can fail
pub enum Error {
    MissingTxBegin,
    Invalid,
}

pub enum Message {
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

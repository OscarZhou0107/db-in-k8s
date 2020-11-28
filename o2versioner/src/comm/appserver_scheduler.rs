use crate::msql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum QueryResponseMessage {
    Write(String),
    Read(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMessage {
    InvalidRequest,
    BeginTx(Result<String, String>),
    Query(String),
    EndTx(String),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    /// For use by appserver using Rust
    RequestMsql(msql::Msql),
    RequestMsqlText(msql::MsqlText),
    Response(ResponseMessage),
}

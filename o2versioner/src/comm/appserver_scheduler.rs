use crate::msql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ReplyMessage {
    BeginTx(Result<(), String>),
    Query(Result<String, String>),
    EndTx(Result<String, String>),
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Message {
    RequestMsql(msql::Msql),
    RequestMsqlText(msql::MsqlText),
    InvalidRequest,
    InvalidMsqlText(String),
    Reply(ReplyMessage),
    Test(String),
}

impl Message {
    pub fn test<S: Into<String>>(s: S) -> Self {
        Message::Test(s.into())
    }
}

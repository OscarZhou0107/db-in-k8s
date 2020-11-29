use crate::core::msql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MsqlResponse {
    BeginTx(Result<(), String>),
    Query(Result<String, String>),
    EndTx(Result<String, String>),
}

/// Message for communication to Sceduler from appserver
///
/// # Examples - Json conversion
/// ```
/// use o2versioner::comm::appserver_scheduler::Message;
/// use o2versioner::core::msql::{MsqlEndTxMode, MsqlText};
///
/// let rmt_begin_tx_str = r#"
/// {
///     "request_msql_text":{
///         "op":"begin_tx",
///         "tableops":"read table0 write table1 t2"
///     }
/// }"#;
/// let rmt_begin_tx: Message = serde_json::from_str(rmt_begin_tx_str).unwrap();
/// assert_eq!(
///     rmt_begin_tx,
///     Message::RequestMsqlText(MsqlText::BeginTx {
///         tx: None,
///         tableops: String::from("read table0 write table1 t2")
///     })
/// );
///
/// let rmt_end_tx_str = r#"
/// {   
///     "request_msql_text":{
///         "op":"end_tx",
///         "tx":"tx0",
///         "mode":"commit"
///     }
/// }"#;
/// let rmt_end_tx: Message = serde_json::from_str(rmt_end_tx_str).unwrap();
/// assert_eq!(
///     rmt_end_tx,
///     Message::RequestMsqlText(MsqlText::EndTx {
///         tx: Some(String::from("tx0")),
///         mode: MsqlEndTxMode::Commit
///     })
/// );
/// ```
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Message {
    RequestMsql(msql::Msql),
    RequestMsqlText(msql::MsqlText),
    InvalidRequest,
    InvalidMsqlText(String),
    Reply(MsqlResponse),
    Test(String),
}

impl Message {
    pub fn test<S: Into<String>>(s: S) -> Self {
        Message::Test(s.into())
    }
}

/// Unit test for `Message`
#[cfg(test)]
mod tests_message {
    use super::*;
    use crate::core::msql::*;

    #[test]
    fn test_requestmsqltext_json() {
        println!("SERIALIZE");

        let a = Message::RequestMsqlText(MsqlText::BeginTx {
            tx: None,
            tableops: String::from("read table0 write table1 t2"),
        });
        println!("{}", serde_json::to_string(&a).unwrap());

        let a = Message::RequestMsqlText(MsqlText::EndTx {
            tx: Some(String::from("tx0")),
            mode: MsqlEndTxMode::Commit,
        });
        println!("{}", serde_json::to_string(&a).unwrap());

        println!("DESERIALIZE");

        let a = r#"
        {
            "request_msql_text":{
                "op":"begin_tx",
                "tableops":"read table0 write table1 t2"
            }
        }"#;
        let b: Message = serde_json::from_str(a).unwrap();
        println!("{:?}", b);

        let a = r#"
        {   
            "request_msql_text":{
                "op":"end_tx",
                "tx":"tx0",
                "mode":"commit"
            }
        }"#;
        let b: Message = serde_json::from_str(a).unwrap();
        println!("{:?}", b);
    }
}

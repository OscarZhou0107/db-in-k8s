use super::msql_response::MsqlResponse;
use crate::core::{Msql, MsqlText};
use serde::{Deserialize, Serialize};

/// Message for communication to Sceduler from appserver
///
/// The input bytes stream should be encoded in such a way like,
/// where the first part (unsigned 4 bytes) denotes the length of data in big endian.
/// ```
/// // +---- len: u32 ----+---- data ----+
/// // | \x00\x00\x00\x0b |  hello world |
/// // +------------------+--------------+
/// ```
///
/// # Examples - Json conversion
///
/// To do a `Message::RequestMsqlText(MsqlText::BeginTx {..})`:
/// ```
/// use o2versioner::comm::scheduler_api::Message;
/// use o2versioner::core::{MsqlEndTxMode, MsqlText};
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
/// ```
///
/// To do a `Message::RequestMsqlText(MsqlText::EndTx {..})`:
/// ```
/// use o2versioner::comm::scheduler_api::Message;
/// use o2versioner::core::{MsqlEndTxMode, MsqlText};
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
///
/// To do a `Message::RequestCrash(..)`:
/// ```
/// use o2versioner::comm::scheduler_api::Message;
/// use o2versioner::core::{MsqlEndTxMode, MsqlText};
///
/// let crash_req_str = r#"
/// {
///     "request_crash":"just for fun"
/// }"#;
/// let crash_req_msg_recovered: Message = serde_json::from_str(crash_req_str).unwrap();
/// assert_eq!(
///     crash_req_msg_recovered,
///     Message::RequestCrash(String::from("just for fun"))
/// );
/// ```
#[derive(Debug, strum::AsRefStr, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Message {
    /// Request in `Msql` format
    RequestMsql(Msql),
    /// Request in `MsqlText` format
    RequestMsqlText(MsqlText),
    /// Request the Scheduler to crash with session info dumped out
    RequestCrash(String),
    /// Unimplemented yet, reserved for testing
    Test(String),
    /// Response to an invalid request, for exmample, sending `Reply(MsqlResponse)` to the Scheduler
    InvalidRequest,
    /// Invalid `MsqlText`, happens when `MsqlText` in the `RequestMsqlText` request cannot be converted into `Msql`
    InvalidMsqlText(String),
    /// Response to the incoming `RequestMsql` or `RequestMsqlText`
    Reply(MsqlResponse),
}

impl Message {
    pub fn test<S: Into<String>>(s: S) -> Self {
        Message::Test(s.into())
    }

    pub fn request_crash<S: Into<String>>(s: S) -> Self {
        Message::RequestCrash(s.into())
    }
}

/// Unit test for `Message`
#[cfg(test)]
mod tests_message {
    use super::*;
    use crate::core::MsqlEndTxMode;

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

        let a = r#"
        {   
            "request_msql_text":{
                "op":"query",
                "query":"select * from t;",
                "tableops":"read t"
            }
        }"#;
        let b: Message = serde_json::from_str(a).unwrap();
        println!("{:?}", b);
    }

    #[test]
    fn test_request_crash() {
        let crash_req_msg = Message::RequestCrash(String::from("just for fun"));
        println!("{}", serde_json::to_string(&crash_req_msg).unwrap());

        let crash_req_str = r#"
        {
            "request_crash":"just for fun"
        }"#;
        let crash_req_msg_recovered: Message = serde_json::from_str(crash_req_str).unwrap();
        println!("{:?}", crash_req_msg_recovered);

        assert_eq!(crash_req_msg_recovered, crash_req_msg);
    }
}

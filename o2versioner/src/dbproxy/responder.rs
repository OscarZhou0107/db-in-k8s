use crate::comm::{msql_response::MsqlResponse, scheduler_dbproxy::Message};

use super::core::{DbVersion, QueryResult, QueryResultType};
use futures::SinkExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{net::tcp::OwnedWriteHalf, sync::mpsc};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub struct Responder {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Responder {
    pub fn run(mut receiver: mpsc::Receiver<QueryResult>, version: Arc<Mutex<DbVersion>>, tcp_write: OwnedWriteHalf) {
        tokio::spawn(async move {
            let mut serializer = SymmetricallyFramed::new(
                FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
                SymmetricalJson::<Message>::default(),
            );

            while let Some(result) = receiver.recv().await {
                match result.result_type {
                    QueryResultType::END => {
                        version
                            .lock()
                            .await
                            .release_on_tables(result.contained_newer_versions.clone());
                    }
                    _ => {}
                }

                serializer
                    .send(Message::MsqlResponse(result.into_msql_response()))
                    .await
                    .unwrap();
            }
        });
    }
}

#[cfg(test)]
mod tests_test {
    // use super::Responder;
    // use crate::comm::scheduler_dbproxy::Message;
    // use crate::core::operation::Operation as OperationType;
    // use crate::core::transaction_version::TxTableVN;
    // use crate::dbproxy::core::{DbVersion, QueryResult};
    // use futures::prelude::*;
    // use std::{collections::HashMap, sync::Arc};
    // use tokio::net::TcpListener;
    // use tokio::net::TcpStream;
    // use tokio::sync::mpsc;
    // use tokio::sync::Mutex;
    // use tokio_serde::formats::SymmetricalJson;
    // use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    // #[tokio::test]
    // #[ignore]
    // async fn test_send_items_to_from_multiple_channel() {
    //     //Prepare - Network

    //     let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
    //         mpsc::channel(100);

    //     let mut mock_db = HashMap::new();
    //     mock_db.insert("table1".to_string(), 0);
    //     mock_db.insert("table2".to_string(), 0);
    //     let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //     //Prepare - Responder
    //     let _r = tokio::spawn(async move {
    //         println!("a-1");
    //         let addr = "127.0.0.1:2345";
    //         let listener = TcpListener::bind(addr).await.unwrap();
    //         let (tcp_stream, _) = listener.accept().await.unwrap();
    //         let (_, tcp_write) = tcp_stream.into_split();

    //         Responder::run(responder_receiver, version, tcp_write);
    //     });

    //     let result = Arc::new(Mutex::new(Vec::new()));
    //     let result_2 = Arc::clone(&result);
    //     //Action
    //     let _a = tokio::spawn(async move {
    //         let addr = "127.0.0.1:2345";
    //         let socket = TcpStream::connect(addr).await.unwrap();
    //         let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());
    //         let mut deserialize = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

    //         //Action
    //         while let Some(msg) = deserialize.try_next().await.unwrap() {
    //             match msg {
    //                 Message::SqlResponse(op) => {
    //                     result.lock().await.push(op);
    //                 }
    //                 _other => {
    //                     println!("nope");
    //                 }
    //             }
    //         }
    //     });

    //     //Prepare - Data
    //     let mock_table_vs = vec![
    //         TxTableVN {
    //             table: "table1".to_string(),
    //             vn: 0,
    //             op: OperationType::R,
    //         },
    //         TxTableVN {
    //             table: "table2".to_string(),
    //             vn: 0,
    //             op: OperationType::R,
    //         },
    //     ];
    //     let r = QueryResult {
    //         result: "r".to_string(),
    //         version_release: false,
    //         contained_newer_versions: mock_table_vs.clone(),
    //     };
    //     let worker_num: i32 = 5;

    //     //Action - Spwan worker thread to send response
    //     let _w = tokio::spawn(async move {
    //         for _ in 0..worker_num {
    //             let s = responder_sender.clone();
    //             let r_c = r.clone();
    //             tokio::spawn(async move {
    //                 let _a = s.send(r_c).await;
    //             });
    //         }
    //     });

    //     loop {
    //         if result_2.lock().await.len() == 5 {
    //             break;
    //         }
    //     }
    //     assert!(true);
    // }
}

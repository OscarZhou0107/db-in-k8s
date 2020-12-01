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
    use super::Responder;
    use crate::comm::scheduler_dbproxy::Message;
    use crate::comm::msql_response::MsqlResponse;
    use crate::core::operation::Operation as OperationType;
    use crate::core::transaction_version::TxTableVN;
    use crate::dbproxy::core::{DbVersion, QueryResult, QueryResultType};
    use futures::prelude::*;
    use std::{collections::HashMap, sync::Arc};
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;
    use tokio_serde::formats::SymmetricalJson;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    #[tokio::test]
    async fn test_send_items_to_from_multiple_channel() {

        //Prepare - Mock db related context
        let mut mock_db = HashMap::new();
        mock_db.insert("table1".to_string(), 0);
        mock_db.insert("table2".to_string(), 0);
        let mock_table_vs = vec![
            TxTableVN {
                table: "table1".to_string(),
                vn: 0,
                op: OperationType::R,
            },
            TxTableVN {
                table: "table2".to_string(),
                vn: 0,
                op: OperationType::R,
            },
        ];
        let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));
        
        //Prepare - Network
        let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
            mpsc::channel(100);

        //Prepare - Verifying queue
        let verifying_queue : Arc<Mutex<Vec<MsqlResponse>>> = Arc::new(Mutex::new(Vec::new()));
        let verifying_queue_2 = Arc::clone(&verifying_queue);

        //Prepare - Data 
        let r = QueryResult {
            result: "r".to_string(),
            succeed : true,
            result_type: QueryResultType::BEGIN,
            contained_newer_versions: mock_table_vs.clone(),
        };
        

        //Prepare - Responder
        helper_spawn_responder(version.clone(), responder_receiver);
        helper_spawn_mock_client(verifying_queue);

        let worker_num: u32 = 5;
        //Action - Spwan worker thread to send response
        helper_spawn_mock_workers(worker_num, r, responder_sender);
        loop {
            if verifying_queue_2.lock().await.len() == 5 {
                break;
            }
        }
        assert!(true);
    }

    fn helper_spawn_responder(version : Arc<Mutex<DbVersion>>, receiver :  mpsc::Receiver<QueryResult>) {
        tokio::spawn(async move {
            let addr = "127.0.0.1:2345";
            let listener = TcpListener::bind(addr).await.unwrap();
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let (_, tcp_write) = tcp_stream.into_split();

            Responder::run(receiver, version, tcp_write);
        });
    }

    fn helper_spawn_mock_client(vertifying_queue : Arc<Mutex<Vec<MsqlResponse>>>) {

        tokio::spawn(async move {
            let addr = "127.0.0.1:2345";
            let socket = TcpStream::connect(addr).await.unwrap();
            let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());
            let mut deserialize = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

            //Action
            while let Some(msg) = deserialize.try_next().await.unwrap() {
                match msg {
                    Message::MsqlResponse(op) => {
                        vertifying_queue.lock().await.push(op);
                    }
                    _other => {
                        println!("nope");
                    }
                }
            }
        });

    }

    fn helper_spawn_mock_workers(worker_num : u32, query_result : QueryResult, sender : mpsc::Sender<QueryResult>) {
        let _ = tokio::spawn(async move {
            for _ in 0..worker_num {
                let s = sender.clone();
                let r_c = query_result.clone();
                tokio::spawn(async move {
                    let _ = s.send(r_c).await;
                });
            }
        });
    }
}

use super::core::{PendingQueue, QueueMessage, Task};
use crate::{
    comm::scheduler_dbproxy::Message,
    core::msql::{Msql, MsqlEndTxMode},
};
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
use tokio::stream::StreamExt;
use tokio::sync::Mutex;
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub struct Receiver {}

impl Receiver {
    pub fn run(pending_queue: Arc<Mutex<PendingQueue>>, tcp_read: OwnedReadHalf) {
        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        tokio::spawn(async move {
            while let Some(msg) = deserializer.try_next().await.unwrap() {
                match msg {
                    Message::MsqlRequest(addr, request, version_number) => {
                        let mut operation_type = Task::BEGIN;
                        let mut query = String::new();

                        match request {
                            Msql::BeginTx(op) => {
                                operation_type = Task::BEGIN;
                            }

                            Msql::Query(op) => {
                                operation_type = Task::READ;
                                query = op.query().to_string();
                            }

                            Msql::EndTx(op) => match op.mode() {
                                MsqlEndTxMode::Commit => {
                                    operation_type = Task::COMMIT;
                                }
                                MsqlEndTxMode::Rollback => {
                                    operation_type = Task::ABORT;
                                }
                            },
                        };

                        let message = QueueMessage {
                            identifier: addr,
                            operation_type: operation_type,
                            query: query,
                            versions: version_number,
                        };

                        pending_queue.lock().await.push(message);
                    }
                    _ => println!("nope"),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests_receiver {
    // use super::Receiver;
    // use crate::comm::scheduler_dbproxy::Message;
    // use crate::core::operation::Operation as OperationType;
    // use crate::core::transaction_version::TxTableVN;
    // use crate::dbproxy::core::{Operation, PendingQueue, Task};
    // use futures::prelude::*;
    // use std::sync::Arc;
    // use tokio::net::{TcpListener, TcpStream};
    // use tokio::sync::Mutex;
    // use tokio_serde::formats::SymmetricalJson;
    // use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

    // #[tokio::test]
    // async fn test_send_single_item_to_receiver() {
    //     //Prepare - Network
    //     let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    //     let pending_queue_2 = Arc::clone(&pending_queue);

    //     //Prepare - Receiver
    //     tokio::spawn(async {
    //         let addr = "127.0.0.1:2345";
    //         let listener = TcpListener::bind(addr).await.unwrap();
    //         let (tcp_stream, _) = listener.accept().await.unwrap();
    //         let (tcp_read, _) = tcp_stream.into_split();

    //         Receiver::run(pending_queue, tcp_read);
    //     });

    //     //Action - Send item
    //     tokio::spawn(async {
    //         let addr = "127.0.0.1:2345";
    //         let mock_table_vs = vec![
    //             TxTableVN {
    //                 table: "table1".to_string(),
    //                 vn: 0,
    //                 op: OperationType::R,
    //             },
    //             TxTableVN {
    //                 table: "table2".to_string(),
    //                 vn: 0,
    //                 op: OperationType::R,
    //             },
    //         ];
    //         let socket = TcpStream::connect(addr).await.unwrap();
    //         let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
    //         let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

    //         let item = Message::SqlRequest(Operation {
    //             transaction_id: "t1".to_string(),
    //             txtablevns: mock_table_vs.clone(),
    //             task: Task::READ,
    //         });
    //         //Action
    //         serialized.send(item).await.unwrap();
    //     });

    //     //Assert
    //     loop {
    //         if pending_queue_2.lock().await.queue.len() == 1 {
    //             break;
    //         }
    //     }
    //     assert!(true);
    // }

    // #[tokio::test]
    // #[ignore]
    // async fn test_send_an_invalid_item_to_receiver_should_panic() {
    //     //Prepare - Network
    //     //PendingQueue
    //     let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    //     let pending_queue_2 = Arc::clone(&pending_queue);

    //     //Prepare - Receiver
    //     tokio::spawn(async {
    //         let addr = "127.0.0.1:2345";
    //         let listener = TcpListener::bind(addr).await.unwrap();
    //         let (tcp_stream, _) = listener.accept().await.unwrap();
    //         let (tcp_read, _) = tcp_stream.into_split();

    //         Receiver::run(pending_queue, tcp_read);
    //     });

    //     //Action - Send item
    //     tokio::spawn(async {
    //         let addr = "127.0.0.1:2345";
    //         let mock_table_vs = vec![
    //             TxTableVN {
    //                 table: "table1".to_string(),
    //                 vn: 0,
    //                 op: OperationType::R,
    //             },
    //             TxTableVN {
    //                 table: "table2".to_string(),
    //                 vn: 0,
    //                 op: OperationType::R,
    //             },
    //         ];

    //         let socket = TcpStream::connect(addr).await.unwrap();
    //         let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
    //         let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

    //         let item = Operation {
    //             transaction_id: "t1".to_string(),
    //             txtablevns: mock_table_vs.clone(),
    //             task: Task::READ,
    //         };
    //         //Action
    //         serialized.send(item).await.unwrap();
    //     });

    //     //Assert
    //     assert!(pending_queue_2.lock().await.queue.len() == 0);
    // }
}

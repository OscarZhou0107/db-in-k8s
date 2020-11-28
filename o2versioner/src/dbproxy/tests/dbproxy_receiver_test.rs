use super::Receiver;
use crate::comm::scheduler_dbproxy::Message;
use crate::core::version_number::TableVN;
use crate::dbproxy::core::{Operation, PendingQueue, Task};
use crate::msql::Operation as OperationType;
use futures::prelude::*;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

#[tokio::test(threaded_scheduler)]
//#[ignore]
async fn test_send_single_item_to_receiver() {
    //Prepare - Network
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);

    //Prepare - Receiver
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();

        Receiver::run(pending_queue, tcp_read);
    });

    //Action - Send item
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mock_table_vs = vec![
            TableVN {
                table: "table1".to_string(),
                vn: 0,
                op: OperationType::R,
            },
            TableVN {
                table: "table2".to_string(),
                vn: 0,
                op: OperationType::R,
            },
        ];
        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        let item = Message::SqlRequest(Operation {
            transaction_id: "t1".to_string(),
            table_vns: mock_table_vs.clone(),
            task: Task::READ,
        });
        //Action
        serialized.send(item).await.unwrap();
    });

    //Assert
    loop {
        if pending_queue_2.lock().unwrap().queue.len() == 1 {
            break;
        }
    }
    assert!(true);
}

#[tokio::test]
#[ignore]
async fn test_send_an_invalid_item_to_receiver_should_panic() {
    //Prepare - Network
    //PendingQueue
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);

    //Prepare - Receiver
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();

        Receiver::run(pending_queue, tcp_read);
    });

    //Action - Send item
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mock_table_vs = vec![
            TableVN {
                table: "table1".to_string(),
                vn: 0,
                op: OperationType::R,
            },
            TableVN {
                table: "table2".to_string(),
                vn: 0,
                op: OperationType::R,
            },
        ];

        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        let item = Operation {
            transaction_id: "t1".to_string(),
            table_vns: mock_table_vs.clone(),
            task: Task::READ,
        };
        //Action
        serialized.send(item).await.unwrap();
    });

    //Assert
    assert!(pending_queue_2.lock().unwrap().queue.len() == 0);
}

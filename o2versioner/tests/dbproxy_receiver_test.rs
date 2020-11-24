use futures::prelude::*;
use o2versioner::comm::scheduler_dbproxy::Message;
use o2versioner::core::sql::Operation as OperationType;
use o2versioner::core::version_number::TableVN;
use o2versioner::dbproxy::core::{Operation, Task};
use o2versioner::dbproxy::receiver::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::Notify;
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

#[tokio::test]
async fn test_send_single_item_to_receiver() {
    //Prepare - Network
    let pending_queue: Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);
    let notify = Arc::new(Notify::new());
    let notify_2 = notify.clone();

    //Prepare - Receiver
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();

        Receiver::run(pending_queue, notify, tcp_read);
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
    notify_2.notified().await;
    assert!(pending_queue_2.lock().unwrap().len() == 1);
}

#[tokio::test]
#[ignore]
async fn test_send_an_invalid_item_to_receiver_should_panic() {
    //Prepare - Network
    let pending_queue: Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);
    let notify = Arc::new(Notify::new());
    let notify_2 = notify.clone();

    //Prepare - Receiver
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();

        Receiver::run(pending_queue, notify, tcp_read);
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
    notify_2.notified().await;
    assert!(pending_queue_2.lock().unwrap().len() == 0);
}

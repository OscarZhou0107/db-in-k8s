use futures::prelude::*;
use o2versioner::core::msql::Operation as OperationType;
use o2versioner::dbproxy;
use o2versioner::dbproxy::core::{Operation, Task};
use o2versioner::{comm::scheduler_dbproxy::Message, core::version_number::TableVN};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpStream;
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[tokio::test(threaded_scheduler)]
#[ignore]
async fn test_dbproxy_end_to_end() {
    tokio::spawn(async {
        dbproxy::main("127.0.0.1:2345", "mysql://root:Rayh8768@localhost:3306/test").await;
    });

    let result = Arc::new(Mutex::new(Vec::new()));
    let result_2 = Arc::clone(&result);

    tokio::spawn(async {
        let tcp_stream = TcpStream::connect("127.0.0.1:2345").await.unwrap();
        let (tcp_read, tcp_write) = tcp_stream.into_split();

        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        let mut serializer = SymmetricallyFramed::new(
            FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        tokio::spawn(async move {
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
            let item = Message::SqlRequest(Operation {
                transaction_id: "t1".to_string(),
                tablevns: mock_table_vs.clone(),
                task: Task::READ,
            });
            //Action
            serializer.send(item).await.unwrap();
        });

        tokio::spawn(async move {
            while let Some(msg) = deserializer.try_next().await.unwrap() {
                match msg {
                    Message::SqlResponse(op) => {
                        println!("{}", op.result);
                        result.lock().unwrap().push(op);
                    }
                    _other => {
                        println!("nope");
                    }
                }
            }
        });
    });

    loop {
        if result_2.lock().unwrap().len() == 1 {
            break;
        }
    }

    assert!(true);
}

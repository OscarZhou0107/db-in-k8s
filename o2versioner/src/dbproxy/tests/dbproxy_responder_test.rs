use super::Responder;
use crate::comm::scheduler_dbproxy::Message;
use crate::core::msql::Operation as OperationType;
use crate::core::version_number::TableVN;
use crate::dbproxy::core::{DbVersion, QueryResult};
use futures::prelude::*;
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

#[tokio::test(threaded_scheduler)]
#[ignore]
async fn test_send_items_to_from_multiple_channel() {
    //Prepare - Network

    let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(), 0);
    mock_db.insert("table2".to_string(), 0);
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //Prepare - Responder
    let _r = tokio::spawn(async move {
        println!("a-1");
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (_, tcp_write) = tcp_stream.into_split();

        Responder::run(responder_receiver, version, tcp_write);
    });

    let result = Arc::new(Mutex::new(Vec::new()));
    let result_2 = Arc::clone(&result);
    //Action
    let _a = tokio::spawn(async move {
        let addr = "127.0.0.1:2345";
        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());
        let mut deserialize = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        //Action
        while let Some(msg) = deserialize.try_next().await.unwrap() {
            match msg {
                Message::SqlResponse(op) => {
                    result.lock().unwrap().push(op);
                }
                _other => {
                    println!("nope");
                }
            }
        }
    });

    //Prepare - Data
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
    let r = QueryResult {
        result: "r".to_string(),
        version_release: false,
        contained_newer_versions: mock_table_vs.clone(),
    };
    let worker_num: i32 = 5;

    //Action - Spwan worker thread to send response
    let _w = tokio::spawn(async move {
        for _ in 0..worker_num {
            let mut s = responder_sender.clone();
            let r_c = r.clone();
            tokio::spawn(async move {
                let _a = s.send(r_c).await;
            });
        }
    });

    loop {
        if result_2.lock().unwrap().len() == 5 {
            break;
        }
    }
    assert!(true);
}

#![allow(warnings)]
use o2versioner::{core::sql::Operation as OperationType, dbproxy::{receiver::Receiver, core::{DbVersion, Operation, Repository, Task, QueryResult}}};
use o2versioner::core::version_number::{TableVN, TxVN, VN};
use o2versioner::dbproxy::responder::{Responder};
use o2versioner::dbproxy::dispatcher::{Dispatcher};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use std::{collections::HashMap, sync::Mutex};
use mysql_async::prelude::*;
use tokio::{net::TcpStream, io};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::runtime::Runtime;
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};
use o2versioner::comm::scheduler_dbproxy::Message;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use futures::prelude::*;
use std::{thread, time};

#[tokio::test] 
async fn test_send_single_item_to_receiver(){

    //Prepare - Network
    let mut pending_queue:Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);
    let mut notify = Arc::new(Notify::new());
    let notify_2 = notify.clone();

    //Prepare - Receiver
    tokio::spawn(async {
        let addr = "127.0.0.1:2345";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();
    
        Receiver::run(pending_queue, 
            notify, 
            tcp_read);
    });

    //Action - Send item
    tokio::spawn( async {
        let addr = "127.0.0.1:2345";
        let mock_table_vs = vec![
            TableVN {table : "table1".to_string(), vn : 0, op : OperationType::R},
            TableVN {table : "table2".to_string(), vn : 0, op : OperationType::R},
            ];
        
        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
        let mut serialized =
                tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());
       
        let item = Message::SqlRequest(Operation {transaction_id : "t1".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ});
        //Action
        serialized
                .send(item)
                .await
                .unwrap();
    });

    //Assert
    notify_2.notified().await;
    assert!(pending_queue_2.lock().unwrap().len() == 1);
} 

#[tokio::test]
#[ignore]
async fn test_send_an_invalid_item_to_receiver_should_panic() {

        //Prepare - Network
        let mut pending_queue:Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
        let pending_queue_2 = Arc::clone(&pending_queue);
        let mut notify = Arc::new(Notify::new());
        let notify_2 = notify.clone();
    
        //Prepare - Receiver
        tokio::spawn(async {
            let addr = "127.0.0.1:2345";
            let mut listener = TcpListener::bind(addr).await.unwrap();
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let (tcp_read, _) = tcp_stream.into_split();
        
            Receiver::run(pending_queue, 
                notify, 
                tcp_read);
        });
    
        //Action - Send item
        tokio::spawn( async {
            let addr = "127.0.0.1:2345";
            let mock_table_vs = vec![
                TableVN {table : "table1".to_string(), vn : 0, op : OperationType::R},
                TableVN {table : "table2".to_string(), vn : 0, op : OperationType::R},
                ];
            
            let socket = TcpStream::connect(addr).await.unwrap();
            let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
            let mut serialized =
                    tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());
           
            let item = Operation {transaction_id : "t1".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ};
            //Action
            serialized
                    .send(item)
                    .await
                    .unwrap();
        });
    
        //Assert
        notify_2.notified().await;
        assert!(pending_queue_2.lock().unwrap().len() == 0);

}


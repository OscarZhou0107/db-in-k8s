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


#[tokio::main]
async fn main() {

    //=====================================Continue an ongoing transaction=======================================//
    //Map that holds all ongoing transactions
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(),0);
    mock_db.insert("table2".to_string(),0);
    let version: Arc<Mutex<DbVersion>>  = Arc::new(Mutex::new(DbVersion::new(mock_db)));
    let version_2 = Arc::clone(&version);

    //PendingQueue
    let mut pending_queue:Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let mut pending_queue_2:Arc<Mutex<Vec<Operation>>> = Arc::clone(&pending_queue);

    //Dispatcher & Responder
    let mut version_notify = Arc::new(Notify::new());
    let version_notify_2 = version_notify.clone();
    //Dispatcher & Main Loop
    let mut new_task_notify = Arc::new(Notify::new());
    let new_task_notify_2 = new_task_notify.clone();

    //Responder sender and receiver
    let (mut responder_sender, mut responder_receiver) : (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =  mpsc::channel(100);
      
    let url = "mysql://root:Rayh8768@localhost:3306/test"; 

        let addr = "127.0.0.1:6699";
        let mut listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, tcp_write) = tcp_stream.into_split();
  
        Dispatcher::run(pending_queue, 
                        responder_sender, 
                        url.to_string(), 
                        version,  
                        new_task_notify, 
                        version_notify, 
                        transactions);
    
        //Spwan Responder//
        Responder::run(responder_receiver,  
                       version_2, 
                version_notify_2,
                       tcp_write);

        Receiver::run(pending_queue_2, 
               new_task_notify_2, 
                      tcp_read);

}





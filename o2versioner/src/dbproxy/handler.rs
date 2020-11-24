#![allow(warnings)]
use super::core::{DbVersion, Operation, QueryResult, Repository, Task};
use super::dispatcher::Dispatcher;
use super::receiver::Receiver;
use super::responder::Responder;
use crate::comm::scheduler_dbproxy::Message;
use crate::core::sql::Operation as OperationType;
use crate::core::version_number::{TableVN, TxVN, VN};
use futures::prelude::*;
use mysql_async::prelude::*;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use std::{thread, time};
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::sync::Semaphore;
use tokio::{io, net::TcpStream};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{Framed, FramedRead, FramedWrite, LengthDelimitedCodec};

pub async fn main() {
    //=====================================Continue an ongoing transaction=======================================//
    //Map that holds all ongoing transactions
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(), 0);
    mock_db.insert("table2".to_string(), 0);
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));
    let version_2 = Arc::clone(&version);

    //PendingQueue
    let mut pending_queue: Arc<Mutex<Vec<Operation>>> = Arc::new(Mutex::new(Vec::new()));
    let mut pending_queue_2: Arc<Mutex<Vec<Operation>>> = Arc::clone(&pending_queue);

    //Dispatcher & Responder
    let mut version_notify = Arc::new(Notify::new());
    let version_notify_2 = version_notify.clone();
    //Dispatcher & Main Loop
    let mut new_task_notify = Arc::new(Notify::new());
    let new_task_notify_2 = new_task_notify.clone();

    //Responder sender and receiver
    let (mut responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let url = "mysql://root:Rayh8768@localhost:3306/test";

    let addr = "127.0.0.1:6699";
    let mut listener = TcpListener::bind(addr).await.unwrap();
    let (tcp_stream, _) = listener.accept().await.unwrap();
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    Dispatcher::run(
        pending_queue,
        responder_sender,
        url.to_string(),
        version,
        new_task_notify,
        version_notify,
        transactions,
    );

    //Spwan Responder//
    Responder::run(responder_receiver, version_2, version_notify_2, tcp_write);

    Receiver::run(pending_queue_2, new_task_notify_2, tcp_read);
}

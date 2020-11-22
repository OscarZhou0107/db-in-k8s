#![allow(warnings)]
use o2versioner::{core::sql::Operation as OperationType, dbproxy::core::{DbVersion, Operation, Repository, Task, QueryResult}};
use o2versioner::core::version_number::{TableVN, TxVN, VN};
use o2versioner::dbproxy::responder::{Responder};
use o2versioner::dbproxy::dispatcher::{Dispatcher};
use std::{collections::HashMap, sync::Mutex};
use mysql_async::prelude::*;
use tokio::io;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use std::sync::Arc;
use tokio::sync::Notify;
//use tokio::test;
use tokio::runtime::Runtime;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() {
    //let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //let (socket, _) = listener.accept().await.unwrap();
    //let (mut rd, mut wr) = io::split(socket);

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
   
    //Allowable SQL connection
    let conn_semephor = Arc::new(Semaphore::new(100));
    let conn_semephor_2 = conn_semephor.clone();
    
    let url = "mysql://root:Rayh8768@localhost:3306/test";

    //Spawn Dispatcher//
    Dispatcher::run(pending_queue, 
        responder_sender, 
        url.to_string(), 
        version, 
        conn_semephor, 
        new_task_notify, 
        version_notify, 
        transactions);

    //Spwan Responder//
    Responder::run(responder_receiver, 
        conn_semephor_2, 
        version_2, 
        version_notify_2);
       
    //Main Receiving loop//
    let mut operations = Vec::new();
    let mock_table_vs = vec![
        TableVN {table : "table1".to_string(), vn : 0, op : OperationType::R},
        TableVN {table : "table2".to_string(), vn : 0, op : OperationType::R},
        ];

    operations.push(Operation {transaction_id : "t1".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ});
    operations.push(Operation {transaction_id : "t2".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ});
    operations.push(Operation {transaction_id : "t3".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ});
    operations.push(Operation {transaction_id : "t1".to_string(), table_vns : mock_table_vs.clone(), task : Task::READ});
    
    //TBD Receive from socket//
    while !operations.is_empty() {
        pending_queue_2.lock().unwrap().push(operations.pop().unwrap());
        new_task_notify_2.notify();
    }

    //Loop//
    loop {

    }
}







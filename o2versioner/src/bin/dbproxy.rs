#![allow(warnings)]
use o2versioner::dbproxy::*;
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
//use crate::common::version_number::{TxVN, VN};

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

    //Dispatcher
    let mut version_notify = Arc::new(Notify::new());
    let version_notify_2 = version_notify.clone();
    let mut new_task_notify = Arc::new(Notify::new());
    let new_task_notify_2 = new_task_notify.clone();

    //Responder sender and receiver
    let (mut responder_sender, mut responder_receiver) : (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =  mpsc::channel(100);
   
    //Allowable SQL connection
    let conn_semephor = Arc::new(Semaphore::new(100));
    let conn_semephor_2 = conn_semephor.clone();
    
    let url = "mysql://root:Rayh8768@localhost:3306/test";

    //Spawn Dispatcher//
    tokio::spawn( async move {
        //Move global strcutures
        let mut pending_queue = pending_queue;
        let responder_sender = responder_sender;
        let pool = mysql_async::Pool::new(url);
        let mut version = version;
        let conn_semephor = conn_semephor;

        loop {
    
            wait_for_new_task_or_version_release(&mut new_task_notify, &mut version_notify).await;
            let operations = get_all_version_ready_task(&mut pending_queue, &mut version);
           
            operations
            .iter()
            .for_each(|op| {
                let op = op.clone();
                let mut lock = transactions.lock().unwrap();
                if !lock.contains_key(&op.transaction_id) {

                        let (mut ts, mut tr) : (mpsc::Sender<Operation>, mpsc::Receiver<Operation>)=  mpsc::channel(1);
        
                        lock.insert(op.transaction_id.clone(), ts);
                        let pool_clone = pool.clone();
                        let conn_semephor_clone = conn_semephor.clone();
                        let mut res_sender = responder_sender.clone();
                        
                        tokio::spawn(async move {
                            {  
                                conn_semephor_clone.acquire().await;
                                let mut conn = pool_clone.get_conn().await.unwrap();
            
                                let mut result;
                                conn.query_drop("START TRANSACTION;");
                                while let Some(operation) = tr.recv().await { 

                                    result = handle_sql_operation(&mut conn,  operation.clone()).await;
                                    
                                    match  operation.task {
                                        Task::READ => {},
                                        Task::WRITE => {},
                                        Task::COMMIT => {break},
                                        Task::ABORT => {break},

                                    }

                                    let mock_result = QueryResult {result : "r".to_string(), version_release : false};
                                    res_sender.send(mock_result).await;
                                }
                            }

                            let mock_result = QueryResult {result : "r".to_string(), version_release : true};
                            res_sender.send(mock_result).await;
                        });
                }
            });
            
            operations
            .iter()
            .for_each( |op| {
                let op = op.clone();
                let mut sender;
                {
                    sender = transactions.lock().unwrap().get(&op.transaction_id).unwrap().clone();
                }
                tokio::spawn(async move { 
                    sender.send(op).await;
                });
            });
        }
    });
 
    //Spwan Responder//
    tokio::spawn(async move {

        while let Some(result) = responder_receiver.recv().await {
            conn_semephor_2.add_permits(1);
            println!("{}", result.result);
            //Notify the dispatcher
            if result.version_release {
                let mock_table_vs = vec![
                    TableVN {table : "table1".to_string(), vn : 0, op : " ".to_string()},
                    TableVN {table : "table2".to_string(), vn : 0, op : " ".to_string()},
                ];
                let mock_transaction = TxVN {tx_name : "t1".to_string(), table_vns : mock_table_vs};
                version_2.lock().unwrap().release(mock_transaction);
                version_notify_2.notify();
            }
            //TBD Reply to socket//
        }
        
    });
   
    //Main Receiving loop//

    let mut operations = Vec::new();
    let mock_table_vs = vec![
        TableVN {table : "table1".to_string(), vn : 0, op : " ".to_string()},
        TableVN {table : "table2".to_string(), vn : 0, op : " ".to_string()},
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

fn get_all_version_ready_task(pending_queue : &mut Arc<Mutex<Vec<Operation>>>, version : &mut Arc<Mutex<DbVersion>>) -> Vec<Operation> {
    let mut lock = pending_queue.lock().unwrap();

    //Unstable feature
    // lock
    // .drain_filter(|op| {
    //         version.lock().unwrap().violate_version(op.clone())
    //     })
    // .collect()

    let ready_ops = lock
    .split(|op| {
        version.lock().unwrap().violate_version(op.clone())
    })
    .fold(Vec::new(),| mut acc_ops , ops|  {
        ops.iter().for_each(|op| {
            acc_ops.push(op.clone());
        });
        acc_ops
    });

    lock.retain(|op| 
        version.lock().unwrap().violate_version(op.clone())
    );
    ready_ops
}

async fn wait_for_new_task_or_version_release(new_task_notify : &mut Arc<Notify>, version_notify : &mut Arc<Notify>){
    let n1 = new_task_notify.notified();
    let n2 = version_notify.notified();

    tokio::select! { 
        _ = n1 => {}
        _ = n2 => {}
     };
}

async fn handle_sql_operation(conn : &mut mysql_async::Conn, operation : Operation) -> () {
    conn.query_drop("INSERT INTO cats (name, owner, birth) VALUES ('haha2', 'hehe2', CURDATE())").await.unwrap()
}

#[derive(Clone)]
enum Task {
    READ,
    WRITE,
    ABORT,
    COMMIT,
}

struct QueryResult {
    result : String,
    version_release : bool,
}

struct PendingQueue {
    
}

struct DbVersion {
    table_versions : HashMap<String, u64>
}

impl DbVersion {
    fn new(table_versions : HashMap<String, u64>) -> DbVersion{
        DbVersion {table_versions : table_versions }
    }

    pub fn release(&mut self, transaction_version : TxVN) {
        transaction_version.table_vns
        .iter()
        .for_each(|t|{
            match self.table_versions.get_mut(&t.table) {
                Some(v) => *v = t.vn + 1,
                None => println!("Table {} not found to release version.",t.table),
            }       
        })
    }

    pub fn violate_version(& self, transaction_version : Operation) -> bool {
        transaction_version.table_vns
        .iter()
        .any(|t|{
            if let Some(v) = self.table_versions.get(&t.table) {
                return *v < t.vn; 
            } else {
                return true;
            }})
    }
}

/// Version number
pub type VN = u64;

#[derive(Clone)]
struct Operation {
    transaction_id : String,
    task : Task,
    table_vns: Vec<TableVN>,
}

/// Version number of a table
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TableVN {
    pub table: String,
    pub vn: VN,
    pub op: String,
}

/// Version numbers of tables declared by a transaction
#[derive(Default, Debug, Eq, PartialEq)]
pub struct TxVN {
    pub tx_name: String,
    // A single vec storing all W and R `TableVN` for now
    pub table_vns: Vec<TableVN>,
}
#[test]
fn voilate_dbversion_should_return_true() {
    //Prepare 
    let mut table_versions = HashMap::new();
    table_versions.insert("table1".to_string(), 0);
    table_versions.insert("table2".to_string(), 0);
    let db_version = DbVersion {table_versions : table_versions};
    let versions = vec![
        TableVN {table : "table1".to_string(), vn : 0, op : " ".to_string()},
        TableVN {table : "table2".to_string(), vn : 1, op : " ".to_string()},
        ];
    let operation = Operation { table_vns : versions, transaction_id : "t1".to_string() , task : Task::READ};
    //Action
    //Assert
    assert!(db_version.violate_version(operation));
}

#[test]
fn obey_dbversion_should_return_false() {
    //Prepare 
    let mut table_versions = HashMap::new();
    table_versions.insert("table1".to_string(), 0);
    table_versions.insert("table2".to_string(), 0);
    let db_version = DbVersion {table_versions : table_versions};
    let versions = vec![
        TableVN {table : "table1".to_string(), vn : 0, op : " ".to_string()},
        TableVN {table : "table2".to_string(), vn : 0, op : " ".to_string()},
        ];
    let operation = Operation { table_vns : versions, transaction_id : "t1".to_string() , task : Task::READ};
    //Action
    //Assert
    assert!(!db_version.violate_version(operation));
}

#[test]
fn test_sql_connection() {

    let mut rt = Runtime::new().unwrap();
    

    rt.block_on(async  {
        let url = "mysql://root:Rayh8768@localhost:3306/test";
        let pool = mysql_async::Pool::new(url);
    
        match pool.get_conn().await {
            Ok(_) => {println!{"OK"}},
            Err(e) => {println!("error is =============================== : {}",e)}
        };
    })

}

#[test]
fn run_sql_query() {

    let mut rt = Runtime::new().unwrap();
    
    rt.block_on(async  {
        let url = "mysql://root:Rayh8768@localhost:3306/test";
        let pool = mysql_async::Pool::new(url);
    
        let mut conn = pool.get_conn().await.unwrap(); 

        let mut raw = conn.query_iter("select * from cats").await.unwrap();
        let results: Vec<mysql_async::Row>= raw.collect().await.unwrap();

        results.iter().for_each(|r| {
            r.columns().first().unwrap();
        });
    })

}




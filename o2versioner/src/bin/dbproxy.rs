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

#[tokio::main]
async fn main() {
    //let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //let (socket, _) = listener.accept().await.unwrap();
    //let (mut rd, mut wr) = io::split(socket);

    //=====================================Continue an ongoing transaction=======================================//
    //Map that holds all ongoing transactions
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let version: Arc<Mutex<u64>>  = Arc::new(Mutex::new(0));
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
        let conn_semephor = conn_semephor;

        loop {
    
            wait_for_new_task_or_version_release(&mut new_task_notify, &mut version_notify).await;
            let operation = get_all_version_ready_task(&mut pending_queue);
            let t_id = operation.transaction_id.clone();

            {
                let mut lock = transactions.lock().unwrap();
                if !lock.contains_key(&t_id) {

                        let (mut ts, mut tr) : (mpsc::Sender<Operation>, mpsc::Receiver<Operation>)=  mpsc::channel(1);
        
                        lock.insert(t_id.clone(), ts);
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
            }
            
            let mut sender;
            {
                sender = transactions.lock().unwrap().get(&t_id).unwrap().clone();
            }

            sender.send(operation).await;
        }
    });
 
    //Spwan Responder//
    tokio::spawn(async move {

        while let Some(result) = responder_receiver.recv().await {
            conn_semephor_2.add_permits(1);
            println!("{}", result.result);
            //Notify the dispatcher
            if result.version_release {
                *(version_2.lock().unwrap()) += 1;
                version_notify_2.notify();
            }
            //TBD Reply to socket//
        }
        
    });
   
    //Main Receiving loop//
    let mut operations = Vec::new();
    operations.push(Operation {transaction_id : "t1".to_string(), version_number : 0, task : Task::READ});
    operations.push(Operation {transaction_id : "t2".to_string(), version_number : 0, task : Task::READ});
    operations.push(Operation {transaction_id : "t3".to_string(), version_number : 0, task : Task::READ});
    operations.push(Operation {transaction_id : "t1".to_string(), version_number : 0, task : Task::READ});
    
    //TBD Receive from socket//
    while !operations.is_empty() {
        pending_queue_2.lock().unwrap().push(operations.pop().unwrap());
        new_task_notify_2.notify();
    }

    //Loop//
    loop {

    }
}


//TBD
fn get_all_version_ready_task(pending_queue : &mut Arc<Mutex<Vec<Operation>>>) -> Operation {
    pending_queue.lock().unwrap().pop().unwrap()
}

//TBD
fn check_version(operation_version: u64, global_version : & Mutex<u64>) -> bool
{true}

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
struct Operation {
    transaction_id : String,
    version_number : u64,
    task : Task,
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




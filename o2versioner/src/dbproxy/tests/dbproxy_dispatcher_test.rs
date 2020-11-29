use super::Dispatcher;
use crate::core::msql::Operation as OperationType;
use crate::core::transaction_version::TxTableVN;
use crate::dbproxy::core::{DbVersion, Operation, PendingQueue, QueryResult, Task};
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;

#[tokio::test(threaded_scheduler)]
#[ignore]
async fn test_receive_response_from_new_transactions() {
    //Prepare - Network
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));
    let transactions_2 = Arc::clone(&transactions);

    //Global version//
    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(), 0);
    mock_db.insert("table2".to_string(), 0);
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //PendingQueue
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);
    //Responder sender and receiver
    let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);
    Dispatcher::run(
        pending_queue,
        responder_sender,
        "mysql://root:Rayh8768@localhost:3306/test".to_string(),
        version,
        transactions,
    );

    let mut mock_vs = Vec::new();
    mock_vs.push(TxTableVN {
        table: "table2".to_string(),
        vn: 0,
        op: OperationType::R,
    });
    mock_vs.push(TxTableVN {
        table: "table1".to_string(),
        vn: 0,
        op: OperationType::R,
    });

    let mut mock_ops = Vec::new();
    mock_ops.push(Operation {
        transaction_id: "t1".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t2".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t3".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t4".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });

    while !mock_ops.is_empty() {
        pending_queue_2.lock().unwrap().push(mock_ops.pop().unwrap());
    }

    let mut task_num: u64 = 0;
    while let Some(_) = responder_receiver.recv().await {
        task_num += 1;
        if task_num == 4 {
            break;
        }
    }
    assert!(transactions_2.lock().unwrap().len() == 4);
}

#[tokio::test(threaded_scheduler)]
#[ignore]
async fn test_receive_response_from_same_transactions() {
    //Prepare - Network
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));
    let transactions_2 = Arc::clone(&transactions);

    //Global version//
    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(), 0);
    mock_db.insert("table2".to_string(), 0);
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //PendingQueue
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2 = Arc::clone(&pending_queue);

    //Responder sender and receiver
    let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);
    Dispatcher::run(
        pending_queue,
        responder_sender,
        "mysql://root:Rayh8768@localhost:3306/test".to_string(),
        version,
        transactions,
    );

    let mut mock_vs = Vec::new();
    mock_vs.push(TxTableVN {
        table: "table2".to_string(),
        vn: 0,
        op: OperationType::R,
    });
    mock_vs.push(TxTableVN {
        table: "table1".to_string(),
        vn: 0,
        op: OperationType::R,
    });

    let mut mock_ops = Vec::new();
    mock_ops.push(Operation {
        transaction_id: "t1".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t2".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t3".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });
    mock_ops.push(Operation {
        transaction_id: "t1".to_string(),
        task: Task::READ,
        txtablevns: mock_vs.clone(),
    });

    while !mock_ops.is_empty() {
        pending_queue_2.lock().unwrap().push(mock_ops.pop().unwrap());
    }

    let mut task_num: u64 = 0;
    while let Some(_) = responder_receiver.recv().await {
        task_num += 1;
        if task_num == 4 {
            break;
        }
    }
    assert!(transactions_2.lock().unwrap().len() == 3);
}

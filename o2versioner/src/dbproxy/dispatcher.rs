use super::core::{
    DbVersion, PendingQueue, PostgreToCsvWriter, PostgresSqlConnPool, QueryResult, QueryResultType, QueueMessage, Task,
};
use crate::core::transaction_version::{TxTableVN, TxVN};
use std::collections::HashMap;
use std::sync::Arc;
//use mysql_async::Pool;
use std::net::SocketAddr;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::{stream, sync::mpsc};

use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use tokio_postgres::NoTls;

pub struct Dispatcher {}

impl Dispatcher {
    pub fn run(
        pending_queue: Arc<Mutex<PendingQueue>>,
        sender: mpsc::Sender<QueryResult>,
        sql_addr: String,
        mut version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>>,
    ) {
        tokio::spawn(async move {
            let mut task_notify = pending_queue.lock().await.get_notify();
            let mut version_notify = version.lock().await.get_notify();

            let mut config = tokio_postgres::Config::new();
            config.user("postgres");
            config.password("Rayh8768");
            config.host("localhost");
            config.port(5432);
            config.dbname("Test");

            let manager = PostgresConnectionManager::new(config, NoTls);
            let pool = Pool::builder().max_size(50).build(manager).await.unwrap();

            loop {
                Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                let operations = pending_queue
                    .lock()
                    .await
                    .get_all_version_ready_task(&mut version)
                    .await;

                {
                    let mut lock = transactions.lock().await;

                    operations.iter().for_each(|op| {
                        let op_cloned = op.clone();
                        let pool_cloned = pool.clone();
                        let sender_cloned = sender.clone();

                        match op_cloned.operation_type {
                            Task::BEGIN => {
                                if !lock.contains_key(&op_cloned.identifier) {
                                    let (ts, tr): (mpsc::Sender<QueueMessage>, mpsc::Receiver<QueueMessage>) =
                                        mpsc::channel(1);
                                    lock.insert(op_cloned.identifier.clone(), ts);
                                    Self::spawn_transaction(pool_cloned, tr, sender_cloned);
                                };
                            }
                            _ => {}
                        };
                    });
                }
                {
                    let lock = transactions.lock().await;

                    operations.iter().for_each(|op| {
                        let op_cloned = op.clone();
                        let sender_cloned = lock.get(&op_cloned.identifier).unwrap().clone();

                        Self::spawn_unblock_send(sender_cloned, op_cloned);
                    });
                }
            }
        });
    }

    fn spawn_transaction(
        pool: Pool<PostgresConnectionManager<NoTls>>,
        mut rec: mpsc::Receiver<QueueMessage>,
        sender: mpsc::Sender<QueryResult>,
    ) {
        tokio::spawn(async move {
            let mut result: QueryResult;
            {
                let mut finish = false;
                let conn = pool.get().await.unwrap();
                while let Some(operation) = rec.recv().await {
                    match operation.operation_type {
                        Task::BEGIN => {
                            result = prepare_query_result(
                                operation.operation_type,
                                operation.versions,
                                conn.simple_query("START TRANSACTION;").await,
                            );
                        }
                        Task::READ => {
                            result = prepare_query_result(
                                operation.operation_type,
                                operation.versions,
                                conn.simple_query(&operation.query).await,
                            );
                        }
                        Task::WRITE => {
                            result = prepare_query_result(
                                operation.operation_type,
                                operation.versions,
                                conn.simple_query(&operation.query).await,
                            );
                        }
                        Task::COMMIT => {
                            result = prepare_query_result(
                                operation.operation_type,
                                operation.versions,
                                conn.simple_query("COMMIT;").await,
                            );
                            finish = true;
                        }
                        Task::ABORT => {
                            result = prepare_query_result(
                                operation.operation_type,
                                operation.versions,
                                conn.simple_query("ROLLBACK;").await,
                            );
                            finish = true;
                        }
                    }

                    let _ = sender.send(result.clone()).await;
                    if finish {
                        break;
                    }
                }
            }
        });
    }

    fn spawn_unblock_send(sender: mpsc::Sender<QueueMessage>, op: QueueMessage) {
        tokio::spawn(async move {
            let _ = sender.send(op).await;
        });
    }

    async fn wait_for_new_task_or_version_release(new_task_notify: &mut Arc<Notify>, version_notify: &mut Arc<Notify>) {
        let n1 = new_task_notify.notified();
        let n2 = version_notify.notified();
        tokio::select! {
           _ = n1 => {}
           _ = n2 => {}
        };
    }
}

fn prepare_query_result(
    mode: Task,
    transaction_version: Option<TxVN>,
    raw: Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::error::Error>,
) -> QueryResult {
    let mut result_type;
    let mut contained_newer_versions = Vec::new();
    match mode {
        Task::BEGIN => {
            result_type = QueryResultType::BEGIN;
            match transaction_version {
                Some(versions) => {
                    contained_newer_versions = versions.txtablevns;
                }
                None => {}
            }
        }
        Task::READ => {
            result_type = QueryResultType::QUERY;
        }
        Task::WRITE => {
            result_type = QueryResultType::QUERY;
        }
        Task::COMMIT => {
            result_type = QueryResultType::END;
        }
        Task::ABORT => {
            result_type = QueryResultType::END;
        }
    };

    let result;
    let succeed;
    let writer = PostgreToCsvWriter::new(mode);
    match raw {
        Ok(message) => {
            result = writer.to_csv(message);
            succeed = true;
        }
        Err(err) => {
            result = "There was an error".to_string();
            succeed = false;
        }
    }

    QueryResult {
        result: result,
        result_type: QueryResultType::BEGIN,
        succeed: succeed,
        contained_newer_versions: contained_newer_versions,
    }
}

#[cfg(test)]
mod tests_dispatcher {
    // use super::Dispatcher;
    // use crate::core::operation::Operation as OperationType;
    // use crate::core::transaction_version::TxTableVN;
    // use crate::dbproxy::core::{DbVersion, Operation, PendingQueue, QueryResult, Task};
    // use std::{collections::HashMap, sync::Arc};
    // use tokio::sync::mpsc;
    // use tokio::sync::Mutex;

    // #[tokio::test]
    // #[ignore]
    // async fn test_receive_response_from_new_transactions() {
    //     //Prepare - Network
    //     let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));
    //     let transactions_2 = Arc::clone(&transactions);

    //     //Global version//
    //     let mut mock_db = HashMap::new();
    //     mock_db.insert("table1".to_string(), 0);
    //     mock_db.insert("table2".to_string(), 0);
    //     let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //     //PendingQueue
    //     let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    //     let pending_queue_2 = Arc::clone(&pending_queue);
    //     //Responder sender and receiver
    //     let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
    //         mpsc::channel(100);
    //     Dispatcher::run(
    //         pending_queue,
    //         responder_sender,
    //         "mysql://root:Rayh8768@localhost:3306/test".to_string(),
    //         version,
    //         transactions,
    //     );

    //     let mut mock_vs = Vec::new();
    //     mock_vs.push(TxTableVN {
    //         table: "table2".to_string(),
    //         vn: 0,
    //         op: OperationType::R,
    //     });
    //     mock_vs.push(TxTableVN {
    //         table: "table1".to_string(),
    //         vn: 0,
    //         op: OperationType::R,
    //     });

    //     let mut mock_ops = Vec::new();
    //     mock_ops.push(Operation {
    //         transaction_id: "t1".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t2".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t3".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t4".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });

    //     while !mock_ops.is_empty() {
    //         pending_queue_2.lock().await.push(mock_ops.pop().unwrap());
    //     }

    //     let mut task_num: u64 = 0;
    //     while let Some(_) = responder_receiver.recv().await {
    //         task_num += 1;
    //         if task_num == 4 {
    //             break;
    //         }
    //     }
    //     assert!(transactions_2.lock().await.len() == 4);
    // }

    // #[tokio::test]
    // #[ignore]
    // async fn test_receive_response_from_same_transactions() {
    //     //Prepare - Network
    //     let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));
    //     let transactions_2 = Arc::clone(&transactions);

    //     //Global version//
    //     let mut mock_db = HashMap::new();
    //     mock_db.insert("table1".to_string(), 0);
    //     mock_db.insert("table2".to_string(), 0);
    //     let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));

    //     //PendingQueue
    //     let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    //     let pending_queue_2 = Arc::clone(&pending_queue);

    //     //Responder sender and receiver
    //     let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
    //         mpsc::channel(100);
    //     Dispatcher::run(
    //         pending_queue,
    //         responder_sender,
    //         "mysql://root:Rayh8768@localhost:3306/test".to_string(),
    //         version,
    //         transactions,
    //     );

    //     let mut mock_vs = Vec::new();
    //     mock_vs.push(TxTableVN {
    //         table: "table2".to_string(),
    //         vn: 0,
    //         op: OperationType::R,
    //     });
    //     mock_vs.push(TxTableVN {
    //         table: "table1".to_string(),
    //         vn: 0,
    //         op: OperationType::R,
    //     });

    //     let mut mock_ops = Vec::new();
    //     mock_ops.push(Operation {
    //         transaction_id: "t1".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t2".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t3".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });
    //     mock_ops.push(Operation {
    //         transaction_id: "t1".to_string(),
    //         task: Task::READ,
    //         txtablevns: mock_vs.clone(),
    //     });

    //     while !mock_ops.is_empty() {
    //         pending_queue_2.lock().await.push(mock_ops.pop().unwrap());
    //     }

    //     let mut task_num: u64 = 0;
    //     while let Some(_) = responder_receiver.recv().await {
    //         task_num += 1;
    //         if task_num == 4 {
    //             break;
    //         }
    //     }
    //     assert!(transactions_2.lock().await.len() == 3);
    // }
}

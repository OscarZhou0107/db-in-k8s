use super::core::{DbVersion, PendingQueue, PostgresSqlConnPool, QueryResult, QueryResultType, QueueMessage, Task};
use std::collections::HashMap;
use std::sync::Arc;
//use mysql_async::Pool;
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
        transactions: Arc<Mutex<HashMap<String, mpsc::Sender<QueueMessage>>>>,
    ) {
        tokio::spawn(async move {
            let pool = mysql_async::Pool::new(sql_addr);
            let mut task_notify = pending_queue.lock().await.get_notify();
            let mut version_notify = version.lock().await.get_notify();

            let mut config = tokio_postgres::Config::new();
            config.user("postgres");
            config.password("Rayh8768");
            config.host("localhost");
            config.port(5432);

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
        mut sender: mpsc::Sender<QueryResult>,
    ) {
        tokio::spawn(async move {
            let conn = pool.get().await.unwrap();

            let mut result: QueryResult = QueryResult {
                result : " ".to_string(),
                result_type : QueryResultType::BEGIN,
                succeed : true,
                contained_newer_versions : Vec::new(),
            };

            let r_ref = &mut result;
            {
                while let Some(operation) = rec.recv().await {
                    match operation.operation_type {
                        Task::READ => {
                            let r = conn.simple_query("").await;
                        }
                        Task::WRITE => {}
                        Task::COMMIT => {
                            break;
                        }
                        Task::ABORT => {
                            break;
                        }
                        Task::BEGIN => {}
                    }
                    let inner_result: QueryResult = QueryResult {
                        result : " ".to_string(),
                        result_type : QueryResultType::BEGIN,
                        succeed : true,
                        contained_newer_versions : Vec::new(),
                    };
                    let _ = sender.send(inner_result.clone()).await;
                }
            }
            let _ = sender.send(result).await;
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

#[cfg(test)]
#[path = "tests/dbproxy_dispatcher_test.rs"]
mod dispatcher_test;

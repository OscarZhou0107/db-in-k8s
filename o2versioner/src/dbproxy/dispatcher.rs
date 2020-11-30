use super::core::{DbVersion, Operation, PendingQueue, QueryResult, Repository, Task};
use futures::prelude::*;
use mysql_async::Pool;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::Notify;

pub struct Dispatcher {}

impl Dispatcher {
    pub fn run(
        pending_queue: Arc<Mutex<PendingQueue>>,
        sender: mpsc::Sender<QueryResult>,
        sql_addr: String,
        mut version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>>,
    ) {
        tokio::spawn(async move {
            let pool = mysql_async::Pool::new(sql_addr);
            let mut task_notify = pending_queue.lock().await.get_notify();
            let mut version_notify = version.lock().await.get_notify();

            loop {
                Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                let operations = pending_queue
                    .lock()
                    .await
                    .get_all_version_ready_task(&mut version)
                    .await;
                stream::iter(operations.iter()).for_each(|op| {
                    let op_cloned = op.clone();
                    let transactions_cloned = transactions.clone();
                    let pool_cloned = pool.clone();
                    let sender_cloned = sender.clone();
                    async move {
                        let mut lock = transactions_cloned.lock().await;
                        if !lock.contains_key(&op_cloned.transaction_id) {
                            let (ts, tr): (mpsc::Sender<Operation>, mpsc::Receiver<Operation>) = mpsc::channel(1);
                            lock.insert(op_cloned.transaction_id.clone(), ts);
                            Self::spawn_transaction(pool_cloned, tr, sender_cloned);
                        }
                    }
                }).await;

                stream::iter(operations.iter()).for_each(|op| {
                    let op_cloned = op.clone();
                    let transactions_cloned = transactions.clone();
                    async move {
                        let sender = transactions_cloned
                            .lock()
                            .await
                            .get(&op_cloned.transaction_id)
                            .unwrap()
                            .clone();

                        Self::spawn_unblock_send(sender, op_cloned);
                    }
                }).await;
            }
        });
    }

    fn spawn_transaction(pool: Pool, mut rec: mpsc::Receiver<Operation>, sender: mpsc::Sender<QueryResult>) {
        tokio::spawn(async move {
            let mut result: QueryResult = Default::default();
            let r_ref = &mut result;
            {
                let mut repo = Repository::new(pool).await;
                repo.start_transaction().await;

                while let Some(operation) = rec.recv().await {
                    let inner_result;
                    match operation.task {
                        Task::READ => {
                            inner_result = repo.execute_read().await;
                        }
                        Task::WRITE => {
                            inner_result = repo.execute_write().await;
                        }
                        Task::COMMIT => {
                            *r_ref = repo.commit().await;
                            break;
                        }
                        Task::ABORT => {
                            *r_ref = repo.abort().await;
                            break;
                        }
                    }
                    let _ = sender.send(inner_result.clone()).await;
                }
            }
            let _ = sender.send(result).await;
        });
    }

    fn spawn_unblock_send(sender: mpsc::Sender<Operation>, op: Operation) {
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

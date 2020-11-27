use super::core::{DbVersion, Operation, QueryResult, Repository, Task, PendingQueue};
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::mpsc;
use tokio::sync::Notify;

pub struct Dispatcher {}

impl Dispatcher {
    pub fn run(
        mut pending_queue: Arc<Mutex<PendingQueue>>,
        sender: mpsc::Sender<QueryResult>,
        sql_url: String,
        mut version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>>,
    ) {
        tokio::spawn(async move {
            let pool = mysql_async::Pool::new(sql_url);
            let mut task_notify = pending_queue.lock().unwrap().get_notify();
            let mut version_notify = version.lock().unwrap().get_notify();

            loop {
                Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                let operations = pending_queue.lock().unwrap().get_all_version_ready_task(&mut version);
                operations.iter().for_each(|op| {

                    let op = op.clone();
                    let mut lock = transactions.lock().unwrap();
                    if !lock.contains_key(&op.transaction_id) {
                        
                        let (ts, mut tr): (mpsc::Sender<Operation>, mpsc::Receiver<Operation>) = mpsc::channel(1);
                        lock.insert(op.transaction_id.clone(), ts);

                        let pool_clone = pool.clone();
                        let mut sender_clone = sender.clone();

                        tokio::spawn(async move {
                            #[allow(unused_assignments)]
                            let mut result: QueryResult = Default::default();
                            let r_ref = &mut result;
                            {
                                let mut repo = Repository::new(pool_clone).await;
                                repo.start_transaction().await;

                                while let Some(operation) = tr.recv().await {
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
                                    #[allow(unused_must_use)]
                                    {
                                        sender_clone.send(inner_result.clone()).await;
                                    }
                                }
                            }
                            #[allow(unused_must_use)]
                            {
                                sender_clone.send(result).await;
                            }
                        });
                    }
                });

                operations.iter().for_each(|op| {
                    let op = op.clone();
                    let mut sender;
                    {
                        sender = transactions.lock().unwrap().get(&op.transaction_id).unwrap().clone();
                    }
                    tokio::spawn(async move {
                        #[allow(unused_must_use)]
                        {
                            sender.send(op).await;
                        }
                    });
                });
            }
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
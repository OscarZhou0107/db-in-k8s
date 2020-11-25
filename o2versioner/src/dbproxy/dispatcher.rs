use super::core::{DbVersion, Operation, QueryResult, Repository, Task};
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use tokio::sync::mpsc;
use tokio::sync::Notify;

pub struct Dispatcher {}

impl Dispatcher {
    pub fn run(
        mut pending_queue: Arc<Mutex<Vec<Operation>>>,
        sender: mpsc::Sender<QueryResult>,
        sql_url: String,
        mut version: Arc<Mutex<DbVersion>>,
        mut task_notify: Arc<Notify>,
        mut version_notify: Arc<Notify>,
        transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>>,
    ) {
        tokio::spawn(async move {
            let pool = mysql_async::Pool::new(sql_url);
            loop {
                wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                let operations = get_all_version_ready_task(&mut pending_queue, &mut version);
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
}

async fn wait_for_new_task_or_version_release(new_task_notify: &mut Arc<Notify>, version_notify: &mut Arc<Notify>) {
    let n1 = new_task_notify.notified();
    let n2 = version_notify.notified();

    tokio::select! {
       _ = n1 => {}
       _ = n2 => {}
    };
}

fn get_all_version_ready_task(
    pending_queue: &mut Arc<Mutex<Vec<Operation>>>,
    version: &mut Arc<Mutex<DbVersion>>,
) -> Vec<Operation> {
    let mut lock = pending_queue.lock().unwrap();

    let ready_ops = lock
        .split(|op| version.lock().unwrap().violate_version(op.clone()))
        .fold(Vec::new(), |mut acc_ops, ops| {
            ops.iter().for_each(|op| {
                acc_ops.push(op.clone());
            });
            acc_ops
        });

    lock.retain(|op| version.lock().unwrap().violate_version(op.clone()));
    ready_ops
}

#[cfg(test)]
#[path = "tests/dbproxy_dispatcher_test.rs"]
mod dispatcher_test;
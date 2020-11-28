use super::core::{DbVersion, Operation, QueryResult, Repository, Task, PendingQueue};
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use mysql_async::Pool;
use tokio::sync::mpsc;
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
            let mut task_notify = pending_queue.lock().unwrap().get_notify();
            let mut version_notify = version.lock().unwrap().get_notify();

            loop {
                Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                let operations = pending_queue.lock().unwrap().get_all_version_ready_task(&mut version);
                operations.iter().for_each(|op| {

                    let op = op.clone();
                    let mut lock = transactions.lock().unwrap();
                    if !lock.contains_key(&op.transaction_id) {
                        
                        let (ts, tr): (mpsc::Sender<Operation>, mpsc::Receiver<Operation>) = mpsc::channel(1);
                        lock.insert(op.transaction_id.clone(), ts);
                        Self::spawn_transaction(pool.clone(), tr, sender.clone());
                  
                    }
                });

                operations.iter().for_each(|op| {
                    let op = op.clone();
                    let sender;
                    {
                        sender = transactions.lock().unwrap().get(&op.transaction_id).unwrap().clone();
                    }
                    
                    Self::spawn_unblock_send(sender, op);
                });
            }
        });
    }

    fn spawn_transaction(pool : Pool, mut rec : mpsc::Receiver<Operation>, mut sender : mpsc::Sender<QueryResult>){
        
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

    fn spawn_unblock_send(mut sender : mpsc::Sender<Operation>, op : Operation){
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
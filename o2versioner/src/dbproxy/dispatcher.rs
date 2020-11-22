use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use std::{sync::Mutex, collections::HashMap};
use std::sync::Arc;
use tokio::sync::Notify;
use super::core::{QueryResult, DbVersion, Task, Operation, Repository};

pub struct Dispatcher {}

impl Dispatcher {
    
    pub fn run(mut pending_queue : Arc<Mutex<Vec<Operation>>>,
            sender : mpsc::Sender<QueryResult>,
            sql_url : String,
            mut version: Arc<Mutex<DbVersion>>,
            semaphor : Arc<Semaphore>,
            mut task_notify : Arc<Notify>,
            mut version_notify : Arc<Notify>,
            transactions : Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>>,
    ) {
        tokio::spawn( async move {
            let pool = mysql_async::Pool::new(sql_url);
    
            loop {
        
                wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;
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
                            let semaphor_clone = semaphor.clone();
                            let mut sender_clone = sender.clone();
                            
                            tokio::spawn(async move {
                                let mut result : QueryResult = Default::default();
                                {  
                                    semaphor_clone.acquire().await;

                                    let mut repo = Repository::new(pool_clone).await;
                                    repo.start_transaction().await;

                                    while let Some(operation) = tr.recv().await { 
                                        match  operation.task {
                                            Task::READ => {result = repo.execute_read().await;},
                                            Task::WRITE => {result = repo.execute_write().await;},
                                            Task::COMMIT => {result = repo.commit().await; break},
                                            Task::ABORT => {result = repo.abort().await; break},
                                        }
                                        sender_clone.send(result).await;
                                    }
                                }
                                //sender_clone.send(result).await;
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
    }

}

async fn wait_for_new_task_or_version_release(new_task_notify : &mut Arc<Notify>, version_notify : &mut Arc<Notify>){
    let n1 = new_task_notify.notified();
    let n2 = version_notify.notified();

    tokio::select! { 
        _ = n1 => {}
        _ = n2 => {}
     };
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
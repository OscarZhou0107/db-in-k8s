use super::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
use futures::StreamExt;
use mpsc::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Notify;
use tokio::{stream, sync::Mutex};
use tokio_postgres::NoTls;

pub struct Dispatcher {}

impl Dispatcher {
    pub fn run(
        pending_queue: Arc<Mutex<PendingQueue>>,
        sender: mpsc::Sender<QueryResult>,
        config: tokio_postgres::Config,
        mut version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>>,
    ) {
        tokio::spawn(async move {
            let mut task_notify = pending_queue.lock().await.get_notify();
            let mut version_notify = version.lock().await.get_notify();

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
                                    let (ts, tr): (Sender<QueueMessage>, Receiver<QueueMessage>) = mpsc::channel(1);
                                    lock.insert(op_cloned.identifier.clone(), ts);
                                    Self::spawn_transaction(pool_cloned, tr, sender_cloned);
                                };
                            }
                            _ => {}
                        };
                    });
                }
                let senders: Arc<Mutex<HashMap<SocketAddr, Sender<QueueMessage>>>> =
                    Arc::new(Mutex::new(HashMap::new()));
                {
                    let lock = transactions.lock().await;
                    let mut lock_2 = senders.lock().await;

                    operations.iter().for_each(|op| {
                        if !lock_2.contains_key(&op.identifier) {
                            lock_2.insert(op.identifier.clone(), lock.get(&op.identifier).unwrap().clone());
                        }
                    });
                }

                {
                    let lock = senders.lock().await;
                    stream::iter(operations)
                        .for_each(|op| async {
                            let op = op;
                            lock.get(&op.clone().identifier)
                                .unwrap()
                                .send(op.clone())
                                .await
                                .map_err(|e| e.to_string())
                                .unwrap();
                        })
                        .await;
                }
            }
        });
    }

    fn spawn_transaction(
        pool: Pool<PostgresConnectionManager<NoTls>>,
        mut rec: Receiver<QueueMessage>,
        sender: Sender<QueryResult>,
    ) {
        tokio::spawn(async move {
            {
                let mut finish = false;
                let conn = pool.get().await.unwrap();
                while let Some(operation) = rec.recv().await {
                    let raw;
                    match operation.operation_type {
                        Task::BEGIN => {
                            raw = conn.simple_query("START TRANSACTION;").await;
                        }
                        Task::READ => {
                            raw = conn.simple_query(&operation.query).await;
                        }
                        Task::WRITE => {
                            raw = conn.simple_query(&operation.query).await;
                        }
                        Task::COMMIT => {
                            raw = conn.simple_query("COMMIT;").await;
                            finish = true;
                        }
                        Task::ABORT => {
                            raw = conn.simple_query("ROLLBACK;").await;
                            finish = true;
                        }
                    }

                    let _ = sender.send(operation.into_sqlresponse(raw)).await;

                    if finish {
                        break;
                    }
                }
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
mod tests_dispatcher {
    use super::Dispatcher;
    use crate::core::RWOperation;
    use crate::core::*;
    use crate::dbproxy::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
    use std::{collections::HashMap, net::IpAddr, net::Ipv4Addr, net::SocketAddr, sync::Arc};
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;

    #[tokio::test]
    #[ignore]
    async fn test_receive_response_from_new_transactions() {
        //Prepare - Network
        let transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let transactions_2 = Arc::clone(&transactions);

        //Global version//
        let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(Default::default())));

        //PendingQueue
        let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
        let pending_queue_2 = Arc::clone(&pending_queue);
        //Responder sender and receiver
        let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
            mpsc::channel(100);
        let mut mock_vs = Vec::new();
        mock_vs.push(TxTableVN {
            table: "table2".to_string(),
            vn: 0,
            op: RWOperation::R,
        });
        mock_vs.push(TxTableVN {
            table: "table1".to_string(),
            vn: 0,
            op: RWOperation::R,
        });

        let mut mock_ops = Vec::new();
        mock_ops.push(QueueMessage {
            identifier: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            operation_type: Task::ABORT,
            query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
            versions: None,
        });
        mock_ops.push(QueueMessage {
            identifier: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            operation_type: Task::READ,
            query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
            versions: None,
        });
        mock_ops.push(QueueMessage {
            identifier: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            operation_type: Task::READ,
            query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
            versions: None,
        });
        mock_ops.push(QueueMessage {
            identifier: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
            operation_type: Task::BEGIN,
            query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
            versions: None,
        });

        helper_spawn_dispatcher(pending_queue, responder_sender, version, transactions);
        helper_mock_client(pending_queue_2, mock_ops).await;

        let mut task_num: u64 = 0;
        while let Some(_) = responder_receiver.recv().await {
            task_num += 1;
            //println!("{}", q.result);
            if task_num == 4 {
                break;
            }
        }

        //Only one unique transaction
        assert!(transactions_2.lock().await.len() == 1);
    }

    fn helper_spawn_dispatcher(
        pending_queue: Arc<Mutex<PendingQueue>>,
        sender: mpsc::Sender<QueryResult>,
        version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>>,
    ) {
        let mut config = tokio_postgres::Config::new();
        config.user("postgres");
        config.password("Abc@123");
        config.host("localhost");
        config.port(5432);
        config.dbname("Test");

        Dispatcher::run(pending_queue, sender, config, version, transactions)
    }

    async fn helper_mock_client(pending_queue: Arc<Mutex<PendingQueue>>, mut messages: Vec<QueueMessage>) {
        while !messages.is_empty() {
            pending_queue.lock().await.push(messages.pop().unwrap());
        }
    }
}

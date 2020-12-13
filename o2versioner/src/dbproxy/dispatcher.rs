use super::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
use crate::core::*;
use crate::util::config::DbProxyConfig;
use crate::util::executor::Executor;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_postgres::NoTls;
use tracing::{debug, field, info, instrument, trace, Span};
use uuid::Uuid;

pub struct Dispatcher {
    pending_queue: Arc<Mutex<PendingQueue>>,
    responder_sender: mpsc::Sender<QueryResult>,
    conf: DbProxyConfig,
    version: Arc<Mutex<DbVersion>>,
    transactions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<QueueMessage>>>>,
}

impl Dispatcher {
    pub fn new(
        pending_queue: Arc<Mutex<PendingQueue>>,
        responder_sender: mpsc::Sender<QueryResult>,
        conf: DbProxyConfig,
        version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<QueueMessage>>>>,
    ) -> Self {
        Self {
            pending_queue,
            responder_sender,
            conf,
            version,
            transactions,
        }
    }

    async fn wait_for_new_task_or_version_release(new_task_notify: &mut Arc<Notify>, version_notify: &mut Arc<Notify>) {
        let n1 = new_task_notify.notified();
        let n2 = version_notify.notified();
        tokio::select! {
           _ = n1 => {debug!("new_task_notify")}
           _ = n2 => {debug!("version_notify")}
        };
    }
}

#[async_trait]
impl Executor for Dispatcher {
    #[instrument(name = "dispatcher", skip(self))]
    async fn run(mut self: Box<Self>) {
        let postgres_pool_size = 80;
        let transaction_channel_queue_size = 100;
        info!("Dispatcher Started");

        let mut task_notify = self.pending_queue.lock().await.get_notify();
        let mut version_notify = self.version.lock().await.get_notify();

        let pool_opt = if let Some(sql_conf) = &self.conf.sql_conf {
            info!("Connecting to db with: {}", sql_conf);
            Some(
                Pool::builder()
                    .max_size(postgres_pool_size)
                    .build(PostgresConnectionManager::new(
                        tokio_postgres::Config::from_str(sql_conf).unwrap(),
                        NoTls,
                    ))
                    .await
                    .unwrap(),
            )
        } else {
            info!("Using mocked db");
            None
        };

        let mut previously_has_operation = false;
        loop {
            if !previously_has_operation {
                Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

                debug!("Dispatcher get a notification");
                debug!("Pending queue size is {}", self.pending_queue.lock().await.queue.len());
                debug!("Current Db Version is: {:?}", self.version.lock().await.db_version);
            }

            let operations = self
                .pending_queue
                .lock()
                .await
                .get_all_version_ready_task(self.version.clone())
                .await;
            previously_has_operation = !operations.is_empty();
            debug!("Operation batch size is {}", operations.len());
            trace!("Ready tasks are {:?}", operations);

            stream::iter(operations.clone())
                .for_each(|op| {
                    let pool_opt_cloned = pool_opt.clone();
                    let responder_sender_cloned = self.responder_sender.clone();
                    let transactions_cloned = self.transactions.clone();
                    async move {
                        match op.operation_type {
                            Task::SINGLEREAD => {
                                let transaction_uuid = op.versions.uuid().clone();
                                let singleread_executor: Box<dyn Executor> = if let Some(pool) = pool_opt_cloned {
                                    Box::new(SingleReadExecutor::new(
                                        transaction_uuid,
                                        op.identifier.to_client_meta(),
                                        pool,
                                        op.clone(),
                                        responder_sender_cloned,
                                    ))
                                } else {
                                    let transaction_uuid = op.versions.uuid().clone();
                                    Box::new(MockSingleReadExecutor::new(
                                        transaction_uuid,
                                        op.identifier.to_client_meta(),
                                        op.clone(),
                                        responder_sender_cloned,
                                    ))
                                };
                                tokio::spawn(async move {
                                    singleread_executor.run().await;
                                });
                            }
                            _ => {
                                let transaction_uuid = op.versions.uuid().clone();
                                let mut transactions_lock = transactions_cloned.lock().await;
                                let transaction_sender =
                                    transactions_lock.entry(transaction_uuid.clone()).or_insert_with(|| {
                                        // Start a new transaction if not existed yet
                                        // Save this newly opened transaction, associates an tx with the transaction, so
                                        // upcoming requests can go to the transaction via tx
                                        debug!("Opening a new transaction for ({})", transaction_uuid);
                                        let (transaction_tx, transaction_executor): (_, Box<dyn Executor>) =
                                            if let Some(pool) = pool_opt_cloned {
                                                let (transaction_tx, transaction_executor) = TransactionExecutor::new(
                                                    transaction_uuid.clone(),
                                                    op.identifier.to_client_meta(),
                                                    transaction_channel_queue_size,
                                                    pool,
                                                    responder_sender_cloned,
                                                );
                                                (transaction_tx, Box::new(transaction_executor))
                                            } else {
                                                let (transaction_tx, transaction_executor) =
                                                    MockTransactionExecutor::new(
                                                        transaction_uuid.clone(),
                                                        op.identifier.to_client_meta(),
                                                        transaction_channel_queue_size,
                                                        responder_sender_cloned,
                                                    );
                                                (transaction_tx, Box::new(transaction_executor))
                                            };
                                        let transactions = transactions_cloned.clone();
                                        tokio::spawn(async move {
                                            transaction_executor.run().await;
                                            // Pop the transaction_tx of the finished running transaction executor from the transactions list
                                            transactions.lock().await.remove(&transaction_uuid).unwrap();
                                        });
                                        // Insert the transaction_tx of the newly spanwed transaction executor into the transactions list
                                        transaction_tx
                                    });
                                // Send the request to the existing transaction listener
                                debug!(
                                    "Sending a new operation to an existing transaction ({})",
                                    op.versions.uuid()
                                );
                                transaction_sender.send(op).await.map_err(|e| e.to_string()).unwrap();
                            }
                        }
                    }
                })
                .await;
        }
    }
}

struct TransactionExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    transaction_listener: mpsc::Receiver<QueueMessage>,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl TransactionExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        transaction_channel_queue_size: usize,
        pool: Pool<PostgresConnectionManager<NoTls>>,
        responder_sender: mpsc::Sender<QueryResult>,
    ) -> (mpsc::Sender<QueueMessage>, Self) {
        let (transaction_tx, transaction_listener) = mpsc::channel(transaction_channel_queue_size);

        (
            transaction_tx,
            Self {
                transaction_uuid,
                client_meta,
                transaction_listener,
                pool,
                responder_sender,
            },
        )
    }
}

#[async_trait]
impl Executor for TransactionExecutor {
    #[instrument(name = "trans_exec", skip(self), fields(message=field::Empty))]
    pub async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);
        let mut conn = self.pool.get().await.unwrap();
        info!("Deploying {}", self.transaction_uuid);
        let mut total_sql_cmd: usize = 0;
        let mut transc = Some(conn.transaction().await.expect("Cannot create a new transaction"));
        total_sql_cmd += 1;

        // Default is rollback
        let mut is_ended_by_rollback = true;
        while let Some(operation) = self.transaction_listener.recv().await {
            let raw = match operation.operation_type {
                Task::READ | Task::WRITE => {
                    transc
                        .as_ref()
                        .unwrap()
                        .simple_query(&MsqlFinalString::from(operation.msql.clone()).into_inner())
                        .await
                }
                Task::COMMIT => {
                    is_ended_by_rollback = false;
                    transc.take().unwrap().commit().await.map(|_| Vec::new())
                }
                Task::ABORT => transc.take().unwrap().rollback().await.map(|_| Vec::new()),
                _ => panic!("Unexpected operation type"),
            };

            self.responder_sender
                .send(operation.into_sqlresponse(raw))
                .await
                .map_err(|e| e.to_string())
                .unwrap();

            total_sql_cmd += 1;

            if transc.is_none() {
                break;
            }
        }

        let ending = if is_ended_by_rollback { "ROLLBACK" } else { "Commit" };
        info!(
            "Finishing {} after executing {} commands. {}",
            self.transaction_uuid, total_sql_cmd, ending
        );
    }
}

struct SingleReadExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    pool: Pool<PostgresConnectionManager<NoTls>>,
    operation: QueueMessage,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl SingleReadExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        pool: Pool<PostgresConnectionManager<NoTls>>,
        operation: QueueMessage,
        responder_sender: mpsc::Sender<QueryResult>,
    ) -> Self {
        Self {
            transaction_uuid,
            client_meta,
            pool,
            operation,
            responder_sender,
        }
    }
}

#[async_trait]
impl Executor for SingleReadExecutor {
    #[instrument(name = "trans_exec_single_read", skip(self), fields(message=field::Empty))]
    async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);
        let conn = self.pool.get().await.unwrap();
        info!("Deploying {}", self.transaction_uuid);

        let raw = conn
            .simple_query(&MsqlFinalString::from(self.operation.msql.clone()).into_inner())
            .await;

        self.responder_sender
            .send(self.operation.into_sqlresponse(raw))
            .await
            .map_err(|e| e.to_string())
            .unwrap();
    }
}

struct MockSingleReadExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    operation: QueueMessage,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl MockSingleReadExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        operation: QueueMessage,
        responder_sender: mpsc::Sender<QueryResult>,
    ) -> Self {
        Self {
            transaction_uuid,
            client_meta,
            operation,
            responder_sender,
        }
    }
}

#[async_trait]
impl Executor for MockSingleReadExecutor {
    #[instrument(name = "trans_exec_single_read", skip(self), fields(message=field::Empty))]
    async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);
        info!("Deploying {}", self.transaction_uuid);
        self.responder_sender
            .send(self.operation.into_sqlresponse(Ok(Vec::new())))
            .await
            .map_err(|e| e.to_string())
            .unwrap();
    }
}

struct MockTransactionExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    transaction_listener: mpsc::Receiver<QueueMessage>,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl MockTransactionExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        transaction_channel_queue_size: usize,
        responder_sender: mpsc::Sender<QueryResult>,
    ) -> (mpsc::Sender<QueueMessage>, Self) {
        let (transaction_tx, transaction_listener) = mpsc::channel(transaction_channel_queue_size);

        (
            transaction_tx,
            Self {
                transaction_uuid,
                client_meta,
                transaction_listener,
                responder_sender,
            },
        )
    }
}

#[async_trait]
impl Executor for MockTransactionExecutor {
    #[instrument(name = "trans_exec", skip(self), fields(message=field::Empty))]
    pub async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);
        info!("Deploying {}", self.transaction_uuid);
        let mut total_sql_cmd: usize = 0;
        total_sql_cmd += 1;

        // Default is rollback
        let mut is_finished = false;
        let mut is_ended_by_rollback = true;
        while let Some(operation) = self.transaction_listener.recv().await {
            let raw = match operation.operation_type {
                Task::READ => Ok(vec![]),
                Task::WRITE => Ok(vec![]),
                Task::COMMIT => {
                    is_finished = true;
                    is_ended_by_rollback = false;
                    Ok(vec![])
                }
                Task::ABORT => {
                    is_finished = true;
                    Ok(vec![])
                }
                _ => panic!("Unexpected operation type"),
            };

            self.responder_sender
                .send(operation.into_sqlresponse(raw))
                .await
                .map_err(|e| e.to_string())
                .unwrap();

            total_sql_cmd += 1;

            if is_finished {
                break;
            }
        }

        let ending = if is_ended_by_rollback { "ROLLBACK" } else { "Commit" };
        info!(
            "Finishing {} after executing {} commands. {}",
            self.transaction_uuid, total_sql_cmd, ending
        );
    }
}

// #[cfg(test)]
// #[ignore]
// mod tests_dispatcher {
//     use super::Dispatcher;
//     use crate::core::RWOperation;
//     use crate::core::*;
//     use crate::dbproxy::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
//     use std::{collections::HashMap, net::IpAddr, net::Ipv4Addr, net::SocketAddr, sync::Arc};
//     use tokio::sync::mpsc;
//     use tokio::sync::Mutex;

//     #[tokio::test]
//     #[ignore]
//     async fn test_receive_response_from_new_transactions() {
//         //Prepare - Network
//         let transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>> =
//             Arc::new(Mutex::new(HashMap::new()));
//         let transactions_2 = Arc::clone(&transactions);

//         //Global version//
//         let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(Default::default())));

//         //PendingQueue
//         let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
//         let pending_queue_2 = Arc::clone(&pending_queue);
//         //Responder sender and receiver
//         let (responder_sender, mut responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
//             mpsc::channel(100);
//         let mut mock_vs = Vec::new();
//         mock_vs.push(TxTableVN {
//             table: "table2".to_string(),
//             vn: 0,
//             op: RWOperation::R,
//         });
//         mock_vs.push(TxTableVN {
//             table: "table1".to_string(),
//             vn: 0,
//             op: RWOperation::R,
//         });

//         let mut mock_ops = Vec::new();
//         mock_ops.push(QueueMessage {
//             identifier: RequestMeta {
//                 client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
//                 cur_txid: 0,
//                 request_id: 0,
//             },
//             operation_type: Task::ABORT,
//             query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
//             versions: None,
//             early_release: None,
//         });
//         mock_ops.push(QueueMessage {
//             identifier: RequestMeta {
//                 client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
//                 cur_txid: 0,
//                 request_id: 0,
//             },
//             operation_type: Task::READ,
//             query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
//             versions: None,
//             early_release: None,
//         });
//         mock_ops.push(QueueMessage {
//             identifier: RequestMeta {
//                 client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
//                 cur_txid: 0,
//                 request_id: 0,
//             },
//             operation_type: Task::READ,
//             query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
//             versions: None,
//             early_release: None,
//         });
//         mock_ops.push(QueueMessage {
//             identifier: RequestMeta {
//                 client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
//                 cur_txid: 0,
//                 request_id: 0,
//             },
//             operation_type: Task::BEGIN,
//             query: "SELECT name, age, designation, salary FROM public.tbltest;".to_string(),
//             versions: None,
//             early_release: None,
//         });

//         helper_spawn_dispatcher(pending_queue, responder_sender, version, transactions);
//         helper_mock_client(pending_queue_2, mock_ops).await;

//         let mut task_num: u64 = 0;
//         while let Some(_) = responder_receiver.recv().await {
//             task_num += 1;
//             //println!("{}", q.result);
//             if task_num == 4 {
//                 break;
//             }
//         }

//         //Only one unique transaction
//         assert!(transactions_2.lock().await.len() == 1);
//     }

//     fn helper_spawn_dispatcher(
//         pending_queue: Arc<Mutex<PendingQueue>>,
//         sender: mpsc::Sender<QueryResult>,
//         version: Arc<Mutex<DbVersion>>,
//         transactions: Arc<Mutex<HashMap<SocketAddr, mpsc::Sender<QueueMessage>>>>,
//     ) {
//         let mut config = tokio_postgres::Config::new();
//         config.user("postgres");
//         config.password("Abc@123");
//         config.host("localhost");
//         config.port(5432);
//         config.dbname("Test");

//         Dispatcher::run(pending_queue, sender, config, version, transactions)
//     }

//     async fn helper_mock_client(pending_queue: Arc<Mutex<PendingQueue>>, mut messages: Vec<QueueMessage>) {
//         while !messages.is_empty() {
//             pending_queue.lock().await.push(messages.pop().unwrap());
//         }
//     }
// }

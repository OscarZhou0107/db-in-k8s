use super::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
use super::mockdb;
use super::postgresdb;
use crate::util::conf::DbProxyConfig;
use crate::util::executor::Executor;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_postgres::NoTls;
use tracing::{debug, info, instrument, trace, Instrument};
use uuid::Uuid;

/// An event loop to process the incoming Msql request
/// from the queue (enqueued by the receiver) once they are
/// ready for versions, and reply back via the responder
///
/// Every transaction is spawned as a task, and will be done
/// once the transaction is properly terminated via Sql transaction
/// commit or rollback
pub struct Dispatcher {
    pending_queue: Arc<Mutex<PendingQueue>>,
    responder_sender: mpsc::Sender<QueryResult>,
    conf: DbProxyConfig,
    version: Arc<Mutex<DbVersion>>,
    transactions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<QueueMessage>>>>,
    stop_receiver: Option<oneshot::Receiver<()>>,
}

impl Dispatcher {
    pub fn new(
        pending_queue: Arc<Mutex<PendingQueue>>,
        responder_sender: mpsc::Sender<QueryResult>,
        conf: DbProxyConfig,
        version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<QueueMessage>>>>,
    ) -> (oneshot::Sender<()>, Self) {
        let (sender, receiver) = oneshot::channel();

        (
            sender,
            Self {
                pending_queue,
                responder_sender,
                conf,
                version,
                transactions,
                stop_receiver: Some(receiver),
            },
        )
    }

    async fn wait_for_new_task_or_version_release(new_task_notify: &mut Arc<Notify>, version_notify: &mut Arc<Notify>) {
        let n1 = new_task_notify.notified();
        let n2 = version_notify.notified();
        tokio::select! {
           _ = n1 => {debug!("new_task_notify")}
           _ = n2 => {debug!("version_notify")}
        }
    }
}

#[async_trait]
impl Executor for Dispatcher {
    #[instrument(name = "dispatcher", skip(self))]
    async fn run(mut self: Box<Self>) {
        let postgres_pool_size = 80;
        let transaction_channel_queue_size = 100;
        info!("started");

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

        if let Some(db_mock_latency) = self.conf.db_mock_latency.as_ref() {
            info!("Using mocked db latency: {:?}", db_mock_latency);
        } else {
            info!("Not using mocked db latency");
        }
        let mock_db_latency_provider_opt = self.conf.db_mock_latency.clone().map(|c| c.into());

        let mut previously_has_operation = false;
        let mut stop_receiver = self.stop_receiver.take().unwrap();
        loop {
            if !previously_has_operation {
                let new_task_or_version_release =
                    Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify);
                tokio::select! {
                    _ = new_task_or_version_release => {},
                    _ = &mut stop_receiver => {info!("Dispatcher main loop terminated"); break;}
                }

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
                    let mock_db_latency_provider_opt_cloned = mock_db_latency_provider_opt.clone();
                    let pool_opt_cloned = pool_opt.clone();
                    let responder_sender_cloned = self.responder_sender.clone();
                    let transactions_cloned = self.transactions.clone();
                    async move {
                        match op.operation_type {
                            Task::SINGLEREAD => {
                                let transaction_uuid = op.versions.uuid().clone();
                                let singleread_executor: Box<dyn Executor> = if let Some(pool) = pool_opt_cloned {
                                    Box::new(postgresdb::SingleReadExecutor::new(
                                        transaction_uuid,
                                        op.identifier.to_client_meta(),
                                        pool,
                                        op.clone(),
                                        responder_sender_cloned,
                                    ))
                                } else {
                                    let transaction_uuid = op.versions.uuid().clone();
                                    Box::new(mockdb::SingleReadExecutor::new(
                                        transaction_uuid,
                                        op.identifier.to_client_meta(),
                                        op.clone(),
                                        responder_sender_cloned,
                                        mock_db_latency_provider_opt_cloned.clone(),
                                    ))
                                };
                                tokio::spawn(singleread_executor.run().in_current_span());
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
                                                let (transaction_tx, transaction_executor) =
                                                    postgresdb::TransactionExecutor::new(
                                                        transaction_uuid.clone(),
                                                        op.identifier.to_client_meta(),
                                                        transaction_channel_queue_size,
                                                        pool,
                                                        responder_sender_cloned,
                                                    );
                                                (transaction_tx, Box::new(transaction_executor))
                                            } else {
                                                let (transaction_tx, transaction_executor) =
                                                    mockdb::TransactionExecutor::new(
                                                        transaction_uuid.clone(),
                                                        op.identifier.to_client_meta(),
                                                        transaction_channel_queue_size,
                                                        responder_sender_cloned,
                                                        mock_db_latency_provider_opt_cloned.clone(),
                                                    );
                                                (transaction_tx, Box::new(transaction_executor))
                                            };
                                        let transactions = transactions_cloned.clone();
                                        tokio::spawn(
                                            async move {
                                                transaction_executor.run().await;
                                                // Pop the transaction_tx of the finished running transaction executor from the transactions list
                                                transactions.lock().await.remove(&transaction_uuid).unwrap();
                                            }
                                            .in_current_span(),
                                        );
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

use super::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
use crate::core::*;
use bb8_postgres::bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use futures::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_postgres::NoTls;
use tracing::{debug, field, info, instrument, Span};
use uuid::Uuid;

pub struct Dispatcher;

impl Dispatcher {
    #[instrument(
        name = "dispatcher",
        skip(pending_queue, responder_sender, config, version, transactions)
    )]
    pub async fn run(
        pending_queue: Arc<Mutex<PendingQueue>>,
        responder_sender: mpsc::Sender<QueryResult>,
        config: tokio_postgres::Config,
        mut version: Arc<Mutex<DbVersion>>,
        transactions: Arc<Mutex<HashMap<Uuid, mpsc::Sender<QueueMessage>>>>,
    ) {
        let postgres_pool_size = 80;
        let transaction_channel_queue_size = 100;
        info!("Dispatcher Started");

        let mut task_notify = pending_queue.lock().await.get_notify();
        let mut version_notify = version.lock().await.get_notify();

        let pool = Pool::builder()
            .max_size(postgres_pool_size)
            .build(PostgresConnectionManager::new(config, NoTls))
            .await
            .unwrap();

        let mut empty_spinning_count = 0;
        loop {
            Self::wait_for_new_task_or_version_release(&mut task_notify, &mut version_notify).await;

            debug!("Dispatcher get a notification");
            debug!("Pending queue size is {}", pending_queue.lock().await.queue.len());
            debug!("version is: {:?}", version.lock().await.db_version);
            let operations = pending_queue
                .lock()
                .await
                .get_all_version_ready_task(&mut version)
                .await;
            debug!("Operation batch size is {}", operations.len());

            // DEBUG. PANIC WHEN POTENTIAL BLOCKING
            if operations.is_empty() {
                empty_spinning_count += 1;
                if empty_spinning_count >= 100 {
                    panic!("Probably blocked due to version conflicts");
                }
            } else {
                empty_spinning_count = 0;
            }

            stream::iter(operations.clone())
                .for_each(|op| {
                    let pool_cloned = pool.clone();
                    let responder_sender_cloned = responder_sender.clone();
                    let transactions_cloned = transactions.clone();

                    async move {
                        let transaction_uuid = op.versions.as_ref().unwrap().uuid().clone();
                        let mut transactions_lock = transactions_cloned.lock().await;

                        let transaction_sender =
                            transactions_lock.entry(transaction_uuid.clone()).or_insert_with(|| {
                                // Start a new transaction if not existed yet
                                // Save this newly opened transaction, associates an tx with the transaction, so
                                // upcoming requests can go to the transaction via tx
                                debug!("Opening a new transaction for ({})", transaction_uuid);

                                let (transaction_tx, transaction_rx) = mpsc::channel(transaction_channel_queue_size);

                                // A task represents an ongoing transaction, probably just leaves it hanging around.
                                // listens on the rx for upcoming requests
                                let transaction_executor = TransactionExecutor::new(
                                    transaction_uuid.clone(),
                                    op.identifier.to_client_meta(),
                                    transaction_rx,
                                    pool_cloned,
                                    responder_sender_cloned,
                                );
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
                            op.versions.as_ref().unwrap().uuid()
                        );
                        transaction_sender.send(op).await.map_err(|e| e.to_string()).unwrap();
                    }
                })
                .await;
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
        transaction_listener: mpsc::Receiver<QueueMessage>,
        pool: Pool<PostgresConnectionManager<NoTls>>,
        responder_sender: mpsc::Sender<QueryResult>,
    ) -> Self {
        Self {
            transaction_uuid,
            client_meta,
            transaction_listener,
            pool,
            responder_sender,
        }
    }

    #[instrument(name = "trans_exec", skip(self), fields(message=field::Empty))]
    pub async fn run(mut self) {
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
                Task::READ => {
                    transc
                        .as_ref()
                        .unwrap()
                        .simple_query(&MsqlFinalString::from(operation.msql.clone()).into_inner())
                        .await
                }
                Task::WRITE => {
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

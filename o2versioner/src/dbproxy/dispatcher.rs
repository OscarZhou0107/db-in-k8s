use super::core::{DbVersion, PendingQueue, QueryResult, QueueMessage, Task};
use crate::core::MsqlFinalString;
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

            stream::iter(operations.clone())
                .for_each(|op| {
                    let pool_cloned = pool.clone();
                    let responder_sender_cloned = responder_sender.clone();
                    let transactions_cloned = transactions.clone();

                    async move {
                        let mut transactions_lock = transactions_cloned.lock().await;

                        let transaction_sender = transactions_lock
                            .entry(op.versions.as_ref().unwrap().uuid().clone())
                            .or_insert_with(|| {
                                // Start a new transaction if not existed yet
                                // Save this newly opened transaction, associates an tx with the transaction, so
                                // upcoming requests can go to the transaction via tx
                                debug!(
                                    "Opening a new transaction for ({})",
                                    op.versions.as_ref().unwrap().uuid()
                                );

                                let (transaction_tx, transaction_rx) = mpsc::channel(transaction_channel_queue_size);

                                // A task represents an ongoing transaction, probably just leaves it hanging around.
                                // listens on the rx for upcoming requests
                                tokio::spawn(TransactionExecutor::run(
                                    pool_cloned,
                                    transaction_rx,
                                    responder_sender_cloned,
                                    op.versions.as_ref().unwrap().uuid().clone(),
                                ));

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

struct TransactionExecutor;

impl TransactionExecutor {
    #[instrument(
        name = "trans_exec",
        skip(pool, transaction_listener, responder_sender, transaction_uuid),
        fields(message=field::Empty)
    )]
    pub async fn run(
        pool: Pool<PostgresConnectionManager<NoTls>>,
        mut transaction_listener: mpsc::Receiver<QueueMessage>,
        responder_sender: mpsc::Sender<QueryResult>,
        transaction_uuid: Uuid,
    ) {
        Span::current().record("message", &&transaction_uuid.to_string()[..]);

        let mut finish = false;
        let conn = pool.get().await.unwrap();
        info!("Deploying");
        conn.simple_query("START TRANSACTION;").await.unwrap();
        while let Some(operation) = transaction_listener.recv().await {
            let raw;
            match operation.operation_type {
                Task::READ => {
                    raw = conn
                        .simple_query(&MsqlFinalString::from(operation.msql.clone()).into_inner())
                        .await;
                }
                Task::WRITE => {
                    raw = conn
                        .simple_query(&MsqlFinalString::from(operation.msql.clone()).into_inner())
                        .await;
                }
                Task::COMMIT => {
                    raw = conn.simple_query("COMMIT;").await;
                    finish = true;
                }
                Task::ABORT => {
                    raw = conn.simple_query("ROLLBACK;").await;
                    finish = true;
                }
                _ => {
                    panic!("Unexpected operation type");
                }
            }

            responder_sender
                .send(operation.into_sqlresponse(raw))
                .await
                .map_err(|e| e.to_string())
                .unwrap();

            if finish {
                break;
            }
        }

        info!("Finishing");
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

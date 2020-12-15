use super::core::{QueryResult, QueueMessage, Task};
use crate::core::*;
use crate::util::executor::Executor;
use async_trait::async_trait;
use bb8_postgres::bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;
use tracing::{field, info, instrument, Span};
use uuid::Uuid;

/// Executes a Sql transaction with a postgre DBMS
pub struct TransactionExecutor {
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

/// Executes a single Sql read query without transaction with a postgre DBMS
pub struct SingleReadExecutor {
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

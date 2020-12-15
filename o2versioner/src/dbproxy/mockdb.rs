use super::core::{QueryResult, QueueMessage, Task};
use crate::core::*;
use crate::util::executor::Executor;
use async_trait::async_trait;
use tokio::sync::mpsc;
use tracing::{field, info, instrument, Span};
use uuid::Uuid;

/// Executes a single Sql read query without transaction without a real DBMS
pub struct SingleReadExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    operation: QueueMessage,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl SingleReadExecutor {
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
impl Executor for SingleReadExecutor {
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

/// Executes a Sql transaction without a real DBMS
pub struct TransactionExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    transaction_listener: mpsc::Receiver<QueueMessage>,
    responder_sender: mpsc::Sender<QueryResult>,
}

impl TransactionExecutor {
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
impl Executor for TransactionExecutor {
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

use super::core::{QueryResult, QueueMessage, Task};
use crate::core::*;
use crate::util::config::{DbMockLatency, LatencyDistr};
use crate::util::executor::Executor;
use async_trait::async_trait;
use rand::rngs::StdRng;
use rand::SeedableRng;
use rand_distr::Distribution;
use rand_distr::Normal;
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tracing::{field, info, instrument, trace, Span};
use uuid::Uuid;

/// Latency provider for one kind of operation
#[derive(Debug, Clone)]
struct MockLatencyProvider {
    randomness: Arc<Mutex<StdRng>>,
    distr: Arc<Normal<f64>>,
}

impl From<LatencyDistr> for MockLatencyProvider {
    fn from(latency_distr: LatencyDistr) -> Self {
        Self {
            randomness: Arc::new(Mutex::new(StdRng::from_entropy())),
            distr: Arc::new(Normal::new(latency_distr.mean.into(), latency_distr.stddev.into()).unwrap()),
        }
    }
}

impl MockLatencyProvider {
    async fn get_delay(&self) -> Duration {
        let mut randomness = self.randomness.lock().await;
        let delay_ms = self.distr.sample(&mut randomness.deref_mut());
        Duration::from_secs_f64(delay_ms / 1000.0)
    }

    async fn wait(&self) {
        let delay = self.get_delay().await;
        trace!("mock latency: {:?}", delay);
        sleep(delay).await;
    }
}

/// Latency provider for all kinds of db operations
#[derive(Debug, Clone)]
pub struct DbMockLatencyProvider {
    begintx: MockLatencyProvider,
    read: MockLatencyProvider,
    write: MockLatencyProvider,
    endtx: MockLatencyProvider,
}

impl From<DbMockLatency> for DbMockLatencyProvider {
    fn from(dbmock_latency: DbMockLatency) -> Self {
        Self {
            begintx: dbmock_latency.begintx.into(),
            read: dbmock_latency.read.into(),
            write: dbmock_latency.write.into(),
            endtx: dbmock_latency.endtx.into(),
        }
    }
}

impl DbMockLatencyProvider {
    async fn wait_begintx(&self) {
        self.begintx.wait().await;
    }

    async fn wait_read(&self) {
        self.read.wait().await;
    }

    async fn wait_write(&self) {
        self.write.wait().await;
    }

    async fn wait_endtx(&self) {
        self.endtx.wait().await;
    }
}

/// Executes a single Sql read query without transaction without a real DBMS
pub struct SingleReadExecutor {
    transaction_uuid: Uuid,
    client_meta: ClientMeta,
    operation: QueueMessage,
    responder_sender: mpsc::Sender<QueryResult>,
    latency_provider: Option<DbMockLatencyProvider>,
}

impl SingleReadExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        operation: QueueMessage,
        responder_sender: mpsc::Sender<QueryResult>,
        latency_provider: Option<DbMockLatencyProvider>,
    ) -> Self {
        Self {
            transaction_uuid,
            client_meta,
            operation,
            responder_sender,
            latency_provider,
        }
    }
}

#[async_trait]
impl Executor for SingleReadExecutor {
    #[instrument(name = "trans_exec_single_read", skip(self), fields(message=field::Empty))]
    async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);
        if let Some(latency) = self.latency_provider.as_ref() {
            latency.wait_read().await;
        }
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
    latency_provider: Option<DbMockLatencyProvider>,
}

impl TransactionExecutor {
    pub fn new(
        transaction_uuid: Uuid,
        client_meta: ClientMeta,
        transaction_channel_queue_size: usize,
        responder_sender: mpsc::Sender<QueryResult>,
        latency_provider: Option<DbMockLatencyProvider>,
    ) -> (mpsc::Sender<QueueMessage>, Self) {
        let (transaction_tx, transaction_listener) = mpsc::channel(transaction_channel_queue_size);

        (
            transaction_tx,
            Self {
                transaction_uuid,
                client_meta,
                transaction_listener,
                responder_sender,
                latency_provider,
            },
        )
    }
}

#[async_trait]
impl Executor for TransactionExecutor {
    #[instrument(name = "trans_exec", skip(self), fields(message=field::Empty))]
    pub async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.client_meta.to_string()[..]);

        if let Some(latency) = self.latency_provider.as_ref() {
            latency.wait_begintx().await;
        }

        info!("Deploying {}", self.transaction_uuid);
        let mut total_sql_cmd: usize = 0;
        total_sql_cmd += 1;

        // Default is rollback
        let mut is_finished = false;
        let mut is_ended_by_rollback = true;
        while let Some(operation) = self.transaction_listener.recv().await {
            let raw = match operation.operation_type {
                Task::READ => {
                    if let Some(latency) = self.latency_provider.as_ref() {
                        latency.wait_read().await;
                    }
                    Ok(vec![])
                }
                Task::WRITE => {
                    if let Some(latency) = self.latency_provider.as_ref() {
                        latency.wait_write().await;
                    }
                    Ok(vec![])
                }
                Task::COMMIT => {
                    if let Some(latency) = self.latency_provider.as_ref() {
                        latency.wait_endtx().await;
                    }
                    is_finished = true;
                    is_ended_by_rollback = false;
                    Ok(vec![])
                }
                Task::ABORT => {
                    if let Some(latency) = self.latency_provider.as_ref() {
                        latency.wait_endtx().await;
                    }
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

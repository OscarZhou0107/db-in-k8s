use crate::comm::MsqlResponse;
use crate::core::*;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, info};

/// Response sent from dbproxy to handler direction
#[derive(Debug)]
pub struct Reply {
    /// Response to the `Msql` command
    pub msql_res: MsqlResponse,
    /// The modified `TxVN`,
    /// this should be used to update the `ConnectionState`.
    pub txvn_res: Option<TxVN>,
}

impl Reply {
    pub fn new(msql_res: MsqlResponse, txvn_res: Option<TxVN>) -> Self {
        Self { msql_res, txvn_res }
    }
}

/// Request sent from handler to dbproxy direction
pub struct Request {
    pub client_meta: ClientMeta,
    pub command: Msql,
    pub txvn: Option<TxVN>,
    /// A single use reply channel
    pub reply: Option<oneshot::Sender<Reply>>,
}

/// Encloses a way to talk to the Executor of the `Request`
#[derive(Debug, Clone)]
pub struct ExecutorAddr {
    request_tx: mpsc::Sender<Request>,
}

impl ExecutorAddr {
    pub fn new(queue_size: usize) -> (Self, mpsc::Receiver<Request>) {
        let (tx, rx) = mpsc::channel(queue_size);
        (Self { request_tx: tx }, rx)
    }

    pub async fn request(&self, client_meta: ClientMeta, command: Msql, txvn: Option<TxVN>) -> Result<Reply, String> {
        // Create a reply oneshot channel
        let (tx, rx) = oneshot::channel();

        // Construct the request to sent
        let request = Request {
            client_meta,
            command,
            txvn,
            reply: Some(tx),
        };

        debug!(
            "-> execute {:?} {:?} {:?}",
            request.client_meta.client_addr(),
            request.command,
            request.txvn
        );

        // Send the request
        self.request_tx.send(request).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

impl Drop for ExecutorAddr {
    fn drop(&mut self) {
        info!("Dropping ExecutorAddr");
    }
}

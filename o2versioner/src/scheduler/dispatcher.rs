#![allow(dead_code)]
use super::core::DbVNManager;
use super::dbproxy_manager::DbproxyManager;
use crate::comm::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use crate::core::version_number::*;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::sync::RwLock;

/// Sent from `DispatcherAddr` to `Dispatcher`
struct Request {
    /// For debugging
    client_addr: SocketAddr,
    command: Msql,
    txvn: Option<TxVN>,
    /// A single use reply channel
    reply: oneshot::Sender<MsqlResponse>,
}

impl Request {
    async fn work(self, state: State) {
        match &self.command {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => match msqlquery.tableops().access_pattern() {
                AccessPattern::Mixed => panic!("Does not supported query with mixed R and W"),
                AccessPattern::ReadOnly => self.work_readonly_query(state).await,
                AccessPattern::WriteOnly => self.work_writeonly_query(state).await,
            },
            Msql::EndTx(_) => self.work_endtx(state).await,
        };
    }

    async fn work_readonly_query(&self, _state: State) {}

    async fn work_writeonly_query(&self, _state: State) {}

    async fn work_endtx(&self, _state: State) {}
}

/// A state containing shareed variables
#[derive(Clone)]
pub struct State {
    dbvn_manager: Arc<RwLock<DbVNManager>>,
    dbvn_manager_notify: Arc<Notify>,
    dbproxy_manager: DbproxyManager,
}

impl State {
    pub fn new(dbvn_manager: DbVNManager, dbproxy_manager: DbproxyManager) -> Self {
        Self {
            dbvn_manager: Arc::new(RwLock::new(dbvn_manager)),
            dbvn_manager_notify: Arc::new(Notify::new()),
            dbproxy_manager,
        }
    }

    // pub fn can_
}

pub struct Dispatcher {
    state: State,
    rx: mpsc::Receiver<Request>,
}

impl Dispatcher {
    pub fn new(queue_size: usize, state: State) -> (DispatcherAddr, Dispatcher) {
        let (tx, rx) = mpsc::channel(queue_size);
        (DispatcherAddr { tx }, Dispatcher { state, rx })
    }

    pub async fn run(self) {
        // Handle each Request concurrently
        let Dispatcher { state, rx } = self;
        rx.for_each_concurrent(None, |dispatch_request| async {
            dispatch_request.work(state.clone()).await
        })
        .await;
    }
}

/// Encloses a way to talk to the Dispatcher
///
/// TODO: provide a way to shutdown the `Dispatcher`
#[derive(Debug, Clone)]
pub struct DispatcherAddr {
    tx: mpsc::Sender<Request>,
}

impl DispatcherAddr {
    /// `Option<TxVN>` is to support single read query in the future
    async fn request(
        &mut self,
        client_addr: SocketAddr,
        command: Msql,
        txvn: Option<TxVN>,
    ) -> Result<MsqlResponse, String> {
        // Create a reply oneshot channel
        let (tx, rx) = oneshot::channel();

        // Construct the request to sent
        let request = Request {
            client_addr,
            command,
            txvn,
            reply: tx,
        };

        // Send the request
        self.tx.send(request).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

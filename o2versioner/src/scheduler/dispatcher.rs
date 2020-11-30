#![allow(dead_code)]
use super::core::DbVNManager;
use super::dbproxy_manager::DbproxyManager;
use crate::comm::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use crate::core::operation::*;
use crate::core::transaction_version::*;
use crate::util::tcp::*;
use bb8::Pool;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::sync::RwLock;
use tracing::debug;

/// Sent from `DispatcherAddr` to `Dispatcher`
struct Request {
    /// For debugging
    client_addr: SocketAddr,
    command: Msql,
    txvn: Option<TxVN>,
    /// A single use reply channel
    reply: Option<oneshot::Sender<MsqlResponse>>,
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

    async fn process(&self, request: Request) {
        match &request.command {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => match msqlquery.tableops().access_pattern() {
                AccessPattern::Mixed => panic!("Does not supported query with mixed R and W"),
                _ => self.execute_query(request).await,
            },
            Msql::EndTx(_) => self.execute_endtx(request).await,
        };
    }

    async fn execute_query(&self, _request: Request) {
        todo!()
    }

    async fn execute_endtx(&self, _request: Request) {
        todo!()
    }

    async fn assign_dbproxy_for_execution(
        &self,
        msql: &Msql,
        txvn: &Option<TxVN>,
    ) -> Vec<(SocketAddr, Pool<TcpStreamConnectionManager>)> {
        match msql {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => match msqlquery.tableops().access_pattern() {
                AccessPattern::Mixed => panic!("Does not supported query with mixed R and W"),
                AccessPattern::ReadOnly => vec![self.wait_on_version(msqlquery, txvn).await],
                AccessPattern::WriteOnly => self.dbproxy_manager.to_vec(),
            },
            Msql::EndTx(_) => self.dbproxy_manager.to_vec(),
        }
    }

    async fn wait_on_version(
        &self,
        msqlquery: &MsqlQuery,
        txvn: &Option<TxVN>,
    ) -> (SocketAddr, Pool<TcpStreamConnectionManager>) {
        assert_eq!(msqlquery.tableops().access_pattern(), AccessPattern::ReadOnly);

        if let Some(txvn) = txvn {
            // The scheduler blocks read queries until at least one database has, for all tables in the query,
            // version numbers that are greater than or equal to the version numbers assigned to the transaction
            // for these tables. If there are several such replicas, the least loaded replica is chosen

            let mut avail_dbproxy;
            // Need to wait on version
            loop {
                avail_dbproxy = self
                    .dbvn_manager
                    .read()
                    .await
                    .get_all_that_can_execute_read_query(msqlquery.tableops(), txvn);

                // Found a dbproxy that can execute the read query
                if avail_dbproxy.len() > 0 {
                    break;
                } else {
                    // Did not find any dbproxy that can execute the read quer, need to wait on version
                    // Wait for any updates on dbvn_manager
                    self.dbvn_manager_notify.notified().await;
                }
            }

            // For now, pick the first dbproxy from all available
            let dbproxy_addr = avail_dbproxy[0].0;
            debug!(
                "Found {} for executing the ReadOnly {:?} with {:?}",
                dbproxy_addr, msqlquery, txvn
            );
            (dbproxy_addr, self.dbproxy_manager.get(&dbproxy_addr))
        } else {
            // Single read operation that does not have a TxVN
            // Since a single-read transaction executes only at one replica,
            // there is no need to assign cluster-wide version numbers to such a transaction. Instead,
            // the scheduler forwards the transaction to the chosen replica, without assigning version
            // numbers.
            // Because the order of execution for a single-read transaction is ultimately decided
            // by the database proxy, the scheduler does not block such queries.

            // Find the replica that has the highest version number for the query
            unimplemented!()

            // TODO:
            // The scheduler attempts to reduce this wait by
            // selecting a replica that has an up-to-date version of each table needed by the query. In
            // this case, up-to-date version means that the table has a version number greater than or
            // equal to the highest version number assigned to any previous transaction on that table.
            // Such a replica may not necessarily exist.
        }
    }

    /// This should be called whenever dbproxy sent a response back for a `Msql::EndTx`
    async fn release_version(&self, dbproxy_addr: &SocketAddr, txvn: &TxVN) {
        self.dbvn_manager.write().await.release_version(dbproxy_addr, txvn);

        // Notify all others on any DbVN changes
        self.dbvn_manager_notify.notify_waiters();
    }
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
            state.clone().process(dispatch_request).await
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
    pub async fn request(
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
            reply: Some(tx),
        };

        // Send the request
        self.tx.send(request).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

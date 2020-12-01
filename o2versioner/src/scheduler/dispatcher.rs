use super::core::{DbVNManager, DbproxyManager};
use crate::comm::msql_response::MsqlResponse;
use crate::comm::scheduler_dbproxy::*;
use crate::core::msql::*;
use crate::core::operation::*;
use crate::core::transaction_version::*;
use crate::util::tcp::*;
use bb8::Pool;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock};
use tracing::{debug, info};

/// Sent from `DispatcherAddr` to `Dispatcher`
struct Request {
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

    async fn execute(&self, request: Request) {
        // Check for allowed executions
        let (is_endtx, op_str) = match &request.command {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => match msqlquery.tableops().access_pattern() {
                AccessPattern::Mixed => panic!("Does not support query with mixed R and W"),
                _ => (false, "query"),
            },
            Msql::EndTx(_) => (true, "endtx"),
        };

        let dbproxy_tasks_stream = stream::iter(self.assign_dbproxy_for_execution(&request).await);

        let Request {
            client_addr,
            command,
            txvn,
            reply,
        } = request;

        let msg = Message::MsqlRequest(client_addr, command, txvn.clone());
        let shared_reply_channel = Arc::new(Mutex::new(reply));

        // Each communication with dbproxy is spawned as a separate task
        dbproxy_tasks_stream
            .for_each_concurrent(None, move |(dbproxy_addr, dbproxy_pool)| {
                let msg_cloned = msg.clone();
                let txvn_cloned = txvn.clone();
                let shared_reply_channel_cloned = shared_reply_channel.clone();
                async move {
                    let msqlresponse = send_and_receive_single_as_json(
                        &mut dbproxy_pool.get().await.unwrap(),
                        msg_cloned,
                        format!("Scheduler dispatcher-{}", op_str),
                    )
                    .map_err(|e| e.to_string())
                    .and_then(|res| match res {
                        Message::MsqlResponse(msqlresponse) => future::ok(msqlresponse),
                        _ => future::err(String::from("Invalid response from Dbproxy")),
                    })
                    .map_ok_or_else(
                        |e| {
                            if is_endtx {
                                MsqlResponse::EndTx(Err(e))
                            } else {
                                MsqlResponse::Query(Err(e))
                            }
                        },
                        |msqlresponse| msqlresponse,
                    )
                    .await;

                    // if this is a EndTx, need to release the version and notifier other queries waiting on versions
                    if is_endtx {
                        self.release_version(
                            &dbproxy_addr,
                            txvn_cloned
                                .expect("EndTx must include Some(TxVN)")
                                .into_dbvn_release_request(),
                        )
                        .await;
                    }

                    // If the oneshot channel is not consumed, consume it to send the reply back to handler
                    if let Some(reply) = shared_reply_channel_cloned.lock().await.take() {
                        debug!(
                            "<- [{}] Scheduler dispatcher-{} reply response to handler: {:?}",
                            dbproxy_addr, op_str, msqlresponse
                        );
                        reply.send(msqlresponse).expect(&format!(
                            "Scheduler dispatcher-{} cannot reply response to handler",
                            op_str
                        ));
                    } else {
                        debug!(
                            "<- [{}] Scheduler dispatcher-{} ignore response: {:?}",
                            dbproxy_addr, op_str, msqlresponse
                        );
                    }
                }
            })
            .await;
    }

    async fn assign_dbproxy_for_execution(
        &self,
        request: &Request,
    ) -> Vec<(SocketAddr, Pool<TcpStreamConnectionManager>)> {
        match &request.command {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => match msqlquery.tableops().access_pattern() {
                AccessPattern::Mixed => panic!("Does not supported query with mixed R and W"),
                AccessPattern::ReadOnly => vec![self.wait_on_version(msqlquery, &request.txvn).await],
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
        assert_eq!(
            msqlquery.tableops().access_pattern(),
            AccessPattern::ReadOnly,
            "Expecting ReadOnly access pattern for the query"
        );

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
    async fn release_version(&self, dbproxy_addr: &SocketAddr, release_request: DbVNReleaseRequest) {
        debug!("Releasing version for {} with {:?}", dbproxy_addr, release_request);

        self.dbvn_manager
            .write()
            .await
            .release_version(dbproxy_addr, release_request);

        // Notify all others on any DbVN changes
        self.dbvn_manager_notify.notify_waiters();
    }
}

pub struct Dispatcher {
    state: State,
    request_rx: mpsc::Receiver<Request>,
}

impl Dispatcher {
    pub fn new(queue_size: usize, state: State) -> (DispatcherAddr, Dispatcher) {
        let (request_tx, request_rx) = mpsc::channel(queue_size);
        (DispatcherAddr { request_tx }, Dispatcher { state, request_rx })
    }

    pub async fn run(mut self) {
        info!(
            "Scheduler dispatcher running with {} DbVNManager and {} DbproxyManager dbproxy targets!",
            Arc::get_mut(&mut self.state.dbvn_manager)
                .unwrap()
                .get_mut()
                .inner()
                .len(),
            self.state.dbproxy_manager.inner().len()
        );

        // Handle each Request concurrently
        let Dispatcher { state, request_rx } = self;

        request_rx
            .for_each_concurrent(None, |dispatch_request| async {
                state.clone().execute(dispatch_request).await
            })
            .await;

        info!("Scheduler dispatcher terminated after finishing all requests");
    }
}

/// Encloses a way to talk to the Dispatcher
#[derive(Debug, Clone)]
pub struct DispatcherAddr {
    request_tx: mpsc::Sender<Request>,
}

impl DispatcherAddr {
    /// `Option<TxVN>` is to support single read query in the future
    pub async fn request(
        &self,
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
        self.request_tx.send(request).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

impl Drop for DispatcherAddr {
    fn drop(&mut self) {
        info!("Dropping DispatcherAddr");
    }
}

/// Unit test for `Dispatcher`
#[cfg(test)]
mod tests_dispatcher {
    use super::*;
    use crate::util::tests_helper::init_logger;
    use std::iter::FromIterator;

    #[tokio::test]
    async fn test_drop_dispatcher_addr() {
        let _guard = init_logger();
        let (dispatcher_addr, dispatcher) = Dispatcher::new(
            10,
            State::new(
                DbVNManager::from_iter(vec![]),
                DbproxyManager::from_iter(vec![], 1).await,
            ),
        );

        let dispatcher_handler = tokio::spawn(dispatcher.run());

        fn drop_dispatcher_addr(_: DispatcherAddr) {}
        drop_dispatcher_addr(dispatcher_addr);

        tokio::try_join!(dispatcher_handler).unwrap();
    }
}

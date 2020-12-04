use super::core::{DbVNManager, DbproxyManager};
use super::transceiver::Reply;
use crate::comm::scheduler_dbproxy::*;
use crate::comm::MsqlResponse;
use crate::core::*;
use crate::util::executor_addr::*;
use crate::util::tcp::*;
use bb8::Pool;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, field, info, info_span, instrument, warn, Instrument, Span};

/// DispatcherRequest sent from handler to dbproxy direction
pub struct DispatcherRequest {
    client_meta: ClientMeta,
    command: Msql,
    txvn: Option<TxVN>,
}

impl ExecutorRequest for DispatcherRequest {
    type ReplyType = Reply;
}

impl DispatcherRequest {
    pub fn new(client_meta: ClientMeta, command: Msql, txvn: Option<TxVN>) -> Self {
        Self {
            client_meta,
            command,
            txvn,
        }
    }
}

/// A state containing shared variables
#[derive(Clone)]
struct State {
    dbvn_manager: Arc<RwLock<DbVNManager>>,
    dbvn_manager_notify: Arc<Notify>,
    dbproxy_manager: DbproxyManager,
}

impl State {
    fn new(dbvn_manager: Arc<RwLock<DbVNManager>>, dbproxy_manager: DbproxyManager) -> Self {
        Self {
            dbvn_manager,
            dbvn_manager_notify: Arc::new(Notify::new()),
            dbproxy_manager,
        }
    }

    #[instrument(name="execute", skip(self, request), fields(message=field::Empty, cmd=field::Empty, op=field::Empty))]
    async fn execute(&self, request: RequestWrapper<DispatcherRequest>) {
        let (request, reply_ch) = request.unwrap();

        Span::current().record("message", &&request.client_meta.to_string()[..]);
        Span::current().record("cmd", &request.command.as_ref());
        debug!("<- {:?} {:?}", request.command, request.txvn);

        // Check whether there are no dbproxies in managers at all,
        // if such case, early exit
        if !self.is_dbproxy_existed().await {
            warn!("There are currently no dbproxy servers online, Scheduler dispatcher skipped the work");

            reply_ch
                .unwrap()
                .send(Reply::new(
                    MsqlResponse::err("Dbproxy servers are all offline", &request.command),
                    request.txvn,
                ))
                .expect("Dispatcher cannot reply response to handler");

            return;
        }

        let dbproxy_addrs = match &request.command {
            Msql::BeginTx(_) => panic!("Dispatcher does not support Msql::BeginTx command"),
            Msql::Query(msqlquery) => {
                Span::current().record("op", &&format!("{:?}", msqlquery.tableops().access_pattern())[..]);
                match msqlquery.tableops().access_pattern() {
                    AccessPattern::Mixed => panic!("Does not support query with mixed R and W"),
                    AccessPattern::ReadOnly => vec![self.wait_on_version(msqlquery, &request.txvn).await],
                    AccessPattern::WriteOnly => self.dbproxy_manager.to_vec(),
                }
            }
            Msql::EndTx(msqlendtx) => {
                Span::current().record("op", &&format!("{:?}", msqlendtx.mode())[..]);
                self.dbproxy_manager.to_vec()
            }
        };

        let is_endtx = request.command.is_endtx();

        let num_dbproxy = dbproxy_addrs.len();
        let dbproxy_tasks_stream = stream::iter(dbproxy_addrs);

        let DispatcherRequest {
            client_meta,
            command,
            txvn,
        } = request;

        let msg = Message::MsqlRequest(client_meta.client_addr(), command, txvn.clone());
        let shared_reply_channel = Arc::new(Mutex::new(reply_ch));

        // Each communication with dbproxy is spawned as a separate task
        dbproxy_tasks_stream
            .for_each_concurrent(None, move |(dbproxy_addr, dbproxy_pool)| {
                let msg_cloned = msg.clone();
                let txvn_cloned = txvn.clone();
                let shared_reply_channel_cloned = shared_reply_channel.clone();
                let process = async move {
                    Span::current().record("message", &&dbproxy_addr.to_string()[..]);
                    debug!("-> {:?}", msg_cloned);
                    let mut tcpsocket = dbproxy_pool.get().await.unwrap();
                    let msqlresponse = send_and_receive_single_as_json(&mut tcpsocket, msg_cloned)
                        .inspect_err(|e| warn!("Cannot send: {:?}", e))
                        .map_err(|e| e.to_string())
                        .and_then(|res| match res {
                            Message::MsqlResponse(_, msqlresponse) => future::ok(msqlresponse),
                            _ => future::err(String::from("Invalid response from Dbproxy")),
                        })
                        .map_ok_or_else(
                            |e| {
                                if is_endtx {
                                    MsqlResponse::endtx_err(e)
                                } else {
                                    MsqlResponse::query_err(e)
                                }
                            },
                            |msqlresponse| msqlresponse,
                        )
                        .await;

                    // if this is a EndTx, need to release the version and notifier other queries waiting on versions
                    let txvn = if is_endtx {
                        self.release_version(
                            &dbproxy_addr,
                            txvn_cloned
                                .expect("EndTx must include Some(TxVN)")
                                .into_dbvn_release_request(),
                        )
                        .await;
                        None
                    } else {
                        txvn_cloned
                    };

                    // If the oneshot channel is not consumed, consume it to send the reply back to handler
                    if let Some(reply) = shared_reply_channel_cloned.lock().await.take() {
                        debug!("~~ {:?}", msqlresponse);
                        reply
                            .send(Reply::new(msqlresponse, txvn))
                            .expect(&format!("Cannot reply response to handler"));
                    } else {
                        debug!("Not reply to handler: {:?}", msqlresponse);
                    }
                };

                process.instrument(info_span!("<->dbproxy", message = field::Empty))
            })
            .instrument(info_span!("dbproxies", message = num_dbproxy))
            .await;
    }

    async fn is_dbproxy_existed(&self) -> bool {
        let num_dbproxy_pool = self.dbproxy_manager.inner().len();
        let num_dbproxy_dbvn = self.dbvn_manager.read().await.inner().len();

        assert_eq!(num_dbproxy_pool, num_dbproxy_dbvn);
        return num_dbproxy_pool > 0;
    }

    #[instrument(skip(self), fields(msqlquery, txvn))]
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
            debug!("Found dbproxy {} for executing the ReadOnly query", dbproxy_addr);
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
        debug!("release_version: {:?}", release_request);

        self.dbvn_manager
            .write()
            .await
            .release_version(dbproxy_addr, release_request);

        // Notify all others on any DbVN changes
        self.dbvn_manager_notify.notify_waiters();
    }
}

pub type DispatcherAddr = ExecutorAddr<DispatcherRequest>;

pub struct Dispatcher {
    state: State,
    request_rx: RequestReceiver<DispatcherRequest>,
}

impl Dispatcher {
    pub fn new(
        queue_size: usize,
        dbvn_manager: Arc<RwLock<DbVNManager>>,
        dbproxy_manager: DbproxyManager,
    ) -> (DispatcherAddr, Dispatcher) {
        let state = State::new(dbvn_manager, dbproxy_manager);

        let (addr, request_rx) = DispatcherAddr::new(queue_size);
        (addr, Dispatcher { state, request_rx })
    }

    #[instrument(name="dispatch", skip(self), fields(dbvn=field::Empty))]
    pub async fn run(mut self) {
        let num_dbvn_manager = Arc::get_mut(&mut self.state.dbvn_manager)
            .unwrap()
            .get_mut()
            .inner()
            .len();
        Span::current().record("dbvn", &num_dbvn_manager);

        let Dispatcher { state, request_rx } = self;

        // Handle each DispatcherRequest concurrently
        request_rx
            .for_each_concurrent(None, |dispatch_request| async {
                state.clone().execute(dispatch_request).await
            })
            .await;

        info!("Scheduler dispatcher terminated after finishing all requests");
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
            Arc::new(RwLock::new(DbVNManager::from_iter(vec![]))),
            DbproxyManager::from_iter(vec![]).await,
        );

        let dispatcher_handler = tokio::spawn(dispatcher.run());

        fn drop_dispatcher_addr(_: DispatcherAddr) {}
        drop_dispatcher_addr(dispatcher_addr);

        tokio::try_join!(dispatcher_handler).unwrap();
    }
}

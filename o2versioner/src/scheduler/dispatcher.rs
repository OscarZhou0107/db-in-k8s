use super::core::{DbVNManager, DbproxyManager};
use super::transceiver::*;
use crate::comm::scheduler_dbproxy::*;
use crate::comm::MsqlResponse;
use crate::core::*;
use crate::util::executor_addr::*;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use tracing::{debug, error, field, info, info_span, instrument, warn, Instrument, Span};

/// Response sent from dispatcher to handler
#[derive(Debug)]
pub struct DispatcherReply {
    /// Response to the `Msql` command
    pub msql_res: MsqlResponse,
    /// The modified `TxVN`,
    /// this should be used to update the `ConnectionState`.
    pub txvn_res: Option<TxVN>,
}

impl DispatcherReply {
    pub fn new(msql_res: MsqlResponse, txvn_res: Option<TxVN>) -> Self {
        Self { msql_res, txvn_res }
    }
}

/// DispatcherRequest sent from handler to dispatcher
pub struct DispatcherRequest {
    request_meta: RequestMeta,
    command: Msql,
    txvn: Option<TxVN>,
}

impl ExecutorRequest for DispatcherRequest {
    type ReplyType = DispatcherReply;
}

impl DispatcherRequest {
    pub fn new(client_meta: ClientMeta, command: Msql, txvn: Option<TxVN>, current_request_id: usize) -> Self {
        Self {
            request_meta: RequestMeta::new(&client_meta, current_request_id),
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

        Span::current().record("message", &&request.request_meta.to_string()[..]);
        Span::current().record("cmd", &request.command.as_ref());
        debug!("<- {:?} {:?}", request.command, request.txvn);

        // Check whether there are no dbproxies in managers at all,
        // if such case, early exit
        if self.dbproxy_manager.inner().is_empty() {
            warn!("There are currently no dbproxy servers online, Scheduler dispatcher skipped the work");

            reply_ch
                .unwrap()
                .send(DispatcherReply::new(
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

        let num_dbproxy = dbproxy_addrs.len();
        let dbproxy_tasks_stream = stream::iter(dbproxy_addrs);

        let DispatcherRequest {
            request_meta,
            command,
            txvn,
        } = request;

        let msg = Message::MsqlRequest(request_meta, command.clone(), txvn.clone());
        let shared_reply_channel = Arc::new(Mutex::new(reply_ch));

        // Send all requests to transceivers
        let addr_receipts: Vec<_> = dbproxy_tasks_stream
            .then(move |(dbproxy_addr, transceiver_addr)| {
                let msg = msg.clone();
                let dbproxy_addr_clone = dbproxy_addr.clone();
                debug!("-> {:?}", msg);
                async move {
                    transceiver_addr
                        .request_nowait(TransceiverRequest {
                            dbproxy_addr: dbproxy_addr.clone(),
                            dbproxy_msg: msg,
                        })
                        .inspect_err(|e| error!("Cannot send: {:?}", e))
                        .map_ok(|receipt| (dbproxy_addr, receipt))
                        .await
                }
                .instrument(info_span!("->dbproxy", N = num_dbproxy, message = %dbproxy_addr_clone))
            })
            .filter_map(|r| async move {
                match r {
                    Err(_) => None,
                    Ok(r) => Some(r),
                }
            })
            .collect()
            .await;

        // Must join here before continue, so that the next query from the same user in the same transaction won't
        // get ahead of current query by any chances

        // Wait for responses from transceivers concurrently
        stream::iter(addr_receipts)
            .for_each_concurrent(None, move |(dbproxy_addr, transceiver_receipt)| {
                let command_cloned = command.clone();
                let txvn_cloned = txvn.clone();
                let shared_reply_channel_cloned = shared_reply_channel.clone();
                async move {
                    let is_endtx = command_cloned.is_endtx();
                    let msqlresponse = transceiver_receipt
                        .wait_request()
                        .and_then(|res| match res.msg {
                            Message::MsqlResponse(_, msqlresponse) => future::ok(msqlresponse),
                            _ => future::err(String::from("Invalid response from Dbproxy")),
                        })
                        .unwrap_or_else(|e| {
                            if is_endtx {
                                MsqlResponse::endtx_err(e)
                            } else {
                                MsqlResponse::query_err(e)
                            }
                        })
                        .await;

                    // Release table versions
                    let txvn = match command_cloned {
                        Msql::Query(query) if query.has_early_release() && txvn_cloned.is_some() => {
                            let mut txvn = txvn_cloned.unwrap();
                            let (_, _, ertables) = query.unwrap();
                            let er_token = txvn
                                .early_release_request(ertables)
                                .expect("Early release requesting tables not in TxVN");
                            debug!("Early releasing {:?}", er_token);
                            self.release_version(&dbproxy_addr, er_token).await;
                            Some(txvn)
                        }
                        Msql::EndTx(_) => {
                            let re_token = txvn_cloned
                                .expect("EndTx must include Some(TxVN)")
                                .into_dbvn_release_request();
                            debug!("Releasing {:?}", re_token);
                            self.release_version(&dbproxy_addr, re_token).await;
                            None
                        }
                        _ => txvn_cloned,
                    };

                    // If the oneshot channel is not consumed, consume it to send the reply back to handler
                    if let Some(reply) = shared_reply_channel_cloned.lock().await.take() {
                        debug!("~~ {:?}", msqlresponse);
                        reply
                            .send(DispatcherReply::new(msqlresponse, txvn))
                            .expect(&format!("Cannot reply response to handler"));
                    } else {
                        debug!("Not reply to handler: {:?}", msqlresponse);
                    }
                }
                .instrument(info_span!("<-dbproxy", N = num_dbproxy, message = %dbproxy_addr))
            })
            .await;

        info!("all tasks done");
    }

    #[instrument(skip(self), fields(msqlquery, txvn))]
    async fn wait_on_version(&self, msqlquery: &MsqlQuery, txvn: &Option<TxVN>) -> (SocketAddr, TransceiverAddr) {
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
                let dbvn_manager = self.dbvn_manager.read().await;

                avail_dbproxy = dbvn_manager.get_all_that_can_execute_read_query(msqlquery.tableops(), txvn);

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
            let selected_dbproxy = &avail_dbproxy[0];
            debug!(
                "Found dbproxy {} for executing the ReadOnly query: {:?}",
                selected_dbproxy.0, selected_dbproxy.1
            );
            (
                selected_dbproxy.0.clone(),
                self.dbproxy_manager.get(&selected_dbproxy.0),
            )
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
    pub async fn run(self) {
        let num_dbvn_manager = self.state.dbvn_manager.read().await.inner().len();
        Span::current().record("dbvn", &num_dbvn_manager);

        let Dispatcher { state, request_rx } = self;

        // Handle each DispatcherRequest concurrently
        request_rx
            .for_each_concurrent(None, |dispatch_request| async {
                state.clone().execute(dispatch_request).await
            })
            .await;

        info!("DIES");
    }
}

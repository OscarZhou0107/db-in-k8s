use super::core::*;
use super::dispatcher::*;
use crate::comm::MsqlResponse;
use crate::comm::{scheduler_api, scheduler_sequencer};
use crate::core::Msql;
use crate::util::admin_handler::*;
use crate::util::config::*;
use crate::util::tcp;
use bb8::Pool;
use futures::prelude::*;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, error, info};

/// Main entrance for the Scheduler from appserver
///
/// The incoming packet is checked:
/// 1. Connection Phase, SSL off, compression off. Bypassed
/// 2. Command Phase, Text Protocol. Deserialized and handled
///   - `BEGIN {TRAN | TRANSACTION} [transaction_name] WITH MARK 'READ table_0 table_1 WRITE table_2' [;]`
///   - UPDATE or SELECT
///   - `COMMIT [{TRAN | TRANSACTION} [transaction_name]] [;]`
///
/// `{}` - Keyword list;
/// `|`  - Or;
/// `[]` - Optional
///
pub async fn main(conf: Config) {
    // The current task completes as soon as start_tcplistener finishes,
    // which happens when it reaches the max_conn_till_dropped if not None,
    // which is really depending on the incoming connections into Scheduler.
    // So the sequencer_socket_pool here does not require an explicit
    // max_lifetime being set.
    // Prepare sequencer pool
    let sequencer_socket_pool = Pool::builder()
        .max_size(conf.scheduler.sequencer_pool_size)
        .build(tcp::TcpStreamConnectionManager::new(conf.sequencer.to_addr()).await)
        .await
        .unwrap();

    // prepare dispatcher
    let (dispatcher_addr, dispatcher) = Dispatcher::new(
        conf.scheduler.dispatcher_queue_size,
        State::new(
            DbVNManager::from_iter(conf.to_dbproxy_addrs()),
            DbproxyManager::from_iter(conf.to_dbproxy_addrs(), conf.scheduler.dbproxy_pool_size).await,
        ),
    );

    // Launch dispatcher as a new task
    let dispatcher_handle = tokio::spawn(dispatcher.run());

    // Create a stop_signal channel if admin mode is turned on
    let (stop_tx, stop_rx) = if conf.scheduler.admin_addr.is_some() {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // Launch main handler as a new task
    let handler_handle = tokio::spawn(tcp::start_tcplistener(
        conf.scheduler.to_addr(),
        move |tcp_stream| {
            let sequencer_socket_pool = sequencer_socket_pool.clone();
            // Connection/session specific storage
            // Note: this closure contains one copy of dispatcher_addr
            // Then, for each connection, a new dispatcher_addr is cloned
            let dispatcher_addr = Arc::new(dispatcher_addr.clone());
            async move {
                process_connection(tcp_stream, sequencer_socket_pool, dispatcher_addr).await;
            }
        },
        conf.scheduler.max_connection,
        "Scheduler",
        stop_rx,
    ));

    // Combine the dispatcher handle and main handler handle into a main_handle
    let main_handle = future::try_join(dispatcher_handle, handler_handle);

    // Allow scheduler to be terminated by admin
    if let Some(admin_addr) = &conf.scheduler.admin_addr {
        let admin_addr = admin_addr.clone();
        let admin_handle = tokio::spawn(async move {
            start_admin_tcplistener(admin_addr, basic_admin_command_handler, "Scheduler").await;
            stop_tx.unwrap().send(()).unwrap();
        });

        // main_handle can either run to finish or be the result
        // of the above stop_tx.send()
        main_handle.await.unwrap();

        // At this point, we just want to cancel the admin_handle
        tokio::select! {
            _ = future::ready(()) => info!("Scheduler admin is terminated" ),
            _ = admin_handle => {}
        };
    } else {
        main_handle.await.unwrap();
    }
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(
    mut socket: TcpStream,
    sequencer_socket_pool: Pool<tcp::TcpStreamConnectionManager>,
    dispatcher_addr: Arc<DispatcherAddr>,
) {
    let peer_addr = socket.peer_addr().unwrap();
    let local_addr = socket.local_addr().unwrap();
    let (tcp_read, tcp_write) = socket.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<scheduler_api::Message>::default(),
    );
    let serded_write = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<scheduler_api::Message>::default(),
    );

    // Each individual connection communication is executed in blocking order,
    // the socket is dedicated for one session only, opposed to being shared for multiple sessions.
    // At any given point, there is at most one transaction.
    // Connection/session specific storage
    let conn_state = Arc::new(Mutex::new(ConnectionState::default()));

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(move |msg| {
            let conn_state_cloned = conn_state.clone();
            let sequencer_socket_pool_cloned = sequencer_socket_pool.clone();
            let dispatcher_addr_cloned = dispatcher_addr.clone();
            let client_addr_cloned = peer_addr.clone();
            async move {
                debug!("<- [{}] Received {:?}", peer_addr, msg);
                let response = match msg {
                    scheduler_api::Message::RequestMsql(msql) => {
                        process_msql(
                            msql,
                            client_addr_cloned,
                            conn_state_cloned,
                            sequencer_socket_pool_cloned,
                            dispatcher_addr_cloned,
                        )
                        .await
                    }
                    scheduler_api::Message::RequestMsqlText(msqltext) => match Msql::try_from(msqltext) {
                        // Try to convert MsqlText to Msql first
                        Ok(msql) => {
                            process_msql(
                                msql,
                                client_addr_cloned,
                                conn_state_cloned,
                                sequencer_socket_pool_cloned,
                                dispatcher_addr_cloned,
                            )
                            .await
                        }
                        Err(e) => scheduler_api::Message::InvalidMsqlText(e.to_owned()),
                    },
                    scheduler_api::Message::RequestCrash(reason) => {
                        error!(
                            "[{}] <- [{}] Requested for a soft crash because of {}",
                            local_addr, peer_addr, reason
                        );
                        error!(
                            "[{}] -- [{}] Current connection state: {:?}",
                            local_addr, peer_addr, conn_state_cloned
                        );
                        panic!("Received a soft crash request");
                    }
                    _ => scheduler_api::Message::InvalidRequest,
                };
                debug!("-> [{}] Reply {:?}", peer_addr, response);
                Ok(response)
            }
        })
        .forward(serded_write)
        .map(|_| ())
        .await;

    info!("[{}] <- [{}] connection disconnected", local_addr, peer_addr);
}

async fn process_msql(
    msql: Msql,
    client_addr: SocketAddr,
    conn_state: Arc<Mutex<ConnectionState>>,
    sequencer_socket_pool: Pool<tcp::TcpStreamConnectionManager>,
    dispatcher_addr: Arc<DispatcherAddr>,
) -> scheduler_api::Message {
    // Not creating any critical session indeed, process_msql will always be executing in serial
    let mut conn_state_guard = conn_state.lock().await;
    match msql {
        Msql::BeginTx(msqlbegintx) => {
            if conn_state_guard.current_txvn().is_some() {
                scheduler_api::Message::Reply(MsqlResponse::begintx_err("Previous transaction not finished yet"))
            } else {
                let mut sequencer_socket = sequencer_socket_pool.get().await.unwrap();
                let msg = scheduler_sequencer::Message::RequestTxVN(msqlbegintx);
                debug!(
                    "[{}] -> Request to Sequencer: {:?}",
                    sequencer_socket.local_addr().unwrap(),
                    msg
                );
                tcp::send_and_receive_single_as_json(&mut sequencer_socket, msg, "Scheduler handler")
                    .map_err(|e| e.to_string())
                    .and_then(|res| async {
                        match res {
                            scheduler_sequencer::Message::ReplyTxVN(txvn) => {
                                let existing = conn_state_guard.replace_txvn(Some(txvn));
                                assert!(existing.is_none());
                                Ok(())
                            }
                            _ => Err(String::from("Invalid response from Sequencer")),
                        }
                    })
                    .map_ok_or_else(
                        |e| scheduler_api::Message::Reply(MsqlResponse::begintx_err(e)),
                        |_| scheduler_api::Message::Reply(MsqlResponse::begintx_ok()),
                    )
                    .await
            }
        }
        Msql::Query(_) => {
            if conn_state_guard.current_txvn().is_none() {
                error!(
                    "<- [{}] Does not support single un-transactioned query for now",
                    client_addr
                );
                panic!("Does not support single un-transactioned query for now");
            }

            dispatcher_addr
                .request(client_addr, msql, conn_state_guard.current_txvn().clone())
                .map_ok_or_else(
                    |e| scheduler_api::Message::Reply(MsqlResponse::query_err(e)),
                    |res| {
                        let DispatcherReply { msql_res, txvn_res } = res;
                        conn_state_guard.replace_txvn(txvn_res);
                        scheduler_api::Message::Reply(msql_res)
                    },
                )
                .await
        }
        Msql::EndTx(_) => {
            let txvn = conn_state_guard.replace_txvn(None);
            if txvn.is_none() {
                scheduler_api::Message::Reply(MsqlResponse::endtx_err("There is not transaction to end"))
            } else {
                dispatcher_addr
                    .request(client_addr, msql, txvn)
                    .map_ok_or_else(
                        |e| scheduler_api::Message::Reply(MsqlResponse::endtx_err(e)),
                        |res| {
                            let DispatcherReply { msql_res, txvn_res } = res;
                            let existing = conn_state_guard.replace_txvn(txvn_res);
                            assert!(existing.is_none());
                            scheduler_api::Message::Reply(msql_res)
                        },
                    )
                    .await
            }
        }
    }
}

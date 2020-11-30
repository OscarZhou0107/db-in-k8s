use super::core::*;
use super::dispatcher::*;
use crate::comm::{appserver_scheduler, scheduler_sequencer};
use crate::core::msql::*;
use crate::util::config::*;
use crate::util::tcp;
use bb8::Pool;
use futures::prelude::*;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::debug;

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

    // Launch main handler as a new task
    let handler_handle = tokio::spawn(tcp::start_tcplistener(
        conf.scheduler.to_addr(),
        move |tcp_stream| {
            let sequencer_socket_pool = sequencer_socket_pool.clone();
            let dispatcher_addr = dispatcher_addr.clone();
            async move {
                process_connection(tcp_stream, sequencer_socket_pool, dispatcher_addr).await;
            }
        },
        conf.scheduler.max_connection,
        "Scheduler",
    ));

    // Wait for tasks to join
    tokio::try_join!(dispatcher_handle, handler_handle).unwrap();
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(
    mut socket: TcpStream,
    sequencer_socket_pool: Pool<tcp::TcpStreamConnectionManager>,
    dispatcher_addr: DispatcherAddr,
) {
    let peer_addr = socket.peer_addr().unwrap();
    let (tcp_read, tcp_write) = socket.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<appserver_scheduler::Message>::default(),
    );
    let serded_write = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<appserver_scheduler::Message>::default(),
    );

    // Each individual connection communication is executed in blocking order,
    // the socket is dedicated for one session only, opposed to being shared for multiple sessions.
    // At any given point, there is at most one transaction.
    // Connection specific storage
    let conn_state = Arc::new(Mutex::new(ConnectionState::default()));

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(move |msg| {
            let conn_state_cloned = conn_state.clone();
            let sequencer_socket_pool_cloned = sequencer_socket_pool.clone();
            let dispatcher_addr_cloned = dispatcher_addr.clone();
            async move {
                debug!("<- [{}] Received {:?}", peer_addr, msg);
                // process_request(msg, conn_state_cloned, peer_addr, sequencer_socket_pool_cloned).await
                let response = match msg {
                    appserver_scheduler::Message::RequestMsql(msql) => {
                        process_msql(
                            msql,
                            conn_state_cloned,
                            sequencer_socket_pool_cloned,
                            dispatcher_addr_cloned,
                        )
                        .await
                    }
                    appserver_scheduler::Message::RequestMsqlText(msqltext) => match Msql::try_from(msqltext) {
                        // Try to convert MsqlText to Msql first
                        Ok(msql) => {
                            process_msql(
                                msql,
                                conn_state_cloned,
                                sequencer_socket_pool_cloned,
                                dispatcher_addr_cloned,
                            )
                            .await
                        }
                        Err(e) => appserver_scheduler::Message::InvalidMsqlText(e.to_owned()),
                    },
                    _ => appserver_scheduler::Message::InvalidRequest,
                };
                debug!("-> [{}] Reply {:?}", peer_addr, response);
                Ok(response)
            }
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

async fn process_msql(
    msql: Msql,
    conn_state: Arc<Mutex<ConnectionState>>,
    sequencer_socket_pool: Pool<tcp::TcpStreamConnectionManager>,
    dispatcher_addr: DispatcherAddr,
) -> appserver_scheduler::Message {
    match msql {
        Msql::BeginTx(msqlbegintx) => {
            if conn_state.lock().await.current_txvn().is_some() {
                appserver_scheduler::Message::Reply(appserver_scheduler::MsqlResponse::BeginTx(Err(String::from(
                    "Previous transaction not finished yet",
                ))))
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
                                conn_state.lock().await.insert_txvn(txvn);
                                Ok(())
                            }
                            _ => Err(String::from("Invalid response from Sequencer")),
                        }
                    })
                    .map_ok_or_else(
                        |e| appserver_scheduler::Message::Reply(appserver_scheduler::MsqlResponse::BeginTx(Err(e))),
                        |_| appserver_scheduler::Message::Reply(appserver_scheduler::MsqlResponse::BeginTx(Ok(()))),
                    )
                    .await
            }
        }
        _ => unimplemented!(),
    }
}

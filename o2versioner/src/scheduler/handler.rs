//use crate::comm::scheduler_sequencer;
use crate::core::sql::{SqlRawString, TxTable};
use crate::core::version_number::TxVN;
use crate::util::tcp::*;
use bb8::Pool;
use futures::prelude::*;
use log::info;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Main entrance for the scheduler from appserver
///
/// 1. `addr` is the tcp port to bind to
/// 2. `max_connection` can be specified to limit the max number of connections allowed
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
pub async fn main<A>(
    scheduler_addr: A,
    sceduler_max_connection: Option<u32>,
    sequencer_addr: A,
    sequencer_max_connection: u32,
    sequencer_connection_max_lifetime: Option<Duration>,
) where
    A: ToSocketAddrs + std::fmt::Debug + Clone,
{
    let sequencer_socket_pool = Pool::builder()
        .max_size(sequencer_max_connection)
        .max_lifetime(sequencer_connection_max_lifetime)
        // .connection_timeout(Duration::from_millis(300))
        // .reaper_rate(Duration::from_millis(300))
        .build(TcpStreamConnectionManager::new(sequencer_addr).await)
        .await
        .unwrap();

    start_tcplistener(
        scheduler_addr,
        |tcp_stream| process_connection(tcp_stream, sequencer_socket_pool.clone()),
        sceduler_max_connection,
        Some("Scheduler"),
    )
    .await;
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(socket: TcpStream, sequencer_socket_pool: Pool<TcpStreamConnectionManager>) {
    let peer_addr = socket.peer_addr().unwrap();
    let (tcp_read, tcp_write) = socket.into_split();

    // Need to use mysql client/server codec

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read: SymmetricallyFramed<_, SqlRawString, _> =
        SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<SqlRawString>::default());
    let serded_write: SymmetricallyFramed<_, SqlRawString, _> =
        SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<SqlRawString>::default());

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(|msg| {
            info!("Received [{:?}] {:?}", peer_addr, msg);
            process_request(peer_addr, msg, sequencer_socket_pool.clone())
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

async fn process_request(
    peer_addr: SocketAddr,
    request: SqlRawString,
    _sequencer_socket_pool: Pool<TcpStreamConnectionManager>,
) -> std::io::Result<SqlRawString> {
    let response = request;
    info!("    Reply [{:?}] {:?}", peer_addr, response);
    Ok(response)
}

#[allow(dead_code)]
async fn request_txvn(_tx_table: TxTable, mut _sequencer_socket_pool: Pool<TcpStreamConnectionManager>) -> TxVN {
    TxVN {
        tx_name: String::from("hello"),
        table_vns: Vec::new(),
    }
}

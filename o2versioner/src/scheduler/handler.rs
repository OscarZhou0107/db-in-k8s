use crate::comm::scheduler_sequencer;
use crate::core::sql::{SqlString, TxTable};
use crate::core::version_number::TxVN;
use crate::util::tcp::*;
use bb8::Pool;
use futures::prelude::*;
use log::debug;
use std::net::SocketAddr;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// Main entrance for the Scheduler from appserver
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
) where
    A: ToSocketAddrs,
{
    // The current task completes as soon as start_tcplistener finishes,
    // which happens when it reaches the sceduler_max_connection if not None,
    // which is really depending on the incoming connections into Scheduler.
    // So the sequencer_socket_pool here does not require an explicit
    // max_lifetime being set.
    let sequencer_socket_pool = Pool::builder()
        .max_size(sequencer_max_connection)
        .build(TcpStreamConnectionManager::new(sequencer_addr).await)
        .await
        .unwrap();

    start_tcplistener(
        scheduler_addr,
        |tcp_stream| process_connection(tcp_stream, sequencer_socket_pool.clone()),
        sceduler_max_connection,
        "Scheduler",
    )
    .await;
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(mut socket: TcpStream, sequencer_socket_pool: Pool<TcpStreamConnectionManager>) {
    let peer_addr = socket.peer_addr().unwrap();
    let (tcp_read, tcp_write) = socket.split();

    // Need to use mysql client/server codec

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read = SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<SqlString>::default());
    let serded_write = SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<SqlString>::default());

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(|msg| {
            debug!("<- [{}] Received {:?}", peer_addr, msg);
            process_request(msg, peer_addr, sequencer_socket_pool.clone())
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

/// Process the argument `request` and return a `Result` of response
async fn process_request(
    request: SqlString,
    peer_addr: SocketAddr,
    sequencer_socket_pool: Pool<TcpStreamConnectionManager>,
) -> std::io::Result<SqlString> {
    let response = if let Some(txtable) = request.to_txtable(true) {
        request_txvn(txtable, &mut sequencer_socket_pool.get().await.unwrap())
            .await
            .map_or_else(
                |e| SqlString(format!("[{}] failed due to: {}", request.0, e)),
                |_txvn| SqlString(format!("[{}] successful", request.0)),
            )
    } else {
        SqlString(format!("[{}] not recognized", request.0))
    };
    debug!("-> [{}] Reply {:?}", peer_addr, response);
    Ok(response)
}

/// Attempt to request a `TxVN` from the Sequencer based on the argument `TxTable`
async fn request_txvn(txtable: TxTable, sequencer_socket: &mut TcpStream) -> Result<TxVN, String> {
    let local_addr = sequencer_socket.local_addr().unwrap();
    let (tcp_read, tcp_write) = sequencer_socket.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let mut serded_read = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );
    let mut serded_write = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );

    let msg = scheduler_sequencer::Message::TxVNRequest(txtable);
    debug!("[{}] -> Request to Sequencer: {:?}", local_addr, msg);
    let sequencer_response = serded_write
        .send(msg)
        .and_then(|_| serded_read.try_next())
        .map_ok(|received_msg| {
            let received_msg = received_msg.unwrap();
            debug!("[{}] <- Reply from Sequencer: {:?}", local_addr, received_msg);
            received_msg
        })
        .await;

    sequencer_response.map_err(|e| e.to_string()).and_then(|res| match res {
        scheduler_sequencer::Message::TxVNResponse(txvn) => Ok(txvn),
        _ => Err(String::from("Invalid response from Sequencer")),
    })
}

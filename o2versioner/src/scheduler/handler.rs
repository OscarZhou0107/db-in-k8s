//use crate::comm::scheduler_sequencer;
use crate::core::sql::{SqlRawString, TxTable};
use crate::core::version_number::TxVN;
use futures::prelude::*;
use log::info;
use std::net::SocketAddr;
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
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
pub async fn main<A: ToSocketAddrs>(addr: A, max_connection: Option<usize>) {
    let mut listener = TcpListener::bind(addr).await.unwrap();

    let mut cur_num_connection = 0;
    let mut spawned_tasks = Vec::new();
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await.unwrap();

        info!("Connection established with [{}] {}", cur_num_connection, peer_addr);
        cur_num_connection += 1;

        // Spawn a new thread for each tcp connection
        spawned_tasks.push(tokio::spawn(process_connection(tcp_stream)));

        // An optional max number of connections allowed
        if let Some(nmax) = max_connection {
            if cur_num_connection >= nmax {
                break;
            }
        }
    }

    // Wait on all spawned tasks to finish
    futures::future::join_all(spawned_tasks).await;
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(tcp_stream: TcpStream) {
    let peer_addr = tcp_stream.peer_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.into_split();

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
            process_request(peer_addr, msg)
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

async fn process_request(peer_addr: SocketAddr, request: SqlRawString) -> std::io::Result<SqlRawString> {
    let response = request;
    info!("    Reply [{:?}] {:?}", peer_addr, response);
    Ok(response)
}

#[allow(dead_code)]
async fn request_txvn(_tx_table: TxTable) -> TxVN {
    TxVN {
        tx_name: String::from("hello"),
        table_vns: Vec::new(),
    }
}

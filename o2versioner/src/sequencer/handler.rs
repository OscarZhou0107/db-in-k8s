use super::core::State;
use crate::comm::scheduler_sequencer;
use futures::prelude::*;
use log::{info, warn};
use std::sync::{Arc, Mutex};
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

type ArcState = Arc<Mutex<State>>;

/// Main entrance for the sequencer
///
/// 1. `addr` is the tcp port to bind to
/// 2. `max_connection` can be specified to limit the max number of connections allowed
pub async fn main<A: ToSocketAddrs>(addr: A, max_connection: Option<usize>) {
    let state = Arc::new(Mutex::new(State::new()));

    let mut listener = TcpListener::bind(addr).await.unwrap();

    let mut cur_num_connection = 0;
    let mut spawned_tasks = Vec::new();
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await.unwrap();

        info!("Connection established with [{}] {}", cur_num_connection, peer_addr);
        cur_num_connection += 1;

        // Spawn a new thread for each tcp connection
        spawned_tasks.push(tokio::spawn(process_connection(tcp_stream, state.clone())));

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
async fn process_connection(tcp_stream: TcpStream, state: ArcState) {
    let peer_addr = tcp_stream.peer_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read: SymmetricallyFramed<_, scheduler_sequencer::Message, _> = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );
    let serded_write: SymmetricallyFramed<_, scheduler_sequencer::Message, _> = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );

    // Process a stream of incomming messages from a single tcp connection
    serded_read
        .and_then(|msg| match msg {
            scheduler_sequencer::Message::TxVNRequest(tx_table) => {
                info!("Received [{:?}] TxVNRequest on {:?}", peer_addr, tx_table);
                let mut state = state.lock().unwrap();
                let tx_vn = state.assign_vn(tx_table);
                info!("    Reply [{:?}] {:?}", peer_addr, tx_vn);
                future::ok(scheduler_sequencer::Message::TxVNResponse(tx_vn))
            }
            other => {
                warn!("Unsupported message [{:?}] {:?}", peer_addr, other);
                future::ok(scheduler_sequencer::Message::Invalid)
            }
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

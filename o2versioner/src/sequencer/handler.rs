use super::core::State;
use crate::comm::scheduler_sequencer;
use futures::prelude::*;
use log::{info, warn};
use std::sync::{Arc, Mutex};
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

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
    // Delimit frames from bytes using a length header
    let length_delimited = Framed::new(tcp_stream, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded: SymmetricallyFramed<_, scheduler_sequencer::Message, _> = SymmetricallyFramed::new(
        length_delimited,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );

    // Process a stream of incomming messages from a single tcp connection
    serded
        .for_each(|msg| {
            match msg {
                Ok(msg) => match msg {
                    scheduler_sequencer::Message::TxVNRequest(tx_table) => {
                        info!("Ready to process TxVNRequest on {:?}", tx_table);
                        let mut state = state.lock().unwrap();
                        let tx_vn = state.assign_vn(tx_table);
                        info!("    {:?}", tx_vn);
                    }
                    other => warn!("Unsupported message {:?}", other),
                },
                Err(e) => warn!("Encoutered {} when decoding incoming TCP message", e),
            }
            future::ready(())
        })
        .await;
}

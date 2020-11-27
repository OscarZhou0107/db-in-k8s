use super::core::State;
use crate::comm::scheduler_sequencer;
use crate::util::tcp;
use futures::prelude::*;
use log::{debug, warn};
use std::sync::{Arc, Mutex};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

type ArcState = Arc<Mutex<State>>;

/// Main entrance for Sequencer
pub async fn main<A>(addr: A, max_connection: Option<u32>)
where
    A: ToSocketAddrs,
{
    let state = Arc::new(Mutex::new(State::new()));

    tcp::start_tcplistener(
        addr,
        |tcp_stream| {
            let state_cloned = state.clone();
            process_connection(tcp_stream, state_cloned)
        },
        max_connection,
        "Sequencer",
    )
    .await;
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
    let serded_read = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );
    let serded_write = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(|msg| match msg {
            scheduler_sequencer::Message::TxVNRequest(sqlbegintx) => {
                debug!("<- [{}] TxVNRequest on {:?}", peer_addr, sqlbegintx);
                let mut state = state.lock().unwrap();
                let txvn = state.assign_vn(sqlbegintx);
                debug!("-> [{}] Reply {:?}", peer_addr, txvn);
                future::ok(scheduler_sequencer::Message::TxVNResponse(txvn))
            }
            other => {
                warn!("<- [{}] Unsupported message {:?}", peer_addr, other);
                future::ok(scheduler_sequencer::Message::Invalid)
            }
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

use super::core::State;
use crate::comm::scheduler_sequencer;
use crate::util::admin_handler::*;
use crate::util::config::SequencerConfig;
use crate::util::tcp;
use futures::prelude::*;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, info, warn};

/// Main entrance for Sequencer
/// 
/// Three flavors:
/// 1. Unlimited, the server will keep running forever.
/// 2. Limit the total maximum number of input connections,
/// once reaching that limit, no new connections are accepted.
/// 3. Admin port, can send `kill`, `exit` or `quit` in raw bytes
/// to the admin port, which will then force to not accept any new
/// connections.
pub async fn main(conf: SequencerConfig) {
    let state = Arc::new(Mutex::new(State::new()));

    // Create a stop_signal channel if admin mode is turned on
    let (stop_tx, stop_rx) = if conf.admin_addr.is_some() {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    let handler_handle = tokio::spawn(tcp::start_tcplistener(
        conf.to_addr(),
        move |tcp_stream| {
            let state_cloned = state.clone();
            process_connection(tcp_stream, state_cloned)
        },
        conf.max_connection,
        "Sequencer",
        stop_rx,
    ));

    // Allow sequencer to be terminated by admin
    if let Some(admin_addr) = &conf.admin_addr {
        let admin_addr = admin_addr.clone();
        let admin_handle = tokio::spawn(async move {
            start_admin_tcplistener(admin_addr, basic_admin_command_handler, "Sequencer").await;
            stop_tx.unwrap().send(()).unwrap();
        });

        // handler_handle can either run to finish or be the result
        // of the above stop_tx.send()
        handler_handle.await.unwrap();

        // At this point, we just want to cancel the admin_handle
        tokio::select! {
            _ = future::ready(()) => info!("Sequencer admin is terminated" ),
            _ = admin_handle => {}
        };
    } else {
        handler_handle.await.unwrap();
    }
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
async fn process_connection(mut tcp_stream: TcpStream, state: Arc<Mutex<State>>) {
    let peer_addr = tcp_stream.peer_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.split();

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
        .and_then(move |msg| {
            let state_cloned = state.clone();
            async move {
                match msg {
                    scheduler_sequencer::Message::RequestTxVN(sqlbegintx) => {
                        debug!("<- [{}] RequestTxVN on {:?}", peer_addr, sqlbegintx);
                        let txvn = state_cloned.lock().await.assign_vn(sqlbegintx);
                        debug!("-> [{}] Reply {:?}", peer_addr, txvn);
                        Ok(scheduler_sequencer::Message::ReplyTxVN(txvn))
                    }
                    other => {
                        warn!("<- [{}] Unsupported message {:?}", peer_addr, other);
                        Ok(scheduler_sequencer::Message::Invalid)
                    }
                }
            }
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

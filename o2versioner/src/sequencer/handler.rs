use super::core::State;
use crate::comm::scheduler_sequencer::*;
use crate::util::config::SequencerConfig;
use crate::util::tcp;
use futures::prelude::*;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, field, info_span, instrument, warn, Instrument, Span};

/// Main entrance for Sequencer
///
/// Three flavors:
/// 1. Unlimited, the server will keep running forever.
/// 2. Limit the total maximum number of input connections,
/// once reaching that limit, no new connections are accepted.
/// 3. Admin port, can send `kill`, `exit` or `quit` in raw bytes
/// to the admin port, which will then force to not accept any new
/// connections.
#[instrument(name = "sequencer", skip(conf))]
pub async fn main(conf: SequencerConfig) {
    let state = Arc::new(Mutex::new(State::new()));

    let (stop_tx, stop_rx) = oneshot::channel();
    let stop_tx = Arc::new(Mutex::new(Some(stop_tx)));

    let handler_handle = tokio::spawn(
        tcp::start_tcplistener(
            conf.to_addr(),
            move |tcp_stream| {
                let stop_tx_cloned = stop_tx.clone();
                let state_cloned = state.clone();
                process_connection(tcp_stream, state_cloned, stop_tx_cloned)
            },
            conf.max_connection,
            Some(stop_rx),
        )
        .in_current_span(),
    );

    handler_handle.await.unwrap();
}

/// Process the `tcp_stream` for a single connection
///
/// Will process all messages sent via this `tcp_stream` on this tcp connection.
/// Once this tcp connection is closed, this function will return
#[instrument(name="conn", skip(tcp_stream, state, stop_tx), fields(message=field::Empty))]
async fn process_connection(
    mut tcp_stream: TcpStream,
    state: Arc<Mutex<State>>,
    stop_tx: Arc<Mutex<Option<tcp::StopTx>>>,
) {
    let peer_addr = tcp_stream.peer_addr().unwrap();

    Span::current().record("message", &&peer_addr.to_string()[..]);

    let (tcp_read, tcp_write) = tcp_stream.split();

    // Delimit frames from bytes using a length header
    let delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read = SymmetricallyFramed::new(delimited_read, SymmetricalJson::<Message>::default());
    let serded_write = SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());

    // Process a stream of incoming messages from a single tcp connection
    serded_read
        .and_then(move |msg| {
            let state_cloned = state.clone();
            let stop_tx_cloned = stop_tx.clone();

            let process_req = async move {
                Span::current().record("message", &msg.as_ref());
                match msg {
                    Message::RequestTxVN(client_meta, sqlbegintx) => {
                        Span::current().record("client", &&client_meta.to_string()[..]);
                        debug!("<- {:?}", sqlbegintx);
                        let txvn = state_cloned.lock().await.assign_vn(sqlbegintx);
                        debug!("-> {:?}", txvn);
                        Ok(Message::ReplyTxVN(txvn))
                    }
                    Message::RequestBlock => {
                        let prev_is_blocked = state_cloned.lock().await.set_vn_record_blocked(true);
                        let status = if prev_is_blocked {
                            "is already blocked"
                        } else {
                            "set blocked successfully"
                        };

                        warn!("{}", status);
                        Ok(Message::ReplyBlockUnblock(String::from(status)))
                    }
                    Message::RequestUnblock => {
                        let prev_is_blocked = state_cloned.lock().await.set_vn_record_blocked(false);

                        let status = if prev_is_blocked {
                            "set unblocked successfully"
                        } else {
                            "is already unblocked"
                        };

                        warn!("{}", status);
                        Ok(Message::ReplyBlockUnblock(String::from(status)))
                    }
                    Message::RequestStop => {
                        warn!("Receiving stop request, will shutdown soon");
                        stop_tx_cloned
                            .lock()
                            .await
                            .take()
                            .expect("The StopTx is already used!")
                            .send(())
                            .unwrap();
                        Ok(Message::ReplyStop)
                    }
                    other => {
                        warn!("<- Unsupported: {:?}", other);
                        Ok(Message::Invalid)
                    }
                }
            };

            process_req.instrument(info_span!("request", message = field::Empty, client = field::Empty))
        })
        .forward(serded_write)
        .map(|_| ())
        .await;
}

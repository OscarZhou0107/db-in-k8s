#![allow(warnings)]
use super::inner_comm::*;
use crate::comm::scheduler_dbproxy::*;
use crate::comm::MsqlResponse;
use crate::core::*;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, field, info, instrument, warn, Span};

/// A state containing shared variables
#[derive(Clone)]
struct State {
    transmitters: Arc<Mutex<HashMap<SocketAddr, OwnedWriteHalf>>>,
    outstanding_req: Arc<Mutex<HashMap<(SocketAddr, SocketAddr), Request>>>,
}

impl State {
    fn new(transmitters: HashMap<SocketAddr, OwnedWriteHalf>) -> Self {
        Self {
            transmitters: Arc::new(Mutex::new(transmitters)),
            outstanding_req: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[instrument(name="execute_request", skip(self, request), fields(message=field::Empty, cmd=field::Empty, op=field::Empty))]
    async fn execute_request(&self, request: Request) {
        Span::current().record("message", &&request.client_meta.to_string()[..]);
        Span::current().record("cmd", &request.command.as_ref());
        debug!("<- {:?} {:?}", request.command, request.txvn);
    }

    #[instrument(name="execute_response", skip(self, msg), fields(message=field::Empty))]
    async fn execute_response(&self, msg: Message) {
        match msg {
            Message::MsqlResponseNew(client_addr, msqlresponse) => {
                todo!();
            }
            other => warn!("Unsupported {:?}", other),
        };
    }
}

pub struct Transceiver {
    state: State,
    receivers: Vec<(SocketAddr, OwnedReadHalf)>,

    request_rx: mpsc::Receiver<Request>,
}

impl Transceiver {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` into `Transceiver`
    pub async fn new<I>(queue_size: usize, iter: I) -> (ExecutorAddr, Self)
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let sockets: Vec<_> = stream::iter(iter)
            .then(|dbproxy_port| async move {
                TcpStream::connect(dbproxy_port)
                    .await
                    .expect("Cannot connect to dbproxy")
            })
            .collect()
            .await;

        let (receivers, transmitters) = sockets
            .into_iter()
            .map(|tcp_stream| {
                let dbproxy_addr = tcp_stream.peer_addr().unwrap();
                let (rx, tx) = tcp_stream.into_split();
                ((dbproxy_addr.clone(), rx), (dbproxy_addr, tx))
            })
            .unzip();

        let state = State::new(transmitters);

        let (addr, request_rx) = ExecutorAddr::new(queue_size);

        (
            addr,
            Self {
                state,
                receivers,
                request_rx,
            },
        )
    }

    #[instrument(name="transceive", skip(self), fields(dbproxy=field::Empty))]
    pub async fn run(self) {
        Span::current().record("dbproxy", &self.receivers.len());
        let Transceiver {
            state,
            receivers,
            request_rx,
        } = self;

        // Every dbproxy receiver is spawned as separate task
        let state_clone = state.clone();
        let receiver_handle = tokio::spawn(stream::iter(receivers).for_each_concurrent(
            None,
            move |(dbproxy_addr, reader)| {
                let state = state_clone.clone();
                async move {
                    let delimited_read = FramedRead::new(reader, LengthDelimitedCodec::new());
                    let serded_read = SymmetricallyFramed::new(delimited_read, SymmetricalJson::<Message>::default());

                    // Each response execution is spawned as separate task
                    serded_read
                        .try_for_each_concurrent(None, |msg| async {
                            state.clone().execute_response(msg).await;
                            Ok(())
                        })
                        .await;
                }
            },
        ));

        // Every incoming request_rx is spawned as separate task
        let state_clone = state.clone();
        let request_rx_handle = tokio::spawn(request_rx.for_each_concurrent(None, move |tranceive_request| {
            let state = state_clone.clone();
            async move {
                state.clone().execute_request(tranceive_request).await;
            }
        }));

        tokio::try_join!(receiver_handle, request_rx_handle);
    }
}

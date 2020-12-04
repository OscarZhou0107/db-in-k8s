use crate::comm::scheduler_dbproxy::*;
use crate::util::executor_addr::*;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, field, instrument, warn, Span};

/// Request sent from dispatcher direction
pub struct TransceiverRequest {
    pub dbproxy_addr: SocketAddr,
    pub dbproxy_msg: Message,
}

impl ExecutorRequest for TransceiverRequest {
    type ReplyType = Message;
}

/// A state containing shared variables
#[derive(Clone)]
struct State {
    transmitters: Arc<Mutex<HashMap<SocketAddr, OwnedWriteHalf>>>,
    /// (DbProxyAddr, ClientAddr) -> RequestWrapper<TransceiverRequest>
    outstanding_req: Arc<Mutex<HashMap<(SocketAddr, SocketAddr), RequestWrapper<TransceiverRequest>>>>,
}

impl State {
    fn new(transmitters: HashMap<SocketAddr, OwnedWriteHalf>) -> Self {
        Self {
            transmitters: Arc::new(Mutex::new(transmitters)),
            outstanding_req: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[instrument(name="execute_request", skip(self, request), fields(message=field::Empty))]
    async fn execute_request(&self, request: RequestWrapper<TransceiverRequest>) {
        let dbproxy_msg = request.request().dbproxy_msg.clone();
        let client_addr = dbproxy_msg.get_client_addr().clone().unwrap();
        let dbproxy_addr = request.request().dbproxy_addr.clone();

        Span::current().record("message", &&dbproxy_addr.to_string()[..]);

        let prev = self
            .outstanding_req
            .lock()
            .await
            .insert((dbproxy_addr, client_addr), request);
        assert!(prev.is_none());

        let mut transimtters_guard = self.transmitters.lock().await;
        let writer = transimtters_guard.get_mut(&dbproxy_addr).expect("dbproxy_addr unknown");
        let delimited_write = FramedWrite::new(writer, LengthDelimitedCodec::new());
        let mut serded_write = SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());

        debug!("-> {:?}", dbproxy_msg);
        serded_write.send(dbproxy_msg).await.expect("cannot send to dbproxy");

        // transmitters_guard is released
    }

    #[instrument(name="execute_response", skip(self, dbproxy_addr, msg), fields(message=field::Empty, from=field::Empty))]
    async fn execute_response(&self, dbproxy_addr: SocketAddr, msg: Message) {
        Span::current().record("from", &&dbproxy_addr.to_string()[..]);
        match &msg {
            Message::MsqlResponse(client_addr, _msqlresponse) => {
                Span::current().record("message", &&client_addr.to_string()[..]);
                let request_wrapper = self
                    .outstanding_req
                    .lock()
                    .await
                    .remove(&(dbproxy_addr, client_addr.clone()))
                    .expect("No record in outstanding_req");
                let (_request, reply_ch) = request_wrapper.unwrap();
                debug!("-> {:?}", msg);
                reply_ch.unwrap().send(msg).unwrap();
            }
            other => warn!("Unsupported {:?}", other),
        };
    }
}

pub type TransceiverAddr = ExecutorAddr<TransceiverRequest>;

pub struct Transceiver {
    state: State,
    receivers: Vec<(SocketAddr, OwnedReadHalf)>,

    request_rx: RequestReceiver<TransceiverRequest>,
}

impl Transceiver {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` into `Transceiver`
    pub async fn new<I>(queue_size: usize, iter: I) -> (TransceiverAddr, Self)
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let sockets: Vec<_> = stream::iter(iter)
            .inspect(|addr| debug!("{}", addr))
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
                            state.clone().execute_response(dbproxy_addr, msg).await;
                            Ok(())
                        })
                        .await
                        .unwrap();
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

        tokio::try_join!(receiver_handle, request_rx_handle).unwrap();
    }
}

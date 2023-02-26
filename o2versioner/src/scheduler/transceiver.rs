use crate::comm::scheduler_dbproxy::*;
use crate::util::executor::Executor;
use crate::util::executor_addr::*;
use async_trait::async_trait;
use futures::prelude::*;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::net::Shutdown;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{field, info, info_span, instrument, trace, warn, Instrument, Span};

/// Request sent from dispatcher direction
#[derive(Debug)]
pub enum TransceiverRequest {
    DbproxyMsg {
        dbproxy_addr: SocketAddr,
        dbproxy_msg: Message,
    },
    DbproxyLoad,
    Dbreplica{
        dbproxy_addr: SocketAddr,
        dbproxy_msg: Message,
    },
}

impl TransceiverRequest {
    fn try_get_dbproxy_msg_inner(&self) -> Result<(&SocketAddr, &Message), ()> {
        match self {
            Self::DbproxyMsg {
                dbproxy_addr,
                dbproxy_msg,
            } => Ok((dbproxy_addr, dbproxy_msg)),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub enum TransceiverReply {
    DbproxyMsg(Message),
    DbproxyLoad(usize),
}

impl ExecutorRequest for TransceiverRequest {
    type ReplyType = TransceiverReply;
}

pub type TransceiverAddr = ExecutorAddr<TransceiverRequest>;

/// Responding for the tcp communication with a single dbproxy
///
/// # Notes
/// 1. Uses a single TCP connection for both sending and receiving
/// 2. Sending and receiving are split so can send and receive concurrently
#[derive(Debug)]
pub struct Transceiver {
    dbproxy_addr: SocketAddr,
    request_rx: RequestReceiver<TransceiverRequest>,
    admin_stop_signal: Arc<Mutex<bool>>,
}

impl Transceiver {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` into `Transceiver`
    pub fn new(queue_size: usize, dbproxy_addr: SocketAddr, admin_stop_signal: Arc<Mutex<bool>>) -> (TransceiverAddr, Self) {
        let (addr, request_rx) = ExecutorAddr::new(queue_size);
        (
            addr,
            Self {
                dbproxy_addr,
                request_rx,
                admin_stop_signal, 
            },
        )
    }
}

#[async_trait]
impl Executor for Transceiver {
    #[instrument(name="transceive", skip(self), fields(message=field::Empty))]
    async fn run(mut self: Box<Self>) {
        Span::current().record("message", &&self.dbproxy_addr.to_string()[..]);

        let Transceiver {
            dbproxy_addr,
            mut request_rx,
            admin_stop_signal,  
        } = *self;

        let socket = TcpStream::connect(dbproxy_addr)
            .await
            .expect("Cannot connect to dbproxy");
        let (reader, mut writer) = socket.into_split();

        // (ClientAddr) -> LinkedList<RequestWrapper<TransceiverRequest>>
        let outstanding_req: HashMap<SocketAddr, VecDeque<RequestWrapper<TransceiverRequest>>> = HashMap::new();
        let outstanding_req = Arc::new(Mutex::new(outstanding_req));

        // Spawn dbproxy reader, processing reply from dbproxy in serial one after one
        let delimited_read = FramedRead::new(reader, LengthDelimitedCodec::new());
        let mut serded_read = SymmetricallyFramed::new(delimited_read, SymmetricalJson::<Message>::default());
        let outstanding_req_clone = outstanding_req.clone();
        let reader_task = async move {
            while let Some(msg) = serded_read.try_next().await.unwrap() {
                async {
                    match &msg {
                        Message::MsqlResponse(arrived_request_meta, _msqlresponse) => {
                            Span::current().record("message", &&arrived_request_meta.to_string()[..]);
                            let mut guard = outstanding_req_clone.lock().await;
                            let queue = guard
                                .get_mut(&arrived_request_meta.client_addr)
                                .expect("No client addr in outstanding_req");

                            let queue_idx = queue
                                .iter()
                                .position(|req_wrapper| {
                                    req_wrapper
                                        .request()
                                        .try_get_dbproxy_msg_inner()
                                        .unwrap()
                                        .1
                                        .try_get_request_meta()
                                        .unwrap()
                                        == arrived_request_meta
                                })
                                .expect("No record in outstanding_req (position)");
                            let (popped_request, reply_ch) = queue
                                .remove(queue_idx)
                                .expect("No record in outstanding_req (remove)")
                                .unwrap();

                            if queue_idx != 0 {
                                warn!("conn fifo is popping at an out-of-order position {}", queue_idx);
                            }
                            if queue.len() > 0 {
                                info!("conn fifo has {} after Pop", queue.len());
                            } else {
                                trace!("conn fifo has {} after Pop", queue.len());
                            }

                            assert_eq!(
                                popped_request
                                    .try_get_dbproxy_msg_inner()
                                    .unwrap()
                                    .1
                                    .try_get_request_meta()
                                    .unwrap(),
                                arrived_request_meta
                            );

                            trace!("-> {:?}", msg);
                            reply_ch.unwrap().send(TransceiverReply::DbproxyMsg(msg)).unwrap();
                        }
                        other => warn!("Unsupported {:?}", other),
                    };
                }
                .instrument(info_span!("<-dbproxy", message = field::Empty))
                .await;
            }

            info!("Tcp receiver service terminated");
        };
        let reader_handle = tokio::spawn(reader_task.in_current_span());

        // Spawn request_rx, processing request from dispatcher in serial one after one
        let request_rx_task = async move {
            while let Some(request) = request_rx.next().await {
                let task = async {
                    match request.request() {
                        TransceiverRequest::DbproxyMsg {
                            dbproxy_addr: _dbproxy_addr,
                            dbproxy_msg,
                        } => {
                            let dbproxy_msg = dbproxy_msg.clone();
                            
                            let meta = dbproxy_msg.try_get_request_meta().unwrap();
                            let client_addr = meta.client_addr.clone();
                            Span::current().record("message", &&meta.to_string()[..]);

                            trace!("-> {:?}", dbproxy_msg);
                            let mut guard = outstanding_req.lock().await;
                            let queue = guard.entry(client_addr.clone()).or_default();
                            if queue.len() > 0 {
                                info!("conn fifo has {} before Push", queue.len());
                            } else {
                                trace!("conn fifo has {} before Push", queue.len());
                            }
                            while *admin_stop_signal.lock().await{
                            }
                            queue.push_back(request);
                            let delimited_write = FramedWrite::new(&mut writer, LengthDelimitedCodec::new());
                            let mut serded_write =
                                SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());
                            serded_write.send(dbproxy_msg).await.expect("cannot send to dbproxy");
                        }
                        TransceiverRequest::DbproxyLoad => {
                            Span::current().record("message", &"Load");
                            let load: usize = outstanding_req.lock().await.values().map(|queue| queue.len()).sum();
                            trace!("{}", load);
                            request
                                .unwrap()
                                .1
                                .unwrap()
                                .send(TransceiverReply::DbproxyLoad(load))
                                .unwrap();
                        }
                        TransceiverRequest::Dbreplica {
                            dbproxy_addr: _dbproxy_addr,
                            dbproxy_msg,
                        } => {
                            let dbproxy_msg = dbproxy_msg.clone();
                            let delimited_write = FramedWrite::new(&mut writer, LengthDelimitedCodec::new());
                            let mut serded_write =
                                SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());
                            serded_write.send(dbproxy_msg).await.expect("cannot send to dbproxy");
                        }
                    }
                };
                task.instrument(info_span!("->dbproxy", message = field::Empty)).await;
            }

            // When the request_rx channel is disconnected, shutdown the tcp socket
            writer.as_ref().shutdown(Shutdown::Both).unwrap();
            info!("Request rx service terminated");
        };
        let request_rx_handle = tokio::spawn(request_rx_task.in_current_span());

        tokio::try_join!(reader_handle, request_rx_handle).unwrap();

        info!("DIES");
    }
}

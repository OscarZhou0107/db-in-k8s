use crate::comm::scheduler_dbproxy::*;
use crate::util::executor_addr::*;
use futures::prelude::*;
use std::collections::HashMap;
use std::collections::LinkedList;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, field, info, info_span, instrument, warn, Instrument, Span};

/// Request sent from dispatcher direction
#[derive(Debug)]
pub struct TransceiverRequest {
    pub dbproxy_addr: SocketAddr,
    pub dbproxy_msg: Message,
    pub current_request_id: usize,
}

#[derive(Debug)]
pub struct TransceiverReply {
    pub msg: Message,
    pub current_request_id: usize,
}

impl ExecutorRequest for TransceiverRequest {
    type ReplyType = TransceiverReply;
}

pub type TransceiverAddr = ExecutorAddr<TransceiverRequest>;

pub struct Transceiver {
    dbproxy_addr: SocketAddr,
    request_rx: RequestReceiver<TransceiverRequest>,
}

impl Transceiver {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` into `Transceiver`
    pub fn new(queue_size: usize, dbproxy_addr: SocketAddr) -> (TransceiverAddr, Self) {
        let (addr, request_rx) = ExecutorAddr::new(queue_size);
        (
            addr,
            Self {
                dbproxy_addr,
                request_rx,
            },
        )
    }

    #[instrument(name="transceive", skip(self), fields(message=field::Empty))]
    pub async fn run(self) {
        Span::current().record("message", &&self.dbproxy_addr.to_string()[..]);

        let Transceiver {
            dbproxy_addr,
            mut request_rx,
        } = self;

        let socket = TcpStream::connect(dbproxy_addr)
            .await
            .expect("Cannot connect to dbproxy");
        let (reader, mut writer) = socket.into_split();

        // (ClientAddr) -> LinkedList<RequestWrapper<TransceiverRequest>>
        let outstanding_req: HashMap<SocketAddr, LinkedList<RequestWrapper<TransceiverRequest>>> = HashMap::new();
        let outstanding_req = Arc::new(Mutex::new(outstanding_req));

        // Spawn dbproxy reader, processing reply from dbproxy in serial one after one
        let delimited_read = FramedRead::new(reader, LengthDelimitedCodec::new());
        let mut serded_read = SymmetricallyFramed::new(delimited_read, SymmetricalJson::<Message>::default());
        let outstanding_req_clone = outstanding_req.clone();
        let reader_task = async move {
            while let Some(msg) = serded_read.try_next().await.unwrap() {
                let task = async {
                    match &msg {
                        Message::MsqlResponse(client_addr, _msqlresponse) => {
                            Span::current().record("client", &&client_addr.to_string()[..]);
                            let mut guard = outstanding_req_clone.lock().await;
                            let queue = guard.get_mut(&client_addr).expect("No client addr in outstanding_req");
                            let request_wrapper = queue.pop_back().expect("No record in outstanding_req");
                            let (request, reply_ch) = request_wrapper.unwrap();
                            info!("conn fifo has {} after Pop back", queue.len());
                            debug!("-> {:?}", msg);
                            reply_ch
                                .unwrap()
                                .send(TransceiverReply {
                                    msg,
                                    current_request_id: request.current_request_id,
                                })
                                .unwrap();
                        }
                        other => warn!("Unsupported {:?}", other),
                    };
                };
                task.instrument(info_span!("reply", client = field::Empty)).await;
            }
        };
        let reader_handle = tokio::spawn(reader_task.in_current_span());

        // Spawn request_rx, processing request from dispatcher in serial one after one
        let request_rx_task = async move {
            while let Some(request) = request_rx.next().await {
                let task = async {
                    let dbproxy_msg = request.request().dbproxy_msg.clone();
                    let client_addr = dbproxy_msg.get_client_addr().clone().unwrap();
                    Span::current().record("client", &&client_addr.to_string()[..]);
                    debug!("-> {:?}", dbproxy_msg);
                    let mut guard = outstanding_req.lock().await;
                    let queue = guard.entry(client_addr.clone()).or_default();
                    info!("conn fifo has {} before Push front", queue.len());
                    queue.push_front(request);
                    let delimited_write = FramedWrite::new(&mut writer, LengthDelimitedCodec::new());
                    let mut serded_write =
                        SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());
                    serded_write.send(dbproxy_msg).await.expect("cannot send to dbproxy");
                };
                task.instrument(info_span!("request", client = field::Empty)).await;
            }
        };
        let request_rx_handle = tokio::spawn(request_rx_task.in_current_span());

        tokio::try_join!(reader_handle, request_rx_handle).unwrap();

        info!("all communications done");
    }
}

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
use tracing::{trace, field, info, info_span, instrument, warn, Instrument, Span};

/// Request sent from dispatcher direction
#[derive(Debug)]
pub struct TransceiverRequest {
    pub dbproxy_addr: SocketAddr,
    pub dbproxy_msg: Message,
}

#[derive(Debug)]
pub struct TransceiverReply {
    pub msg: Message,
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
                async {
                    match &msg {
                        Message::MsqlResponse(arrived_request_meta, _msqlresponse) => {
                            Span::current().record("message", &&arrived_request_meta.to_string()[..]);
                            let mut guard = outstanding_req_clone.lock().await;
                            let queue = guard
                                .get_mut(&arrived_request_meta.client_addr)
                                .expect("No client addr in outstanding_req");
                            let (popped_request, reply_ch) =
                                queue.pop_back().expect("No record in outstanding_req").unwrap();
                            if queue.len() > 0 {
                                info!("conn fifo has {} after Pop back", queue.len());
                            } else {
                                trace!("conn fifo has {} after Pop back", queue.len());
                            }
                            let popped_request_meta = popped_request.dbproxy_msg.try_get_request_meta().unwrap();

                            assert_eq!(popped_request_meta, arrived_request_meta);

                            trace!("-> {:?}", msg);
                            reply_ch.unwrap().send(TransceiverReply { msg }).unwrap();
                        }
                        other => warn!("Unsupported {:?}", other),
                    };
                }
                .instrument(info_span!("<-dbproxy", message = field::Empty))
                .await;
            }
        };
        let reader_handle = tokio::spawn(reader_task.in_current_span());

        // Spawn request_rx, processing request from dispatcher in serial one after one
        let request_rx_task = async move {
            while let Some(request) = request_rx.next().await {
                let task = async {
                    let dbproxy_msg = request.request().dbproxy_msg.clone();
                    let meta = dbproxy_msg.try_get_request_meta().unwrap();
                    let client_addr = meta.client_addr.clone();
                    Span::current().record("message", &&meta.to_string()[..]);

                    trace!("-> {:?}", dbproxy_msg);
                    let mut guard = outstanding_req.lock().await;
                    let queue = guard.entry(client_addr.clone()).or_default();
                    if queue.len() > 0 {
                        info!("conn fifo has {} before Push front", queue.len());
                    } else {
                        trace!("conn fifo has {} before Push front", queue.len());
                    }
                    queue.push_front(request);
                    let delimited_write = FramedWrite::new(&mut writer, LengthDelimitedCodec::new());
                    let mut serded_write =
                        SymmetricallyFramed::new(delimited_write, SymmetricalJson::<Message>::default());
                    serded_write.send(dbproxy_msg).await.expect("cannot send to dbproxy");
                };
                task.instrument(info_span!("->dbproxy", message = field::Empty)).await;
            }
        };
        let request_rx_handle = tokio::spawn(request_rx_task.in_current_span());

        tokio::try_join!(reader_handle, request_rx_handle).unwrap();

        info!("DIES");
    }
}

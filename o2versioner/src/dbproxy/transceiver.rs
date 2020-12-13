use super::core::PendingQueue;
use super::core::{DbVersion, QueryResult, QueryResultType};
use crate::comm::scheduler_dbproxy::Message;
use crate::util::executor::Executor;
use async_trait::async_trait;
use futures::prelude::*;
use futures::SinkExt;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, field, info, info_span, instrument, Instrument, Span};

pub async fn connection<A>(
    addr: A,
    pending_queue: Arc<Mutex<PendingQueue>>,
    responder_receiver: mpsc::Receiver<QueryResult>,
    version: Arc<Mutex<DbVersion>>,
) -> (Receiver, Responder)
where
    A: ToSocketAddrs,
{
    let listener = TcpListener::bind(addr).await.unwrap();
    info!("Binded to port");
    let (tcp_stream, _) = listener.accept().await.unwrap();
    info!("Connection established with scheduler...");
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    let receiver = Receiver::new(pending_queue.clone(), tcp_read);
    let responder = Responder::new(responder_receiver, version, tcp_write);
    (receiver, responder)
}

pub struct Receiver {
    pending_queue: Arc<Mutex<PendingQueue>>,
    tcp_read: OwnedReadHalf,
}

impl Receiver {
    pub fn new(pending_queue: Arc<Mutex<PendingQueue>>, tcp_read: OwnedReadHalf) -> Self {
        Self {
            pending_queue,
            tcp_read,
        }
    }
}

#[async_trait]
impl Executor for Receiver {
    #[instrument(name = "receiver", skip(self))]
    async fn run(mut self: Box<Self>) {
        let Self {
            pending_queue,
            tcp_read,
        } = *self;

        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        info!("started");

        while let Some(msg) = deserializer.try_next().await.unwrap() {
            debug!("Receiver received a new request from scheduler");

            async {
                match msg {
                    Message::MsqlRequest(meta, request, versions) => {
                        Span::current().record("message", &&meta.to_string()[..]);
                        Span::current().record("type", &request.as_ref());
                        debug!("Txvn: {:?}", versions.clone());
                        debug!("Request content is: {:?}", request.clone());
                        pending_queue.lock().await.emplace(meta, request, versions);
                    }
                    _ => debug!("nope"),
                }
            }
            .instrument(info_span!("receiver", message = field::Empty, "type" = field::Empty))
            .await;
        }

        info!("Receiver finished its jobs");
    }
}

pub struct Responder {
    receiver: mpsc::Receiver<QueryResult>,
    version: Arc<Mutex<DbVersion>>,
    tcp_write: OwnedWriteHalf,
}

impl Responder {
    pub fn new(
        receiver: mpsc::Receiver<QueryResult>,
        version: Arc<Mutex<DbVersion>>,
        tcp_write: OwnedWriteHalf,
    ) -> Self {
        Self {
            receiver,
            version,
            tcp_write,
        }
    }
}

#[async_trait]
impl Executor for Responder {
    #[instrument(name = "responder", skip(self))]
    async fn run(mut self: Box<Self>) {
        let Self {
            mut receiver,
            version,
            tcp_write,
        } = *self;

        info!("started");
        let mut serializer = SymmetricallyFramed::new(
            FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        while let Some(mut result) = receiver.recv().await {
            debug!("Responder got a result to return");
            match result.result_type {
                QueryResultType::END => {
                    debug!("Trying to release a version");
                    debug!("Responder: {:?}", result.contained_newer_versions);
                    version
                        .lock()
                        .await
                        .release_on_transaction(result.contained_newer_versions.clone());
                }
                QueryResultType::QUERY => match result.acquire_early_release() {
                    Ok(request) => {
                        debug!("Doing a early release {:?}", request);
                        version.lock().await.release_on_request(request);
                    }
                    Err(_) => {}
                },
                _ => {}
            }

            serializer
                .send(Message::MsqlResponse(
                    result.identifier.clone(),
                    result.into_msql_response(),
                ))
                .await
                .unwrap();
        }

        info!("Responder finishes its job");
    }
}

#[cfg(test)]
mod tests_receiver {
    use super::Receiver;
    use super::*;
    use crate::comm::scheduler_dbproxy::Message;
    use crate::core::*;
    use crate::dbproxy::core::PendingQueue;
    use futures::SinkExt;
    use std::net::*;
    use std::sync::Arc;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::Mutex;
    use tokio_serde::formats::SymmetricalJson;
    use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

    #[tokio::test]
    #[ignore]
    async fn test_send_single_item_to_receiver() {
        let details = "127.0.0.1:2345";
        let addr: SocketAddr = details.parse().expect("Unable to parse socket address");
        // Prepare - Data
        let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
        let pending_queue_2 = Arc::clone(&pending_queue);

        let item = Message::MsqlRequest(
            RequestMeta {
                client_addr: "127.0.0.1:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::BeginTx(
                MsqlBeginTx::default()
                    .set_name(Some("tx0"))
                    .set_tableops(TableOps::from("READ WRIte")),
            ),
            None,
        );

        let mut items: Vec<Message> = Vec::new();
        for _ in 0..1 {
            items.push(item.clone());
        }

        // Prepare - Receiver
        // These handles should be properly await
        let _receiver_handle = tokio::spawn(helper_spawn_receiver(pending_queue, addr.clone()));
        let _mock_client_handle = tokio::spawn(helper_spawn_mock_client(items, addr));

        // Assert
        loop {
            if pending_queue_2.lock().await.queue.len() == 1 {
                break;
            }
        }
        assert!(true);
    }

    #[tokio::test]
    #[ignore]
    async fn test_send_ten_items_to_receiver() {
        let details = "127.0.0.1:2344";
        let addr: SocketAddr = details.parse().expect("Unable to parse socket address");
        // Prepare - Data
        let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
        let pending_queue_2 = Arc::clone(&pending_queue);

        let item = Message::MsqlRequest(
            RequestMeta {
                client_addr: "127.0.0.1:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::BeginTx(
                MsqlBeginTx::default()
                    .set_name(Some("tx0"))
                    .set_tableops(TableOps::from("READ WRIte")),
            ),
            None,
        );

        let mut items: Vec<Message> = Vec::new();
        for _ in 0..10 {
            items.push(item.clone());
        }

        // Prepare - Receiver
        // These handles should be properly await
        let _receiver_handle = tokio::spawn(helper_spawn_receiver(pending_queue, addr.clone()));
        let _mock_client_handle = tokio::spawn(helper_spawn_mock_client(items, addr));

        // Assert
        loop {
            if pending_queue_2.lock().await.queue.len() == 10 {
                break;
            }
        }
        assert!(true);
    }

    async fn helper_spawn_receiver(pending_queue: Arc<Mutex<PendingQueue>>, addr: SocketAddr) {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (tcp_read, _) = tcp_stream.into_split();

        Box::new(Receiver::new(pending_queue, tcp_read)).run().await;
    }

    async fn helper_spawn_mock_client(mut items: Vec<Message>, addr: SocketAddr) {
        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
        let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());
        // Action
        while !items.is_empty() {
            serialized.send(items.pop().unwrap()).await.unwrap();
        }
    }
}

#[cfg(test)]
mod tests_responder {
    use super::Responder;
    use super::*;
    use crate::comm::MsqlResponse;
    use crate::core::*;
    use crate::dbproxy::core::{DbVersion, QueryResult, QueryResultType};
    use crate::{comm::scheduler_dbproxy::Message, core::RequestMeta};
    use std::{net::SocketAddr, sync::Arc};
    use tokio::net::TcpListener;
    use tokio::net::TcpStream;
    use tokio::sync::mpsc;
    use tokio::sync::Mutex;
    use tokio_serde::formats::SymmetricalJson;
    use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

    #[tokio::test]
    async fn test_send_items_to_from_multiple_channel() {
        let addr = RequestMeta {
            client_addr: "127.0.0.1:8080".parse().unwrap(),
            cur_txid: 0,
            request_id: 0,
        };
        // Prepare - Mock db related context
        let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(Default::default())));
        // Prepare - Network
        let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
            mpsc::channel(100);

        // Prepare - Verifying queue
        let verifying_queue: Arc<Mutex<Vec<MsqlResponse>>> = Arc::new(Mutex::new(Vec::new()));
        let verifying_queue_2 = Arc::clone(&verifying_queue);

        // Prepare - Data
        let r = QueryResult {
            identifier: addr.clone(),
            result: "r".to_string(),
            succeed: true,
            result_type: QueryResultType::BEGIN,
            contained_newer_versions: TxVN::new(),
            contained_early_release_version: None,
        };

        // Prepare - Responder
        // These handles should be properly await
        let _responder_handle = tokio::spawn(helper_spawn_responder(
            version.clone(),
            responder_receiver,
            addr.client_addr.clone(),
        ));
        let _mock_client_handle = tokio::spawn(helper_spawn_mock_client(verifying_queue, addr.client_addr));

        let worker_num: u32 = 5;
        // Action - Spwan worker thread to send response
        let _mock_worker_handle = tokio::spawn(helper_spawn_mock_workers(worker_num, r, responder_sender));
        loop {
            if verifying_queue_2.lock().await.len() == 5 {
                break;
            }
        }
        assert!(true);
    }

    async fn helper_spawn_responder(
        version: Arc<Mutex<DbVersion>>,
        receiver: mpsc::Receiver<QueryResult>,
        addr: SocketAddr,
    ) {
        let listener = TcpListener::bind(addr).await.unwrap();
        let (tcp_stream, _) = listener.accept().await.unwrap();
        let (_, tcp_write) = tcp_stream.into_split();

        Box::new(Responder::new(receiver, version, tcp_write)).run().await;
    }

    async fn helper_spawn_mock_client(vertifying_queue: Arc<Mutex<Vec<MsqlResponse>>>, addr: SocketAddr) {
        let socket = TcpStream::connect(addr).await.unwrap();
        let length_delimited = FramedRead::new(socket, LengthDelimitedCodec::new());
        let mut deserialize = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());

        // Action
        while let Some(msg) = deserialize.try_next().await.unwrap() {
            match msg {
                Message::MsqlResponse(_, op) => {
                    vertifying_queue.lock().await.push(op);
                }
                _other => {
                    println!("nope");
                }
            }
        }
    }

    async fn helper_spawn_mock_workers(worker_num: u32, query_result: QueryResult, sender: mpsc::Sender<QueryResult>) {
        for _ in 0..worker_num {
            sender
                .send(query_result.clone())
                .await
                .map_err(|e| e.to_string())
                .unwrap();
        }
    }
}

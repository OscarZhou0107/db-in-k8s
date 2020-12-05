use super::core::{PendingQueue, QueueMessage};
use crate::comm::scheduler_dbproxy::Message;
use futures::prelude::*;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
use tokio::sync::Mutex;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub struct Receiver {}

impl Receiver {
    pub fn run(pending_queue: Arc<Mutex<PendingQueue>>, tcp_read: OwnedReadHalf) {
        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        tokio::spawn(async move {
            while let Some(msg) = deserializer.try_next().await.unwrap() {
                match msg {
                    Message::MsqlRequest(meta, request, versions) => {
                        pending_queue
                            .lock()
                            .await
                            .push(QueueMessage::new(meta, request, versions));
                    }
                    _ => println!("nope"),
                }
            }
        });
    }
}

#[cfg(test)]
mod tests_receiver {
    use super::Receiver;
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
    async fn test_send_single_item_to_receiver() {
        let details = "127.0.0.1:2345";
        let addr: SocketAddr = details.parse().expect("Unable to parse socket address");
        //Prepare - Data
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

        //Prepare - Receiver
        helper_spawn_receiver(pending_queue, addr.clone());
        helper_spawn_mock_client(items, addr);

        //Assert
        loop {
            if pending_queue_2.lock().await.queue.len() == 1 {
                break;
            }
        }
        assert!(true);
    }

    #[tokio::test]
    async fn test_send_ten_items_to_receiver() {
        let details = "127.0.0.1:2344";
        let addr: SocketAddr = details.parse().expect("Unable to parse socket address");
        //Prepare - Data
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

        //Prepare - Receiver
        helper_spawn_receiver(pending_queue, addr.clone());
        helper_spawn_mock_client(items, addr);

        //Assert
        loop {
            if pending_queue_2.lock().await.queue.len() == 10 {
                break;
            }
        }
        assert!(true);
    }

    fn helper_spawn_receiver(pending_queue: Arc<Mutex<PendingQueue>>, addr: SocketAddr) {
        tokio::spawn(async move {
            let listener = TcpListener::bind(addr).await.unwrap();
            let (tcp_stream, _) = listener.accept().await.unwrap();
            let (tcp_read, _) = tcp_stream.into_split();

            Receiver::run(pending_queue, tcp_read);
        });
    }

    fn helper_spawn_mock_client(mut items: Vec<Message>, addr: SocketAddr) {
        tokio::spawn(async move {
            let socket = TcpStream::connect(addr).await.unwrap();
            let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());
            let mut serialized = tokio_serde::SymmetricallyFramed::new(length_delimited, SymmetricalJson::default());
            //Action
            while !items.is_empty() {
                serialized.send(items.pop().unwrap()).await.unwrap();
            }
        });
    }
}

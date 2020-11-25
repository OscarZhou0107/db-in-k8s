use super::core::Operation;
use crate::comm::scheduler_dbproxy::Message;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::tcp::OwnedReadHalf;
use tokio::stream::StreamExt;
use tokio::sync::Notify;
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub struct Receiver {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Receiver {
    pub fn run(pending_queue: Arc<Mutex<Vec<Operation>>>, notify: Arc<Notify>, tcp_read: OwnedReadHalf) {
        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
        );

        tokio::spawn(async move {
            while let Some(msg) = deserializer.try_next().await.unwrap() {
                match msg {
                    Message::SqlRequest(op) => {
                        pending_queue.lock().unwrap().push(op);
                        notify.notify();
                    }
                    _other => {
                        println!("nope")
                    }
                }
            }
        });
    }
}

#[cfg(test)]
#[path = "tests/dbproxy_receiver_test.rs"]
mod receiver_test;

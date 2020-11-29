use super::core::PendingQueue;
use crate::comm::scheduler_dbproxy::Message;
use std::sync::Arc;
use tokio::net::tcp::OwnedReadHalf;
use tokio::stream::StreamExt;
use tokio::sync::Mutex;
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
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
                    Message::SqlRequest(op) => {
                        pending_queue.lock().await.push(op);
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

use super::core::{PendingQueue, QueueMessage, Task};
use crate::{
    comm::scheduler_dbproxy::Message,
    core::msql::{Msql, MsqlEndTxMode},
};
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
                    Message::MsqlRequest(addr, request, versions) => {
                        pending_queue
                            .lock()
                            .await
                            .push(QueueMessage::new(addr, request, versions));
                    }
                    _ => println!("nope"),
                }
            }
        });
    }
}

#[cfg(test)]
#[path = "tests/dbproxy_receiver_test.rs"]
mod receiver_test;

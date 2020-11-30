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
                    Message::MsqlRequest(addr, request, version_number) => {
                        let mut operation_type = Task::BEGIN;
                        let mut query = String::new();

                        match request {
                            Msql::BeginTx(op) => {
                                operation_type = Task::BEGIN;
                            }

                            Msql::Query(op) => {
                                operation_type = Task::READ;
                                query = op.query().to_string();
                            }

                            Msql::EndTx(op) => match op.mode() {
                                MsqlEndTxMode::Commit => {
                                    operation_type = Task::COMMIT;
                                }
                                MsqlEndTxMode::Rollback => {
                                    operation_type = Task::ABORT;
                                }
                            },
                        };

                        let message = QueueMessage {
                            identifier: addr,
                            operation_type: operation_type,
                            query: query,
                            versions: version_number,
                        };

                        pending_queue.lock().await.push(message);
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

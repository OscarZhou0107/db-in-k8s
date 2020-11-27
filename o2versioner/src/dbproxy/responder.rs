use super::core::{DbVersion, QueryResult};
use crate::comm::scheduler_dbproxy::Message;
use futures::SinkExt;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::{net::tcp::OwnedWriteHalf, sync::mpsc};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub struct Responder {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Responder {
    pub fn run(
        mut receiver: mpsc::Receiver<QueryResult>,
        version: Arc<Mutex<DbVersion>>,
        tcp_write: OwnedWriteHalf,
    ) {
        tokio::spawn(async move {
            let mut serializer = SymmetricallyFramed::new(
                FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
                SymmetricalJson::<Message>::default(),
            );

            while let Some(result) = receiver.recv().await {
                let version_release = result.version_release;
                if version_release {
                    version
                        .lock()
                        .unwrap()
                        .release_on_tables(result.contained_newer_versions.clone());
                }

                serializer.send(Message::SqlResponse(result)).await.unwrap();
            }
        });
    }
}

#[cfg(test)]
#[path = "tests/dbproxy_responder_test.rs"]
mod responder_test;

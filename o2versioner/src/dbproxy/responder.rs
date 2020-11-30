use crate::comm::{appserver_scheduler::MsqlResponse, scheduler_dbproxy::NewMessage};

use super::core::{DbVersion, QueryResult, QueryResultType};
use futures::SinkExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::{net::tcp::OwnedWriteHalf, sync::mpsc};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};

pub struct Responder {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Responder {
    pub fn run(mut receiver: mpsc::Receiver<QueryResult>, version: Arc<Mutex<DbVersion>>, tcp_write: OwnedWriteHalf) {
        tokio::spawn(async move {
            let mut serializer = SymmetricallyFramed::new(
                FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
                SymmetricalJson::<NewMessage>::default(),
            );

            while let Some(result) = receiver.recv().await {
                match result.result_type {
                    QueryResultType::END => {
                        version
                        .lock()
                        .await
                        .release_on_tables(result.contained_newer_versions.clone());
                    },
                    _ => {}
                }

                let response;
                let message;
                if result.succeed {
                    response = Ok(result.result);
                } else {
                    response = Err(result.result);
                }

                match result.result_type {
                    QueryResultType::BEGIN => { 
                        let response = Ok(());
                        message = MsqlResponse::BeginTx(response);
                    }
                    QueryResultType::QUERY => {
                        message = MsqlResponse::Query(response);
                    }
                    QueryResultType::END => {
                        message = MsqlResponse::EndTx(response);
                    }
                }

                serializer.send(NewMessage::MsqlResponse(message)).await.unwrap();
            }
        });
    }
}

#[cfg(test)]
#[path = "tests/dbproxy_responder_test.rs"]
mod responder_test;

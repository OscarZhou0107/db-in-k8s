use tokio::{net::tcp::OwnedWriteHalf, sync::mpsc};
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use futures::SinkExt;
use std::{sync::Mutex};
use std::sync::Arc;
use tokio::sync::Notify;
use crate::comm::scheduler_dbproxy::Message;
use super::core::{QueryResult, DbVersion};

pub struct Responder {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Responder {
    pub fn run(mut receiver : mpsc::Receiver<QueryResult>, 
            version: Arc<Mutex<DbVersion>>,
            notify : Arc<Notify>,
            tcp_write : OwnedWriteHalf,
        ) {
          
        tokio::spawn(async move {

            let mut serializer= SymmetricallyFramed::new(
                FramedWrite::new(tcp_write, LengthDelimitedCodec::new()),
                SymmetricalJson::<Message>::default(),
            );

            while let Some(result) = receiver.recv().await {
                println!("rrrrrrr");
                
                let version_release = result.version_release;
                if version_release {
                    version.lock().unwrap().release_on_tables(result.contained_newer_versions.clone());
                    notify.notify();
                }

                serializer.
                send( Message::SqlResponse(result))
                .await
                .unwrap()
            }
        });
    }
}
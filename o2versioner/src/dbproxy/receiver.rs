use tokio::{net::tcp::OwnedReadHalf, sync::mpsc};
use tokio::stream::StreamExt;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tokio_serde::{formats::SymmetricalJson, SymmetricallyFramed};
use std::{sync::Mutex};
use std::sync::Arc;
use tokio::sync::Notify;
use crate::comm::scheduler_dbproxy::Message;
use super::core::Operation;

pub struct Receiver {}

// Box<SymmetricallyFramed<FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,Message,SymmetricalJson<Message>>>
impl Receiver {
    pub fn run(pending_queue: Arc<Mutex<Vec<Operation>>>,
            notify : Arc<Notify>,
            tcp_read : OwnedReadHalf,
        ) {
          
        let mut deserializer = SymmetricallyFramed::new(
            FramedRead::new(tcp_read, LengthDelimitedCodec::new()),
            SymmetricalJson::<Message>::default(),
            );

        tokio::spawn( async move {
            while let Some(msg) = deserializer.try_next().await.unwrap() {
                match msg {
                    Message::SqlRequest(op) => {
                        pending_queue.lock().unwrap().push(op);
                        notify.notify();
                        }
                    other => {println!("nope")}
                }
            }
        });  
    }
}
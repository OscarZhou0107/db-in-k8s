#![allow(warnings)]
use super::inner_comm::*;
use crate::comm::MsqlResponse;
use crate::core::*;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::{debug, field, info, instrument};

pub struct Transceiver {
    receivers: HashMap<SocketAddr, OwnedReadHalf>,
    transmitters: HashMap<SocketAddr, OwnedWriteHalf>,
    request_rx: mpsc::Receiver<Request>,
}

impl Transceiver {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` into `Transceiver`
    pub async fn new<I>(queue_size: usize, iter: I) -> (ExecutorAddr, Self)
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let sockets: Vec<_> = stream::iter(iter)
            .then(|dbproxy_port| async move {
                TcpStream::connect(dbproxy_port)
                    .await
                    .expect("Cannot connect to dbproxy")
            })
            .collect()
            .await;

        let (receivers, transmitters) = sockets
            .into_iter()
            .map(|tcp_stream| {
                let dbproxy_addr = tcp_stream.peer_addr().unwrap();
                let (rx, tx) = tcp_stream.into_split();
                ((dbproxy_addr.clone(), rx), (dbproxy_addr, tx))
            })
            .unzip();

        let (addr, request_rx) = ExecutorAddr::new(queue_size);

        (
            addr,
            Self {
                receivers,
                transmitters,
                request_rx,
            },
        )
    }

    #[instrument(name="dispatch", skip(self), fields(dbvn=field::Empty, dbproxy=field::Empty))]
    pub async fn run(mut self) {}
}

use crate::comm::scheduler_sequencer;
use futures::prelude::*;
use log::info;
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub async fn main<A: ToSocketAddrs>(addr: A, max_connection: Option<usize>) {
    let mut listener = TcpListener::bind(addr).await.unwrap();

    let mut cur_num_connection = 0;
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await.unwrap();

        info!(
            "Connection established with [{}] {}",
            cur_num_connection, peer_addr
        );
        cur_num_connection += 1;

        // Spawn a new thread for each tcp connection
        tokio::spawn(async move {
            process_connection(tcp_stream).await;
        });

        // An optional max number of connections allowed
        if let Some(nmax) = max_connection {
            if cur_num_connection >= nmax {
                break;
            }
        }
    }
}

async fn process_connection(tcp_stream: TcpStream) {
    // Delimit frames from bytes using a length header
    let length_delimited = Framed::new(tcp_stream, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded: tokio_serde::SymmetricallyFramed<_, scheduler_sequencer::Message, _> =
        tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            tokio_serde::formats::SymmetricalJson::<scheduler_sequencer::Message>::default(),
        );

    // Process a stream of incomming messages from a single tcp connection
    serded
        .try_for_each(|msg| {
            info!("GOT: {:?}", msg);
            future::ok(())
        })
        .await
        .unwrap();
}

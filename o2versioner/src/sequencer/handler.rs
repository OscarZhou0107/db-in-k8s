use crate::comm::scheduler_sequencer;
use futures::prelude::*;
use tokio::net::ToSocketAddrs;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

pub async fn main<A: ToSocketAddrs>(addr: A) {
    let mut listener = TcpListener::bind(addr).await.unwrap();

    loop {
        let (tcp_stream, _) = listener.accept().await.unwrap();
        // A new task is spawned for each inbound socket. The socket is
        // moved to the new task and processed there.
        // TODO: add spawn here
        process_connection(tcp_stream).await;
    }
}

async fn process_connection(tcp_stream: TcpStream) {
    println!("Received message from {:?}", tcp_stream.peer_addr());

    // Delimit frames from bytes using a length header
    let length_delimited = FramedRead::new(tcp_stream, LengthDelimitedCodec::new());

    // Deserialize frames using JSON codec
    let mut deserialized: tokio_serde::SymmetricallyFramed<_, scheduler_sequencer::Message, _> =
        tokio_serde::SymmetricallyFramed::new(
            length_delimited,
            tokio_serde::formats::SymmetricalJson::<scheduler_sequencer::Message>::default(),
        );

    while let Some(msg) = deserialized.try_next().await.unwrap() {
        println!("GOT: {:?}", msg);
    }
}

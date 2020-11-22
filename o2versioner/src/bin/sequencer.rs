use futures::prelude::*;
use o2versioner::comm::scheduler_sequencer;
use o2versioner::sequencer::handler;
use tokio::net::TcpStream;
use tokio_util::codec::FramedWrite;
use tokio_util::codec::LengthDelimitedCodec;

/// main() for sequencer.exe
///
/// Test this on dd:
/// dd if=/dev/zero bs=9000 count=1000 > /dev/tcp/127.0.0.1/6379
#[tokio::main]
async fn main() {
    let listener_handler = tokio::spawn(handler::main("127.0.0.1:6379"));

    let sender_handler = tokio::spawn(async {
        // Connect to a socket
        let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();

        // Delimit frames from bytes using a length header
        let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());

        // Serialize frames using JSON codec
        let mut serialized: tokio_serde::SymmetricallyFramed<_, scheduler_sequencer::Message, _> =
            tokio_serde::SymmetricallyFramed::new(
                length_delimited,
                tokio_serde::formats::SymmetricalJson::default(),
            );

        // Send the value
        serialized
            .send(scheduler_sequencer::Message::Invalid)
            .await
            .unwrap()
    });

    listener_handler.await.unwrap();
    sender_handler.await.unwrap();
}

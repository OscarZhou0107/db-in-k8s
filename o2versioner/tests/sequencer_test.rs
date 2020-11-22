use env_logger;
use futures::prelude::*;
use o2versioner::comm::scheduler_sequencer;
use o2versioner::sequencer::handler;
use std::net::Shutdown;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;
use tokio_util::codec::LengthDelimitedCodec;

fn init_logger() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.filter_level(log::LevelFilter::Debug);
    builder.init();
}

#[test]
fn mock_sequencer_connection() {
    init_logger();
    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let sequencer_handler = tokio::spawn(handler::main("127.0.0.1:6379", Some(1)));

        tokio::spawn(async {
            // Connect to a socket
            let socket = TcpStream::connect("127.0.0.1:6379").await.unwrap();

            // Delimit frames from bytes using a length header
            let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());

            // Deserialize/Serialize frames using JSON codec
            let mut serded: tokio_serde::SymmetricallyFramed<_, scheduler_sequencer::Message, _> =
                tokio_serde::SymmetricallyFramed::new(
                    length_delimited,
                    tokio_serde::formats::SymmetricalJson::default(),
                );

            // Send the value
            serded
                .send(scheduler_sequencer::Message::Invalid)
                .await
                .unwrap();

            // Shutdown the connection
            serded
                .into_inner()
                .into_inner()
                .shutdown(Shutdown::Both)
                .unwrap();
        });

        // Must run the sequencer_handler, otherwise it won't do the work
        sequencer_handler.await.unwrap();
    })
}

use env_logger;
use futures::prelude::*;
use o2versioner::comm::scheduler_sequencer;
use o2versioner::core::sql::*;
use o2versioner::sequencer::handler;
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
    let port = "127.0.0.1:6389";

    let mut rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let sequencer_handle = tokio::spawn(handler::main(port, Some(1)));

        tokio::spawn(async move {
            // Connect to a socket
            let socket = TcpStream::connect(port).await.unwrap();

            // Delimit frames from bytes using a length header
            let length_delimited = Framed::new(socket, LengthDelimitedCodec::new());

            // Deserialize/Serialize frames using JSON codec
            let mut serded: tokio_serde::SymmetricallyFramed<_, scheduler_sequencer::Message, _> =
                tokio_serde::SymmetricallyFramed::new(
                    length_delimited,
                    tokio_serde::formats::SymmetricalJson::default(),
                );

            // Send a message
            serded
                .send(scheduler_sequencer::Message::TxVNRequest(
                    SqlRawString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';")
                        .to_tx_table(false)
                        .unwrap(),
                ))
                .await
                .unwrap();

            // Send a message
            serded
                .send(scheduler_sequencer::Message::TxVNRequest(
                    SqlRawString::from(
                        "BeGin TraNsaction tx1 with MarK 'table0 read table1 read write table2 table3 read table 2';",
                    )
                    .to_tx_table(false)
                    .unwrap(),
                ))
                .await
                .unwrap();
        })
        .await
        .unwrap();

        // Must run the sequencer_handler, otherwise it won't do the work
        sequencer_handle.await.unwrap();
    })
}

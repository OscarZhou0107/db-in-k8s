use env_logger;
use futures::prelude::*;
use log::info;
use o2versioner::comm::scheduler_sequencer;
use o2versioner::core::sql::*;
use o2versioner::core::version_number::*;
use o2versioner::sequencer::handler;
use tokio::net::TcpStream;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

fn init_logger() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.filter_level(log::LevelFilter::Debug);
    builder.init();
}

#[tokio::test]
async fn test_sequencer() {
    init_logger();
    let port = "127.0.0.1:6389";

    let sequencer_handle = tokio::spawn(handler::main(port, Some(2)));

    let tester_handle_0 =
        tokio::spawn(async move {
            let msgs =
                vec![scheduler_sequencer::Message::TxVNRequest(
                SqlRawString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';")
                    .to_tx_table(false)
                    .unwrap(),
            ),scheduler_sequencer::Message::TxVNRequest(
                SqlRawString::from(
                    "BeGin TraNsaction tx1 with MarK 'table0 read table1 read write table2 table3 read table 2';",
                )
                .to_tx_table(false)
                .unwrap(),
            ), scheduler_sequencer::Message::TxVNResponse(TxVN {
                tx_name: String::from("tx2"),
                // A single vec storing all W and R `TableVN` for now
                table_vns: vec![
                    TableVN {
                        table: String::from("table0"),
                        vn: 0,
                        op: Operation::W,
                    },
                    TableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: Operation::R,
                    },
                ],
            }), scheduler_sequencer::Message::Invalid];

            mock_connection(port, msgs).await
        });

    let tester_handle_1 =
        tokio::spawn(async move {
            let msgs =
                vec![scheduler_sequencer::Message::TxVNRequest(
                SqlRawString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';")
                    .to_tx_table(false)
                    .unwrap(),
            ),scheduler_sequencer::Message::TxVNRequest(
                SqlRawString::from(
                    "BeGin TraNsaction tx1 with MarK 'table0 read table1 read write table2 table3 read table 2';",
                )
                .to_tx_table(false)
                .unwrap(),
            ), scheduler_sequencer::Message::TxVNResponse(TxVN {
                tx_name: String::from("tx2"),
                // A single vec storing all W and R `TableVN` for now
                table_vns: vec![
                    TableVN {
                        table: String::from("table0"),
                        vn: 0,
                        op: Operation::W,
                    },
                    TableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: Operation::R,
                    },
                ],
            }), scheduler_sequencer::Message::Invalid];

            mock_connection(port, msgs).await
        });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(tester_handle_0, tester_handle_1, sequencer_handle).unwrap();
}

async fn mock_connection(sequencer_port: &str, msgs: Vec<scheduler_sequencer::Message>) {
    // Connect to a socket
    let socket = TcpStream::connect(sequencer_port).await.unwrap();
    let local_addr = socket.local_addr().unwrap();
    let (tcp_read, tcp_write) = socket.into_split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let mut serded_read: SymmetricallyFramed<_, scheduler_sequencer::Message, _> = SymmetricallyFramed::new(
        length_delimited_read,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );
    let mut serded_write: SymmetricallyFramed<_, scheduler_sequencer::Message, _> = SymmetricallyFramed::new(
        length_delimited_write,
        SymmetricalJson::<scheduler_sequencer::Message>::default(),
    );

    for msg in msgs {
        serded_write.send(msg).await.unwrap();
        if let Some(msg) = serded_read.try_next().await.unwrap() {
            info!("[{:?}] GOT RESPONSE: {:?}", local_addr, msg);
        }
    }
}

use o2versioner::comm::scheduler_sequencer::Message;
use o2versioner::core::*;
use o2versioner::sequencer_main;
use o2versioner::util::config::SequencerConfig;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tracing::{info_span, Instrument};

#[tokio::test]
async fn test_sequencer() {
    let _guard = tests_helper::init_logger();

    let sequencer_addr = "127.0.0.1:58111";
    let conf = SequencerConfig {
        addr: String::from(sequencer_addr),
        max_connection: Some(2),
    };

    let sequencer_handle = tokio::spawn(sequencer_main(conf));

    sleep(Duration::from_millis(200)).await;

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            Message::RequestTxVN(
                ClientMeta::new("127.0.0.1:8080".parse().unwrap()),
                MsqlBeginTx::from(TableOps::from("table0 read table1 read write table2 table3 read"))
                    .set_name(Some("tx0")),
            ),
            Message::RequestTxVN(
                ClientMeta::new("127.0.0.1:8080".parse().unwrap()),
                MsqlBeginTx::from(TableOps::from(
                    "table0 read table1 read write table2 table3 read table 2",
                ))
                .set_name(Some("tx1")),
            ),
            Message::ReplyTxVN(TxVN {
                tx: Some(String::from("tx2")),
                // A single vec storing all W and R `TxTableVN` for now
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("table0"),
                        vn: 0,
                        op: RWOperation::W,
                    },
                    TxTableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: RWOperation::R,
                    },
                ],
            }),
            Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(sequencer_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs)
            .instrument(info_span!("tester0"))
            .await
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            Message::RequestTxVN(
                ClientMeta::new("127.0.0.1:8080".parse().unwrap()),
                MsqlBeginTx::from(TableOps::from("table0 read table1 read write table2 table3 read"))
                    .set_name(Some("tx0")),
            ),
            Message::RequestTxVN(
                ClientMeta::new("127.0.0.1:8080".parse().unwrap()),
                MsqlBeginTx::from(TableOps::from(
                    "table0 read table1 read write table2 table3 read table 2",
                ))
                .set_name(Some("tx1")),
            ),
            Message::ReplyTxVN(TxVN {
                tx: Some(String::from("tx2")),
                // A single vec storing all W and R `TxTableVN` for now
                txtablevns: vec![
                    TxTableVN {
                        table: String::from("table0"),
                        vn: 0,
                        op: RWOperation::W,
                    },
                    TxTableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: RWOperation::R,
                    },
                ],
            }),
            Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(sequencer_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs)
            .instrument(info_span!("tester1"))
            .await
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(tester_handle_0, tester_handle_1, sequencer_handle).unwrap();
}

use o2versioner::comm::scheduler_sequencer;
use o2versioner::core::msql::*;
use o2versioner::core::*;
use o2versioner::sequencer_main;
use o2versioner::util::config::SequencerConfig;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_sequencer() {
    let _guard = tests_helper::init_logger();
    let conf = SequencerConfig {
        addr: String::from("127.0.0.1:6389"),
        max_connection: Some(2),
    };

    let sequencer_handle = tokio::spawn(sequencer_main(conf.clone()));

    let conf_cloned = conf.clone();
    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_sequencer::Message::RequestTxVN(
                MsqlBeginTx::from(TableOps::from("table0 read table1 read write table2 table3 read"))
                    .set_name(Some("tx0")),
            ),
            scheduler_sequencer::Message::RequestTxVN(
                MsqlBeginTx::from(TableOps::from(
                    "table0 read table1 read write table2 table3 read table 2",
                ))
                .set_name(Some("tx1")),
            ),
            scheduler_sequencer::Message::ReplyTxVN(TxVN {
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
            scheduler_sequencer::Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(conf_cloned.to_addr()).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 0").await
    });

    let conf_cloned = conf.clone();
    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_sequencer::Message::RequestTxVN(
                MsqlBeginTx::from(TableOps::from("table0 read table1 read write table2 table3 read"))
                    .set_name(Some("tx0")),
            ),
            scheduler_sequencer::Message::RequestTxVN(
                MsqlBeginTx::from(TableOps::from(
                    "table0 read table1 read write table2 table3 read table 2",
                ))
                .set_name(Some("tx1")),
            ),
            scheduler_sequencer::Message::ReplyTxVN(TxVN {
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
            scheduler_sequencer::Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(conf_cloned.to_addr()).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(tester_handle_0, tester_handle_1, sequencer_handle).unwrap();
}

use o2versioner::comm::scheduler_sequencer;
use o2versioner::core::msql::*;
use o2versioner::core::operation::*;
use o2versioner::core::transaction_version::*;
use o2versioner::sequencer;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_sequencer() {
    let _guard = tests_helper::init_logger();
    let port = "127.0.0.1:6389";

    let sequencer_handle = tokio::spawn(sequencer::main(port, Some(2)));

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
                        op: Operation::W,
                    },
                    TxTableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: Operation::R,
                    },
                ],
            }),
            scheduler_sequencer::Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(port).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 0").await
    });

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
                        op: Operation::W,
                    },
                    TxTableVN {
                        table: String::from("table1"),
                        vn: 2,
                        op: Operation::R,
                    },
                ],
            }),
            scheduler_sequencer::Message::Invalid,
        ];

        let mut tcp_stream = TcpStream::connect(port).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(tester_handle_0, tester_handle_1, sequencer_handle).unwrap();
}

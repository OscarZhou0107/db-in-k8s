use o2versioner::comm::appserver_scheduler;
use o2versioner::core::msql::*;
use o2versioner::core::operation::*;
use o2versioner::scheduler;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_scheduler() {
    let _guard = tests_helper::init_logger();

    let sequencer_addr = "127.0.0.1:6379";
    let scheduler_addr = "127.0.0.1:16379";
    let scheduler_max_connection = 2;
    let sequencer_max_connection = 2;

    let scheduler_handle = tokio::spawn(scheduler::main(
        scheduler_addr,
        Some(scheduler_max_connection),
        sequencer_addr,
        sequencer_max_connection,
    ));

    let sequencer_handle = tokio::spawn(tests_helper::mock_echo_server(
        sequencer_addr,
        Some(sequencer_max_connection),
        "Mock Sequencer",
    ));

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            appserver_scheduler::Message::test("0-hello"),
            appserver_scheduler::Message::test("0-world"),
            appserver_scheduler::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            appserver_scheduler::Message::test("0-hello"),
            appserver_scheduler::Message::test("0-world"),
            appserver_scheduler::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

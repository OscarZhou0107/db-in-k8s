use o2versioner::core::sql::*;
use o2versioner::scheduler::handler;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_scheduler() {
    tests_helper::init_logger();

    let sequencer_addr = "127.0.0.1:6379";
    let scheduler_addr = "127.0.0.1:16379";
    let scheduler_max_connection = 2;
    let sequencer_max_connection = 2;

    let scheduler_handle = tokio::spawn(handler::main(
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
            SqlRawString::from("0-hello"),
            SqlRawString::from("0-world"),
            SqlRawString::from(" BeGin TraNsaction tx0 with MarK 'table4 read table4 read write table4 table3 read'"),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            SqlRawString::from("1-hello"),
            SqlRawString::from("1-world"),
            SqlRawString::from("BeGin TraN tx0 with MarK 'table0 read table1 read write table2 table3 read';"),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

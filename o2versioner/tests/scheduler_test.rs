use o2versioner::core::sql::*;
use o2versioner::scheduler::handler;
use o2versioner::util::tests_helper;
use std::time::Duration;
use tokio::net::TcpStream;

#[tokio::test]
async fn test_sequencer() {
    tests_helper::init_logger();

    let sequencer_addr = "127.0.0.1:6379";
    let scheduler_addr = "127.0.0.1:16379";

    let scheduler_handle = tokio::spawn(handler::main(scheduler_addr, Some(2), sequencer_addr, 2, None)); //Some(Duration::from_millis(300)

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![SqlRawString::from("0-hello"), SqlRawString::from("0-world")];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs).await
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![SqlRawString::from("1-hello"), SqlRawString::from("1-world")];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs).await
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, tester_handle_0, tester_handle_1).unwrap();
}

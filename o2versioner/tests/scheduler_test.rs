use o2versioner::comm::appserver_scheduler;
use o2versioner::core::msql::*;
use o2versioner::core::operation::*;
use o2versioner::scheduler;
use o2versioner::util::config::*;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_scheduler() {
    let _guard = tests_helper::init_logger();

    let sequencer_max_connection = 2;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from("127.0.0.1:16379"),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dbproxy_pool_size: 1,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:6379"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![],
    };

    let conf_clone = conf.clone();
    let scheduler_handle = tokio::spawn(scheduler::main(conf_clone));

    let conf_clone = conf.clone();
    let sequencer_handle = tokio::spawn(tests_helper::mock_echo_server(
        conf_clone.sequencer.to_addr(),
        conf_clone.sequencer.max_connection,
        "Mock Sequencer",
    ));

    sleep(Duration::from_millis(200)).await;

    let conf_clone = conf.clone();
    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            appserver_scheduler::Message::test("0-hello"),
            appserver_scheduler::Message::test("0-world"),
            appserver_scheduler::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(conf_clone.scheduler.to_addr()).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await;
    });

    let conf_clone = conf.clone();
    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            appserver_scheduler::Message::test("0-hello"),
            appserver_scheduler::Message::test("0-world"),
            appserver_scheduler::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(conf_clone.scheduler.to_addr()).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await;
    });

    // Must run the sequencer_handler, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

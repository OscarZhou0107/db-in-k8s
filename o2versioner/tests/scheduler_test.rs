use o2versioner::comm::scheduler_api;
use o2versioner::core::*;
use o2versioner::scheduler_main;
use o2versioner::util::config::*;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_scheduler() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:16379";
    let sequencer_max_connection = 2;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dbproxy_pool_size: 1,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:6379"),
            admin_addr: None,
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![],
    };

    let scheduler_handle = tokio::spawn(scheduler_main(conf.clone()));

    let sequencer_handle = tokio::spawn(tests_helper::mock_echo_server(
        conf.sequencer.to_addr(),
        conf.sequencer.max_connection,
        "Mock Sequencer",
    ));

    sleep(Duration::from_millis(300)).await;

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await;
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await;
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

#[tokio::test]
async fn test_scheduler_with_admin() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:14579";
    let scheduler_admin_addr = "127.0.0.1:39582";
    let sequencer_addr = "127.0.0.1:43279";
    let sequencer_max_connection = 2;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: Some(String::from(scheduler_admin_addr)),
            max_connection: None,
            sequencer_pool_size: sequencer_max_connection,
            dbproxy_pool_size: 1,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from(sequencer_addr),
            admin_addr: None,
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![],
    };

    let scheduler_handle = tokio::spawn(async move {
        scheduler_main(conf).await;
        println!("scheduler_handle DONE");
    });

    let sequencer_handle = tokio::spawn(async move {
        tests_helper::mock_echo_server(sequencer_addr, Some(sequencer_max_connection), "Mock Sequencer").await;

        println!("sequencer_handle DONE");
    });

    sleep(Duration::from_millis(300)).await;

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await;
        println!("tester_handle_0 DONE");
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await;
        println!("tester_handle_1 DONE");
    });

    sleep(Duration::from_millis(300)).await;

    let admin_client_handle = tokio::spawn(async move {
        let mut tcp_stream = TcpStream::connect(scheduler_admin_addr).await.unwrap();
        let res =
            tests_helper::mock_ascii_client(&mut tcp_stream, vec!["help", "exit"], "admin tcplistener tester").await;
        println!("admin_client_handle DONE: All responses received: {:?}", res);
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(
        scheduler_handle,
        sequencer_handle,
        tester_handle_0,
        tester_handle_1,
        admin_client_handle,
    )
    .unwrap();
}

#[tokio::test]
#[should_panic]
async fn test_scheduler_with_request_crash() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:32523";
    let sequencer_addr = "127.0.0.1:43582";
    let sequencer_max_connection = 2;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dbproxy_pool_size: 1,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from(sequencer_addr),
            admin_addr: None,
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![],
    };

    let scheduler_handle = tokio::spawn(async move {
        scheduler_main(conf).await;
        println!("scheduler_handle DONE");
    });

    let sequencer_handle = tokio::spawn(async move {
        tests_helper::mock_echo_server(sequencer_addr, Some(sequencer_max_connection), "Mock Sequencer").await;

        println!("sequencer_handle DONE");
    });

    sleep(Duration::from_millis(300)).await;

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await;
        println!("tester_handle_0 DONE");
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            scheduler_api::Message::test("0-hello"),
            scheduler_api::Message::test("0-world"),
            scheduler_api::Message::RequestMsql(Msql::BeginTx(MsqlBeginTx::from(TableOps::from(
                "READ table0 WRITE table1 table2 read table3",
            )))),
            scheduler_api::Message::request_crash("just for fun"),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await;
        println!("tester_handle_1 DONE");
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1,).unwrap();
}

use o2versioner::comm::scheduler_api::*;
use o2versioner::core::*;
use o2versioner::scheduler_main;
use o2versioner::sequencer_main;
use o2versioner::util::config::*;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};

#[tokio::test]
async fn test_double_s() {
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
    let sequencer_handle = tokio::spawn(sequencer_main(conf.sequencer.clone()));

    sleep(Duration::from_millis(300)).await;

    let tester_handle_0 = tokio::spawn(async move {
        let msgs = vec![
            Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 WRITE w1 w2")),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0")),
            Message::RequestMsqlText(MsqlText::query("update w1 set name=\"ray\" where id = 20;", "write w1")),
            Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
            Message::RequestMsqlText(MsqlText::query("update w2 set name=\"ray\" where id = 22;", "write w2")),
            Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 2").await;
    });

    let tester_handle_1 = tokio::spawn(async move {
        let msgs = vec![
            Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 WRITE w1 w2")),
            Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0")),
            Message::RequestMsqlText(MsqlText::query("update w1 set name=\"ray\" where id = 20;", "write w1")),
            Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
            Message::RequestMsqlText(MsqlText::query("update w2 set name=\"ray\" where id = 22;", "write w2")),
            Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
        ];

        let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
        tests_helper::mock_json_client(&mut tcp_stream, msgs, "Tester 1").await;
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

#[tokio::test]
#[ignore]
/// Run `cargo test run_double_s -- --nocapture`
async fn run_double_s() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:56728";
    let dbproxy_addr = "127.0.0.1:32223";
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: None,
            max_connection: None,
            sequencer_pool_size: 10,
            dbproxy_pool_size: 1,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:24212"),
            admin_addr: None,
            max_connection: None,
        },
        dbproxy: vec![DbProxyConfig {
            addr: String::from(dbproxy_addr),
            sql_addr: String::from("THIS IS NOT NEEDED"),
        }],
    };

    let scheduler_handle = tokio::spawn(scheduler_main(conf.clone()));
    let sequencer_handle = tokio::spawn(sequencer_main(conf.sequencer.clone()));
    let dbproxy_handle = tokio::spawn(tests_helper::mock_echo_server(dbproxy_addr, Some(1), "Mock dbproxy"));

    sleep(Duration::from_millis(300)).await;

    // let tester_handle_0 = tokio::spawn(async move {
    //     let msgs = vec![
    //         Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 WRITE w1 w2")),
    //         Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0")),
    //         Message::RequestMsqlText(MsqlText::query("update w1 set name=\"ray\" where id = 20;", "write w1")),
    //         Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
    //         Message::RequestMsqlText(MsqlText::query("update w2 set name=\"ray\" where id = 22;", "write w2")),
    //         Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
    //     ];

    //     let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
    //     tests_helper::mock_json_client(&mut tcp_stream, msgs, "Client Tester 0").await;
    // });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, dbproxy_handle).unwrap();
}

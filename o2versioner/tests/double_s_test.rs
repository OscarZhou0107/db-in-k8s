use futures::prelude::*;
use o2versioner::comm::scheduler_api::*;
use o2versioner::comm::scheduler_dbproxy;
use o2versioner::comm::MsqlResponse;
use o2versioner::core::*;
use o2versioner::scheduler_main;
use o2versioner::sequencer_main;
use o2versioner::util::config::*;
use o2versioner::util::tcp::*;
use o2versioner::util::tests_helper;
use rand::Rng;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::{sleep, Duration};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, info, info_span, instrument, Instrument};

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
        tests_helper::mock_json_client(&mut tcp_stream, msgs)
            .instrument(info_span!("tester0"))
            .await;
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
        tests_helper::mock_json_client(&mut tcp_stream, msgs)
            .instrument(info_span!("tester1"))
            .await;
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

#[tokio::test]
#[ignore]
/// Run `cargo test run_double_s -- --ignored --nocapture`
async fn run_double_s() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:56728";
    let dbproxy0_addr = "127.0.0.1:32223";
    let dbproxy1_addr = "127.0.0.1:32224";
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: None,
            max_connection: None,
            sequencer_pool_size: 10,
            dbproxy_pool_size: 10,
            dispatcher_queue_size: 1,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:24212"),
            max_connection: None,
        },
        dbproxy: vec![
            DbProxyConfig {
                addr: String::from(dbproxy0_addr),
                sql_addr: String::from("THIS IS NOT NEEDED"),
            },
            DbProxyConfig {
                addr: String::from(dbproxy1_addr),
                sql_addr: String::from("THIS IS NOT NEEDED"),
            },
        ],
    };

    let scheduler_handle = tokio::spawn(scheduler_main(conf.clone()));
    let sequencer_handle = tokio::spawn(sequencer_main(conf.sequencer.clone()));
    let dbproxy0_handle = tokio::spawn(mock_dbproxy(dbproxy0_addr));
    let dbproxy1_handle = tokio::spawn(mock_dbproxy(dbproxy1_addr));

    sleep(Duration::from_millis(300)).await;

    // let tx0 = vec![
    //     Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 WRITE w1 w2")),
    //     Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0")),
    //     Message::RequestMsqlText(MsqlText::query("update w1 set name=\"ray\" where id = 20;", "write w1")),
    //     Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
    //     Message::RequestMsqlText(MsqlText::query("update w2 set name=\"ray\" where id = 22;", "write w2")),
    //     Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
    // ];

    // let tx1 = vec![
    //     Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ r0 r1 WRITE w2 w3")),
    //     Message::RequestMsqlText(MsqlText::query("select * from r0;", "read r0")),
    //     Message::RequestMsqlText(MsqlText::query("update w2 set name=\"ray\" where id = 20;", "write w2")),
    //     Message::RequestMsqlText(MsqlText::query("select * from r1;", "read r1")),
    //     Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
    //     Message::RequestMsqlText(MsqlText::query("update w3 set name=\"ray\" where id = 22;", "write w3")),
    //     Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
    // ];

    // let tx2 = vec![
    //     Message::RequestMsqlText(MsqlText::begintx(Option::<String>::None, "READ w1 w2 WRITE r0 r1")),
    //     Message::RequestMsqlText(MsqlText::query("select * from w1;", "read w1")),
    //     Message::RequestMsqlText(MsqlText::query("update r0 set name=\"ray\" where id = 20;", "write r0")),
    //     Message::RequestMsqlText(MsqlText::query("select * from r1;", "read r1")),
    //     Message::RequestMsqlText(MsqlText::query("select * from w2;", "read w2")),
    //     Message::RequestMsqlText(MsqlText::query("update r1 set name=\"ray\" where id = 22;", "write r1")),
    //     Message::RequestMsqlText(MsqlText::endtx(Option::<String>::None, MsqlEndTxMode::Commit)),
    // ];

    // let msgs0 = [
    //     tx0.clone(),
    //     tx1.clone(),
    //     tx2.clone(),
    //     tx1.clone(),
    //     tx0.clone(),
    //     tx2.clone(),
    // ]
    // .concat();
    // let tester_handle_0 = tokio::spawn(async move {
    //     let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
    //     tests_helper::mock_json_client(&mut tcp_stream, msgs0)
    //         .instrument(info_span!("tester0"))
    //         .await;

    //     println!("\ntester_handle_0 DONE\n");
    // });

    // let msgs1 = [
    //     tx2.clone(),
    //     tx0.clone(),
    //     tx1.clone(),
    //     tx0.clone(),
    //     tx2.clone(),
    //     tx1.clone(),
    // ]
    // .concat();
    // let tester_handle_1 = tokio::spawn(async move {
    //     let mut tcp_stream = TcpStream::connect(scheduler_addr).await.unwrap();
    //     tests_helper::mock_json_client(&mut tcp_stream, msgs1)
    //         .instrument(info_span!("tester1"))
    //         .await;

    //     println!("\ntester_handle_1 DONE\n");
    // });

    // Must run, otherwise it won't do the work
    tokio::try_join!(
        scheduler_handle,
        sequencer_handle,
        dbproxy0_handle,
        dbproxy1_handle,
        // tester_handle_0,
        // tester_handle_1
    )
    .unwrap();
}

#[instrument(name="dbproxy(mock)" skip(addr))]
pub async fn mock_dbproxy<A>(addr: A)
where
    A: ToSocketAddrs,
{
    start_tcplistener(
        addr,
        move |mut tcp_stream| {
            async move {
                let _peer_addr = tcp_stream.peer_addr().unwrap();
                let (tcp_read, tcp_write) = tcp_stream.split();

                // Delimit frames from bytes using a length header
                let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
                let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

                // Deserialize/Serialize frames using JSON codec
                let serded_read = SymmetricallyFramed::new(
                    length_delimited_read,
                    SymmetricalJson::<scheduler_dbproxy::Message>::default(),
                );
                let serded_write = SymmetricallyFramed::new(
                    length_delimited_write,
                    SymmetricalJson::<scheduler_dbproxy::Message>::default(),
                );

                // Process a stream of incoming messages from a single tcp connection
                serded_read
                    .and_then(move |msg| {
                        async move {
                            debug!("Receives {:?}", msg);

                            // Simulate some load
                            let sleep_time = rand::rngs::OsRng::default().gen_range(20, 200);
                            info!("Works for {} ms", sleep_time);
                            sleep(Duration::from_millis(sleep_time)).await;

                            match msg {
                                scheduler_dbproxy::Message::MsqlRequest(_client, msql, _txvn) => {
                                    Ok(scheduler_dbproxy::Message::MsqlResponse(match msql {
                                        Msql::BeginTx(_) => {
                                            MsqlResponse::begintx_err("Dbproxy does not handle BeginTx")
                                        }
                                        Msql::Query(_) => MsqlResponse::query_ok("QUERY GOOD"),
                                        Msql::EndTx(_) => MsqlResponse::endtx_ok("ENDTX GOOD"),
                                    }))
                                }
                                _ => Ok(scheduler_dbproxy::Message::Invalid),
                            }
                        }
                    })
                    .inspect_ok(|m| debug!("dbproxy mock rpelies {:?}", m))
                    .forward(serded_write)
                    .map(|_| ())
                    .await;
            }
        },
        None,
        None,
    )
    .await;
}

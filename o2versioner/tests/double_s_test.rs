use futures::prelude::*;
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
use tracing::{info, info_span, instrument, trace, Instrument};
mod common;

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
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: None,
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: false,
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

    let transaction_samples = common::sql_transaction_samples();

    let tester0_reqs = transaction_samples[0].clone();
    let tester_handle_0 = tokio::spawn(async move {
        tests_helper::mock_json_client(&mut TcpStream::connect(scheduler_addr).await.unwrap(), tester0_reqs)
            .instrument(info_span!("tester0"))
            .await;
    });

    let tester1_reqs = transaction_samples[1].clone();
    let tester_handle_1 = tokio::spawn(async move {
        tests_helper::mock_json_client(&mut TcpStream::connect(scheduler_addr).await.unwrap(), tester1_reqs)
            .instrument(info_span!("tester1"))
            .await;
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(scheduler_handle, sequencer_handle, tester_handle_0, tester_handle_1).unwrap();
}

#[tokio::test]
#[ignore]
/// Run `cargo test run_double_s_limited -- --ignored --nocapture`
async fn run_double_s_limited() {
    let _guard = tests_helper::init_logger();

    let scheduler_addr = "127.0.0.1:56728";
    let dbproxy0_addr = "127.0.0.1:32223";
    let dbproxy1_addr = "127.0.0.1:32224";
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: Some(String::from("127.0.0.1:24251")),
            max_connection: None,
            sequencer_pool_size: 10,
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: Some("./perf".to_owned()),
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: false,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:24212"),
            max_connection: None,
        },
        dbproxy: vec![
            DbProxyConfig {
                addr: String::from(dbproxy0_addr),
                sql_conf: Some("host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()),
            },
            DbProxyConfig {
                addr: String::from(dbproxy1_addr),
                sql_conf: Some("host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()),
            },
        ],
    };

    let scheduler_handle = tokio::spawn(scheduler_main(conf.clone()));
    let sequencer_handle = tokio::spawn(sequencer_main(conf.sequencer.clone()));
    let dbproxy0_handle = tokio::spawn(mock_dbproxy(dbproxy0_addr, None));
    let dbproxy1_handle = tokio::spawn(mock_dbproxy(dbproxy1_addr, None));

    sleep(Duration::from_millis(300)).await;

    let transaction_samples = common::sql_transaction_samples();

    let msgs0 = [
        transaction_samples[3].clone(),
        transaction_samples[1].clone(),
        transaction_samples[4].clone(),
        transaction_samples[2].clone(),
        transaction_samples[4].clone(),
    ]
    .concat();
    let tester_handle_0 = tokio::spawn(async move {
        tests_helper::mock_json_client(&mut TcpStream::connect(scheduler_addr).await.unwrap(), msgs0)
            .instrument(info_span!("tester0"))
            .await;

        println!("\ntester_handle_0 DONE\n");
    });

    let msgs1 = [
        transaction_samples[4].clone(),
        transaction_samples[3].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
        transaction_samples[3].clone(),
    ]
    .concat();
    let tester_handle_1 = tokio::spawn(async move {
        tests_helper::mock_json_client(&mut TcpStream::connect(scheduler_addr).await.unwrap(), msgs1)
            .instrument(info_span!("tester1"))
            .await;

        println!("\ntester_handle_1 DONE\n");
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(
        scheduler_handle,
        sequencer_handle,
        dbproxy0_handle,
        dbproxy1_handle,
        tester_handle_0,
        tester_handle_1
    )
    .unwrap();
}

#[tokio::test]
#[ignore]
/// Run `cargo test run_double_s_unlimited -- --ignored --nocapture`
async fn run_double_s_unlimited() {
    let _guard = tests_helper::init_fast_logger();

    let scheduler_addr = "127.0.0.1:40001";
    let dbproxy0_addr = "127.0.0.1:30001";
    let dbproxy1_addr = "127.0.0.1:30002";
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: Some(String::from("127.0.0.1:19999")),
            max_connection: None,
            sequencer_pool_size: 10,
            dispatcher_queue_size: 5000,
            transceiver_queue_size: 5000,
            performance_logging: Some("./logging".to_owned()),
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: false,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:20001"),
            max_connection: None,
        },
        dbproxy: vec![
            DbProxyConfig {
                addr: String::from(dbproxy0_addr),
                sql_conf: Some("host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()),
            },
            DbProxyConfig {
                addr: String::from(dbproxy1_addr),
                sql_conf: Some("host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()),
            },
        ],
    };

    let scheduler_handle = tokio::spawn(scheduler_main(conf.clone()));
    let sequencer_handle = tokio::spawn(sequencer_main(conf.sequencer.clone()));
    let dbproxy0_handle = tokio::spawn(mock_dbproxy(dbproxy0_addr, Some(Duration::from_millis(2))));
    let dbproxy1_handle = tokio::spawn(mock_dbproxy(dbproxy1_addr, Some(Duration::from_millis(150))));

    sleep(Duration::from_millis(300)).await;

    let transaction_samples = common::sql_transaction_samples();
    let tx_sets = [
        transaction_samples[3].clone(),
        transaction_samples[4].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[4].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
        transaction_samples[0].clone(),
        transaction_samples[3].clone(),
        transaction_samples[2].clone(),
        transaction_samples[3].clone(),
        transaction_samples[4].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
    ]
    .concat();

    let mock_clients_handle = tokio::spawn(async move {
        stream::iter(0..8)
            .for_each_concurrent(None, move |id| {
                let tx_sets = tx_sets.clone();
                async move {
                    tests_helper::mock_json_client(
                        &mut TcpStream::connect(scheduler_addr).await.unwrap(),
                        tx_sets.into_iter().cycle().take(100),
                    )
                    .instrument(info_span!("tester", id))
                    .await;

                    println!("\ntester_handle_{} DONE\n", id);
                }
            })
            .await;
    });

    // Must run, otherwise it won't do the work
    tokio::try_join!(
        scheduler_handle,
        sequencer_handle,
        dbproxy0_handle,
        dbproxy1_handle,
        mock_clients_handle
    )
    .unwrap();
}

#[instrument(name="dbproxy(mock)" skip(addr, delay))]
pub async fn mock_dbproxy<A>(addr: A, delay: Option<Duration>)
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
                            trace!("<- {:?}", msg);

                            // Simulate some load
                            let sleep_time = if let Some(delay) = delay {
                                delay
                            } else {
                                Duration::from_millis(rand::rngs::OsRng::default().gen_range(20, 200))
                            };
                            info!("Works for {:?}", sleep_time);
                            sleep(sleep_time).await;

                            match msg {
                                scheduler_dbproxy::Message::MsqlRequest(client_addr, msql, _txvn) => {
                                    let response = match msql {
                                        Msql::BeginTx(_) => {
                                            MsqlResponse::begintx_err("Dbproxy does not handle BeginTx")
                                        }
                                        Msql::Query(_) => MsqlResponse::query_ok("QUERY GOOD"),
                                        Msql::EndTx(_) => MsqlResponse::endtx_ok("ENDTX GOOD"),
                                    };

                                    Ok(scheduler_dbproxy::Message::MsqlResponse(client_addr, response))
                                }
                                _ => Ok(scheduler_dbproxy::Message::Invalid),
                            }
                        }
                    })
                    .inspect_ok(|m| trace!("dbproxy mock replies {:?}", m))
                    .forward(serded_write)
                    .map(|_| ())
                    .await;
            }
        },
        // Single socket
        Some(1),
        None,
    )
    .await;
}

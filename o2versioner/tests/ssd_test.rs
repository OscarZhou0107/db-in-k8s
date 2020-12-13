use futures::prelude::*;
use o2versioner::dbproxy_main;
use o2versioner::scheduler_main;
use o2versioner::sequencer_main;
use o2versioner::util::config::*;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tracing::{info_span, Instrument};
mod common;

#[tokio::test]
#[ignore]
async fn test_ssd_constrained() {
    let _guard = tests_helper::init_fast_logger();

    let scheduler_addr = "127.0.0.1:45000";
    let scheduler_max_connection = 2;
    let sequencer_max_connection = 1;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from(scheduler_addr),
            admin_addr: None,
            max_connection: Some(scheduler_max_connection),
            sequencer_pool_size: sequencer_max_connection,
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: None,
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: false,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:45001"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![
            DbProxyConfig {
                addr: String::from("127.0.0.1:45002"),
                sql_conf: None,
            },
            DbProxyConfig {
                addr: String::from("127.0.0.1:45003"),
                sql_conf: None,
            },
        ],
    };

    let confc = conf.clone();
    let scheduler_handle = tokio::spawn(async move {
        scheduler_main(confc.clone()).await;

        println!("\nscheduler DONE\n");
    });

    let confc = conf.clone();
    let sequencer_handle = tokio::spawn(async move {
        sequencer_main(confc.sequencer.clone()).await;

        println!("\nsequencer DONE\n");
    });

    let confc = conf.clone();
    let dbproxies_handle = tokio::spawn(async move {
        stream::iter(confc.clone().dbproxy.into_iter().enumerate())
            .for_each_concurrent(None, move |(idx, conf)| async move {
                dbproxy_main(conf).await;

                println!("\ndbproxy {} DONE\n", idx);
            })
            .await;
    });

    sleep(Duration::from_millis(300)).await;

    let transaction_samples = common::sql_transaction_samples();
    let tx_sets = [
        transaction_samples[3].clone(),
        transaction_samples[4].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
        transaction_samples[4].clone(),
        transaction_samples[0].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
        transaction_samples[0].clone(),
        transaction_samples[3].clone(),
        transaction_samples[2].clone(),
        transaction_samples[1].clone(),
        transaction_samples[2].clone(),
    ]
    .concat();

    let mock_clients_handle = tokio::spawn(async move {
        stream::iter(0..scheduler_max_connection)
            .for_each_concurrent(None, move |id| {
                let tx_sets = tx_sets.clone();
                async move {
                    tests_helper::mock_json_client(
                        &mut TcpStream::connect(scheduler_addr).await.unwrap(),
                        tx_sets.into_iter(), //.cycle().take(100),
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
        dbproxies_handle,
        mock_clients_handle
    )
    .unwrap();
}

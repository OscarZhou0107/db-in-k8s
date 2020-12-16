use futures::prelude::*;
use o2versioner::comm::scheduler_api::Message;
use o2versioner::dbproxy_main;
use o2versioner::scheduler_main;
use o2versioner::sequencer_main;
use o2versioner::util::conf::*;
use o2versioner::util::tests_helper;
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration};
use tracing::{info_span, Instrument};
mod common;

#[tokio::test]
async fn test_ssd_constrained_default() {
    let _guard = tests_helper::init_fast_logger();

    let sequencer_max_connection = 1;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from("127.0.0.1:45000"),
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
            addr: String::from("127.0.0.1:45001"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![
            DbProxyConfig::new("127.0.0.1:45002"),
            DbProxyConfig::new("127.0.0.1:45003"),
        ],
    };

    test_suites(conf, &common::sql_transaction_samples()).await;
}

#[tokio::test]
async fn test_ssd_constrained_disable_single_read_opt() {
    let _guard = tests_helper::init_fast_logger();

    let sequencer_max_connection = 1;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from("127.0.0.1:45120"),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: None,
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: true,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:45121"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![
            DbProxyConfig::new("127.0.0.1:45122"),
            DbProxyConfig::new("127.0.0.1:45123"),
        ],
    };

    test_suites(conf, &common::sql_transaction_samples()).await;
}

#[tokio::test]
async fn test_ssd_constrained_disable_early_release() {
    let _guard = tests_helper::init_fast_logger();

    let sequencer_max_connection = 1;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from("127.0.0.1:45040"),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: None,
            detailed_logging: None,
            disable_early_release: true,
            disable_single_read_optimization: false,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:45041"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![
            DbProxyConfig::new("127.0.0.1:45042"),
            DbProxyConfig::new("127.0.0.1:45043"),
        ],
    };

    test_suites(conf, &common::sql_transaction_samples()).await;
}

#[tokio::test]
async fn test_ssd_constrained_disable_single_read_opt_and_early_release() {
    let _guard = tests_helper::init_fast_logger();

    let sequencer_max_connection = 1;
    let conf = Config {
        scheduler: SchedulerConfig {
            addr: String::from("127.0.0.1:45060"),
            admin_addr: None,
            max_connection: Some(2),
            sequencer_pool_size: sequencer_max_connection,
            dispatcher_queue_size: 1,
            transceiver_queue_size: 1,
            performance_logging: None,
            detailed_logging: None,
            disable_early_release: true,
            disable_single_read_optimization: true,
        },
        sequencer: SequencerConfig {
            addr: String::from("127.0.0.1:45061"),
            max_connection: Some(sequencer_max_connection),
        },
        dbproxy: vec![
            DbProxyConfig::new("127.0.0.1:45062"),
            DbProxyConfig::new("127.0.0.1:45063"),
        ],
    };

    test_suites(conf, &common::sql_transaction_samples()).await;
}

/// Launch the entire setup based on `Config`, inputs are defined inside this function.
///
/// # Notes:
/// All sub suites defined inside this function must be able to run with any `Config`
async fn test_suites(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
    test_suites_mixed(conf.clone(), transaction_samples).await;
    test_suites_single_read(conf.clone(), transaction_samples).await;
    test_suites_single_write(conf.clone(), transaction_samples).await;
    test_suites_single_read_with_early_release(conf.clone(), transaction_samples).await;
    test_suites_single_write_with_early_release(conf.clone(), transaction_samples).await;
}

async fn test_suites_mixed(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
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

    spawn_test(conf, tx_sets).await;
}

async fn test_suites_single_read(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
    let tx_sets = [transaction_samples[3].clone()].concat();
    spawn_test(conf, tx_sets).await;
}

async fn test_suites_single_write(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
    let tx_sets = [transaction_samples[4].clone()].concat();
    spawn_test(conf.clone(), tx_sets).await;
}

async fn test_suites_single_read_with_early_release(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
    let tx_sets = [transaction_samples[5].clone()].concat();
    spawn_test(conf, tx_sets).await;
}

async fn test_suites_single_write_with_early_release(conf: Config, transaction_samples: &Vec<Vec<Message>>) {
    let tx_sets = [transaction_samples[6].clone()].concat();
    spawn_test(conf, tx_sets).await;
}

/// Launch the entire setup based on `Config`, and with
/// `inputs: Vec<Message>`
async fn spawn_test(conf: Config, inputs: Vec<Message>) {
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

    sleep(Duration::from_millis(500)).await;

    let num_clients: u32 = conf.scheduler.max_connection.as_ref().cloned().unwrap_or(8);
    let scheduler_addr = conf.scheduler.addr.clone();
    let mock_clients_handle = tokio::spawn(async move {
        stream::iter(0..num_clients)
            .for_each_concurrent(None, move |id| {
                let inputs = inputs.clone();
                let scheduler_addr = scheduler_addr.clone();
                async move {
                    tests_helper::mock_json_client(
                        &mut TcpStream::connect(scheduler_addr).await.unwrap(),
                        inputs.into_iter(), //.cycle().take(100),
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

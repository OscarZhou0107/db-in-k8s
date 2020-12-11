use std::env;

use clap::{App, Arg, ArgGroup, ArgMatches};
use o2versioner::util::config::Config;
use o2versioner::{dbproxy_main, scheduler_main, sequencer_main};
use tracing::info;

pub fn init_logger() {}

/// cargo run -- <args>
#[tokio::main]
async fn main() {
    // Parse args
    let matches = parse_args();

    // Set tracing
    let max_level = match matches.occurrences_of("v") {
        0 => tracing::Level::INFO,
        1 => tracing::Level::DEBUG,
        2 | _ => tracing::Level::TRACE,
    };
    println!("Verbosity set to {:?}", max_level);
    let collector = tracing_subscriber::fmt()
        .with_max_level(max_level)
        .with_target(false)
        .without_time()
        .finish();
    tracing::subscriber::set_global_default(collector).unwrap();

    // Parse config
    info!("current dir is: {}", env::current_dir().unwrap().to_str().unwrap());
    let conf = Config::from_file(matches.value_of("config").unwrap());
    info!("{:?}", conf);

    // Launch binary
    if matches.is_present("dbproxy") {
        let index: usize = matches.value_of("dbindex").unwrap().to_string().parse().unwrap();
        dbproxy_main(conf.dbproxy.get(index).unwrap().clone()).await
    } else if matches.is_present("scheduler") {
        scheduler_main(conf).await
    } else if matches.is_present("sequencer") {
        sequencer_main(conf.sequencer).await
    } else {
        panic!("Unknown error!")
    }
}

fn parse_args() -> ArgMatches<'static> {
    App::new("o2versioner")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .value_name("FILE")
                .default_value("o2versioner/config.toml")
                .help("Sets the config file")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("dbproxy")
                .long("dbproxy")
                .help("Run the dbproxy")
                .requires("dbindex"),
        )
        .arg(
            Arg::with_name("dbindex")
                .long("dbindex")
                .help("Indicate the index of dbproxy")
                .index(1),
        )
        .arg(Arg::with_name("scheduler").long("scheduler").help("Run the scheduler"))
        .arg(Arg::with_name("sequencer").long("sequencer").help("Run the sequencer"))
        .group(
            ArgGroup::with_name("binary")
                .args(&["dbproxy", "scheduler", "sequencer"])
                .required(true),
        )
        .arg(Arg::with_name("v").short("v").multiple(true).help("v-debug, vv-trace"))
        .get_matches()
}

use clap::{App, Arg, ArgGroup, ArgMatches};
use env_logger;
use log::info;
use o2versioner::dbproxy;
use o2versioner::scheduler;
use o2versioner::sequencer;
use o2versioner::util::config::Config;

fn init_logger() {
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Stdout)
        .filter_level(log::LevelFilter::Debug)
        .init();
}

/// cargo run -- <args>
#[tokio::main]
async fn main() {
    let matches = parse_args();

    init_logger();
    let conf = Config::from_file(matches.value_of("config").unwrap());
    info!("{:?}", conf);

    let sequencer_addr = "127.0.0.1:6379";
    let scheduler_addr = "127.0.0.1:16379";
    if matches.is_present("dbproxy") {
        dbproxy::handler::main("127.0.0.1:2345", "mysql://root:Rayh8768@localhost:3306/test").await
    } else if matches.is_present("scheduler") {
        scheduler::handler::main(scheduler_addr, None, sequencer_addr, 5).await
    } else if matches.is_present("sequencer") {
        sequencer::handler::main(sequencer_addr, None).await
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
        .arg(Arg::with_name("dbproxy").long("dbproxy").help("Run the dbproxy"))
        .arg(Arg::with_name("scheduler").long("scheduler").help("Run the scheduler"))
        .arg(Arg::with_name("sequencer").long("sequencer").help("Run the sequencer"))
        .group(
            ArgGroup::with_name("binary")
                .args(&["dbproxy", "scheduler", "sequencer"])
                .required(true),
        )
        .get_matches()
}

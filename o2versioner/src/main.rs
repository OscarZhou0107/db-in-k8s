use clap::{App, Arg, ArgGroup, ArgMatches};
use o2versioner::dbproxy;
use o2versioner::util::config::Config;
use o2versioner::{scheduler_main, sequencer_main};
use tracing::info;

pub fn init_logger() {
    let collector = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .without_time()
        .finish();
    tracing::subscriber::set_global_default(collector).unwrap();
}

/// cargo run -- <args>
#[tokio::main]
async fn main() {
    let matches = parse_args();

    init_logger();
    let conf = Config::from_file(matches.value_of("config").unwrap());
    info!("{:?}", conf);

    if matches.is_present("dbproxy") {

        let mut config = tokio_postgres::Config::new();
        config.user("postgres");
        config.password("Rayh8768");
        config.host("localhost");
        config.port(5432);
        config.dbname("Test");

        dbproxy::main("127.0.0.1:2345", config).await
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

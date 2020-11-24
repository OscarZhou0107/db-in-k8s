use env_logger;
use o2versioner::scheduler;

fn init_logger() {
    let mut builder = env_logger::Builder::from_default_env();
    builder.target(env_logger::Target::Stdout);
    builder.filter_level(log::LevelFilter::Debug);
    builder.init();
}

#[tokio::main]
async fn main() {
    init_logger();

    scheduler::handler::main("127.0.0.1:6379", None).await
}

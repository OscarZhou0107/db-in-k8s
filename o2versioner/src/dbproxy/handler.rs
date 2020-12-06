use super::core::{DbVersion, PendingQueue, QueryResult};
use super::dispatcher::Dispatcher;
use super::receiver::Receiver;
use super::responder::Responder;
use crate::util::config::DbProxyConfig;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_postgres::Config;

pub async fn main(conf: DbProxyConfig) {
    println!("Starting dbproxy...");
    let mut config = Config::new();
    config.user(&conf.user);
    config.password(conf.password);
    config.host(&conf.host);
    config.port(conf.port);
    config.dbname(&conf.dbname);

    //Map that holds all ongoing transactions
    let transactions = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(Default::default())));
    let version_2 = Arc::clone(&version);

    //PendingQueue
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2: Arc<Mutex<PendingQueue>> = Arc::clone(&pending_queue);

    //Responder sender and receiver
    let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let listener = TcpListener::bind(conf.addr).await.unwrap();
    println!("Binding to tcp listener...");
    let (tcp_stream, _) = listener.accept().await.unwrap();
    println!("Connection established...");
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    println!("Starting Dispatcher...");
    Dispatcher::run(pending_queue, responder_sender, config, version, transactions);

    println!("Starting Responder...");
    Responder::run(responder_receiver, version_2, tcp_write);

    println!("Starting Receiver...");
    Receiver::run(pending_queue_2, tcp_read).await;
    
    println!("End");
}

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
use tracing::info;

pub async fn main(conf: DbProxyConfig) {
    info!("Starting dbproxy...");
    let mut config = Config::new();
    config.user(&conf.user);
    config.password(conf.password);
    config.host(&conf.host);
    config.port(conf.port);
    config.dbname(&conf.dbname);

    //Map that holds all ongoing transactions
    let transactions = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let version = Arc::new(Mutex::new(DbVersion::new(Default::default())));

    //PendingQueue
    let pending_queue = Arc::new(Mutex::new(PendingQueue::new()));

    //Responder sender and receiver
    let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let listener = TcpListener::bind(conf.addr).await.unwrap();
    info!("Binding to tcp listener...");
    let (tcp_stream, _) = listener.accept().await.unwrap();
    info!("Connection established...");
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    info!("Starting Dispatcher...");
    let dispatcher_handle = tokio::spawn(Dispatcher::run(
        pending_queue.clone(),
        responder_sender,
        config,
        version.clone(),
        transactions,
    ));

    info!("Starting Responder...");
    let responder_handle = tokio::spawn(Responder::run(responder_receiver, version, tcp_write));

    info!("Starting Receiver...");
    let receiver_handle = tokio::spawn(Receiver::run(pending_queue, tcp_read));

    tokio::try_join!(responder_handle, dispatcher_handle, receiver_handle).unwrap();
    info!("End");
}

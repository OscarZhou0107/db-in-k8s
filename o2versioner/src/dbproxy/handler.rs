use super::core::{DbVersion, PendingQueue, QueryResult};
use super::dispatcher::Dispatcher;
use super::receiver::Receiver;
use super::responder::Responder;
use crate::util::config::DbProxyConfig;
use crate::util::executor::Executor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn main(conf: DbProxyConfig) {
    info!("Starting dbproxy...");

    //Map that holds all ongoing transactions
    let transactions = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let version = Arc::new(Mutex::new(DbVersion::new(Default::default())));

    //PendingQueue
    let pending_queue = Arc::new(Mutex::new(PendingQueue::new()));

    //Responder sender and receiver
    let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let listener = TcpListener::bind(&conf.addr).await.unwrap();
    info!("Binding to tcp listener...");
    let (tcp_stream, _) = listener.accept().await.unwrap();
    info!("Connection established...");
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    info!("Starting Dispatcher...");
    let dispatcher = Dispatcher::new(
        pending_queue.clone(),
        responder_sender,
        conf,
        version.clone(),
        transactions.clone(),
    );
    let dispatcher_handle = tokio::spawn(Box::new(dispatcher).run());

    info!("Starting Responder...");
    let responder_handle = tokio::spawn(Responder::run(responder_receiver, version, tcp_write));

    info!("Starting Receiver...");
    let receiver_handle = tokio::spawn(Receiver::run(pending_queue.clone(), tcp_read));

    let res = tokio::try_join!(responder_handle, dispatcher_handle, receiver_handle);
    info!("End");
    info!("Pending queue size is {}", pending_queue.lock().await.queue.len());
    info!("Transactions size is {}", transactions.lock().await.len());

    res.unwrap();
}

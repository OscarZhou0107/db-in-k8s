use super::core::{DbVersion, PendingQueue};
use super::dispatcher::Dispatcher;
use super::transceiver;
use crate::util::config::DbProxyConfig;
use crate::util::executor::Executor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn main(conf: DbProxyConfig) {
    info!("Starting dbproxy...");

    // Map that holds all ongoing transactions
    let transactions = Arc::new(Mutex::new(HashMap::new()));

    // Global version
    let version = Arc::new(Mutex::new(DbVersion::new(Default::default())));

    // PendingQueue
    let pending_queue = Arc::new(Mutex::new(PendingQueue::new()));

    // Responder sender and receiver
    let (responder_sender, responder_receiver) = mpsc::channel(100);

    info!("Starting Dispatcher...");
    let dispatcher = Dispatcher::new(
        pending_queue.clone(),
        responder_sender,
        conf.clone(),
        version.clone(),
        transactions.clone(),
    );
    let dispatcher_handle = tokio::spawn(Box::new(dispatcher).run());

    let (receiver, responder) =
        transceiver::connection(conf.addr, pending_queue.clone(), responder_receiver, version).await;
    info!("Starting Responder...");
    let responder_handle = tokio::spawn(Box::new(responder).run());
    info!("Starting Receiver...");
    let receiver_handle = tokio::spawn(Box::new(receiver).run());

    let res = tokio::try_join!(responder_handle, dispatcher_handle, receiver_handle);
    info!("End");
    info!("Pending queue size is {}", pending_queue.lock().await.queue.len());
    info!("Transactions size is {}", transactions.lock().await.len());

    res.unwrap();
}

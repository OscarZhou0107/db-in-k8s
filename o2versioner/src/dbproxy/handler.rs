use super::core::{DbVersion, PendingQueue};
use super::dispatcher::Dispatcher;
use super::transceiver;
use crate::util::conf::DbProxyConfig;
use crate::util::executor::Executor;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tracing::{field, info, instrument, Instrument, Span};

/// Main entrance for the DbProxy
#[instrument(name = "dbproxy", skip(conf), fields(message=field::Empty))]
pub async fn main(conf: DbProxyConfig) {
    Span::current().record("message", &&conf.addr[..]);

    // Map that holds all ongoing transactions
    let transactions = Arc::new(Mutex::new(HashMap::new()));

    // Global version
    let version = Arc::new(Mutex::new(DbVersion::new(Default::default())));

    // PendingQueue
    let pending_queue = Arc::new(Mutex::new(PendingQueue::new()));

    // Responder sender and receiver
    let (responder_sender, responder_receiver) = mpsc::channel(100);

    // Dispatcher
    let (dispatcher_stopper, dispatcher) = Dispatcher::new(
        pending_queue.clone(),
        responder_sender,
        conf.clone(),
        version.clone(),
        transactions.clone(),
    );
    let dispatcher_handle = tokio::spawn(Box::new(dispatcher).run().in_current_span());

    // Receiver and responder
    let (receiver, responder) =
        transceiver::connection(conf.addr, pending_queue.clone(), responder_receiver, version).await;
    let responder_handle = tokio::spawn(Box::new(responder).run().in_current_span());
    let receiver_handle = tokio::spawn(Box::new(receiver).run().in_current_span());

    // Receiver should die first
    let receiver_res = receiver_handle.await;

    // Then, notify dispatcher to stop
    dispatcher_stopper.send(()).unwrap();
    let dispatcher_res = dispatcher_handle.await;

    // Finally, responder should die after dispatcher releases its channel handle
    let responder_res = responder_handle.await;

    info!("End");
    let pending_queue_size = pending_queue.lock().await.queue.len();
    info!("Pending queue size is {}", pending_queue_size);
    let transactions_size = transactions.lock().await.len();
    info!("Transactions size is {}", transactions_size);

    receiver_res.unwrap();
    responder_res.unwrap();
    dispatcher_res.unwrap();
    assert_eq!(pending_queue_size, 0);
    assert_eq!(transactions_size, 0);
}

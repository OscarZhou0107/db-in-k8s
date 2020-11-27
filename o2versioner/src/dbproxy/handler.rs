use super::core::{DbVersion, Operation, PendingQueue, QueryResult};
use super::dispatcher::Dispatcher;
use super::receiver::Receiver;
use super::responder::Responder;
use std::sync::Arc;
use std::{collections::HashMap, sync::Mutex};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::sync::mpsc;

pub async fn main<A: ToSocketAddrs>(addr: A, sql_url: &str) {
    //=====================================Continue an ongoing transaction=======================================//
    //Map that holds all ongoing transactions
    let transactions: Arc<Mutex<HashMap<String, mpsc::Sender<Operation>>>> = Arc::new(Mutex::new(HashMap::new()));

    //Global version//
    let mut mock_db = HashMap::new();
    mock_db.insert("table1".to_string(), 0);
    mock_db.insert("table2".to_string(), 0);
    let version: Arc<Mutex<DbVersion>> = Arc::new(Mutex::new(DbVersion::new(mock_db)));
    let version_2 = Arc::clone(&version);

    //PendingQueue
    let pending_queue: Arc<Mutex<PendingQueue>> = Arc::new(Mutex::new(PendingQueue::new()));
    let pending_queue_2: Arc<Mutex<PendingQueue>> = Arc::clone(&pending_queue);

    //Responder sender and receiver
    let (responder_sender, responder_receiver): (mpsc::Sender<QueryResult>, mpsc::Receiver<QueryResult>) =
        mpsc::channel(100);

    let mut listener = TcpListener::bind(addr).await.unwrap();
    let (tcp_stream, _) = listener.accept().await.unwrap();
    let (tcp_read, tcp_write) = tcp_stream.into_split();

    Dispatcher::run(
        pending_queue,
        responder_sender,
        sql_url.to_string(),
        version,
        transactions,
    );

    Responder::run(responder_receiver, version_2, tcp_write);

    Receiver::run(pending_queue_2, tcp_read);
}

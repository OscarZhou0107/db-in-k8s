#![allow(warnings)]
use super::core::DbVNManager;
use crate::comm::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use futures::prelude::*;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::Notify;
use tokio::sync::RwLock;

/// Sent from `DispatcherAddr` to `Dispatcher`
struct Request {
    /// For debugging
    client_addr: SocketAddr,
    command: Msql,
    /// A single use reply channel
    reply: oneshot::Sender<MsqlResponse>,
}

impl Request {
    async fn work(self, state: State) {}
}

/// A state containing shareved variables
#[derive(Clone)]
struct State {
    dbvn_manager: Arc<RwLock<DbVNManager>>,
}

impl State {
    fn new() -> Self {
        Self {
            dbvn_manager: Arc::new(RwLock::new(DbVNManager::default())),
        }
    }
}

pub struct Dispatcher {
    state: State,
    rx: mpsc::Receiver<Request>,
}

impl Dispatcher {
    pub fn new(queue_size: usize) -> (DispatcherAddr, Dispatcher) {
        let (tx, rx) = mpsc::channel(queue_size);
        (
            DispatcherAddr { tx },
            Dispatcher {
                state: State::new(),
                rx,
            },
        )
    }

    pub async fn run(self) {
        // Handle each Request concurrently
        let Dispatcher { state, rx } = self;
        rx.for_each_concurrent(None, |dispatch_request| async {
            dispatch_request.work(state.clone()).await
        })
        .await;
    }
}

/// Encloses a way to talk to the Dispatcher
/// 
/// TODO: provide a way to shutdown the `Dispatcher`
#[derive(Debug, Clone)]
pub struct DispatcherAddr {
    tx: mpsc::Sender<Request>,
}

impl DispatcherAddr {
    async fn request(&mut self, client_addr: SocketAddr, command: Msql) -> Result<MsqlResponse, String> {
        // Create a reply oneshot channel
        let (tx, rx) = oneshot::channel();

        // Construct the request to sent
        let request = Request {
            client_addr,
            command,
            reply: tx,
        };

        // Send the request
        self.tx.send(request).await.map_err(|e| e.to_string())?;

        // Wait for the reply
        rx.await.map_err(|e| e.to_string())
    }
}

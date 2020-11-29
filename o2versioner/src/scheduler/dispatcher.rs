#![allow(warnings)]
use super::core::SchedulerState;
use crate::comm::appserver_scheduler::MsqlResponse;
use crate::core::msql::*;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

struct DispatchRequest {
    client_addr: SocketAddr,
    command: Msql,
    reply: oneshot::Sender<MsqlResponse>,
}

pub fn channel(queue_size: usize) -> (DispatcherAddr, Dispatcher) {
    let (tx, rx) = mpsc::channel(queue_size);
    (
        DispatcherAddr { tx },
        Dispatcher {
            scheduler_state: SchedulerState::default(),
            rx,
        },
    )
}

pub struct Dispatcher {
    scheduler_state: SchedulerState,
    rx: mpsc::Receiver<DispatchRequest>,
}

/// Encloses a way to talk to the Dispatcher
#[derive(Debug, Clone)]
pub struct DispatcherAddr {
    tx: mpsc::Sender<DispatchRequest>,
}

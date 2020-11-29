use super::tcp;
use futures::prelude::*;
use std::string::ToString;
use tokio::net::ToSocketAddrs;
use tracing::dispatcher::DefaultGuard;
use tracing::{debug, error};

#[must_use = "Dropping the guard unregisters the subscriber."]
pub fn init_logger() -> DefaultGuard {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .with_test_writer()
        .finish();
    tracing::subscriber::set_default(subscriber)
}

/// A mock echo server for testing
pub async fn mock_echo_server<A, S>(addr: A, max_connection: Option<u32>, server_name: S)
where
    A: ToSocketAddrs,
    S: ToString,
{
    tcp::start_tcplistener(
        addr,
        |mut tcp_stream| async move {
            let peer_addr = tcp_stream.peer_addr().unwrap();
            let (mut reader, mut writer) = tcp_stream.split();

            tokio::io::copy(&mut reader, &mut writer)
                .then(move |result| {
                    match result {
                        Ok(amt) => debug!("-> [{}] ECHOED {} BYTES", peer_addr, amt),
                        Err(e) => error!("-> [{}] ERROR ON ECHOING: {}", peer_addr, e),
                    };
                    future::ready(())
                })
                .await;
        },
        max_connection,
        server_name,
    )
    .await;
}

pub use tcp::send_and_receive_as_json as mock_json_client;

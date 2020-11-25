use super::tcp;
use futures::prelude::*;
use std::string::ToString;
use tokio::net::ToSocketAddrs;

/// A mock echo server for testing
pub async fn mock_echo_server<A, S>(addr: A, max_connection: Option<usize>, server_name: Option<S>)
where
    A: ToSocketAddrs + std::fmt::Debug + Clone,
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
                        Ok(amt) => println!("Echoed {} bytes to [{}]", amt, peer_addr),
                        Err(e) => println!("Error on echoing to [{}]: {}", peer_addr, e),
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

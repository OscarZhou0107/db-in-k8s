use futures::prelude::*;
use tokio::net::{TcpListener, ToSocketAddrs};

pub async fn mock_echo_server<A: ToSocketAddrs>(addr: A, max_connection: Option<usize>) {
    TcpListener::bind(addr)
        .await
        .unwrap()
        .incoming()
        .enumerate()
        .take_while(move |(idx, _)| {
            future::ready(
                max_connection
                    .map(|max_connection| *idx < max_connection)
                    .or(Some(true))
                    .unwrap(),
            )
        })
        .map(|(_, r)| r)
        .try_for_each_concurrent(None, |mut socket| async move {
            let addr = socket.peer_addr().unwrap();
            println!("Connection established with {}", addr);
            let (mut reader, mut writer) = socket.split();

            tokio::io::copy(&mut reader, &mut writer)
                .then(move |result| {
                    match result {
                        Ok(amt) => println!("Echoed {} bytes to {}", amt, addr),
                        Err(e) => println!("error on echoing to {}: {}", addr, e),
                    };
                    future::ready(())
                })
                .await;
            Ok(())
        })
        .await
        .unwrap();
}

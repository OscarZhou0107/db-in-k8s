use super::tcp;
use futures::prelude::*;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::dispatcher::DefaultGuard;
use tracing::{debug, error, info};

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
    S: Into<String>,
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

/// Send a collection of msg through the argument `TcpStream`, and expecting a reply for each of the msg sent.
/// The msgs are send received using a newline-delimited ascii encoding.
///
/// # Notes:
/// 1. For each msg in `msgs`, send it using the argument `TcpStream` and expecting a reply. Pack the reply into a `Vec`.
/// 2. The sent message must not contain any newline characters
/// 3. `<Msgs as IntoInterator>::Item` must be an owned type, and will be used as the type to hold the replies from the server.
pub async fn mock_ascii_client<Msgs, S, MS>(
    tcp_stream: &mut TcpStream,
    msgs: Msgs,
    client_name: S,
) -> Vec<std::io::Result<String>>
where
    Msgs: IntoIterator<Item = MS>,
    S: Into<String>,
    MS: Into<String>,
{
    let local_addr = tcp_stream.local_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.split();
    let line_reader = BufReader::new(tcp_read).lines();

    let mut responses = Vec::new();
    stream::iter(msgs)
        .fold(
            (line_reader, tcp_write, &mut responses),
            |(mut line_reader, mut tcp_write, responses), send_msg| async move {
                let mut send_msg = send_msg.into();
                assert!(
                    !send_msg.contains("\n"),
                    "mock_ascii_client send message should not contain any newline characters"
                );
                debug!("[{}] -> SEND REQUEST: {:?}", local_addr, send_msg);
                send_msg += "\n";
                responses.push(
                    tcp_write
                        .write_all(send_msg.as_bytes())
                        .and_then(|_| line_reader.next_line())
                        .map_ok(|received_msg| {
                            let received_msg = received_msg.unwrap().trim().to_owned();
                            info!("[{}] <- GOT RESPONSE: \"{:?}\"", local_addr, received_msg);
                            received_msg
                        })
                        .await,
                );

                (line_reader, tcp_write, responses)
            },
        )
        .await;

    let client_name = client_name.into();
    debug!(
        "[{}] {} TcpStream says task done, have a good day",
        local_addr, client_name
    );

    responses
}

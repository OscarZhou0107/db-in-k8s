use super::tcp;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::panic::{RefUnwindSafe, UnwindSafe};
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::{TcpStream, ToSocketAddrs};
use tracing::dispatcher::DefaultGuard;
use tracing::{debug, error, field, info, instrument, Span};

#[must_use = "Dropping the guard unregisters the subscriber."]
pub fn init_logger() -> DefaultGuard {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_test_writer()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .without_time()
        .finish();
    tracing::subscriber::set_default(subscriber)
}

/// A mock echo server for testing
#[instrument(name="echo(mock)" skip(addr, max_connection, server_name))]
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
        None,
    )
    .await;
}

#[instrument(name = "client(mock)", skip(tcp_stream, msgs, client_name))]
pub async fn mock_json_client<Msgs, S>(
    tcp_stream: &mut TcpStream,
    msgs: Msgs,
    client_name: S,
) -> Vec<std::io::Result<Msgs::Item>>
where
    Msgs: IntoIterator,
    for<'a> Msgs::Item: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
    S: Into<String>,
{
    tcp::send_and_receive_as_json(tcp_stream, msgs, client_name).await
}

/// Send a collection of msg through the argument `TcpStream`, and expecting a reply for each of the msg sent.
/// The msgs are send received using a newline-delimited ascii encoding.
///
/// # Notes:
/// 1. For each msg in `msgs`, send it using the argument `TcpStream` and expecting a reply. Pack the reply into a `Vec`.
/// 2. The sent message must not contain any newline characters
/// 3. `<Msgs as IntoInterator>::Item` must be an owned type, and will be used as the type to hold the replies from the server.
#[instrument(name="ascii(mock):chat" skip(tcp_stream, msgs, client_name) fields(message=field::Empty, to=field::Empty))]
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
    let peer_addr = tcp_stream.peer_addr().unwrap();
    Span::current().record("message", &&local_addr.to_string()[..]);
    Span::current().record("to", &&peer_addr.to_string()[..]);

    let (tcp_read, tcp_write) = tcp_stream.split();
    // Must send this line_reader as a mut ref into the closure for fold,
    // moving this line_reader into the closure will make its owner to be the closure,
    // which will indefinitely hold the Lines stream without consuming it,
    // raising a compiler warning.
    let mut line_reader = BufReader::new(tcp_read).lines();

    let mut responses = Vec::new();
    stream::iter(msgs)
        .fold(
            (&mut line_reader, tcp_write, &mut responses),
            |(line_reader, mut tcp_write, responses), send_msg| async move {
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
                            info!("[{}] <- GOT RESPONSE: {:?}", local_addr, received_msg);
                            received_msg
                        })
                        .await,
                );

                (line_reader, tcp_write, responses)
            },
        )
        .await;

    let client_name = client_name.into();
    debug!("[{}] {} TcpStream says current task finished", local_addr, client_name);

    responses
}

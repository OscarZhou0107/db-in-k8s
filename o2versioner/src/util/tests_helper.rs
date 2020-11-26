use super::tcp;
use env_logger;
use futures::prelude::*;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::string::ToString;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init();
}

/// A mock echo server for testing
pub async fn mock_echo_server<A, S>(addr: A, max_connection: Option<u32>, server_name: S)
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
                        Ok(amt) => info!("-> [{}] ECHOED {} BYTES", peer_addr, amt),
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

/// Mock a json client with argument `TcpStream` using customized deserialization
///
/// Note:
/// 1. `msgs: Msgs` must be an owned collection that contains owned data types.
/// 2. `<Msgs as IntoInterator>::Item` must be an owned type, and will be used as the type to hold the replies from the server.
///
/// TODO: Consider split the type being sent and the type expected to receive
pub async fn mock_json_client<Msgs, S>(tcp_stream: &mut TcpStream, msgs: Msgs, client_name: S) -> Vec<Msgs::Item>
where
    Msgs: IntoIterator,
    for<'a> Msgs::Item: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
    S: ToString,
{
    let local_addr = tcp_stream.local_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read = SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<Msgs::Item>::default());
    let serded_write = SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<Msgs::Item>::default());

    let mut responses = Vec::new();
    stream::iter(msgs)
        .fold(
            (serded_read, serded_write, &mut responses),
            |(mut serded_read, mut serded_write, responses), send_msg| async move {
                info!("[{}] -> SEND REQUEST: {:?}", local_addr, send_msg);
                responses.push(
                    serded_write
                        .send(send_msg)
                        .and_then(|_| serded_read.try_next())
                        .map_ok(|received_msg| {
                            let received_msg = received_msg.unwrap();
                            info!("[{}] <- GOT RESPONSE: {:?}", local_addr, received_msg);
                            received_msg
                        })
                        .await
                        .unwrap(),
                );

                (serded_read, serded_write, responses)
            },
        )
        .await;

    let client_name = client_name.to_string();
    info!(
        "[{}] {} TcpStream says service terminated, have a good night",
        local_addr, client_name
    );

    responses
}

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
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .init();
}

/// A mock echo server for testing
pub async fn mock_echo_server<A, S>(addr: A, max_connection: Option<u32>, server_name: Option<S>)
where
    A: ToSocketAddrs + std::fmt::Debug + Clone,
    S: ToString,
{
    tcp::start_tcplistener(
        addr,
        |mut tcp_stream| async move {
            let peer_addr = tcp_stream.peer_addr().unwrap();
            let local_addr = tcp_stream.local_addr().unwrap();
            let (mut reader, mut writer) = tcp_stream.split();

            tokio::io::copy(&mut reader, &mut writer)
                .then(move |result| {
                    match result {
                        Ok(amt) => info!("[{}] -> [{}] ECHOED {} BYTES", local_addr, peer_addr, amt),
                        Err(e) => error!("[{}] -> [{}] ERROR ON ECHOING: {}", local_addr, peer_addr, e),
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
pub async fn mock_json_client<Msgs>(tcp_stream: &mut TcpStream, msgs: Msgs) -> Vec<Msgs::Item>
where
    Msgs: IntoIterator,
    for<'a> Msgs::Item: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
{
    let local_addr = tcp_stream.local_addr().unwrap();
    let peer_addr = tcp_stream.peer_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read: SymmetricallyFramed<_, Msgs::Item, _> =
        SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<Msgs::Item>::default());
    let serded_write: SymmetricallyFramed<_, Msgs::Item, _> =
        SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<Msgs::Item>::default());

    let mut responses = Vec::new();
    stream::iter(msgs)
        .fold(
            (serded_read, serded_write, &mut responses),
            |(mut serded_read, mut serded_write, responses), send_msg| async move {
                info!("[{}] -> [{}] SEND REQUEST: {:?}", local_addr, peer_addr, send_msg);
                responses.push(
                    serded_write
                        .send(send_msg)
                        .and_then(|_| serded_read.try_next())
                        .map_ok(|received_msg| {
                            let received_msg = received_msg.unwrap();
                            info!("[{}] <- [{}] GOT RESPONSE: {:?}", local_addr, peer_addr, received_msg);
                            received_msg
                        })
                        .await
                        .unwrap(),
                );

                (serded_read, serded_write, responses)
            },
        )
        .await;

    info!("[{}] {} says goodbye world", "mock_json_client", local_addr);

    responses
}

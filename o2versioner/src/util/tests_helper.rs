use super::tcp;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::string::ToString;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

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

/// Mock a json client with argument `TcpStream` using symmetrical serialization and deserialization
///
/// Notes:
/// `msgs` must be owned type, such as `Vec<String>`. `&[String]', `&[&str]`, `Vec<&str>`, `&Vec<String>`
/// are not accepted. Since the deserializer can not deserialize into a non-owned type.
pub async fn mock_json_client_symm<Msgs>(tcp_stream: &mut TcpStream, msgs: Msgs)
where
    Msgs: IntoIterator,
    for<'a> Msgs::Item: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
{
    return mock_json_client::<_, Msgs::Item>(tcp_stream, msgs).await;
}

/// Mock a json client with argument `TcpStream` using customized deserialization
///
/// Notes:
/// 1. Call the function like `mock_json_client::<_, String>(&mut tcp_stream, msgs)`.
/// 2. `MsgIn` type must be an owned type.
/// 3. If `Msgs` are owned types, can simply call `mock_json_client_symm(&mut tcp_stream, msgs)`.
pub async fn mock_json_client<Msgs, MsgIn>(tcp_stream: &mut TcpStream, msgs: Msgs)
where
    Msgs: IntoIterator,
    Msgs::Item: Serialize + Unpin + Send + Sync + UnwindSafe + RefUnwindSafe,
    for<'a> MsgIn: Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
{
    let local_addr = tcp_stream.local_addr().unwrap();
    let (tcp_read, tcp_write) = tcp_stream.split();

    // Delimit frames from bytes using a length header
    let length_delimited_read = FramedRead::new(tcp_read, LengthDelimitedCodec::new());
    let length_delimited_write = FramedWrite::new(tcp_write, LengthDelimitedCodec::new());

    // Deserialize/Serialize frames using JSON codec
    let serded_read: SymmetricallyFramed<_, MsgIn, _> =
        SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<MsgIn>::default());
    let serded_write: SymmetricallyFramed<_, Msgs::Item, _> =
        SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<Msgs::Item>::default());

    stream::iter(msgs)
        .fold(
            (serded_read, serded_write),
            |(mut serded_read, mut serded_write), send_msg| async move {
                serded_write
                    .send(send_msg)
                    .and_then(|_| serded_read.try_next())
                    .and_then(|received_msg| {
                        println!("[{:?}] GOT RESPONSE: {:?}", local_addr, received_msg);
                        future::ok(())
                    })
                    .await
                    .unwrap();

                (serded_read, serded_write)
            },
        )
        .await;
}

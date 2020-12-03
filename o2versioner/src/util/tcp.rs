use async_trait::async_trait;
use bb8;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::iter;
use std::net::SocketAddr;
use std::panic::{RefUnwindSafe, UnwindSafe};
use tokio::net::{lookup_host, TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::oneshot;
use tokio_serde::formats::SymmetricalJson;
use tokio_serde::SymmetricallyFramed;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use tracing::{debug, info, warn, Instrument};

/// Helper function to bind to a `TcpListener` and forward all incomming `TcpStream` to `connection_handler`.
///
/// Note:
/// 1. `addr` is the tcp port to bind to
/// 2. `connection_handler` is a `FnMut` closure takes in `TcpStream` and returns `Future<Output=()>`
/// 3. `max_connection` can be specified to limit the max number of connections allowed. Server will shutdown immediately once `max_connection` connections are all dropped.
/// 4. `server_name` is a name to be used for output
pub async fn start_tcplistener<A, C, Fut, S>(
    addr: A,
    mut connection_handler: C,
    max_connection: Option<u32>,
    server_name: S,
    stop_rx: Option<oneshot::Receiver<()>>,
) where
    A: ToSocketAddrs,
    C: FnMut(TcpStream) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
    S: Into<String>,
{
    let listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    let server_name = server_name.into();
    info!("[{}] {} successfully binded ", local_addr, server_name);

    let mut cur_num_connection = 0;
    let mut spawned_tasks = Vec::new();

    let mut should_stopped = if let Some(stop_rx) = stop_rx {
        stop_rx.boxed()
    } else {
        // If terminate_sig is None, this future will never resolve,
        // so that select will always shortcut to listener.accept()
        future::pending().boxed()
    };

    loop {
        tokio::select! {
            tcp_stream = listener.accept() => {
                match tcp_stream {
                    Ok((tcp_stream, peer_addr)) => {
                        info!(
                            "[{}] <- [{}] Incomming connection [{}] established",
                            local_addr,
                            peer_addr,
                            cur_num_connection
                        );

                        // Spawn a new thread for each tcp connection
                        spawned_tasks.push(tokio::spawn(connection_handler(tcp_stream).in_current_span()));
                    }
                    Err(e) => {
                        warn!(
                            "[{}] {} TcpListener cannot get client: {:?}",
                            local_addr, server_name, e
                        );
                    }
                }

                cur_num_connection += 1;
                // An optional max number of connections allowed
                if let Some(nmax) = max_connection {
                    if cur_num_connection >= nmax {
                        break;
                    }
                }
            },
            _ = &mut should_stopped => {
                warn!( "[{}] {} TcpListener no longer accepts any new connections", local_addr, server_name);
                break;
            }
        };
    }

    // Wait on all spawned tasks to finish
    futures::future::join_all(spawned_tasks).await;
    info!(
        "[{}] {} TcpListener says service terminated, have a good night",
        local_addr, server_name
    );
}

pub async fn send_and_receive_single_as_json<Msg, S>(
    tcp_stream: &mut TcpStream,
    msg: Msg,
    client_name: S,
) -> std::io::Result<Msg>
where
    for<'a> Msg: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
    S: Into<String>,
{
    send_and_receive_as_json(tcp_stream, iter::once(msg), client_name)
        .await
        .into_iter()
        .next()
        .expect("Expecting one reply message")
}

/// Send a collection of msg through the argument `TcpStream`, and expecting a reply for each of the msg sent.
/// The msgs are send received using a json encoding.
///
/// # Notes:
/// 1. For each msg in `msgs`, send it using the argument `TcpStream` and expecting a reply. Pack the reply into a `Vec`.
/// 2. `msgs: Msgs` must be an owned collection that contains owned data types.
/// 3. `<Msgs as IntoInterator>::Item` must be an owned type, and will be used as the type to hold the replies from the server.
pub async fn send_and_receive_as_json<Msgs, S>(
    tcp_stream: &mut TcpStream,
    msgs: Msgs,
    client_name: S,
) -> Vec<std::io::Result<Msgs::Item>>
where
    Msgs: IntoIterator,
    for<'a> Msgs::Item: Serialize + Deserialize<'a> + Unpin + Send + Sync + Debug + UnwindSafe + RefUnwindSafe,
    S: Into<String>,
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
                debug!("[{}] -> SEND REQUEST: {:?}", local_addr, send_msg);
                responses.push(
                    serded_write
                        .send(send_msg)
                        .and_then(|_| serded_read.try_next())
                        .map_ok(|received_msg| {
                            let received_msg = received_msg.unwrap();
                            debug!("[{}] <- GOT RESPONSE: {:?}", local_addr, received_msg);
                            received_msg
                        })
                        .await,
                );

                (serded_read, serded_write, responses)
            },
        )
        .await;

    let client_name = client_name.into();
    debug!("[{}] {} TcpStream says current task finished", local_addr, client_name);

    responses
}

#[derive(Debug)]
pub struct TcpStreamConnectionManager {
    addrs: Vec<SocketAddr>,
}

impl TcpStreamConnectionManager {
    pub async fn new<A>(addrs: A) -> Self
    where
        A: ToSocketAddrs,
    {
        Self {
            addrs: lookup_host(addrs).await.expect("Unexpected socket addresses").collect(),
        }
    }
}

impl Drop for TcpStreamConnectionManager {
    fn drop(&mut self) {
        info!(
            "TcpStreamConnectionManager with connections to {:?} terminated",
            self.addrs
        );
    }
}

#[async_trait]
impl bb8::ManageConnection for TcpStreamConnectionManager {
    type Connection = TcpStream;
    type Error = std::io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = TcpStream::connect(&self.addrs[..]).await?;
        Ok(stream)
    }

    async fn is_valid(&self, conn: &mut bb8::PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        conn.writable().await
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

/// Unit test for `TcpStreamConnectionManager`
#[cfg(test)]
mod tests_tcppool {
    use super::super::tests_helper;
    use super::TcpStreamConnectionManager;
    use bb8::Pool;
    use futures::future;
    use std::time::Duration;
    use tracing::debug;

    /// cargo test -- --show-output
    #[tokio::test]
    async fn test_pool() {
        let _guard = tests_helper::init_logger();
        let port = "127.0.0.1:24523";

        let pool_size = 2;

        // pool has a lifetime shorter within client_handles,
        // as soon as the two client handles within client_handles
        // are finished, all connections of pool will be dropped.
        // As the result, the server_handle will join after client_handles
        // are joined.

        let server_handle = tokio::spawn(async move {
            tests_helper::mock_echo_server(port, Some(pool_size), "tests_tcppool_server").await;
            debug!("server_handle finished");
        });

        let client_handles = tokio::spawn(async move {
            // As soon as the two client handles finish their tasks,
            // the end of scope of the current task will be reached,
            // and pool will be dropped automatically without any
            // max_lifetime being set.
            let pool = Pool::builder()
                .max_size(pool_size)
                .build(TcpStreamConnectionManager::new(port).await)
                .await
                .unwrap();

            let pool_cloned = pool.clone();
            let client0_handle = tokio::spawn(async move {
                let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
                tests_helper::mock_json_client(
                    &mut tcp_stream,
                    vec![String::from("hello0"), String::from("hello00")],
                    "Pooled client 0",
                )
                .await;
                debug!("client0_handle finished");
            });

            let pool_cloned = pool.clone();
            let client1_handle = tokio::spawn(async move {
                let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
                tests_helper::mock_json_client(
                    &mut tcp_stream,
                    vec!["hello1".to_owned(), "hello11".to_owned()],
                    "Pooled client 1",
                )
                .await;
                debug!("client1_handle finished");
            });

            tokio::try_join!(client0_handle, client1_handle).unwrap();
            debug!("pool dropped automatically")
        });

        tokio::try_join!(server_handle, client_handles).unwrap();
        debug!("test_pool_lifetime finished!")
    }

    /// cargo test -- --show-output
    #[tokio::test]
    async fn test_pool_lifetime() {
        let _guard = tests_helper::init_logger();
        let port = "127.0.0.1:14523";

        let pool_size = 2;
        let pool = Pool::builder()
            .max_size(pool_size)
            .max_lifetime(Some(Duration::from_millis(300)))
            .reaper_rate(Duration::from_millis(100))
            .build(TcpStreamConnectionManager::new(port).await)
            .await
            .unwrap();

        // Since pool has a lifetime longer than server_handle,
        // pool will always hold the connection, and since the connections
        // are still alive, server_handle won't join.
        // In this scenerio, pool must actively timeout,
        // so that all connections are dropped,
        // and then server_handle can properly terminate.
        let server_handle = tokio::spawn(async move {
            tests_helper::mock_echo_server(port, Some(pool_size), "tests_tcppool_server").await;
            debug!("server_handle finished");
        });

        let pool_cloned = pool.clone();
        let client0_handle = tokio::spawn(async move {
            let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
            tests_helper::mock_json_client(
                &mut tcp_stream,
                vec![String::from("hello0"), String::from("hello00")],
                "Pooled client 0",
            )
            .await;
            debug!("client0_handle finished");
        });

        let pool_cloned = pool.clone();
        let client1_handle = tokio::spawn(async move {
            let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
            tests_helper::mock_json_client(
                &mut tcp_stream,
                vec!["hello1".to_owned(), "hello11".to_owned()],
                "Pooled client 1",
            )
            .await;
            debug!("client1_handle finished");
        });

        // pool connections must have already been dropped
        // due to its max_lifetime setting. Create a future
        // that awaits the worker handles and returns Err when a timeout limit is reached.
        assert!(tokio::time::timeout(
            Duration::from_millis(600),
            future::join3(server_handle, client0_handle, client1_handle)
        )
        .await
        .is_ok());
        debug!("test_pool_lifetime finished!")
    }
}

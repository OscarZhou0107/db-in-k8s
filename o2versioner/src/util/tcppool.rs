use async_trait::async_trait;
use bb8;
use futures::future::poll_fn;
use std::fmt;
use std::io;
use tokio::net::TcpStream;
use tokio::net::ToSocketAddrs;

pub struct TcpStreamConnectionManager<A>
where
    A: ToSocketAddrs,
{
    addr: A,
}

impl<A> TcpStreamConnectionManager<A>
where
    A: ToSocketAddrs,
{
    pub fn new(addr: A) -> TcpStreamConnectionManager<A> {
        TcpStreamConnectionManager { addr }
    }
}

#[async_trait]
impl<A> bb8::ManageConnection for TcpStreamConnectionManager<A>
where
    A: ToSocketAddrs + Clone + Send + Sync + 'static,
{
    type Connection = TcpStream;
    type Error = io::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let stream = TcpStream::connect(self.addr.clone()).await?;
        Ok(stream)
    }

    async fn is_valid(&self, conn: &mut bb8::PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        let mut buf = [0; 10];

        poll_fn(|cx| conn.poll_peek(cx, &mut buf)).await.map(|_| ())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

impl<A> fmt::Debug for TcpStreamConnectionManager<A>
where
    A: ToSocketAddrs + fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TcpStreamConnectionManager")
            .field("addr", &self.addr)
            .finish()
    }
}

/// Unit test for `Config`
#[cfg(test)]
mod tests_tcppool {
    use super::TcpStreamConnectionManager;
    use bb8::Pool;
    use futures::prelude::*;
    use std::convert::TryInto;
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::net::ToSocketAddrs;
    use tokio::time::delay_for;
    use tokio_serde::formats::SymmetricalJson;
    use tokio_serde::SymmetricallyFramed;
    use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

    /// cargo test -- --nocapture
    #[tokio::test]
    async fn test_pool() {
        let port = "127.0.0.1:14523";

        let pool_size = 2;
        let pool = Pool::builder()
            .max_size(pool_size)
            .max_lifetime(Some(Duration::from_millis(100)))
            .connection_timeout(Duration::from_millis(100))
            .reaper_rate(Duration::from_millis(100))
            .build(TcpStreamConnectionManager::new(port))
            .await
            .unwrap();

        let server_handle = tokio::spawn(mock_echo_server(port, Some(pool_size.try_into().unwrap())));
        let client0_handle = tokio::spawn(mock_connection(pool.clone(), &["hello0", "hello00"], false));
        let client1_handle = tokio::spawn(mock_connection(pool.clone(), &["hello1", "hello11"], false));

        // Wait for a time period that's greater than Pool's max lifetime
        delay_for(Duration::from_millis(700)).await;

        // Send another Pool::get to trigger Pool's timeout procedure
        let client2_handle = tokio::spawn(mock_connection(pool.clone(), &["hello2", "hello22"], true));
        tokio::try_join!(server_handle, client0_handle, client1_handle, client2_handle).unwrap();
    }

    async fn mock_echo_server<A: ToSocketAddrs>(addr: A, max_connection: Option<usize>) {
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

    async fn mock_connection<A: ToSocketAddrs + Clone + Send + Sync + 'static>(
        connection_pool: Pool<TcpStreamConnectionManager<A>>,
        msgs: &[&str],
        expect_timeout: bool,
    ) {
        // Connect to a socket
        let mut socket = connection_pool.get().await.expect("Can't grab socet from pool");
        let local_addr = socket.local_addr().unwrap();
        let (reader, writer) = socket.split();

        // If expect_timeout, then the socket should have been closed at here

        // Delimit frames from bytes using a length header
        let length_delimited_read = FramedRead::new(reader, LengthDelimitedCodec::new());
        let length_delimited_write = FramedWrite::new(writer, LengthDelimitedCodec::new());

        // Deserialize/Serialize frames using JSON codec
        let mut serded_read: SymmetricallyFramed<_, String, _> =
            SymmetricallyFramed::new(length_delimited_read, SymmetricalJson::<String>::default());
        let mut serded_write: SymmetricallyFramed<_, String, _> =
            SymmetricallyFramed::new(length_delimited_write, SymmetricalJson::<String>::default());

        for msg in msgs {
            serded_write
                .send((*msg).to_owned())
                .await
                .or_else(|e| {
                    if expect_timeout && e.kind() == std::io::ErrorKind::ConnectionReset {
                        Ok(())
                    } else {
                        Err(e)
                    }
                })
                .expect("Unexpected error when sending message");

            if let Some(msg) = serded_read
                .try_next()
                .await
                .or_else(|e| {
                    if expect_timeout && e.kind() == std::io::ErrorKind::ConnectionReset {
                        Ok(None)
                    } else {
                        Err(e)
                    }
                })
                .expect("Unexpected error when receiving message")
            {
                println!("[{:?}] GOT RESPONSE: {:?}", local_addr, msg);
            }
        }
    }
}

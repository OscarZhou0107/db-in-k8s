use async_trait::async_trait;
use bb8;
use futures::future::poll_fn;
use log::info;
use std::future::Future;
use std::string::ToString;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

/// Helper function to bind to a `TcpListener` and forward all incomming `TcpStream` to `connection_handler`.
///
/// Note:
/// 1. `addr` is the tcp port to bind to
/// 2. `connection_handler` is a `FnMut` closure takes in `TcpStream` and returns `Future<Output=()>`
/// 3. `max_connection` can be specified to limit the max number of connections allowed. Server will shutdown immediately once `max_connection` connections are all dropped.
/// 4. `server_name` is an optional name to be used for output
pub async fn start_tcplistener<A, C, Fut, S>(
    addr: A,
    mut connection_handler: C,
    max_connection: Option<usize>,
    server_name: Option<S>,
) where
    A: ToSocketAddrs + std::fmt::Debug + Clone,
    C: FnMut(TcpStream) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
    S: ToString,
{
    let mut listener = TcpListener::bind(addr.clone()).await.unwrap();

    let server_name = server_name.map_or(String::from("Server"), |s| s.to_string());
    info!("{} successfully binded to {:?}", server_name, addr);

    let mut cur_num_connection = 0;
    let mut spawned_tasks = Vec::new();
    loop {
        let (tcp_stream, peer_addr) = listener.accept().await.unwrap();

        info!("Connection established with [{}] {}", cur_num_connection, peer_addr);
        cur_num_connection += 1;

        // Spawn a new thread for each tcp connection
        spawned_tasks.push(tokio::spawn(connection_handler(tcp_stream)));

        // An optional max number of connections allowed
        if let Some(nmax) = max_connection {
            if cur_num_connection >= nmax {
                break;
            }
        }
    }

    // Wait on all spawned tasks to finish
    futures::future::join_all(spawned_tasks).await;
    info!("{} at {:?} says goodbye world", server_name, addr);
}

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
    type Error = std::io::Error;

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

impl<A> std::fmt::Debug for TcpStreamConnectionManager<A>
where
    A: ToSocketAddrs + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("TcpStreamConnectionManager")
            .field("addr", &self.addr)
            .finish()
    }
}

/// Unit test for `Config`
#[cfg(test)]
mod tests_tcppool {
    use super::super::tests_helper;
    use super::TcpStreamConnectionManager;
    use bb8::Pool;
    use std::convert::TryInto;
    use std::time::Duration;

    /// cargo test -- --nocapture
    #[tokio::test]
    async fn test_pool() {
        let port = "127.0.0.1:14523";

        let pool_size = 2;
        let pool = Pool::builder()
            .max_size(pool_size)
            .max_lifetime(Some(Duration::from_millis(300)))
            .connection_timeout(Duration::from_millis(300))
            .reaper_rate(Duration::from_millis(300))
            .build(TcpStreamConnectionManager::new(port.to_owned()))
            .await
            .unwrap();

        let server_handle = tokio::spawn(tests_helper::mock_echo_server(
            port.clone(),
            Some(pool_size.try_into().unwrap()),
            Some("tests_tcppool_server"),
        ));

        let pool_cloned = pool.clone();
        let client0_handle = tokio::spawn(async move {
            let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
            tests_helper::mock_json_client(&mut tcp_stream, vec![String::from("hello0"), String::from("hello00")])
                .await;
        });

        let pool_cloned = pool.clone();
        let client1_handle = tokio::spawn(async move {
            let mut tcp_stream = pool_cloned.get().await.expect("Can't grab socket from pool");
            tests_helper::mock_json_client(&mut tcp_stream, ["hello1".to_owned(), "hello11".to_owned()].to_vec()).await;
        });
        tokio::try_join!(server_handle, client0_handle, client1_handle).unwrap();
    }
}

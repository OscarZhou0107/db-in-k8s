use futures::prelude::*;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::{TcpListener, ToSocketAddrs};
use tracing::{field, info, info_span, instrument, warn, Instrument, Span};
use unicase::UniCase;

/// Helper function to bind to a `TcpListener` as an admin port and forward all incomming `TcpStream` to `connection_handler`.
///
/// # Notes:
/// 1. `addr` is the tcp port to bind to
/// 2. `admin_command_handler` is a `FnMut` closure takes in `String` and returns `Future<Output = (String, bool)>`,
/// with `String` represents the reply response, and `bool` denotes whether to continue the `TcpListener`.
/// 3. The returned `String` should not have any newline characters
/// 4. `server_name` is name to be used for output
#[instrument(name="listen", skip(addr, admin_command_handler, server_name), fields(message=field::Empty))]
pub async fn start_admin_tcplistener<A, C, Fut, S>(addr: A, mut admin_command_handler: C, server_name: S)
where
    A: ToSocketAddrs,
    C: FnMut(String) -> Fut,
    Fut: Future<Output = (String, bool)> + Send + 'static,
    S: Into<String>,
{
    let mut listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    Span::current().record("message", &&local_addr.to_string()[..]);
    let server_name = format!("{} Admin", server_name.into());
    info!("[{}] {} successfully binded ", local_addr, server_name);

    'outer: while let Some(tcp_stream) = listener.next().await {
        match tcp_stream {
            Ok(mut tcp_stream) => {
                let peer_addr = tcp_stream.peer_addr().unwrap();
                info!(
                    "[{}] <- [{}] Admin incomming connection established",
                    local_addr, peer_addr
                );

                let (tcp_read, mut tcp_write) = tcp_stream.split();
                let mut line_reader = BufReader::new(tcp_read).lines();
                while let Ok(line) = line_reader.try_next().await {
                    if let Some(line) = line {
                        let line = line.trim().to_owned();
                        let (mut res, should_continue) = admin_command_handler(line.clone())
                            .instrument(info_span!("request", message = &&line[..]))
                            .await;
                        assert!(
                            !res.contains("\n"),
                            "admin_command_handler reply message should not contain any newline characters"
                        );
                        res += "\n";
                        tcp_write
                            .write_all(res.as_bytes())
                            .map_ok_or_else(
                                |e| {
                                    warn!(
                                        "-> [{}] Admin unable to reply message \"{}\": {:?}",
                                        peer_addr,
                                        res.trim(),
                                        e
                                    )
                                },
                                |_| info!("-> [{}] Admin successfully replied \"{}\"", peer_addr, res.trim()),
                            )
                            .await;
                        if !should_continue {
                            break 'outer;
                        }
                    }
                }
            }
            Err(e) => {
                warn!(
                    "[{}] {} TcpListener cannot get client: {:?}",
                    local_addr, server_name, e
                );
            }
        }
    }

    warn!(
        "[{}] {} TcpListener says service terminated, have a good night",
        local_addr, server_name
    );
}

/// A very basic admin command handler
pub async fn basic_admin_command_handler(command: String) -> (String, bool) {
    let command = UniCase::new(command);
    if command == UniCase::new(String::from("kill"))
        || command == UniCase::new(String::from("exit"))
        || command == UniCase::new(String::from("quit"))
    {
        ("Terminating server".into(), false)
    } else {
        (format!("Unknown command: {}", command), true)
    }
}

/// Unit test for `start_admin_tcplistener`
#[cfg(test)]
mod tests_start_admin_tcplistener {
    use super::*;
    use crate::util::tests_helper::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_admin_tcplistener() {
        let _guard = init_logger();

        let admin_addr = "127.0.0.1:27643";

        let admin_handle = tokio::spawn(start_admin_tcplistener(admin_addr, basic_admin_command_handler, "test"));
        let client_handle = tokio::spawn(async move {
            let mut tcp_stream = TcpStream::connect(admin_addr).await.unwrap();
            let res = mock_ascii_client(&mut tcp_stream, vec!["help", "exit"], "admin tcplistener tester").await;
            info!("All responses received: {:?}", res);
        });

        tokio::try_join!(admin_handle, client_handle).unwrap();
    }
}
